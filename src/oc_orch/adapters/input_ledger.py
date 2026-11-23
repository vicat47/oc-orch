"""S2：session_inputs 输入回执与物理发送身份（InputLedger mixin）。

与 [improve-wake-delivery-reliability design D2/D3](../../../../openspec/changes/improve-wake-delivery-reliability/design.md)
对齐，只提供台账窄端口，不实现运行时编排：

- 一个物理输入 = 一个 (session, message) 投递身份，可关联多条业务通知；
- 同目标最多一个持久未决自动 wake：``outstanding=1`` 的 (session_id) 部分唯一
  索引是 DB 级约束，POST 未返回期间第二发送者无法抢出另一条 wake；
- 候选 message_id 在“任何可能的 POST 之前”持久化（reserve 时生成），未知/
  崩溃/重启保持原 ID；确定未受理才允许以原 ID 有界重试；
- **发送权仲裁（R1）**：reserve/finalize/mark_possible 以 claim_owner 归属 +
  revision CAS（expected_revision）仲裁；finalize 后形成**不可变 finalized
  快照**（正文与批次成员冻结），并发/同 owner 都不能事后暗改；
- **终态守护（R2/R3）**：unknown/defer/retry/confirm/mark/finalize 所有写都
  拒绝改写已 cancelled/released/expired 或已被 nudge 后继替换（replaced_by）
  的输入，不得重开原责任；
- **发送前重校验（R6）**：finalize/mark_possible 事务内复核冻结成员的
  current/人工控制/ACK；成员已终态或任务关闭/暂停 → 拒发（不暗改已 possible
  正文）；
- **nudge 预算（R4/R7）**：nudge 只允许确证 delivered（delivery 证据门，不靠
  HTTP 受理），重复 nudge 幂等返回既有后继，跨代有绝对上限（持久 nudge_count
  + max_nudges，不能靠新物理 ID 无限重置）；
- admission / delivery / execution / business ack 分离，取消不隐式 ACK 业务；
- 批次成员原子补证且幂等：重复受理/投递证据不重计算数、不覆盖 ACK/dead；
- ``text_factory`` 必须是纯函数（仅由传入通知行派生正文），事务内调用，禁止 IO。

安全退役（活性边界）：``release_input_slot`` 除 prepared 释放外，还允许
“确证 delivered/cancelled 且全部批次成员已终结（acked/dead）”的 retire；
绝不以“仅 ACK 或空 inbox”退役 unknown 输入。
"""

from __future__ import annotations

from typing import Callable, Iterable

from ..domain.errors import LedgerError
from ..domain.notification_policy import SEND as POLICY_SEND, notification_decision
from ..util import json_dumps, json_loads, sha256_text, short_id
from .sqlite_db import transaction

# ---- admission_state -------------------------------------------------------
ADMISSION_PREPARED = "prepared"      # 已预留槽位，未发送（finalize 前）
ADMISSION_POSSIBLE = "possible"      # POST 可能已发出（发送前标记）
ADMISSION_ADMITTED = "admitted"      # 服务端受理证据（inbox.enqueued）
ADMISSION_DELIVERED = "delivered"    # 输入进入目标历史（≠ 业务已读/ACK）
ADMISSION_UNKNOWN = "unknown"        # 响应丢失，缺受理/取消证据
ADMISSION_CANCELLED = "cancelled"    # 输入被服务端取消
ADMISSION_EXPIRED = "expired"        # 对账期限耗尽
ADMISSION_RELEASED = "released"      # 确定未发送/已终结后退役，槽位释放

# 终态：任何写都不得重开（R2/R3）。
_TERMINAL_ADMISSION = (
    ADMISSION_CANCELLED, ADMISSION_RELEASED, ADMISSION_EXPIRED,
)

INTENT_EXECUTE = "execute"
INTENT_PARK = "park"
INTENT_RECORD = "record"
INTENT_NUDGE = "nudge"
INTENT_UNKNOWN = "unknown"

SOURCE_AUTOMATIC_WAKE = "automatic_wake"

# nudge 跨代绝对预算上限（R7）。
_DEFAULT_MAX_NUDGES = 3


def _input_from_row(row) -> dict | None:
    if row is None:
        return None
    d = dict(row)
    d["notification_ids"] = json_loads(d.get("notification_ids"), [])
    d["outstanding"] = bool(d["outstanding"])
    d["metadata"] = json_loads(d.get("metadata"), {})
    return d


class InputLedger:
    """session_inputs 回执表方法；混入 sqlite_ledger.Ledger（共享 conn/时钟/审计）。"""

    def check_automatic_wake(self, session_id: str, notifications: list[dict]) -> None:
        from .continuity_guard import check_automatic_wake

        check_automatic_wake(self.conn, self.clock, session_id, notifications)

    # ---- 只读 ---------------------------------------------------------------
    def get_input(self, input_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
        ).fetchone()
        return _input_from_row(row)

    def input_for_receipt(self, session_id: str, message_id: str) -> dict | None:
        """按 (session_id, message_id) 精确匹配物理输入；会话错配返回 None。"""
        row = self.conn.execute(
            "SELECT * FROM session_inputs WHERE session_id=? AND message_id=? "
            "ORDER BY created_at DESC LIMIT 1",
            (session_id, message_id),
        ).fetchone()
        return _input_from_row(row)

    def notification_inputs(self, notification_id: str, *, limit: int = 20) -> list[dict]:
        """通过冻结批次成员反查输入（历史回执也可见）。"""
        rows = self.conn.execute(
            "SELECT * FROM session_inputs WHERE EXISTS (SELECT 1 FROM json_each("
            "session_inputs.notification_ids) WHERE json_each.value=?) "
            "ORDER BY created_at DESC LIMIT ?",
            (notification_id, limit),
        ).fetchall()
        return [_input_from_row(r) for r in rows]

    def list_pending_inputs(self, *, target_session: str | None = None,
                            admission_state: str | None = None,
                            outstanding: bool | None = None,
                            limit: int = 100) -> list[dict]:
        sql = "SELECT * FROM session_inputs"
        clauses: list[str] = []
        args: list = []
        if target_session:
            clauses.append("session_id=?")
            args.append(target_session)
        if admission_state:
            clauses.append("admission_state=?")
            args.append(admission_state)
        if outstanding is not None:
            clauses.append("outstanding=?")
            args.append(1 if outstanding else 0)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at ASC, input_id ASC LIMIT ?"
        args.append(limit)
        return [_input_from_row(r) for r in self.conn.execute(sql, args).fetchall()]

    # ---- 内部守卫（R1/R2/R3/R6） --------------------------------------------
    @staticmethod
    def _require_owner(owner, what: str) -> str:
        """新输入/后继必须有明确的非空字符串 owner；坏类型/空白/None 稳定拒绝，
        且不得创建或修改任何输入（INVALID_OWNER）。"""
        if not isinstance(owner, str) or not owner.strip():
            raise LedgerError(
                "INVALID_OWNER",
                f"{what} 必须提供非空字符串 owner（收到 {owner!r}）")
        return owner

    def _guard_not_terminal(self, d: dict) -> None:
        if d.get("replaced_by"):
            raise LedgerError(
                "INPUT_SUPERSEDED",
                f"输入 {d['input_id']} 已被后继 {d['replaced_by']} 替换，不得重开原责任")
        if d["admission_state"] in _TERMINAL_ADMISSION:
            raise LedgerError(
                "INPUT_TERMINAL",
                f"输入 {d['input_id']} 已处于 {d['admission_state']}，不能改写")

    def _guard_owner_cas(self, d: dict, *, owner: str,
                         expected_revision: int) -> None:
        """发送权仲裁：claim_owner 归属 + revision CAS（同 owner 并发也防）。

        - 持久 claim_owner 缺失（NULL/空，历史/坏形状行）**不得**因此授予任意
          caller 发送权——发送路径必须能证明归属；历史 unknown 读/确认仍可从
          只读/受理路径继续（不调用本守卫）；
        - claim_owner 有值且与 owner 不一致 → INPUT_OWNER_MISMATCH；
        - revision CAS：旧快照不发放送权。
        """
        persisted = d.get("claim_owner")
        if not (isinstance(persisted, str) and persisted.strip()):
            raise LedgerError(
                "INPUT_OWNER_MISMATCH",
                f"输入 {d['input_id']} 无持久 claim_owner，任何 caller 都不能获得发送权",
                {"claim_owner": persisted, "owner": owner})
        if owner != persisted:
            raise LedgerError(
                "INPUT_OWNER_MISMATCH",
                f"输入 {d['input_id']} 由 {persisted} 持有，{owner} 无发送权")
        if int(d.get("revision") or 0) != expected_revision:
            raise LedgerError(
                "INPUT_REVISION_CONFLICT",
                f"输入 {d['input_id']} revision={d.get('revision')} != 期望 "
                f"{expected_revision}；并发修改或过期快照",
                {"revision": d.get("revision"), "expected": expected_revision})

    def _members_in_tx(self, conn, ids: list[str]) -> list:
        if not ids:
            return []
        ph = ",".join("?" for _ in ids)
        return [dict(r) for r in conn.execute(
            f"SELECT * FROM notifications WHERE id IN ({ph})",
            (*ids,)).fetchall()]

    def _members_sendable_in_tx(self, conn, ids: list[str]) -> tuple[bool, str]:
        """发送前重校验：ACK/人工控制及完整代次/条件规则与due/claim一致。"""
        if not ids:
            return False, "empty-members"
        rows = self._members_in_tx(conn, ids)
        by_id = {r["id"]: r for r in rows}
        if len(by_id) != len(set(ids)):
            return False, "member-missing"
        attempts: dict[str, dict] = {}
        attempt_ids = sorted({r["attempt_id"] for r in rows if r["attempt_id"]})
        if attempt_ids:
            aph = ",".join("?" for _ in attempt_ids)
            for a in conn.execute(f"SELECT * FROM attempts WHERE id IN ({aph})", (*attempt_ids,)):
                item = dict(a)
                item["metadata"] = json_loads(item.get("metadata"), None)
                attempts[item["id"]] = item
        tasks: dict[str, dict] = {}
        task_ids = sorted({r["task_id"] for r in rows if r["task_id"]}
                          | {a["task_id"] for a in attempts.values() if a.get("task_id")})
        if task_ids:
            tph = ",".join("?" for _ in task_ids)
            for t in conn.execute(
                    f"SELECT * FROM tasks WHERE id IN ({tph})",
                    (*task_ids,)):
                tasks[t["id"]] = dict(t)
        for i in ids:
            r = by_id[i]
            if r["state"] in ("acked", "dead"):
                return False, f"member-terminal:{i}"
            if r["task_id"]:
                st = tasks.get(r["task_id"])
                if (st is None or st["status"] in ("cancelled", "accepted", "paused")
                        or st["closed_at"] is not None):
                    return False, f"task-protected:{r['task_id']}"
            attempt = attempts.get(r["attempt_id"])
            task_id = r["task_id"] or (attempt or {}).get("task_id")
            notice = dict(r)
            notice["payload"] = json_loads(notice.get("payload"), None)
            decision = notification_decision(notice, task=tasks.get(task_id), attempt=attempt)
            if decision["action"] != POLICY_SEND:
                return False, f"policy:{decision['reason']}:{i}"
        return True, ""

    def _members_terminal_in_tx(self, conn, ids: list[str]) -> bool:
        """安全退役条件（活性边界）：全部批次成员已终结（acked/dead）。"""
        if not ids:
            return False
        rows = self._members_in_tx(conn, ids)
        if len(rows) != len(set(ids)):
            return False
        return all(r["state"] in ("acked", "dead") for r in rows)

    # ---- 槽位预留与批次最终化（R1：owner + revision CAS） ----------------------
    def reserve_target_input(
        self, target_session: str, *,
        notification_ids: Iterable[str] = (),
        body: str = "", body_hash: str = "",
        intent: str = INTENT_EXECUTE,
        owner: str,
        source: str = SOURCE_AUTOMATIC_WAKE, now: int | None = None,
        metadata: dict | None = None,
    ) -> dict:
        """事务内预留目标未决槽位（发送权 = claim_owner=owner）；候选 message_id
        在 POST 前持久化。

        - 目标已有 outstanding=1 输入：
          - prepared 且 claim_owner == owner → 合并待发送通知集合（正文仍在
            finalize 时最终化；不赋予其他 owner 发送权）；
          - prepared 但 claim_owner != owner，或
            possible/unknown/admitted/delivered → 仅返回既有行（held、无发送
            权），禁止以新 ID 制造另一条 wake；网络窗口 target 唯一由 DB 保证。
        - 无既有输入 → 新建 outstanding=1 行并持久化候选 message_id（revision=1）。
        返回 {"input", "created", "merged", "held"}；input 含 revision 供
        finalize/mark_possible 的 expected_revision CAS。
        """
        if not target_session:
            raise LedgerError("INVALID_INPUT", "目标 session 不能为空")
        owner = self._require_owner(owner, "reserve")
        ts = now if now is not None else self._now_ms()
        ids = sorted({str(i) for i in notification_ids})
        with transaction(self.conn):
            cur = self.conn.execute(
                "SELECT * FROM session_inputs WHERE session_id=? AND outstanding=1 "
                "LIMIT 1", (target_session,),
            ).fetchone()
            if cur is not None:
                cur_d = _input_from_row(cur)
                if (cur_d["admission_state"] == ADMISSION_PREPARED
                        and cur_d.get("claim_owner") == owner):
                    merged_ids = sorted(set(cur_d["notification_ids"]) | set(ids))
                    self.conn.execute(
                        "UPDATE session_inputs SET notification_ids=?, updated_at=? "
                        "WHERE input_id=?",
                        (json_dumps(merged_ids), ts, cur_d["input_id"]),
                    )
                    self._audit(self.conn, owner, "input.reserve-merged",
                                "session_input", cur_d["input_id"], None,
                                ADMISSION_PREPARED,
                                {"target": target_session, "members": len(merged_ids)})
                    return {"input": _input_from_row(self.conn.execute(
                        "SELECT * FROM session_inputs WHERE input_id=?",
                        (cur_d["input_id"],)).fetchone()),
                        "created": False, "merged": True, "held": True}
                return {"input": cur_d, "created": False, "merged": False,
                        "held": True, "authority": False,
                        "reason": "slot-held-by-other-or-inflight"}
            input_id = short_id("I-", 12)
            message_id = "msg_ocorch" + short_id("", 18)
            bh = body_hash or (sha256_text(body) if body else "")
            self.conn.execute(
                "INSERT INTO session_inputs(input_id, session_id, message_id, source, "
                "intent, outstanding, revision, owner, body, body_hash, "
                "notification_ids, admission_state, claim_owner, prepared_at, "
                "created_at, updated_at, metadata) "
                "VALUES(?,?,?,?,?,1,1,?,?,?,?,?,?,?,?,?,?)",
                (input_id, target_session, message_id, source, intent, owner,
                 body, bh, json_dumps(ids), ADMISSION_PREPARED, owner, ts, ts, ts,
                 json_dumps(metadata or {})),
            )
            self._audit(self.conn, owner, "input.reserve", "session_input",
                        input_id, None, ADMISSION_PREPARED,
                        {"target": target_session, "intent": intent,
                         "members": len(ids), "message_id": message_id,
                         "owner": owner})
            return {"input": _input_from_row(self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone()), "created": True, "merged": False, "held": True}

    def finalize_input(self, input_id: str, *,
                       final_notification_ids: Iterable[str],
                       text_factory: Callable[[list[dict]], str],
                       owner: str, expected_revision: int,
                       now: int | None = None,
                       metadata: dict | None = None) -> dict:
        """事务内用最终有效 claim 集合派生正文并冻结（不可变 finalized 快照）。

        - 仅 prepared 且 claim_owner==owner 且 expected_revision==revision（CAS，
          同 owner 并发也防）允许最终化；发送后/他 owner/过期版本一律拒绝；
        - **R6**：冻结前重校验冻结成员（current/人工控制/ACK/任务关闭），变化
          时拒发（changed=False + reason），不暗改；
        - ``text_factory`` 必须是纯函数：入参是最终有效通知行列表（仅文本派生），
          禁止任何外部 IO。
        返回 {"input", "changed", "text"}。
        """
        if not callable(text_factory):
            raise LedgerError("INVALID_TEXT_FACTORY", "text_factory 必须是可调用纯函数")
        if not owner:
            raise LedgerError("INVALID_INPUT", "finalize 必须指定发送方 owner")
        ts = now if now is not None else self._now_ms()
        final_ids = sorted({str(i) for i in final_notification_ids})
        with transaction(self.conn):
            row = self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone()
            if row is None:
                raise LedgerError("INPUT_NOT_FOUND", f"输入回执不存在：{input_id}")
            d = _input_from_row(row)
            self._guard_not_terminal(d)
            if d["admission_state"] != ADMISSION_PREPARED:
                return {"input": d, "changed": False, "text": d["body"],
                        "reason": "already-final"}
            self._guard_owner_cas(d, owner=owner,
                                  expected_revision=expected_revision)
            if not final_ids:
                raise LedgerError("EMPTY_CLAIM", "最终有效 claim 集合不能为空")
            ok, why = self._members_sendable_in_tx(self.conn, final_ids)
            if not ok:
                raise LedgerError(
                    "MEMBERS_NOT_SENDABLE",
                    "最终 claim 成员已不可发送（ACK/人工控制/任务保护）；"
                    "不得为已取消/关闭输入冻结发送快照",
                    {"reason": why})
            ph = ",".join("?" for _ in final_ids)
            notif_rows = self.conn.execute(
                f"SELECT * FROM notifications WHERE id IN ({ph})", (*final_ids,)
            ).fetchall()
            members = [dict(r) for r in notif_rows]
            if d.get("source") == SOURCE_AUTOMATIC_WAKE:
                self.check_automatic_wake(d["session_id"], members)
            if {m["id"] for m in members} != set(final_ids):
                raise LedgerError(
                    "CLAIM_MEMBER_MISSING", "最终 claim 集合存在未知通知成员",
                    {"missing": sorted(set(final_ids) - {m["id"] for m in members})})
            body = text_factory(members)
            if not isinstance(body, str):
                raise LedgerError("INVALID_BODY", "text_factory 必须返回正文字符串")
            bh = sha256_text(body)
            meta_out = dict(d["metadata"] or {})
            if metadata:
                meta_out.update(metadata)
            self.conn.execute(
                "UPDATE session_inputs SET notification_ids=?, body=?, body_hash=?, "
                "revision=revision+1, metadata=?, updated_at=? WHERE input_id=?",
                (json_dumps(final_ids), body, bh, json_dumps(meta_out), ts, input_id),
            )
            self._audit(self.conn, owner, "input.finalize", "session_input",
                        input_id, None, ADMISSION_PREPARED,
                        {"members": len(final_ids), "revision": int(d["revision"]) + 1,
                         "text_len": len(body)})
            updated = _input_from_row(self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone())
            return {"input": updated, "changed": True, "text": body}

    def mark_input_possible(self, input_id: str, *, owner: str,
                            expected_revision: int,
                            now: int | None = None) -> dict:
        """发送前标记 possible request（R1/R6）：claim_owner + revision CAS。

        （公开单操作 API；行为契约保持不变。）组合适配器（wake-admission）
        在同一事务内调用 :meth:`_mark_input_possible_in_tx`，把探针花费与
        possible 标记做成同一原子提交/回滚：possible/控制失败整体回滚。
        """
        if not owner:
            raise LedgerError("INVALID_INPUT", "mark_possible 必须指定发送方 owner")
        ts = now if now is not None else self._now_ms()
        with transaction(self.conn):
            return self._mark_input_possible_in_tx(
                input_id, owner=owner, expected_revision=expected_revision,
                now=ts)

    def _mark_input_possible_in_tx(self, input_id: str, *, owner: str,
                                   expected_revision: int, now: int) -> dict:
        """已开启事务内的窄能力（要求同一 connection，不自行 BEGIN/COMMIT）。

        校验、冻结成员重校验、控制守卫与返回形状与原 ``mark_input_possible``
        **逐字一致**；调用方必须已 ``BEGIN IMMEDIATE``。失败/拒绝不产生新的
        发送权；组合流程在失败时整体回滚（探针花费与审计一并撤销）。
        """
        if not self.conn.in_transaction:
            raise RuntimeError("_mark_input_possible_in_tx需要已开启的短事务")
        row = self.conn.execute(
            "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
        ).fetchone()
        if row is None:
            raise LedgerError("INPUT_NOT_FOUND", f"输入回执不存在：{input_id}")
        d = _input_from_row(row)
        self._guard_not_terminal(d)
        if d["admission_state"] == ADMISSION_POSSIBLE:
            return {"input": d, "changed": False, "reason": "already-possible"}
        self._guard_owner_cas(d, owner=owner,
                              expected_revision=expected_revision)
        if d["admission_state"] not in (ADMISSION_PREPARED, ADMISSION_UNKNOWN):
            raise LedgerError(
                "INVALID_INPUT_STATE",
                f"只有 prepared/unknown 可标记 possible（当前 {d['admission_state']}）")
        conn = self.conn
        ok, why = self._members_sendable_in_tx(conn, d["notification_ids"])
        if not ok:
            return {"input": d, "changed": False,
                    "reason": f"members-changed:{why}"}
        if d.get("source") == SOURCE_AUTOMATIC_WAKE:
            # 与 possible 标记同一 BEGIN IMMEDIATE：暂停先提交则拒发；
            # 已可能发出后的暂停不伪称能撤回在途请求。
            self.check_automatic_wake(d["session_id"], self._members_in_tx(
                conn, d["notification_ids"]))
        retry_count = d["retry_count"] + (1 if d["admission_state"] == ADMISSION_UNKNOWN else 0)
        revision_delta = 1 if d["admission_state"] == ADMISSION_UNKNOWN else 1
        self.conn.execute(
            "UPDATE session_inputs SET admission_state='possible', possible_at=?, "
            "claim_owner=?, claim_at=?, observed_at=?, send_count=send_count+1, "
            "retry_count=?, revision=revision+?, updated_at=? WHERE input_id=?",
            (now, owner, now, now, retry_count, revision_delta, now, input_id),
        )
        self._audit(self.conn, owner, "input.possible", "session_input",
                    input_id, d["admission_state"], ADMISSION_POSSIBLE,
                    {"send_count": d["send_count"] + 1, "owner": owner,
                     "revision": int(d["revision"]) + revision_delta})
        updated = _input_from_row(self.conn.execute(
            "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
        ).fetchone())
        return {"input": updated, "changed": True}

    def release_input_slot(self, input_id: str, *, reason: str,
                           now: int | None = None,
                           actor: str = "supervisor") -> dict:
        """释放/安全退役未决槽位（保留历史行与 delivery 事实/原 message ID）。

        - prepared（确定未发送）→ 释放（unadmitted-release）；
        - delivered/cancelled 且**全部批次成员已终结（acked/dead）** → 退役
          （delivered-retire），否则目标会被永久占用（活性边界）。仅 ACK 或
          空 inbox 不足以退役 unknown/possible（可能仍在飞行）；
        - possible/unknown/admitted 或其他条件 → released=False（可能已发出）。
        返回 {"input", "released", "reason"}。
        """
        ts = now if now is not None else self._now_ms()
        with transaction(self.conn):
            row = self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone()
            if row is None:
                raise LedgerError("INPUT_NOT_FOUND", f"输入回执不存在：{input_id}")
            d = _input_from_row(row)
            if d.get("replaced_by"):
                return {"input": d, "released": False,
                        "reason": "superseded-by-nudge"}
            if d["admission_state"] in (ADMISSION_RELEASED, ADMISSION_EXPIRED):
                return {"input": d, "released": False,
                        "reason": f"state={d['admission_state']}"}
            retire = False
            retire_reason = "unadmitted-release"
            if d["admission_state"] == ADMISSION_PREPARED:
                retire = True
                retire_reason = "unadmitted-release"
            elif (d["admission_state"] in (ADMISSION_DELIVERED, ADMISSION_CANCELLED)
                    and self._members_terminal_in_tx(self.conn,
                                                     d["notification_ids"])):
                retire = True
                retire_reason = f"delivered-retired:{reason}"
            if not retire:
                return {"input": d, "released": False,
                        "reason": f"not-retirable:{d['admission_state']}"}
            meta = dict(d["metadata"] or {})
            meta["release_reason"] = retire_reason
            self.conn.execute(
                "UPDATE session_inputs SET admission_state='released', outstanding=0, "
                "observed_at=?, metadata=?, updated_at=? WHERE input_id=?",
                (ts, json_dumps(meta), ts, input_id),
            )
            self._audit(self.conn, actor, "input.release", "session_input",
                        input_id, d["admission_state"], ADMISSION_RELEASED,
                        {"reason": retire_reason})
            return {"input": _input_from_row(self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone()), "released": True, "reason": retire_reason}

    # ---- 受理/投递/取消/未知（R2/R3：终态守护） -------------------------------
    def confirm_input_admitted(self, input_id: str, *, evidence: dict | None = None,
                               now: int | None = None,
                               actor: str = "supervisor") -> dict:
        """受理证据：更新输入为 admitted 并把同一物理输入的全部批次成员原子补证。

        成员只从 pending/inflight → sent、attempts+1（一次），不覆盖 ACK/dead。
        cancelled/released/replaced 输入一律拒绝（不得重开责任）。重复证据幂等。
        返回 {"input", "confirmed", "members"}。
        """
        ts = now if now is not None else self._now_ms()
        with transaction(self.conn):
            row = self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone()
            if row is None:
                raise LedgerError("INPUT_NOT_FOUND", f"输入回执不存在：{input_id}")
            d = _input_from_row(row)
            self._guard_not_terminal(d)
            message_id = d["message_id"]
            ids = d["notification_ids"]
            if d["admission_state"] not in (ADMISSION_PREPARED, ADMISSION_POSSIBLE,
                                            ADMISSION_UNKNOWN):
                return {"input": d, "confirmed": 0, "members": ids,
                        "duplicate": True}
            self.conn.execute(
                "UPDATE session_inputs SET admission_state='admitted', admitted_at=?, "
                "observed_at=?, updated_at=? WHERE input_id=?",
                (ts, ts, ts, input_id),
            )
            confirmed = 0
            if ids:
                ph = ",".join("?" for _ in ids)
                cur = self.conn.execute(
                    "UPDATE notifications SET state='sent', attempts=attempts+1, "
                    "message_id=?, sent_at=?, claim_owner=NULL, claim_at=NULL, "
                    "last_error=NULL WHERE id IN (" + ph + ") AND state IN "
                    "('pending','inflight')",
                    (message_id, ts, *ids),
                )
                confirmed = cur.rowcount
            self._audit(self.conn, actor, "input.admitted-batch", "session_input",
                        input_id, d["admission_state"], ADMISSION_ADMITTED,
                        {"message_id": message_id, "members": len(ids),
                         "confirmed": confirmed, "evidence": bool(evidence)})
            updated = _input_from_row(self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone())
            return {"input": updated, "confirmed": confirmed, "members": ids,
                    "duplicate": False}

    def confirm_input_delivered(self, input_id: str, *, evidence: dict | None = None,
                                now: int | None = None,
                                actor: str = "supervisor") -> dict:
        """投递证据（delivery 证据门）：输入进入目标历史（≠ 已读/业务 ACK）。

        只更新输入行；cancelled/released/replaced 输入拒绝改写（R3）。业务通知
        的已读仍靠显式 ACK。
        """
        ts = now if now is not None else self._now_ms()
        with transaction(self.conn):
            row = self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone()
            if row is None:
                raise LedgerError("INPUT_NOT_FOUND", f"输入回执不存在：{input_id}")
            d = _input_from_row(row)
            self._guard_not_terminal(d)
            if d["admission_state"] == ADMISSION_DELIVERED:
                return {"input": d, "changed": False, "duplicate": True}
            if d["admission_state"] not in (ADMISSION_ADMITTED, ADMISSION_POSSIBLE,
                                            ADMISSION_PREPARED, ADMISSION_UNKNOWN):
                return {"input": d, "changed": False, "duplicate": True}
            self.conn.execute(
                "UPDATE session_inputs SET admission_state='delivered', delivered_at=?, "
                "observed_at=?, updated_at=? WHERE input_id=?",
                (ts, ts, ts, input_id),
            )
            self._audit(self.conn, actor, "input.delivered", "session_input",
                        input_id, d["admission_state"], ADMISSION_DELIVERED,
                        {"evidence": bool(evidence)})
            return {"input": _input_from_row(self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone()), "changed": True, "duplicate": False}

    def record_input_unknown(self, input_id: str, *, reason: str,
                             next_reconcile_at: int | None = None,
                             reconcile_deadline: int | None = None,
                             now: int | None = None,
                             actor: str = "supervisor") -> dict:
        """响应未知/claim 超时/重启：保持原 message_id、不释放目标槽位。

        **R2 终态守护**：已 cancelled/released/expired 或已被后继替换的输入
        拒绝改写（不得把 cancelled 翻 unknown 再复活）；delivered 等不存在
        往返的状态不做翻转。
        返回 {"input", "next_reconcile_at"}。
        """
        ts = now if now is not None else self._now_ms()
        next_at = next_reconcile_at if next_reconcile_at is not None else ts
        with transaction(self.conn):
            row = self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone()
            if row is None:
                raise LedgerError("INPUT_NOT_FOUND", f"输入回执不存在：{input_id}")
            d = _input_from_row(row)
            self._guard_not_terminal(d)
            if d["admission_state"] not in (ADMISSION_PREPARED, ADMISSION_POSSIBLE,
                                            ADMISSION_UNKNOWN):
                return {"input": d, "changed": False, "slot_kept": True,
                        "reason": f"state={d['admission_state']}"}
            self.conn.execute(
                "UPDATE session_inputs SET admission_state='unknown', observed_at=?, "
                "next_reconcile_at=?, reconcile_deadline=COALESCE(?, reconcile_deadline), "
                "updated_at=? WHERE input_id=?",
                (ts, next_at, reconcile_deadline, ts, input_id),
            )
            self._audit(self.conn, actor, "input.unknown", "session_input",
                        input_id, d["admission_state"], ADMISSION_UNKNOWN,
                        {"reason": reason[:300], "same_message_id": d["message_id"],
                         "slot_kept": True})
            updated = _input_from_row(self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone())
            return {"input": updated, "next_reconcile_at": next_at,
                    "slot_kept": True}

    def record_input_cancelled(self, input_id: str | None = None, *,
                               session_id: str | None = None,
                               message_id: str | None = None,
                               reason: str = "cancelled",
                               now: int | None = None,
                               actor: str = "supervisor") -> dict:
        """取消事实：输入 → cancelled 并释放未决槽位。

        **取消不隐式 ACK 业务**：批次成员通知状态保持不变（保持 pending/sent），
        由上层按“整个批次是否均已失效”逐项决定 ack/重发。
        返回 {"matched", "input", "notification_ids", "business_acked": False}。
        """
        ts = now if now is not None else self._now_ms()
        with transaction(self.conn):
            row = None
            if input_id is not None:
                row = self.conn.execute(
                    "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
                ).fetchone()
            elif session_id and message_id:
                row = self.conn.execute(
                    "SELECT * FROM session_inputs WHERE session_id=? AND message_id=? "
                    "ORDER BY created_at DESC LIMIT 1", (session_id, message_id),
                ).fetchone()
            if row is None:
                return {"matched": False, "input": None,
                        "notification_ids": [], "business_acked": False}
            d = _input_from_row(row)
            if d["admission_state"] in (ADMISSION_CANCELLED, ADMISSION_RELEASED,
                                        ADMISSION_EXPIRED):
                return {"matched": True, "input": d, "changed": False,
                        "notification_ids": d["notification_ids"],
                        "business_acked": False}
            self.conn.execute(
                "UPDATE session_inputs SET admission_state='cancelled', outstanding=0, "
                "observed_at=?, updated_at=? WHERE input_id=?",
                (ts, ts, d["input_id"]),
            )
            self._audit(self.conn, actor, "input.cancelled", "session_input",
                        d["input_id"], d["admission_state"], ADMISSION_CANCELLED,
                        {"reason": reason[:200], "slot_released": True,
                         "business_ack": False})
            updated = _input_from_row(self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (d["input_id"],)
            ).fetchone())
            return {"matched": True, "input": updated, "changed": True,
                    "notification_ids": d["notification_ids"],
                    "business_acked": False,
                    "notifications_untouched": True}

    # ---- 有界重试 / nudge / defer -------------------------------------------
    def retry_input(self, input_id: str, *, next_reconcile_at: int,
                    reason: str, now: int | None = None,
                    actor: str = "supervisor") -> dict:
        """确定未受理后以**原 ID** 做有界重试：unknown/prepared → 待发。

        R2：终态/已被后继替换的输入拒绝重试（不得重开原责任）；未释放未知槽位；
        候选 message_id 不变；send/retry 计数在 mark_input_possible 记。
        返回 {"input", "allowed"}。
        """
        ts = now if now is not None else self._now_ms()
        with transaction(self.conn):
            row = self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone()
            if row is None:
                raise LedgerError("INPUT_NOT_FOUND", f"输入回执不存在：{input_id}")
            d = _input_from_row(row)
            self._guard_not_terminal(d)
            if d["admission_state"] not in (ADMISSION_UNKNOWN, ADMISSION_PREPARED):
                return {"input": d, "allowed": False,
                        "reason": f"state={d['admission_state']}"}
            self.conn.execute(
                "UPDATE session_inputs SET next_reconcile_at=?, observed_at=?, "
                "updated_at=? WHERE input_id=?",
                (next_reconcile_at, ts, ts, input_id),
            )
            self._audit(self.conn, actor, "input.retry", "session_input",
                        input_id, d["admission_state"], d["admission_state"],
                        {"same_message_id": d["message_id"], "reason": reason[:200]})
            return {"input": _input_from_row(self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone()), "allowed": True}

    def nudge_input(self, input_id: str, *, text_factory: Callable[[list[dict]], str],
                    owner: str | None = None, now: int | None = None,
                    metadata: dict | None = None,
                    max_nudges: int | None = None) -> dict:
        """delivered 后业务未 ACK 的有限催办：原子替换目标槽位并保留历史。

        - **delivery 证据门**：仅确证 delivered 可 nudge；admitted/unknown/
          possible/prepared 一律拒绝（admitted 可能仍在 inbox；R1/R3）；
        - **R4**：已有后继（replaced_by）时幂等返回后继（created=False），
          绝不新建第二物理输入 / 触发唯一索引冲突；
        - **R7**：跨代绝对预算 nudge_count < max_nudges（默认 3），不能靠新
          物理 ID 无限重置 send/retry 预算；
        - 旧行 outstanding=0、replaced_by=新 input_id（保留为历史回执）；新行
          prior_input_id=旧、intent=nudge、outstanding=1；文本/成员自未 ACK
          成员派生，保留原通知身份。
        返回 {"created", "input", "prior"}。
        """
        if not callable(text_factory):
            raise LedgerError("INVALID_TEXT_FACTORY", "text_factory 必须是可调用纯函数")
        owner = self._require_owner(owner, "nudge")
        ts = now if now is not None else self._now_ms()
        with transaction(self.conn):
            old = self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone()
            if old is None:
                raise LedgerError("INPUT_NOT_FOUND", f"输入回执不存在：{input_id}")
            d = _input_from_row(old)
            if d.get("replaced_by"):
                succ = _input_from_row(self.conn.execute(
                    "SELECT * FROM session_inputs WHERE input_id=?",
                    (d["replaced_by"],)).fetchone())
                return {"created": False, "input": succ, "prior": d,
                        "reason": "successor-exists"}
            self._guard_not_terminal(d)
            if d["admission_state"] != ADMISSION_DELIVERED:
                raise LedgerError(
                    "NUDGE_NOT_ALLOWED",
                    f"仅确证 delivered 的输入可有限 nudge（当前 {d['admission_state']}）；"
                    "admitted/unknown/pending 禁止以 nudge 换 ID")
            cap = max_nudges if isinstance(max_nudges, int) else _DEFAULT_MAX_NUDGES
            if d["nudge_count"] >= cap:
                raise LedgerError(
                    "NUDGE_BUDGET_EXHAUSTED",
                    f"输入 {input_id} nudge 次数 {d['nudge_count']} 已达上限 {cap}；"
                    "不得靠新物理 ID 无限重置发送预算",
                    {"nudge_count": d["nudge_count"], "max_nudges": cap})
            ids = d["notification_ids"]
            if not ids:
                raise LedgerError("NUDGE_NOT_ALLOWED", "输入没有批次成员，无法 nudge")
            ph = ",".join("?" for _ in ids)
            members = [dict(r) for r in self.conn.execute(
                f"SELECT * FROM notifications WHERE id IN ({ph})", (*ids,)
            ).fetchall()]
            unacked = [m for m in members
                       if m["state"] in ("pending", "sent", "inflight")]
            if not unacked:
                return {"created": False, "input": d, "prior": d,
                        "reason": "all-business-acked"}
            self.check_automatic_wake(d["session_id"], unacked)
            body = text_factory(members)
            if not isinstance(body, str):
                raise LedgerError("INVALID_BODY", "text_factory 必须返回正文字符串")
            new_id = short_id("I-", 12)
            new_msg = "msg_ocorch" + short_id("", 18)
            meta_out = dict(metadata or {})
            meta_out["max_nudges"] = cap
            self.conn.execute(
                "UPDATE session_inputs SET outstanding=0, replaced_by=?, updated_at=? "
                "WHERE input_id=?",
                (new_id, ts, input_id),
            )
            self.conn.execute(
                "INSERT INTO session_inputs(input_id, session_id, message_id, source, "
                "intent, outstanding, revision, owner, body, body_hash, "
                "notification_ids, admission_state, claim_owner, prepared_at, "
                "nudge_count, prior_input_id, created_at, updated_at, metadata) "
                "VALUES(?,?,?,?,?,1,1,?,?,?,?,?,?,?,?,?,?,?,?)",
                (new_id, d["session_id"], new_msg, SOURCE_AUTOMATIC_WAKE, INTENT_NUDGE,
                 owner, body, sha256_text(body), json_dumps(ids), ADMISSION_PREPARED,
                 owner, ts, d["nudge_count"] + 1, d["input_id"], ts, ts,
                 json_dumps(meta_out)),
            )
            self._audit(self.conn, owner or "system", "input.nudge", "session_input",
                        new_id, None, ADMISSION_PREPARED,
                        {"prior": d["input_id"], "same_notification_ids": ids,
                         "nudge_count": d["nudge_count"] + 1, "max_nudges": cap,
                         "prior_replaced_by": new_id})
            new = _input_from_row(self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (new_id,)
            ).fetchone())
            return {"created": True, "input": new, "prior": d}

    def defer_input(self, input_id: str, *, next_reconcile_at: int, reason: str,
                    now: int | None = None, actor: str = "supervisor") -> dict:
        """等待/冷却：保持 outstanding=1，只移动下次对账时间，不释放槽位。

        R2：终态/已被后继替换的输入拒绝改写。
        """
        ts = now if now is not None else self._now_ms()
        with transaction(self.conn):
            row = self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone()
            if row is None:
                raise LedgerError("INPUT_NOT_FOUND", f"输入回执不存在：{input_id}")
            d = _input_from_row(row)
            self._guard_not_terminal(d)
            self.conn.execute(
                "UPDATE session_inputs SET next_reconcile_at=?, observed_at=?, "
                "updated_at=? WHERE input_id=?",
                (next_reconcile_at, ts, ts, input_id),
            )
            self._audit(self.conn, actor, "input.defer", "session_input",
                        input_id, None, None, {"reason": reason[:200]})
            return {"input": _input_from_row(self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone())}


__all__ = [
    "ADMISSION_PREPARED", "ADMISSION_POSSIBLE", "ADMISSION_ADMITTED",
    "ADMISSION_DELIVERED", "ADMISSION_UNKNOWN", "ADMISSION_CANCELLED",
    "ADMISSION_EXPIRED", "ADMISSION_RELEASED",
    "INTENT_EXECUTE", "INTENT_PARK", "INTENT_RECORD", "INTENT_NUDGE",
    "InputLedger",
]
