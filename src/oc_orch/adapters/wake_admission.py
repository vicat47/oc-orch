"""原子发送认领适配器：provider 半开探测 + 输入 possible 标记同一事务。

背景（Supervisor``_send_wake_input`` 旧实现的风险）：``target_gate.claim_probe``
与 ``ledger.mark_input_possible`` 是两笔短事务；两事务之间若暂停/控制 CAS 先
提交或输入发送权已被他人取得，probe 预算已被浪费或留下孤立 probe（实际上并未
POST）。本适配器把「重读原输入 + 冻结成员控制守卫 + provider 探测占用 +
possible 唯一发送权」放进**同一 BEGIN IMMEDIATE**：

- 同一事务内重读原输入，复核 session/message（原 ID 不得换）/owner/revision；
- provider 拒绝可保留其只读/超时诊断（COMMIT 该分支，不烧预算——拒绝路径
  本就不花费；``probe-unknown`` 的超时水位是诊断位，不属于新花费）；
- possible 失败或控制失败（暂停/回合/成员变化/CAS/终态）→ **整体 ROLLBACK**，
  撤销 probe 花费/创建与审计行；绝不在未获胜时留下 probe；
- 不执行任何网络；只有 ``allowed=True`` 且事务已提交，父 runtime 才可 POST。

事务语义：``claim`` 自管一笔短事务（等价 `sqlite_db.transaction` 的
BEGIN IMMEDIATE），不允许在既有事务内嵌套调用（进入时 ``conn.in_transaction``
为真则报错）。不修改全局 transaction 嵌套语义；也不引入绕过守卫的外部
``_in_tx`` bool 开关。ledger 与 target_gate 必须是**同一 SQLite 连接对象**，
组合方（composition/runtime）保证；否则构造即失败（fail-fast）。
"""

from __future__ import annotations

import sqlite3
from typing import Any

from ..domain.errors import LedgerError
from .clock import SystemClock


class WakeAdmission:
    """把 provider 半开探测与输入 possible 标记做成同一原子发送认领。

    公开 API：

    ``claim(session_id, *, input_id, message_id, owner, expected_revision,
    now=None) -> {allowed: bool, reason: str, probe: dict|None,
    input: dict|None}``

    - ``allowed=True``（事务已提交）：本调用的 probe 已占用、输入已 ``possible``，
      父 runtime 可以安全 POST；``probe`` 为原 ``claim_probe`` 结果、``input``
      为标记后的最新输入快照；
    - ``allowed=False``：未取得发送权（不产生新 probe 花费/发送权）。``probe``
      仅在「provider 拒绝且已提交其诊断」时为原 claim_probe 结果，其余回滚路径
      为 None；``input`` 为**回滚后/最新**输入快照或 None。
    - ``reason`` 稳定代码：``admitted`` / ``input-not-found`` /
      ``session-mismatch`` / ``input-message-mismatch``（unknown 原 ID 不得换）/
      ``invalid-owner`` / ``gate:<claim_probe.reason>``（provider 拒绝）/
      ``control:<LedgerError.code>``（暂停/回合/终态/CAS 等，整体回滚）/
      ``members-changed:<why>`` / ``already-possible`` / ``input-possible-race``。
    """

    def __init__(self, ledger: Any, target_gate: Any):
        ledger_conn = getattr(ledger, "conn", None)
        gate_conn = getattr(target_gate, "_conn", None)
        if ledger_conn is None or ledger_conn is not gate_conn:
            raise ValueError(
                "WakeAdmission 要求 ledger 与 target_gate 共享同一 SQLite 连接；"
                "组合方（composition/runtime）必须传入同一 conn")
        self._ledger = ledger
        self._gate = target_gate
        self._conn = ledger_conn
        self._clock = (getattr(ledger, "clock", None)
                       or getattr(target_gate, "_clock", None)
                       or SystemClock())

    # ---- 内部 --------------------------------------------------------
    def _now_ms(self) -> int:
        return int(self._clock.now_ms())

    def _read_input_in_tx(self, input_id: str) -> dict | None:
        """当前事务内重读输入行；直接经共享连接（事务快照）。"""
        return self._ledger.get_input(input_id)

    def _read_input_fresh(self, input_id: str) -> dict | None:
        """回滚后重读（事务外最新已提交视图）。"""
        return self._ledger.get_input(input_id)

    @staticmethod
    def _refusal(reason: str, *, probe, input_row) -> dict:
        return {"allowed": False, "reason": reason, "probe": probe,
                "input": input_row}

    # ---- 发送认领 -----------------------------------------------------
    def claim(self, session_id: str, *, input_id: str, message_id: str,
              owner: str, expected_revision: int,
              now: int | None = None, model_observation: dict | None = None) -> dict:
        """同一 BEGIN IMMEDIATE 内完成原输入复核 + provider 探测 + possible 标记。

        任何拒绝都不烧 provider 预算/不建孤立 probe：
        - provider 拒绝（gate:…）→ COMMIT 其只读/超时诊断后返回
          ``allowed=False``（拒绝路径不花费/不创建新 probe）；
        - possible 失败/控制失败/CAS 失败 → 整体 ROLLBACK（probe 花费、审计、
          possible 写入全部撤销）后返回 ``allowed=False`` 与最新输入。
        """
        ts = now if now is not None else self._now_ms()
        if type(expected_revision) is not int or expected_revision < 1:
            return self._refusal("control:INPUT_REVISION_CONFLICT", probe=None, input_row=None)
        conn = self._conn
        if getattr(conn, "in_transaction", False):
            raise RuntimeError(
                "WakeAdmission.claim 必须自管一笔短事务，不能在既有事务内嵌套调用")
        conn.execute("BEGIN IMMEDIATE")
        try:
            self._gate._ensure_schema()  # noqa: SLF001 组合适配器同一事务内确保
            d = self._read_input_in_tx(input_id)
            if d is None:
                conn.execute("COMMIT")
                return self._refusal("input-not-found", probe=None, input_row=None)
            # 原输入身份复核（先于 provider 花费；失败不写任何东西）
            if d.get("session_id") != session_id:
                conn.execute("COMMIT")
                return self._refusal("session-mismatch", probe=None, input_row=d)
            if (not isinstance(message_id, str) or not message_id.strip()
                    or d.get("message_id") != message_id):
                # unknown/逾期输入也保留原 ID；不得以新 ID 换发/重发。
                conn.execute("COMMIT")
                return self._refusal("input-message-mismatch", probe=None,
                                     input_row=d)
            if not (isinstance(owner, str) and owner.strip()):
                conn.execute("COMMIT")
                return self._refusal("invalid-owner", probe=None, input_row=d)
            if d.get("source") != "automatic_wake" or d.get("intent") not in ("execute", "nudge"):
                conn.execute("COMMIT")
                return self._refusal("invalid-automatic-intent", probe=None, input_row=d)

            # 1) provider 半开探测原子占用（本事务内；拒绝保留只读/超时诊断）
            probe = self._gate._claim_probe_in_tx(  # noqa: SLF001
                session_id, owner=owner, message_id=message_id, now=ts)
            if not probe["allowed"]:
                conn.execute("COMMIT")  # 拒绝/诊断位提交（无预算花费）
                return self._refusal(f"gate:{probe['reason']}",
                                     probe=probe, input_row=d)

            # 2) possible 唯一发送权（失败/控制失败 → 整体回滚 probe 花费）
            try:
                pos = self._ledger._mark_input_possible_in_tx(  # noqa: SLF001
                    input_id, owner=owner, expected_revision=expected_revision,
                    now=ts)
            except LedgerError as exc:
                conn.execute("ROLLBACK")  # 撤销 probe 花费/创建与审计
                return self._refusal(f"control:{exc.code}", probe=None,
                                     input_row=self._read_input_fresh(input_id))
            if not pos["changed"]:
                conn.execute("ROLLBACK")  # 唯一赢家已被他人取得/成员变化 → 撤销
                reason = pos.get("reason") or "input-possible-race"
                return self._refusal(reason, probe=None,
                                      input_row=self._read_input_fresh(input_id))

            from .delivery_action import DeliveryActions

            try:
                permit = DeliveryActions(self._ledger, self._clock).input_in_tx(
                    input_id, observation=model_observation or {"session_id": session_id},
                    operation="prompt", owner=owner,
                    expected_revision=pos["input"]["revision"])
            except LedgerError as exc:
                conn.execute("ROLLBACK")  # 资源拒绝也不烧provider预算/不留possible孤儿
                return self._refusal(f"resource:{exc.code}", probe=None,
                                     input_row=self._read_input_fresh(input_id))
            conn.execute("COMMIT")
            return {"allowed": True, "reason": "admitted",
                    "probe": probe, "input": pos["input"], "permit": permit}
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.OperationalError:
                pass  # 事务可能尚未开始或已回滚
            raise
