"""结构化依赖等待出口与业务健康共享应用用例（tasks 3.1–3.4）。

``DependencyWaitService(ledger, clock, store=None)``：

- ``ledger``：窄端口对象（见 ``DependencyStorePort``），通常就是由父级组合根
  装配并注入的共享 ``ResponsibilityStore``（新增逻辑方法已挂在责任台账上）；
  ``store=None`` 时直接复用 ``ledger`` 本身（duck-typed，方法集见
  ``_REQUIRED_STORE_METHODS``），应用层不 import 任何 adapter；
- ``clock``：注入时钟（FakeClock / SystemClock）；
- ``reconcile(limit=...)``：有界公平扫描依赖等待责任，返回结构化
  ``{"scanned", "ready", "waiting", "needs_human", "details"}`` 计数。
  只登记复核通知、**不直接 prompt、不直接通过任务**；真正发送由父级统一控制
  守卫阻止，不在此层唤醒 LLM。
- ``register_condition_wait(...)``：登记/幂等一条依赖等待责任；
- ``business_health(limit=...)``：未完成任务业务健康投影 + 结构化可行动事件
  （C21/C22，供独立邮件出口去重；不发送）。

安全不变量（承接 domain.dependency_wait 与既有 responsibility 语义）：

1. 条件必须结构化（显式 Task ID 集合 + all/any + 所需有效验收状态/结果版本）；
   旧字符串等待无法解读 → 稳定人工责任，不无限顺延；
2. 满足 = 已证明的有效验收（非仅字符串 accepted）；证据版本稳定不含时间；
   复核发布**在责任台账的同一 BEGIN IMMEDIATE 内重读真实事实并重新评估**，
   以（责任 + 条件版本 + 证据版本）去重，拒绝 stale/任意版本证据；
3. 依赖缺失/循环/未知/到期 → 转明确人工责任并落去重告警；owner/deadline/
   budget 跨重启保留，预算不因反复观察而重置；
4. 后继必须显式关联（conditional successor），不按标题猜；旧 rejected 不自动
   accepted（本层只登记复核，验收仍走既有 collect/verify 门禁）；
5. 有界分页/公平扫描：只处理依赖等待责任 + 已登记满足事实（含暂停积累的
   awaiting_review 尚未发布行），不扫全局 session；**下一检查未到期不写观察**
   （不递增 observed、不改 revision、不审计），但新满足事实可在公平扫描时评估；
   分页截断无法证明全图无环 → 保守转人工说明，不把截断当无环。
"""

from __future__ import annotations

from typing import Any, Protocol

from ..domain import dependency_wait as dw
from ..domain import responsibility as duty
from ..domain.errors import LedgerError
from ..domain.rules import TASK_TERMINAL
from .ports import Clock

# 单一轮次处理上限（公平扫描默认值；调用方可按轮次预算覆盖）。
_DEFAULT_RECONCILE_LIMIT = 100
_DEFAULT_HEALTH_LIMIT = 50

# 检查退避与默认期限（有界；不重置 deadline/budget）。
RECHECK_BASE_MS = dw.DEFAULT_DEPENDENCY_RECHECK_BASE_MS
RECHECK_MAX_MS = dw.DEFAULT_DEPENDENCY_RECHECK_MAX_MS
DEFAULT_DEADLINE_MS = dw.DEFAULT_DEPENDENCY_DEADLINE_MS

# 公平扫描的完整环图证明上限（超过视为截断 → unknown/人工，而非“无环”）。
_CYCLE_PROOF_PAGE = 500


class DependencyStorePort(Protocol):
    """依赖等待所需的窄台账能力（实现：组合根装配的 ResponsibilityStore）。"""

    base: Any

    def list_open_responsibilities(self, *, task_id: str | None = None,
                                   owner: str | None = None,
                                   limit: int = 200) -> list[dict]: ...

    def list_dependency_waits_page(self, *, after_id: str | None = None,
                                   limit: int = 100) -> list[dict]: ...

    def count_dependency_waits(self) -> int: ...

    def get_responsibility(self, rid: str) -> dict | None: ...

    def update_responsibility(self, rid: str, *, expected_revision: int,
                              fields: dict[str, Any], actor: str = "supervisor",
                              release_manual_confirmation: str | None = None
                              ) -> dict: ...

    def close_responsibility(self, rid: str, *, expected_revision: int,
                             reason: str, actor: str = "coordinator",
                             superseded_by: str | None = None) -> dict: ...

    def ensure_dependency_wait(
        self, owner: str, project: str, task_id: str, condition: dict, *,
        attempt_id: str | None = None, generation: int | None = None,
        result_version: str | None = None, target_session: str | None = None,
        deadline: int, next_check_at: int, next_action: str,
        actor: str = "coordinator", max_budget: int = 3,
    ) -> dict: ...

    def dependency_task_facts(self, task_ids) -> dict[str, dict]: ...

    def record_dependency_review(
        self, rid: str, *, expected_revision: int, now: int | None = None,
        actor: str = "supervisor") -> dict: ...

    def check_dependency_review_notification(self, notice) -> None: ...

    def escalate_dependency_human(self, rid: str, *, expected_revision: int,
                                  reason: str, detail: str,
                                  next_check_at: int | None = None) -> dict: ...

    def list_unfinished_tasks_page(self, *, after_id: str | None = None,
                                   limit: int = 100) -> list[dict]: ...

    def has_pending_task_notifications(self, task_id: str, *,
                                       result_version: str | None = None) -> int: ...

    def open_responsibility(
        self, owner: str, project: str, task_id: str,
        attempt_id: str | None = None, generation: int | None = None,
        result_version: str | None = None, input_id: str | None = None,
        session_id: str | None = None, message_id: str | None = None,
        target_session: str | None = None, *, state: str = "waiting_input",
        wait_kind: str, wait_condition: str, next_action: str,
        next_check_at: int, deadline: int, condition_key: str,
        evidence: dict | None = None, max_budget: int = 3,
        actor: str = "coordinator",
    ) -> dict: ...


# 服务所需的窄端口方法。实现为组合根装配的共享 ResponsibilityStore（新增逻辑
# 方法已挂在责任台账上）；应用层只依赖本方法集，不 import 任何 adapter。
_REQUIRED_STORE_METHODS = (
    "list_dependency_waits_page", "count_dependency_waits",
    "dependency_task_facts", "ensure_dependency_wait",
    "record_dependency_review", "check_dependency_review_notification",
    "escalate_dependency_human", "list_open_responsibilities",
    "get_responsibility", "update_responsibility", "close_responsibility",
    "list_unfinished_tasks_page", "has_pending_task_notifications",
)

# record_dependency_review 拒绝后应“重查等待”而不是转人工的原因（证据在事务内
# 变化/条件失效/身份不一致——下一轮公平扫描会以当前事实重新评估）。
_WAIT_RECHECK_REASONS = frozenset({
    "condition-not-satisfied", "condition-invalid", "condition-unstructured",
    "condition-identity-mismatch", "evidence-version-missing",
})


class DependencyWaitService:
    """共享依赖等待出口用例（Supervisor/CLI/Web 复用同一声明，窄端口注入）。"""

    def __init__(self, ledger: Any, clock: Clock | None = None, *,
                 store: Any = None, recheck_base_ms: int = RECHECK_BASE_MS,
                 recheck_max_ms: int = RECHECK_MAX_MS):
        # ``ledger`` 本身即窄端口对象（父级传共享 ResponsibilityStore）；
        # ``store`` 为 None 时复用 ledger。不在此层 import adapter。
        self._store = store if store is not None else ledger
        missing = [name for name in _REQUIRED_STORE_METHODS
                   if not hasattr(self._store, name)]
        if missing:
            raise LedgerError(
                "DEPENDENCY_PORT_MISSING",
                f"依赖等待窄端口缺少方法：{missing}；请注入组合根装配的 "
                "ResponsibilityStore（composition.make_responsibilities）",
                {"missing": missing})
        self.clock = clock
        self._base = getattr(self._store, "base", ledger)
        self._recheck_base_ms = int(recheck_base_ms)
        self._recheck_max_ms = int(recheck_max_ms)
        if self._recheck_base_ms <= 0 or self._recheck_max_ms <= 0:
            raise ValueError("recheck 退避上下界必须为正整数")

    def _now(self) -> int:
        if self.clock is not None:
            return self.clock.now_ms()
        return int(self._base._now_ms() if hasattr(self._base, "_now_ms") else 0)

    # ---- 登记 -------------------------------------------------------------
    def register_condition_wait(
        self, owner: str, project: str, task_id: str, condition: Any, *,
        attempt_id: str | None = None, generation: int | None = None,
        result_version: str | None = None, target_session: str | None = None,
        deadline: int | None = None, next_check_at: int | None = None,
        next_action: str | None = None, actor: str = "coordinator",
        max_budget: int = 3,
    ) -> dict:
        """登记/幂等复用一条结构化依赖等待责任。

        ``condition`` 必须先通过 ``dw.validate_condition``（否则 ValueError）。
        缺省 deadline 用 24h、next_check_at 用 now；owner/deadline/next_action/
        budget 均由本方法持久化，等待期间只允许有界观察更新。
        """
        normalized = dw.normalize_condition(condition)
        now = self._now()
        limit_at = deadline if (isinstance(deadline, int)
                                and not isinstance(deadline, bool)) \
            else now + DEFAULT_DEADLINE_MS
        due = next_check_at if (isinstance(next_check_at, int)
                                and not isinstance(next_check_at, bool)) else now
        action = (next_action or
                  f"等待依赖 {dw.describe_condition(normalized)}；"
                  "条件满足后生成去重复核，不自动 accepted")
        return self._store.ensure_dependency_wait(
            owner, project, task_id, normalized, attempt_id=attempt_id,
            generation=generation, result_version=result_version,
            target_session=target_session, deadline=limit_at,
            next_check_at=due, next_action=action, actor=actor,
            max_budget=max_budget)

    # ---- 对账 -------------------------------------------------------------
    def reconcile(self, *, limit: int = _DEFAULT_RECONCILE_LIMIT,
                  now: int | None = None) -> dict:
        """有界公平扫描依赖等待责任并评估结构化条件。

        返回：:

            {"scanned": int, "ready": int, "waiting": int,
             "needs_human": int, "details": [{"responsibility_id", ...}]}

        - ``ready``：条件已达有效验收证据 → 已去重登记复核（新发布或已存在）；
          已发布的复核行幂等跳过，不改 revision/不重复审计；
        - ``waiting``：条件未变且到期时仅推进有界 next_check（不改 owner/deadline/
          预算）；**未到期不写观察**（不递增 observed、不改 revision）；
        - ``needs_human``：失效/循环/未知/到期/条件坏/全图无法证明无环 → 稳定
          人工责任（去重告警）。

        覆盖普通 waiting_dependency 责任 + 已登记满足事实（含暂停积累的
        awaiting_review 尚未发布行，任务 resume 后重新核验原条件 + 原证据并以
        原 dedupe 发布一次）；任务真正 accepted/cancelled 时关闭依赖责任。
        每条细节带 ``bucket/reason/next_action`` 供父级展示与邮件事件去重。
        使用 ``sweep-cursor:dependency-waits`` 持久游标做 keyset 公平扫描：不扫
        全局 session、不抬 limit，尾部回绕到头部，避免头部持续饿死尾部。
        """
        clock_now = self._now() if now is None else int(now)
        counts: dict[str, int] = {"scanned": 0, "ready": 0, "waiting": 0,
                                  "needs_human": 0}
        details: list[dict[str, Any]] = []
        limit = max(1, min(int(limit), 500))
        cursor_key = "sweep-cursor:dependency-waits"
        after = None
        if hasattr(self._base, "get_meta"):
            try:
                raw = self._base.get_meta(cursor_key)
            except Exception:
                raw = None
            after = raw if isinstance(raw, str) and raw else None
        page = self._store.list_dependency_waits_page(after_id=after, limit=limit)
        if not page and after:
            # 尾页回绕：有游标但本页为空 → 从头再取，避免单页后永久漏尾。
            after = None
            page = self._store.list_dependency_waits_page(after_id=None, limit=limit)
        for row in page:
            counts["scanned"] += 1
            rid = row.get("id")
            try:
                outcome = self._reconcile_one(row, now=clock_now)
            except LedgerError as e:
                # 单行异常：写可见 audit（code+message 不丢），不阻塞整页其余行。
                self._audit_rejected(rid, e, phase="reconcile")
                counts["rejected"] = counts.get("rejected", 0) + 1
                details.append({"responsibility_id": rid,
                                "task_id": row.get("task_id"),
                                "bucket": "rejected",
                                "reason": f"{e.code}:{str(e)[:200]}",
                                "next_action": "依赖对账单行异常已审计，后续轮次重试"})
                continue
            bucket = outcome["bucket"]
            counts[bucket] += 1
            details.append(outcome)
        counts.setdefault("rejected", 0)
        if hasattr(self._base, "set_meta"):
            # 只有满页才保留游标继续向前；否则回绕到头部（公平覆盖）。
            next_after = page[-1].get("id") if len(page) >= limit else None
            try:
                self._base.set_meta(cursor_key, next_after or "")
            except Exception:
                pass
        return {**counts, "details": details}

    def _reconcile_one(self, row: dict, *, now: int) -> dict[str, Any]:
        rid = row.get("id")
        fresh = self._store.get_responsibility(rid)
        current = fresh if fresh is not None else row
        base = {"responsibility_id": rid, "task_id": current.get("task_id"),
                "condition_key": current.get("condition_key")}
        due = current.get("next_check_at") is None \
            or int(current.get("next_check_at") or 0) <= now

        if current.get("closed_at") is not None:
            return {**base, "bucket": "waiting", "reason": "responsibility-closed",
                    "next_action": "已关闭"}

        # 责任所属任务自身真收口（accepted/cancelled/closed）：按权威事实关闭依赖
        # 责任（普通 tick 在 skip 前不会替我们关闭，这里必须负责）。
        try:
            task = self._base.get_task(current["task_id"])
        except LedgerError:
            task = None
        if task is not None:
            status = task.get("status")
            if status in TASK_TERMINAL or task.get("closed_at") is not None:
                try:
                    self._store.close_responsibility(
                        rid, expected_revision=current["revision"],
                        reason=("task-accepted" if status == "accepted"
                                else "task-cancelled"
                                if status == "cancelled" else "task-closed-nonterminal"),
                        actor="supervisor")
                    return {**base, "bucket": "waiting",
                            "reason": f"task-{status}",
                            "next_action": "任务已收口；责任按权威事实关闭"}
                except LedgerError as e:
                    return {**base, "bucket": "needs_human",
                            "reason": f"close-refused:{e.code}",
                            "next_action": "关闭被守卫拒绝：转人工确认，不改写旧责任"}
        if current.get("state") == duty.STATE_WAITING_HUMAN:
            return {**base, "bucket": "needs_human", "reason": "manual-hold",
                    "next_action": current.get("next_action")}

        # 已满足且 awaiting_review 的存量行：已发布 → 用只读 validator 复核（上游
        # 事实/current/代次/证据版本/期限），健康重复不写不审计；有变化转明确人工
        # 且不伪改旧通知（ACK 保持）。暂停积累（未发布）→ 跳过，resume 后同路径
        # 重新核验并以原 dedupe 发布一次。
        evidence = current.get("evidence") if isinstance(current.get("evidence"),
                                                        dict) else {}
        dep_fact = evidence.get(dw.DEPENDENCY_KEY)
        dep_fact = dep_fact if isinstance(dep_fact, dict) else {}
        if current.get("state") == duty.STATE_AWAITING_REVIEW \
                and dep_fact.get("satisfied") is True:
            # 只有真正入队（notification_id 落库）才算已发布；暂停积累只有
            # review_dedupe_key（原 dedupe 预留），resume 后仍需重新核验并发布一次。
            published = bool(dep_fact.get("notification_id"))
            if task is not None and task.get("status") == "paused" and not published:
                return {**base, "bucket": "waiting",
                        "reason": "satisfied-accumulated-held",
                        "next_action": (current.get("next_action")
                                        or "复核事实保留，任务恢复后发布"),
                        "published": False, "write_applied": False}
            if published:
                try:
                    self._store.check_dependency_review_notification({
                        "dedupe_key": dep_fact.get("review_dedupe_key"),
                        "metadata": {"responsibility_id": rid,
                                     "condition_identity": dep_fact.get(
                                         "condition_identity"),
                                     "evidence_version": dep_fact.get(
                                         "evidence_version")},
                        "payload": {"classification": "DEPENDENCY_SATISFIED"},
                    })
                except LedgerError as e:
                    # 已发布复核失效力 → 转明确人工；旧通知保留（ACK 不变），
                    # 只阻断后续发送。
                    return self._escalate(
                        rid, current, now, reason=f"review-stale:{e.code}",
                        detail=(f"已发布依赖复核失效（{str(e)[:240]}）；"
                                "转人工复核，不伪造旧通知 ACK"))
                return {**base, "bucket": "waiting",
                        "reason": "review-already-published",
                        "next_action": (current.get("next_action")
                                        or "复核仍有效，等待协调者核收"),
                        "published": True, "write_applied": False}

        # 期限/预算耗尽 → 稳定人工（不重置期限/预算、不无限顺延）。
        decision = duty.deadline_decision(current, now_ms=now)
        if decision.get("exhausted"):
            return self._escalate(
                rid, current, now,
                reason=decision.get("code") or "deadline-exhausted",
                detail=(f"依赖等待期限/预算耗尽（deadline={current.get('deadline')} "
                        f"budget={current.get('budget_used')}/"
                        f"{current.get('max_budget')}）；业务未完成，转人工"))

        condition = dw.extract_dependency_condition(current)
        if condition is None:
            return self._escalate(
                rid, current, now, reason="condition-unstructured",
                detail=("历史字符串/缺失结构化条件无法可靠评估：转人工复核，"
                        "不猜依赖已完成，也不靠滚动 next_check 无限顺延"))

        facts = self._store.dependency_task_facts(condition.get("task_ids"))
        evaluation = dw.evaluate_task_condition(condition, facts)
        status = evaluation["status"]

        if status == dw.UNKNOWN:
            return self._escalate(
                rid, current, now, reason="dependency-unknown",
                detail=(f"依赖证据不足：{evaluation['reason']}；"
                        "保持未知并转人工确认，不猜测依赖已完成"))

        if status in (dw.BROKEN, dw.INVALID):
            return self._escalate(
                rid, current, now,
                reason=("dependency-broken" if status == dw.BROKEN
                        else "condition-invalid"),
                detail=(f"依赖失效/不可达成：{evaluation['reason']}；"
                        "缺失/取消/循环的依赖转明确人工责任"))

        # 依赖环检测：截断（读不到全图）→ 保守转人工，不把截断当“无环”。
        cycle, truncated = self._detect_dependency_cycle(current, condition)
        if truncated:
            return self._escalate(
                rid, current, now, reason="dependency-cycle-unverified",
                detail=("依赖等待责任超过单页证明上限，无法证明全图无环："
                        "转人工复核，不把截断当无环"))
        if cycle:
            return self._escalate(
                rid, current, now, reason="dependency-cycle",
                detail=f"检测到显式依赖环路：{' -> '.join(cycle)}；转人工拆解")

        if status == dw.SATISFIED:
            return self._review_satisfied(rid, current, now=now)

        # waiting：只有到期才写一次有界观察（不递增失败预算、不重置 deadline）；
        # 未到期直接返回，不动责任行（不递增 observed、不改 revision）。
        if not due:
            return {**base, "bucket": "waiting", "reason": "not-due",
                    "next_check_at": current.get("next_check_at"),
                    "next_action": current.get("next_action"),
                    "write_applied": False}
        return self._observe_waiting(current, now)

    def _observ_facts(self, current: dict) -> int:
        evidence = current.get("evidence")
        dep = evidence.get(dw.DEPENDENCY_KEY) if isinstance(evidence, dict) else {}
        observed = dep.get(dw.OBSERVED_KEY) if isinstance(dep, dict) else None
        return int(observed or 0) + 1

    def _observe_waiting(self, current: dict, now: int) -> dict[str, Any]:
        rid = current.get("id")
        observed = self._observ_facts(current)
        candidate = min(self._recheck_base_ms * 2 ** min(observed - 1, 6),
                        self._recheck_max_ms)
        deadline = current.get("deadline")
        if isinstance(deadline, int) and deadline > 0:
            candidate = min(candidate, max(0, deadline - now))
        next_at = now + candidate
        evidence = dict(current.get("evidence") or {})
        dep = dict(evidence.get(dw.DEPENDENCY_KEY)
                   if isinstance(evidence.get(dw.DEPENDENCY_KEY), dict) else {})
        dep[dw.OBSERVED_KEY] = observed
        evidence[dw.DEPENDENCY_KEY] = dep
        try:
            self._store.update_responsibility(
                rid, expected_revision=current["revision"],
                fields={"next_check_at": next_at, "evidence": evidence},
                actor="supervisor")
        except LedgerError as e:
            return {**self._detail_base(current), "bucket": "waiting",
                    "reason": f"update-rejected:{e.code}",
                    "next_action": current.get("next_action"),
                    "next_check_at": next_at}
        return {**self._detail_base(current), "bucket": "waiting",
                "reason": "dependencies-pending", "next_check_at": next_at,
                "next_action": current.get("next_action"),
                "observed": observed, "write_applied": True}

    def _detail_base(self, row: dict) -> dict[str, Any]:
        return {"responsibility_id": row.get("id"),
                "task_id": row.get("task_id"),
                "condition_key": row.get("condition_key")}

    def _audit_rejected(self, rid, err: LedgerError, *, phase: str) -> None:
        """依赖对账的行级异常/拒绝 → 持久可见 audit；经 base ledger 端口写入，
        应用层不写 SQL。action 统一 ``responsibility.tick-rejected``，code 与
        message 不丢（父故障注入 escalate_dependency_human 也走这里）。
        """
        audit = getattr(self._base, "audit", None)
        if not callable(audit):
            return
        try:
            audit(
                "supervisor", "responsibility.tick-rejected", "responsibility",
                str(rid or ""),
                {"phase": phase, "code": getattr(err, "code", "UNKNOWN"),
                 "message": str(err)[:400],
                 "detail": getattr(err, "detail", None)},
            )
        except Exception:
            pass  # audit 失败不吞主流程（后续轮次仍会重试）

    def _review_satisfied(self, rid: str, current: dict, *,
                          now: int) -> dict[str, Any]:
        try:
            result = self._store.record_dependency_review(
                rid, expected_revision=current["revision"], now=now,
                actor="supervisor")
        except LedgerError as e:
            if e.code == "RESPONSIBILITY_STALE":
                return self._escalate(
                    rid, current, now, reason="stale-attribution",
                    detail=f"依赖满足时归属已变化（{str(e)[:200]}）：不登记旧复核")
            if e.code == "RESPONSIBILITY_TASK_TERMINAL":
                return {**self._detail_base(current), "bucket": "waiting",
                        "reason": "task-terminal", "next_action": "任务已收口"}
            return self._escalate(
                rid, current, now, reason=f"review-rejected:{e.code}",
                detail=f"复核登记被守卫拒绝：{str(e)[:200]}")
        reason = result.get("reason")
        if result.get("recorded"):
            if reason == "task-paused":
                # 暂停只积累复核事实不唤醒：事实已落库、状态已到 awaiting_review，
                # 恢复后父级统一守卫再发布（这里不动通知、不额外审计）。
                return {**self._detail_base(current), "bucket": "ready",
                        "reason": "dependency-satisfied-accumulated",
                        "next_action": "依赖已满足且任务暂停：复核事实保留，恢复后发布",
                        "published": False, "accumulated": True}
            if reason == "no-route":
                return {**self._detail_base(current), "bucket": "needs_human",
                        "reason": "no-route",
                        "next_action": "依赖已满足但无可核验协调者：转人工指定责任人",
                        "published": False}
            if reason == "already-published":
                return {**self._detail_base(current), "bucket": "ready",
                        "reason": "review-already-published",
                        "next_action": current.get("next_action")
                        or "复核已发布，等待协调者核收",
                        "published": False, "write_applied": False}
            return {**self._detail_base(current), "bucket": "ready",
                    "reason": ("dependency-satisfied"
                               if result.get("published") else
                               "review-already-recorded"),
                    "next_action": current.get("next_action")
                    or "核收依赖满足并判断后继；不自动 accepted",
                    "published": bool(result.get("published"))}
        # 未登记：事务内重评估不满足（条件失效/身份不一致/伪造 proof）→ 重查等待，
        # 下一轮以当前事实重新评估；其余守卫拒绝（manual-hold/deadline 等）转人工。
        if reason in _WAIT_RECHECK_REASONS:
            return {**self._detail_base(current), "bucket": "waiting",
                    "reason": f"recheck:{reason}",
                    "status": result.get("status"),
                    "next_action": current.get("next_action")
                    or "依赖证据在事务内变化：下一轮以当前事实重查，不猜测完成"}
        return self._escalate(
            rid, current, now, reason=reason or "review-not-recorded",
            detail=(f"依赖满足但无法登记复核（{reason or 'unknown'}）："
                    "事实保留，人工决定后续"))

    def _escalate(self, rid, current: dict, now: int, *, reason: str,
                  detail: str) -> dict[str, Any]:
        try:
            updated = self._store.escalate_dependency_human(
                rid, expected_revision=current["revision"],
                reason=reason, detail=detail,
                next_check_at=now + min(self._recheck_max_ms, 15 * 60_000))
        except LedgerError as e:
            # 转人工被守卫拒绝：写入可见 audit（code+message 不丢），保留诊断。
            self._audit_rejected(rid, e, phase="escalate")
            return {**self._detail_base(current), "bucket": "needs_human",
                    "reason": f"escalate-rejected:{e.code}:{str(e)[:120]}",
                    "audit_logged": True,
                    "next_action": "转人工被守卫拒绝：保留原等待与诊断，已审计待重试"}
        return {**self._detail_base(current), "bucket": "needs_human",
                "reason": reason,
                "next_action": updated.get("next_action")
                or f"人工处理（{reason}）：{detail[:120]}"}

    def _detect_dependency_cycle(self, current: dict, condition: dict,
                                 ) -> tuple[list[str] | None, bool]:
        """依赖边的环路检测：只喂有界页的依赖等待责任（不扫全局 session）。

        返回 ``(cycle_or_None, truncated)``。`truncated=True` 表示未能读完全部
        依赖等待责任（无法证明全图无环）→ 调用方应转 unknown/人工，不能把
        截断当作无环。
        """
        task_id = current.get("task_id")
        edges: dict[str, list[str]] = {}
        try:
            waits = self._store.list_dependency_waits_page(limit=_CYCLE_PROOF_PAGE)
        except LedgerError:
            waits = []
        total = 0
        if getattr(self._store, "count_dependency_waits", None):
            try:
                total = int(self._store.count_dependency_waits() or 0)
            except LedgerError:
                total = -1
        truncated = total < 0 or total > len(waits)
        for row in waits:
            dep = row.get("evidence") if isinstance(row, dict) else {}
            if not isinstance(dep, dict):
                continue
            cond = dep.get(dw.DEPENDENCY_KEY)
            if isinstance(cond, dict):
                cond = cond.get("condition")
            if isinstance(cond, dict) and isinstance(cond.get("task_ids"), list):
                edges[str(row.get("task_id") or "")] = list(cond["task_ids"])
        # 本候选的条件合并进边集（可能尚未登记为独立等待）。
        edges[str(task_id)] = list(condition.get("task_ids") or [])
        if truncated:
            return None, True
        return dw.detect_cycle(edges, str(task_id)), False

    # ---- 业务健康（C21/C22） ----------------------------------------------
    def business_health(self, *, limit: int = _DEFAULT_HEALTH_LIMIT,
                         heartbeat_per_session: dict[str, int] | None = None,
                        advance_cursor: bool = True, after_id: str | None = None,
                        project: str | None = None, session_id: str | None = None,
                         now: int | None = None) -> dict:
        """未完成任务业务健康投影 + 结构化可行动事件（不发送）。

        只扫本台账已登记的未完成任务（``list_unfinished_tasks_page``，keyset
        有界），持久 ``sweep-cursor:business-health-tasks`` 游标公平轮转，不扫
        全局 session；heartbeat 只作进程健康展示，不充当业务进展。任何台账读取
        失败/覆盖不足 → 该任务投影为 unknown 且**不产出** no_progress/未知类
        可行动事件（读不到 ≠ 没有责任/没有通知）。返回：

            {"scanned": int, "tasks": [{task_id, state, process_healthy,
                business_progress, blocked, reasons, next_action,
                evidence_coverage, owner, ...}],
             "events": [{kind, task_id, project, reason, owner,
                next_action, condition, event_key}]}
        """
        clock_now = self._now() if now is None else int(now)
        heartbeat = dict(heartbeat_per_session or {})
        limit = max(1, min(int(limit), 200))
        cursor_key = "sweep-cursor:business-health-tasks"
        after = after_id
        if advance_cursor and after is None and hasattr(self._base, "get_meta"):
            try:
                raw = self._base.get_meta(cursor_key)
            except Exception:
                raw = None
            after = raw if isinstance(raw, str) and raw else None
        filters = {}
        if project is not None:
            filters["project"] = project
        if session_id is not None:
            filters["session_id"] = session_id
        page = self._store.list_unfinished_tasks_page(after_id=after, limit=limit, **filters)
        if not page and after and advance_cursor:
            after = None
            page = self._store.list_unfinished_tasks_page(after_id=None, limit=limit, **filters)
        out_tasks: list[dict[str, Any]] = []
        events: list[dict[str, Any]] = []
        provider_states: dict[str, Any] = {}
        if hasattr(self._base, "provider_states"):
            try:
                provider_states = self._base.provider_states()
            except Exception:
                provider_states = {}
        for task in page:
            task_id = task.get("id")
            attempt = None
            attempt_ok = True
            if task.get("current_attempt_id"):
                try:
                    attempt = self._base.get_attempt(task["current_attempt_id"])
                except LedgerError:
                    attempt, attempt_ok = None, False
            responsibilities = []
            responsibilities_ok = True
            try:
                responsibilities = self._store.list_open_responsibilities(
                    task_id=task_id, limit=200)
            except LedgerError:
                responsibilities, responsibilities_ok = [], False
            latest_result = (((attempt or {}).get("metadata") or {})
                             .get("result") or {}).get("version")
            marks = ([int(r.get("created_at") or 0) for r in responsibilities]
                     + [int(task.get("accepted_at") or 0),
                        int((attempt or {}).get("result_collected_at") or 0)
                        if latest_result else 0])
            business_mark = max(marks) if any(m > 0 for m in marks) else None
            pending_count = 0
            pending_ok = True
            try:
                pending_count = self._store.has_pending_task_notifications(
                    task_id, result_version=latest_result)
            except LedgerError:
                pending_count, pending_ok = 0, False
            coordinator = task.get("coordinator_session")
            heartbeat_at = heartbeat.get(coordinator) if coordinator else None
            provider_blocked = False
            if attempt is not None and isinstance(provider_states, dict):
                model = attempt.get("model")
                state = provider_states.get(model) if model else None
                if isinstance(state, dict):
                    until = state.get("until")
                    provider_blocked = bool(
                        isinstance(until, int) and until > clock_now)
            coverage_ok = (attempt_ok and responsibilities_ok and pending_ok
                           and len(responsibilities) < 200)
            projection = dw.wait_health_projection(
                task=task, attempt=attempt, responsibilities=responsibilities,
                heartbeat_at=heartbeat_at, now=clock_now,
                business_event_at=business_mark,
                latest_result_version=latest_result,
                provider_blocked=provider_blocked,
                pending_notifications=pending_count,
                evidence_coverage=coverage_ok,
            )
            # 责任人：真实责任 holder（最新责任行的 owner）优先，其次任务协调者；
            # 不能因“owner 不以 manual 开头”判断就永远落在 manual:unassigned。
            owner = "manual:unassigned"
            by_latest = sorted(
                (r for r in responsibilities
                 if isinstance(r, dict) and r.get("owner")
                 and r.get("closed_at") is None),
                key=lambda r: int(r.get("updated_at") or 0), reverse=True)
            if by_latest and by_latest[0].get("owner"):
                owner = by_latest[0]["owner"]
            elif coordinator:
                owner = coordinator
            projected = {
                "task_id": task_id,
                "project": task.get("project"),
                "task_status": task.get("status"),
                **projection,
                "owner": owner,
                "coordinator_session": coordinator,
                "current_attempt_id": task.get("current_attempt_id"),
                "latest_result_version": latest_result,
                "pending_notifications": pending_count,
                "latest_business_event_at": business_mark,
                "coverage_ok": coverage_ok,
                "responsibilities": [{k: r.get(k) for k in (
                    "id", "state", "owner", "wait_kind", "next_action", "next_check_at",
                    "deadline", "condition_version", "budget_used", "max_budget")}
                    for r in responsibilities],
                "responsibility_coverage_complete": len(responsibilities) < 200,
            }
            out_tasks.append(projected)
            if not coverage_ok:
                # 读不到 ≠ 无责任/0 通知：不产出可行动事件，避免伪造 no_progress。
                continue
            kind = self._event_kind(projected)
            if kind:
                events.append(dw.actionable_event(
                    kind=kind, task_id=task_id, project=task.get("project") or "",
                    reason=", ".join(projection["reasons"] or ["no-open-progress"])
                    if projection["reasons"] else "no-open-progress",
                    owner=owner, next_action=projection["next_action"],
                    condition_key=(projection.get("facts") or {}).get(
                        "condition_key"),
                    at_ms=clock_now))
        if advance_cursor and hasattr(self._base, "set_meta"):
            next_after = page[-1].get("id") if len(page) >= limit else None
            try:
                self._base.set_meta(cursor_key, next_after or "")
            except Exception:
                pass
        return {"scanned": len(page), "tasks": out_tasks, "events": events,
                "coverage": {"complete": len(page) < limit,
                             "next_after_id": page[-1]["id"] if len(page) >= limit else None,
                             "cursor_advanced": advance_cursor,
                             "scope": {"project": project, "session_id": session_id}}}

    @staticmethod
    def _event_kind(projected: dict) -> str | None:
        state = projected.get("state")
        if state == dw.STATE_NEEDS_HUMAN:
            reasons = projected.get("reasons") or []
            if "waiting-human-responsibility" in reasons:
                return "dependency-unresolvable"
            if any("verification" in r for r in reasons):
                return "verification-missing"
            return "task-unaccepted-no-progress"
        if state == dw.STATE_NO_PROGRESS:
            return "task-unaccepted-no-progress"
        if state == dw.STATE_UNKNOWN:
            return "task-status-unknown"
        return None


__all__ = ["DependencyWaitService", "DependencyStorePort", "RECHECK_BASE_MS",
           "RECHECK_MAX_MS", "DEFAULT_DEADLINE_MS", "_REQUIRED_STORE_METHODS"]
