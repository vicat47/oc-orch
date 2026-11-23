"""独立邮件执行器：有限 tick、每请求独立连接、默认关闭零外部调用。

设计边界（父审查项）：
- 每次 ``tick()`` 自己 ``composition.init(home)`` 打开独立连接、经
  ``composition.make_operator_email`` 装配 svc，结束即关闭——不跑进
  Supervisor 主循环，也不隐式开线程/进程；同 home 主 Superviser 连接不受影响。
- 步骤顺序固定：``reconcile_stale_claimed → verify_receipt(owner) →
  dispatch_once(owner)``；任一真实异常被捕获为结构化可见结果，不向上抛。
- 默认完全无外部能力：未注入 policy 即默认 ``EmailPolicy()``（disabled）；
  未注入 transport 即用硬闸 ``_BlockedTransport``（MailAuthError/
  MailSearchUnavailableError，零外部 CLI）。**实际发送/查询的调用方必须同时
  显式注入已批准 policy 与已验证 transport**——``SenderBinding.verified`` 只是
  可信装配层的断言标记，不是本运行时可自行证明的任意 API bool。
- 心跳/上次结构状态经 ``Ledger.get_meta/set_meta``（不写 SQL）持久在
  ``email-executor:tick`` / ``email-executor:status``；状态内容已脱敏
  （丢弃 item 快照等正文），并限长。
- runtime 层只 import composition/util/application/domain/标准库，不直接
  import adapters：conn/Ledger/svc 均经 composition 再导出。
"""

from __future__ import annotations

import json
from typing import Any

from ..application.mail_port import MailAuthError, MailSearchUnavailableError
from ..composition import Ledger, init, make_operator_email
from ..domain.operator_email import redact_text
from ..util import ensure_home, now_ms

DEFAULT_OWNER = "email-executor"
TICK_META_KEY = "email-executor:tick"
STATUS_META_KEY = "email-executor:status"
MAX_STATUS_BYTES = 8192

# 持久状态只保留这些结果键（丢弃 item 快照、body/subject 等正文与凭据形态）。
_PUBLIC_KEYS = frozenset({
    "result", "reason", "kind", "item_id", "count", "external_ref", "hits",
    "complete", "next_attempt_at", "status", "promoted", "expired", "outcome",
    "created", "seq",
})


class _BlockedTransport:
    """未注入传输时的硬闸：任何发送/查询都不允许且无外部副作用。"""

    def send(self, **kw):
        raise MailAuthError(
            "transport-not-injected",
            detail="未注入已核实的外部传输，fail-closed 拒发")

    def search_receipt(self, **kw):
        raise MailSearchUnavailableError(
            "transport-not-injected", detail="未注入已核实的外部传输")

    def __repr__(self) -> str:  # pragma: no cover - 诊断辅助
        return "<BlockedTransport>"


class _CountingTransport:
    """包装注入传输并统计 send/search 次数；语义与异常原样透传。"""

    def __init__(self, inner: Any):
        object.__setattr__(self, "_inner", inner)
        object.__setattr__(self, "send_count", 0)
        object.__setattr__(self, "search_count", 0)

    def send(self, **kw):
        self.send_count += 1
        return self._inner.send(**kw)

    def search_receipt(self, **kw):
        self.search_count += 1
        return self._inner.search_receipt(**kw)


class OperatorEmailExecutor:
    """独立有限的邮件提醒执行器（多 tick 复用装配参数，连接按 tick 独立）。"""

    def __init__(self, home: str, *, clock=None, policy=None, transport=None,
                 owner: str | None = None):
        self._home = ensure_home(home)
        self._clock = clock
        self._policy = policy
        self._transport = _CountingTransport(
            transport if transport is not None else _BlockedTransport())
        self._owner = owner or DEFAULT_OWNER

    # ---- 每 tick 独立装配 -------------------------------------------------
    def _open(self):
        conn = init(self._home)
        try:
            from ..domain.operator_email import policy_state, POLICY_ENABLED

            svc = make_operator_email(conn, clock=self._clock,
                                     policy=self._policy, transport=self._transport)
            failover_svc = None
            if self._policy is not None and policy_state(self._policy) == POLICY_ENABLED:
                from ..composition import make_failover_email

                failover_svc = make_failover_email(
                    conn, clock=self._clock, policy=self._policy,
                    transport=self._transport)
            ledger = Ledger(conn, self._home, clock=self._clock)
            return conn, svc, failover_svc, ledger
        except BaseException:
            conn.close()
            raise

    def _now_ms(self) -> int:
        if self._clock is not None:
            return int(self._clock.now_ms())
        return now_ms()

    # ---- 有限 tick --------------------------------------------------------
    def tick(self) -> dict:
        result: dict[str, Any] = {"ok": True, "owner": self._owner}
        conn = None
        try:
            conn, svc, failover_svc, ledger = self._open()
            seq = self._next_seq(ledger)
            result["seq"] = seq
            self._run_steps(svc, result, failover_svc)
            result["external_calls"] = {
                "send": self._transport.send_count,
                "search": self._transport.search_count,
            }
            result["persisted"] = self._persist_state(
                ledger, seq, self._now_ms(), result)
        except Exception as exc:  # noqa: BLE001 - 结构化捕获，不向上抛
            result["ok"] = False
            result.setdefault("steps", {})
            result["error"] = _public_error(exc)
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass
        return result

    def _run_steps(self, svc, result: dict, failover_svc=None) -> None:
        steps: dict[str, Any] = {}
        ok = True

        def run(name: str, call) -> None:
            nonlocal ok
            if not ok:
                steps[name] = {"skipped": True, "reason": "previous-step-failed"}
                return
            try:
                out = call()
                steps[name] = out if isinstance(out, dict) \
                    else {"result": str(out)}
            except Exception as exc:  # noqa: BLE001
                ok = False
                steps[name] = {"error": _public_error(exc)}

        run("reconcile_stale_claimed",
            lambda: svc.reconcile_stale_claimed())
        run("verify_receipt",
            lambda: svc.verify_receipt(owner=self._owner))
        run("dispatch_once",
            lambda: svc.dispatch_once(owner=self._owner))
        # 故障知会发送（独立通道）：失败不影响主链路 ok；未配置显式跳过。
        try:
            out = (failover_svc.dispatch_once(owner=self._owner)
                   if failover_svc else {"result": "not-configured"})
            steps["failover_dispatch_once"] = out if isinstance(out, dict) \
                else {"result": str(out)}
        except Exception as exc:  # noqa: BLE001
            steps["failover_dispatch_once"] = {"error": _public_error(exc)}
        result["steps"] = steps
        result["ok"] = ok

    # ---- 持久 heartbeat/status（经 Ledger 方法，不写 SQL） ----------------
    def _next_seq(self, ledger) -> int:
        raw = ledger.get_meta(TICK_META_KEY)
        prev = 0
        if raw:
            try:
                prev = int((json.loads(raw) or {}).get("seq") or 0)
            except (TypeError, ValueError):
                prev = 0
        return prev + 1

    def _persist_state(self, ledger, seq: int, at: int, result: dict) -> dict:
        tick_payload = json.dumps(
            {"at": at, "seq": seq, "owner": self._owner,
             "ok": bool(result.get("ok"))}, ensure_ascii=False)
        status_payload = json.dumps(_public_summary(result),
                                    ensure_ascii=False)
        payload = status_payload.encode("utf-8")
        if len(payload) > MAX_STATUS_BYTES:
            status_payload = json.dumps(
                {"ok": bool(result.get("ok")), "seq": seq,
                 "owner": redact_text(self._owner, limit=80), "truncated": True,
                 "steps": {name: {"result": redact_text(str(step.get("result") or ""), limit=80)}
                           for name, step in list((result.get("steps") or {}).items())[:3]
                           if isinstance(step, dict)}},
                ensure_ascii=False)
        try:
            ledger.set_meta(TICK_META_KEY, tick_payload)
            ledger.set_meta(STATUS_META_KEY, status_payload)
            return {"tick": TICK_META_KEY, "status": STATUS_META_KEY,
                    "seq": seq}
        except Exception as exc:  # noqa: BLE001 - 心跳失败不掩盖执行结果
            return {"error": _public_error(exc)}


# ---- 脱敏与结构化工具 ------------------------------------------------------
def _public_error(exc: Exception) -> dict:
    return {"kind": type(exc).__name__, "message": redact_text(str(exc), limit=200)}


def _public_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return redact_text(value, limit=200)
    if isinstance(value, (list, tuple)):
        return [_public_value(v) for v in value[:20]]
    if isinstance(value, dict):
        return {k: _public_value(v) for k, v in list(value.items())[:20]
                if v is not None}
    return None


def _public_summary(result: dict) -> dict:
    out: dict[str, Any] = {}
    for key in ("ok", "owner", "seq", "error", "external_calls", "persisted"):
        if key in result:
            out[key] = _public_value(result[key])
    steps: dict[str, Any] = {}
    for name, step in (result.get("steps") or {}).items():
        if not isinstance(step, dict):
            steps[name] = _public_value(step)
        elif "error" in step:
            steps[name] = {"error": _public_value(step["error"])}
        elif step.get("skipped"):
            steps[name] = {"skipped": True,
                           "reason": step.get("reason")}
        else:
            steps[name] = {k: _public_value(v) for k, v in step.items()
                           if k in _PUBLIC_KEYS}
    out["steps"] = steps
    return out


__all__ = [
    "DEFAULT_OWNER", "TICK_META_KEY", "STATUS_META_KEY",
    "OperatorEmailExecutor",
]
