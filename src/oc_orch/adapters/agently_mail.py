"""Agently CLI 邮件传输适配器（operator-email-alerts D7；窄 MailPort 实现）。

基于 evidence/2026-11-14-coordinator-probe-recovery/contracts.json 的**已核实**
契约（本机 agently-cli 1.0.18，只读核验，未发送/未登录/未改配置）：

- ``+send --confirmed``：成功输出只有 ``queued`` 字段；``queued=true`` 仅表示
  服务已受理，**不等于送达/已读**；没有幂等发送键/可靠回执。本适配器只把
  ``queued=true`` 报告为受理证据。
- ``+search``：summary 含 ``message_id``、``from``、``to``、``created_at``、
  ``subject``、``snippet``、``is_read``。回执判定只使用这些字段；模糊 snippet
  或无匹配都不能当作“未发送”证据。
- 非零退出/超时/响应丢失 → 一律 ``MailUnknownError``（可能已提交），绝不只凭
  非零退出立即重发（M10）；``confirmation_required``（交互式确认中间态）→
  ``MailAuthError``（非交互未授权），不自动登录。
- 本地可检的请求错误（空收件人/主题、超长正文）在调用 CLI 前即抛
  ``MailRequestError``（永久）；可执行文件缺失 → ``MailChannelError``。

**发件身份 fail-closed**（D7 / 父审查要求）：``+send`` 命令本身完全忽略
``sender`` 参数（发件账号来自 agently 已配置的非交互账号），因此**绝对不得把
任意策略 identity 当作真实发送账号**。只有装配侧注入已核实的
:class:`SenderBinding`（ref + identity + 已核实标记/版本）时，且调用传入的
``sender_ref``/``sender`` 与绑定逐字一致，才允许发送；否则一律
``MailAuthError``（fail-closed）。本模块不做登录/搜索真实邮箱/改配置/安装。

外部副作用经**可注入 runner** 封装：测试注入 subprocess/fake 桩，不触碰真实
邮箱/凭据；默认 runner 为 ``subprocess.run``。网络调用始终在事务外执行。
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from ..application.mail_port import (
    MailAuthError,
    MailChannelError,
    MailRequestError,
    MailSearchUnavailableError,
    MailUnknownError,
)

DEFAULT_SEND_COMMAND = ["agently-cli", "message", "+send"]
DEFAULT_SEARCH_COMMAND = ["agently-cli", "message", "+search"]
MAX_BODY_BYTES = 1_000_000  # +send 契约上限（UTF-8 1MB）
MAX_SUBJECT_CHARS = 4000    # +send 契约上限（4KB）


@dataclass(frozen=True)
class SenderBinding:
    """已核实的发件身份绑定（装配侧注入；缺省为 None）。

    - ``ref``：稳定引用（须与策略 sender.ref 一致）；
    - ``identity``：已核实的确切发件身份（须与策略 sender.identity 一致）；
    - ``verified``：是否完成非交互授权/版本预检（只读预检，本模块不登录）；
    - ``version``：已锁定的 CLI 版本（如 1.0.18），供审计/诊断。
    """

    ref: str
    identity: str
    verified: bool = True
    version: str | None = None


class RunnerResult:
    """runner 返回的最小结构化结果（对齐 subprocess.CompletedProcess 字段）。"""

    def __init__(self, returncode: int, stdout: str = "", stderr: str = ""):
        self.returncode = returncode
        self.stdout = stdout or ""
        self.stderr = stderr or ""


Runner = Callable[[list[str], float], RunnerResult]


def _default_runner(argv: list[str], timeout_seconds: float) -> RunnerResult:
    """真实 subprocess 封装；可执行文件缺失由调用方转 MailChannelError。"""
    proc = subprocess.run(argv, capture_output=True, text=True,
                          timeout=timeout_seconds)
    return RunnerResult(returncode=proc.returncode, stdout=proc.stdout,
                        stderr=proc.stderr)


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(int(ms) / 1000.0,
                                  tz=timezone.utc).isoformat(timespec="seconds")


def _parse_stdout(stdout: str) -> dict | None:
    """解析 CLI stdout JSON；坏 JSON 返回 None（由调用方按不确定处理）。"""
    try:
        data = json.loads(stdout or "")
    except (TypeError, ValueError):
        return None
    return data if isinstance(data, dict) else None


class AgentlyMailTransport:
    """Agently CLI 窄传输；仅依赖已核实字段，保持保守分类。

    ``sender_binding`` 未注入（None）时任何发送都被拒绝（fail-closed）；这防止
    把无证明的策略 identity 当真实发送账号。
    """

    def __init__(self, *, send_command: list[str] | None = None,
                 search_command: list[str] | None = None,
                 runner: Runner | None = None,
                 sender_binding: SenderBinding | None = None):
        self._send_command = list(send_command or DEFAULT_SEND_COMMAND)
        self._search_command = list(search_command or DEFAULT_SEARCH_COMMAND)
        self._runner = runner if runner is not None else _default_runner
        self._sender_binding = sender_binding

    def _check_sender_binding(self, *, sender_ref: str, sender: str) -> None:
        binding = self._sender_binding
        ok = (binding is not None
              and self._check_search_binding_impl()
              and binding.ref == sender_ref
              and binding.identity == sender)
        if not ok:
            raise MailAuthError(
                "sender-binding-unverified",
                detail="未注入或未匹配已核实的发件身份绑定：fail-closed 拒发")

    def _check_search_binding_impl(self) -> bool:
        """搜索也以绑定为准：无绑定/未严格 verified 的账号读取一律拒绝。"""
        binding = self._sender_binding
        return binding is not None and binding.verified is True

    def _check_search_binding(self) -> None:
        if not self._check_search_binding_impl():
            raise MailSearchUnavailableError(
                "sender-binding-unverified",
                detail="回执查询需要已核实的发件身份绑定：fail-closed")

    # ---- 发送 ----------------------------------------------------------
    def send(self, *, to: str, sender_ref: str, sender: str, subject: str,
             body: str, timeout_seconds: float) -> dict:
        """非交互发送：``--confirmed`` + 内联 ``--body``（纯文本）。

        本地校验失败 → ``MailRequestError``（永久）；未注入/不匹配的发件身份
        绑定 → ``MailAuthError``（fail-closed，不把任意 identity 当真账号）；
        超时/非零/无法证明 → ``MailUnknownError``（可能已提交，由上层查回执）；
        交互确认中间态 → ``MailAuthError``。成功受理只返回 ``{"queued": True}``。
        """
        if not isinstance(to, str) or not to.strip():
            raise MailRequestError("empty-recipient", detail="to 为空")
        if not isinstance(subject, str) or not subject.strip():
            raise MailRequestError("empty-subject", detail="subject 为空")
        if len(subject) > MAX_SUBJECT_CHARS:
            raise MailRequestError("subject-too-long", detail="subject 超过 4KB")
        if not isinstance(body, str) or not body.strip():
            raise MailRequestError("empty-body", detail="body 为空")
        if len(body.encode("utf-8")) > MAX_BODY_BYTES:
            raise MailRequestError("body-too-large", detail="body 超过 1MB")
        if not isinstance(sender_ref, str) or not isinstance(sender, str) \
                or not sender.strip():
            raise MailRequestError("invalid-sender-identity",
                                   detail="sender 缺失")
        # 发件身份绑定（唯一可信来源；CLI 本身忽略 sender 参数）
        self._check_sender_binding(sender_ref=sender_ref, sender=sender)

        argv = list(self._send_command) + [
            "--confirmed", "--to", to, "--subject", subject,
            "--body", body, "--body-format", "plain",
        ]
        try:
            result = self._runner(argv, float(timeout_seconds))
        except subprocess.TimeoutExpired as exc:
            raise MailUnknownError("send-timeout",
                                   detail="调用超时，服务可能已受理") from exc
        except FileNotFoundError as exc:
            raise MailChannelError(
                "agently-cli-missing",
                detail="可执行文件缺失，需修复部署依赖") from exc
        except OSError as exc:
            raise MailChannelError("spawn-failed", detail=str(exc)[:200]) from exc

        if result.returncode != 0:
            # 非零退出不能证明未提交：CLI 可能已受理而返回失败（M10）。
            raise MailUnknownError("cli-nonzero-exit",
                                   detail=str(result.stderr or "")[:200])
        data = _parse_stdout(result.stdout)
        if data is None:
            # exit 0 但输出不可解析：无法证明受理，保守按未知。
            raise MailUnknownError("unrecognized-output",
                                   detail="exit 0 但 stdout 非预期 JSON")
        if _confirmation_required(data):
            # 首次调用需要交互确认 → 非交互授权未配置；不自动登录。
            raise MailAuthError(
                "interactive-confirmation-required",
                detail="agently 需要交互确认，非交互授权未配置")
        if data.get("queued") is True:
            # 只证明“服务已受理”；送达/已读未知。
            return {"queued": True}
        raise MailUnknownError("unrecognized-success",
                               detail="exit 0 但缺少 queued=true 受理字段")

    # ---- 最小回执查询（M10） -------------------------------------------
    def search_receipt(self, *, subject: str, to: str, after_ms: int,
                       before_ms: int, limit: int,
                       timeout_seconds: float) -> list[dict]:
        """按主题/收件人/时间窗查**发件箱**（--dir sent）最少回执；只保留已核实字段。

        无绑定/未严格 verified 的账号读取一律拒绝（MailSearchUnavailableError，
        不发外部 CLI）；``--dir sent`` 保证不把收件箱同主题邮件当作自身回执。
        """
        self._check_search_binding()
        argv = list(self._search_command) + [
            "--q", subject, "--search-in", "SEARCH_IN_SUBJECT",
            "--to", to, "--after", _iso(after_ms), "--before", _iso(before_ms),
            "--dir", "sent",
            "--limit", str(int(limit) if limit else 10),
        ]
        try:
            result = self._runner(argv, float(timeout_seconds))
        except subprocess.TimeoutExpired as exc:
            raise MailSearchUnavailableError("search-timeout") from exc
        except FileNotFoundError as exc:
            raise MailSearchUnavailableError("agently-cli-missing") from exc
        except OSError as exc:
            raise MailSearchUnavailableError("spawn-failed",
                                             detail=str(exc)[:200]) from exc
        if result.returncode != 0:
            raise MailSearchUnavailableError("search-nonzero-exit",
                                             detail=str(result.stderr or "")[:200])
        data = _parse_stdout(result.stdout)
        if data is None or data.get("ok") is not True:
            raise MailSearchUnavailableError("search-unavailable-output")
        body = data.get("data")
        if not isinstance(body, dict) or not isinstance(body.get("data"), list):
            raise MailSearchUnavailableError("search-invalid-shape")
        pagination = body.get("pagination")
        if (not isinstance(pagination, dict) or pagination.get("has_more") is not False):
            # 单页一个匹配不证明全窗口唯一；截断/缺覆盖不能升级原unknown。
            raise MailSearchUnavailableError("search-coverage-incomplete")
        raw_items = body["data"]
        return [_verified_summary(item) for item in raw_items]


def _confirmation_required(payload: dict) -> bool:
    """识别交互确认中间态：contracts.json 原文为嵌套 ``data.confirmation_required``，
    并兼容历史 top-level ``confirmation_required`` 形状。"""
    if payload.get("confirmation_required") is True:
        return True
    inner = payload.get("data")
    if isinstance(inner, dict) and inner.get("confirmation_required") is True:
        return True
    return False


def _verified_summary(item: Any) -> dict:
    """只保留 +search schema 已核实字段；其余一律丢弃。"""
    if not isinstance(item, dict):
        return {}
    out: dict = {}
    for key in ("message_id", "created_at", "subject", "snippet"):
        if isinstance(item.get(key), str):
            out[key] = item[key]
    if isinstance(item.get("is_read"), bool):
        out["is_read"] = item["is_read"]
    raw_from = item.get("from")
    if isinstance(raw_from, dict):
        out["from"] = {k: v for k, v in raw_from.items()
                       if k in ("name", "email") and isinstance(v, str)}
    raw_to = item.get("to")
    if isinstance(raw_to, list):
        out["to"] = [{k: v for k, v in r.items()
                      if k in ("name", "email") and isinstance(v, str)}
                     for r in raw_to if isinstance(r, dict)]
    return out


__all__ = [
    "DEFAULT_SEND_COMMAND", "DEFAULT_SEARCH_COMMAND",
    "SenderBinding", "Runner", "RunnerResult", "AgentlyMailTransport",
]
