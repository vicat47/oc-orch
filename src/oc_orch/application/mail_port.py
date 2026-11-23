"""独立邮件传输端口：窄 MailPort（operator-email-alerts Requirement 1/3，D7）。

默认关闭的策略由 domain/application 负责；本模块只声明传输侧的最小契约与
错误分类。设计要点（与 evidence/2026-11-14-coordinator-probe-recovery/
contracts.json 的已核实字段一致）：

- ``send`` 的唯一可信成功字段是 ``queued``（agently-cli 1.0.18 ``+send``
  ``--confirmed`` 成功只返回 ``queued=true``）。``queued`` 只表示“服务已受理”，
  不等于送达/已读；不做 exactly-once 承诺。
- ``search_receipt`` 只消费已核实的摘要字段（``message_id``、``from``、
  ``to``、``created_at``、``subject``、``snippet``、``is_read``）；不把模糊
  snippet 或无匹配当作“未发送”的证据。
- 超时/响应丢失/非零退出一律按**未知**（可能已提交）处理：由上层在授权
  范围内查询最小回执，绝不只凭非零退出立即重发（M10）。
- 错误分类明确隔离：未知、已证未提交瞬态、限频、授权、通道/依赖、永久。
  每个分类对应 outbox 的不同处置（M11）。

实现注意：发送与查询必须由**可注入 runner** 封装（测试使用 subprocess/fake
桩），且网络调用始终在事务外执行。
"""

from __future__ import annotations

from typing import Protocol


class MailError(RuntimeError):
    """邮件传输错误基类；``failure_class`` 供 outbox 分类。"""

    failure_class = "unknown"

    def __init__(self, message: str = "", *, detail: str = ""):
        super().__init__(message or self.__class__.__name__)
        self.detail = detail


class MailUnknownError(MailError):
    """结果未知：调用已发生/响应丢失/非零退出，**可能已提交**。

    必须先在授权范围内查询最小回执，唯一匹配才补记受理；无匹配或匹配歧义
    保持 unknown；**不得从此状态自动重发**（M10）。
    """

    failure_class = "unknown"


class MailTransientNotSubmittedError(MailError):
    """已证未提交的瞬态失败（例如本地拒绝连接/进程未发送前失败）。

    只有带明确“未提交”证据的分类才可进入有界退避重试（M11）。
    """

    failure_class = "transient"


class MailRateLimitError(MailError):
    """限频；携带可信重试时间 ``retry_after_ms``（服务明确给出时）。"""

    failure_class = "rate-limited"

    def __init__(self, message: str = "", *, retry_after_ms: int | None = None,
                 detail: str = ""):
        super().__init__(message, detail=detail)
        self.retry_after_ms = retry_after_ms


class MailAuthError(MailError):
    """授权/凭据失效（非交互配置缺失、凭据过期、权限被撤销）。

    不自动重试、不自动走交互登录；记录可见通道阻塞与修复动作（M04）。
    """

    failure_class = "auth"


class MailChannelError(MailError):
    """传输通道/部署依赖损坏（可执行文件缺失、配置无法解析等）。

    需要修复后才能恢复；不自动重试（M04）。
    """

    failure_class = "channel"


class MailPermanentError(MailError):
    """永久拒绝（请求明确无效/服务明确拒绝）。停止重试，保留可见失败。"""

    failure_class = "permanent"


class MailRequestError(MailPermanentError):
    """本地可检测的请求错误（空收件人/空主题/超长正文等）。

    确定性、重试无意义，在调用外部 CLI 前即拦截。
    """


class MailSearchUnavailableError(MailError):
    """回执查询不可用（传输错误/超时/非零退出）。

    与“查到 0 条”不同：查询失败不能得出“未发送”结论，保持原状态。
    """

    failure_class = "search-unavailable"


class MailTransport(Protocol):
    """窄 MailPort：发送 + 最小回执查询，均由可注入 runner 封装。"""

    def send(self, *, to: str, sender: str, subject: str, body: str,
             timeout_seconds: float) -> dict:
        """发送一封邮件。

        ``to``/``sender`` 为已解析地址（授权策略解析后的产物）。成功返回
        ``{"queued": True}``（服务已受理，非送达）。失败抛 :class:`MailError`
        子类，由调用方按分类处置。
        """
        ...

    def search_receipt(self, *, subject: str, to: str, after_ms: int,
                       before_ms: int, limit: int,
                       timeout_seconds: float) -> list[dict]:
        """按主题/收件人/时间窗查询最小发送回执。

        返回的每条仅含已核实字段：``message_id``、``from``（``email``/``name``）、
        ``to``、``created_at``、``subject``、``snippet``、``is_read``。
        空列表 = 查到无匹配；传输失败抛 :class:`MailSearchUnavailableError`。
        """
        ...


__all__ = [
    "MailError",
    "MailUnknownError",
    "MailTransientNotSubmittedError",
    "MailRateLimitError",
    "MailAuthError",
    "MailChannelError",
    "MailPermanentError",
    "MailRequestError",
    "MailSearchUnavailableError",
    "MailTransport",
]