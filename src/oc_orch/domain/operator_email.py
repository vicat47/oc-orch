"""独立邮件提醒：纯规则域（operator-email-alerts Requirement 1–3 设计 D7/D8）。

零 I/O、零台账写、绝不调用模型。本模块只推导决策与内容；持久化、网络与回执
由 application/adapters 落地。安全不变量（对应场景 M01–M08、M12）：

- 默认关闭：``EmailPolicy`` 出厂即 ``enabled=False``、空范围；即使
  ``enabled=True``，只要收件/发件/项目/事件范围/无人值守任一缺失，`policy_state`
  仍返回 ``missing-scope``，不发送。历史个人邮箱不会自动形成授权（M01/M02）。
- 授权是精确集合匹配：project、event kind、recipient 引用必须**显式在范围内**，
  不能靠任意 ``bool`` 绕过缺少的范围；越界即 ``pending-auth``/不发送（M02/M04）。
- 事件身份稳定：``stable_event_key`` 只由 project、incident、kind、condition
  version、recipient 范围决定，扫描/轮询时间不生成新身份（M06）。
- 事件选择：首次人工责任才建第一封；正常暂停与未知条件不催；condition
  version 实质变化或达到升级点才建新事件；恢复关联原问题且至多一次，没有
  前置“可能已提交”的提醒则不发送孤立恢复（M05/M06/M07）。
- 内容最小化：``digest`` 对**所有**身份/自由字段统一脱敏（红色凭据形态、
  控制字符/换行替换、截断），subject 确定性携带完整事件 ID，不含完整
  聊天/原始台账/交付正文/收件地址/凭据；输出确定、可哈希（M08）。
- 配置冻结：``identity_fingerprint`` 对收件/发件绑定生成稳定摘要；outbox 行
  冻结该摘要，配置变化（地址/发件身份/ref）不能证明旧 unknown 回执（M10/M12）。
- 严格类型：``IncidentFact.from_dict`` 拒绝 bool 版号/``'false'`` 之类隐式
  转换；任何坏字段一律 invalid，不填默认放行（M08）。
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone

# 有效时钟边界（毫秒）：拒绝荒谬时间，避免天文 deadline（与 outbox 同界）。
MAX_CLOCK_MS = 8 * 10 ** 12

# ---- 条件类别（事件选择的输入语义） ---------------------------------------
CONDITION_HUMAN = "human-responsibility"  # 明确人工责任，属授权事件范围
CONDITION_PAUSE = "normal-pause"          # 正常暂停：不催
CONDITION_UNKNOWN = "unknown"             # 缺证据：fail-closed，不猜
CONDITION_KINDS = frozenset({CONDITION_HUMAN, CONDITION_PAUSE, CONDITION_UNKNOWN})

# ---- 事件类型（稳定身份维度之一，也是授权范围维度） -------------------------
KIND_FIRST = "first"          # 首次需要人工介入
KIND_UPDATE = "update"        # 条件版本实质变化
KIND_ESCALATION = "escalation"  # 达到已批准升级点
KIND_RECOVERY = "recovery"    # 已恢复（关联原问题）
EVENT_KINDS = frozenset({KIND_FIRST, KIND_UPDATE, KIND_ESCALATION, KIND_RECOVERY})

# 除 recovery 外的“提醒”事件种类；恢复抑制/去重只按 incident 的提醒历史。
REMINDER_KINDS = frozenset({KIND_FIRST, KIND_UPDATE, KIND_ESCALATION})

# ---- 策略状态 --------------------------------------------------------------
POLICY_DISABLED = "disabled"        # 明确未启用
POLICY_MISSING_SCOPE = "missing-scope"  # 已启用但范围缺失/不完整 → 不发送
POLICY_ENABLED = "enabled"          # 所有范围字段显式齐全且 unattended=True

# ---- outbox 行状态（可见的通道状态；受理≠送达/已读） ------------------------
STATUS_PENDING_AUTH = "pending-auth"      # 待授权（已登记、不可发）
STATUS_PENDING = "pending"                # 待发
STATUS_CLAIMED = "claimed"                # 已认领/在途（网络前已持久 possible）
STATUS_SUBMIT_UNKNOWN = "submit-unknown"  # 已可能提交，结果未知（不盲重发）
STATUS_ACCEPTED = "accepted"              # 服务已受理（queued 或唯一回执，非送达）
STATUS_RETRY_WAIT = "retry-wait"          # 已证未提交瞬态，退避等待
STATUS_RATE_LIMITED = "rate-limited"      # 限频等待（可信 retry-after）
STATUS_AUTH_BLOCKED = "auth-blocked"      # 授权失效/凭据失效，不再自动重试
STATUS_CHANNEL_BLOCKED = "channel-blocked"  # 传输依赖/配置损坏，需修复
STATUS_PERMANENT_FAILED = "permanent-failed"  # 永久失败，保留失败与人工动作
STATUS_EXPIRED_SUPPRESSED = "expired-suppressed"  # 首封过期/恢复前未可能提交

# 可认领（网络前）状态；claimed/submit-unknown/accepted 一律不重复认领。
CLAIMABLE_STATUSES = frozenset({STATUS_PENDING, STATUS_RETRY_WAIT,
                                STATUS_RATE_LIMITED})
# “可能已提交”证据集：恢复/未知处理把它们当作可能已发出，不撤回、不重发。
POSSIBLY_SUBMITTED_STATUSES = frozenset({STATUS_CLAIMED, STATUS_SUBMIT_UNKNOWN,
                                         STATUS_ACCEPTED})
# 终态集合（不再进入发送调度）。
TERMINAL_STATUSES = frozenset({STATUS_AUTH_BLOCKED, STATUS_CHANNEL_BLOCKED,
                               STATUS_PERMANENT_FAILED,
                               STATUS_EXPIRED_SUPPRESSED})

# ---- 摘要内容约束（M08） ---------------------------------------------------
MAX_SUBJECT_LEN = 320
MAX_FIELD_LEN = 200
MAX_TRIED = 4
MAX_ENTRY_POINTS = 6

# 内容白名单：digest 只允许这些键，未知键一律丢弃（fail-closed）。
ALLOWED_DIGEST_KEYS = frozenset({
    "event_id", "project", "incident_id", "incident_type", "condition_key",
    "condition_version", "kind", "priority", "blocked_reason", "impact",
    "tried", "decision_needed", "entry_points", "first_seen_at",
    "observed_at", "recipient_ref", "sender_ref", "escalation_level",
    "recovered", "recovered_evidence", "next_action",
})

_SENSITIVE_KEY_HINTS = ("token", "secret", "password", "credential", "api_key",
                        "apikey", "authorization", "bearer")
_CREDENTIAL_PATTERNS = (
    re.compile(r"sk-[A-Za-z0-9_\-]{8,}"),
    re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*", re.DOTALL),
    re.compile(r"(?i)bearer\s+[a-z0-9._\-]{12,}"),
    re.compile(r"(?i)(password|secret|token)\s*[=:]\s*\S{8,}"),
)
# 控制字符/换行注入：任何身份/自由字段进入摘要前先替换为空格。
CONTROL_CHARACTER_RE = re.compile(r"[\x00-\x1f\x7f]")


# ---- 数据结构 --------------------------------------------------------------
@dataclass(frozen=True)
class Recipient:
    """收件人引用 + 实际地址。真实地址只存在于本机安全配置，不进公共仓库。

    收件范围去重与审计一律用 ``ref``；地址仅由发送时刻的授权策略解析，且
    outbox 行只冻结其 ``identity_fingerprint``（不存原文）。
    """
    ref: str
    address: str


@dataclass(frozen=True)
class Sender:
    """发件身份。对 Agently 而言对应其已配置的非交互账号。"""
    ref: str
    identity: str


@dataclass(frozen=True)
class EmailPolicy:
    """无人值守邮件授权策略；默认关闭、零范围。

    - ``enabled``/范围字段/unattended：**发送**能力；``receipt_queries_authorized``
      是**独立的"最小回执查询"授权**：无人值守发送不等于允许读回执，verify_receipt
      必须显式 True 才在授权范围内查询，拒绝时不耗预算；真实地址只存本机配置。
    """

    enabled: bool = False
    sender: Sender | None = None
    recipients: tuple[Recipient, ...] = ()
    projects: frozenset[str] = frozenset()
    event_kinds: frozenset[str] = frozenset()
    unattended: bool = False
    receipt_queries_authorized: bool = False
    # 发送调优（有界默认；enqueue 时冻结进 outbox 行，重启换配置不重置预算）
    retry_min_ms: int = 60_000
    retry_max_ms: int = 3_600_000
    max_attempts: int = 3
    send_timeout_seconds: float = 30.0
    search_timeout_seconds: float = 30.0
    receipt_check_limit: int = 3
    stale_claim_after_ms: int = 300_000

    def recipient(self, ref: str) -> Recipient | None:
        for r in self.recipients:
            if r.ref == ref:
                return r
        return None


def _strict_int(value, *, name: str) -> int:
    """严格整数：拒绝 bool（True 会隐式转 1）、拒绝任何非 int 类型。"""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} 必须为 int（非 bool），实际 {value!r}")
    return value


def _strict_bool(value, *, name: str) -> bool:
    """严格 bool：拒绝 'false'/'0' 之类隐式真值。"""
    if not isinstance(value, bool):
        raise ValueError(f"{name} 必须为 bool，实际 {value!r}")
    return value


@dataclass(frozen=True)
class IncidentFact:
    """结构化 incident 事实（来自台账的确定性摘要源，不含凭据/正文）。

    只接受有限字段；``from_dict`` 负责未知键丢弃与严格类型校验（fail-closed）。
    """
    incident_id: str
    project: str
    incident_type: str
    condition_key: str
    condition_version: int
    recipient_ref: str
    condition_kind: str
    priority: str = "normal"
    blocked_reason: str = ""
    impact: str = ""
    tried: tuple[str, ...] = ()
    decision_needed: str = ""
    entry_points: tuple[tuple[str, str], ...] = ()
    first_seen_at: int = 0
    observed_at: int = 0
    recovered: bool = False
    recovered_evidence: str = ""
    escalation_level: int = 0

    @classmethod
    def from_dict(cls, data: dict) -> "IncidentFact":
        if not isinstance(data, dict):
            raise ValueError("incident 必须为 dict")
        condition_version = _strict_int(data.get("condition_version", 0),
                                        name="condition_version")
        escalation_level = _strict_int(data.get("escalation_level", 0),
                                       name="escalation_level")
        first_seen_at = _strict_int(data.get("first_seen_at", 0),
                                    name="first_seen_at")
        observed_at = _strict_int(data.get("observed_at", 0), name="observed_at")
        recovered = _strict_bool(data.get("recovered", False), name="recovered")
        fact = cls(
            incident_id=str(data.get("incident_id") or "").strip(),
            project=str(data.get("project") or "").strip(),
            incident_type=str(data.get("incident_type") or "").strip(),
            condition_key=str(data.get("condition_key") or "").strip(),
            condition_version=condition_version,
            recipient_ref=str(data.get("recipient_ref") or "").strip(),
            condition_kind=str(data.get("condition_kind")
                              or CONDITION_UNKNOWN).strip(),
            priority=str(data.get("priority") or "normal").strip(),
            blocked_reason=str(data.get("blocked_reason") or "")[:MAX_FIELD_LEN],
            impact=str(data.get("impact") or "")[:MAX_FIELD_LEN],
            tried=tuple(str(x)[:MAX_FIELD_LEN] for x in
                        (data.get("tried") or [])[:MAX_TRIED]),
            decision_needed=str(data.get("decision_needed")
                                or data.get("next_action")
                                or "")[:MAX_FIELD_LEN],
            entry_points=tuple(
                (str(label)[:80], str(value)[:200])
                for label, value in (data.get("entry_points") or [])[:MAX_ENTRY_POINTS]),
            first_seen_at=first_seen_at,
            observed_at=observed_at,
            recovered=recovered,
            recovered_evidence=str(data.get("recovered_evidence")
                                   or "")[:MAX_FIELD_LEN],
            escalation_level=escalation_level,
        )
        ok, reason = validate_fact(fact)
        if not ok:
            raise ValueError(reason)
        return fact


# ---- 策略/授权 -------------------------------------------------------------
def policy_state(policy: EmailPolicy) -> str:
    """策略生效状态：缺任何显式范围/无人值守授权 → missing-scope/disabled。

    ``enabled`` 本身不是授权：即使为 True，只要范围不全，结果仍是
    missing-scope，绝不允许发送。
    """
    if not isinstance(policy, EmailPolicy) or policy.enabled is not True:
        return POLICY_DISABLED
    if policy.sender is None or not policy.sender.ref or not policy.sender.identity:
        return POLICY_MISSING_SCOPE
    if not policy.recipients:
        return POLICY_MISSING_SCOPE
    if any(not r.ref or not r.address for r in policy.recipients):
        return POLICY_MISSING_SCOPE
    if not policy.projects:
        return POLICY_MISSING_SCOPE
    if not policy.event_kinds:
        return POLICY_MISSING_SCOPE
    if policy.unattended is not True:
        return POLICY_MISSING_SCOPE
    return POLICY_ENABLED


def authorize(policy: EmailPolicy, *, project: str | None = None,
              kind: str | None = None,
              recipient_ref: str | None = None) -> dict:
    """授权矩阵：只有策略 enabled 且 project/kind/recipient 都在显式范围内才放行。

    返回 ``{"allowed": bool, "reason": str, "state": str}``。越界/缺范围一律
    denied，不因为任意 ``bool`` 或历史邮箱自动推定授权。
    """
    state = policy_state(policy)
    if state != POLICY_ENABLED:
        return {"allowed": False, "reason": state, "state": state}
    if project is not None and project not in policy.projects:
        project_label = project or ""
        return {"allowed": False, "reason": "project-out-of-scope",
                "detail": project_label, "state": state}
    if kind is not None and kind not in policy.event_kinds:
        kind_label = kind or ""
        return {"allowed": False, "reason": "kind-out-of-scope",
                "detail": kind_label, "state": state}
    if recipient_ref is not None and policy.recipient(recipient_ref) is None:
        recipient_label = recipient_ref or ""
        return {"allowed": False, "reason": "recipient-out-of-scope",
                "detail": recipient_label, "state": state}
    return {"allowed": True, "reason": "authorized", "state": state}


def query_authorized(policy: EmailPolicy) -> bool:
    """最小回执查询授权：与发送授权（enabled/范围/unattended）**独立**。

    只有显式 ``receipt_queries_authorized is True`` 才允许在授权范围内查询最小
    发送回执；缺省 False（无人值守发送不等于授权读回执）。返回 False 时调用方
    必须拒绝查询且**不消耗**回执预算。
    """
    return isinstance(policy, EmailPolicy) and policy.receipt_queries_authorized is True


# ---- incident 校验 ---------------------------------------------------------
def validate_fact(fact: IncidentFact) -> tuple[bool, str]:
    """结构化事实的前置校验；任何坏字段 → invalid（不写任何行）。"""
    if not isinstance(fact, IncidentFact):
        return False, "invalid-incident-shape"
    if not fact.incident_id or not fact.project:
        return False, "missing-incident-identity"
    if not fact.incident_type:
        return False, "missing-incident-type"
    if not fact.condition_key:
        return False, "missing-condition-key"
    if isinstance(fact.condition_version, bool) \
            or not isinstance(fact.condition_version, int) \
            or fact.condition_version < 1:
        return False, "invalid-condition-version"
    if not fact.recipient_ref:
        return False, "missing-recipient-scope"
    if fact.condition_kind not in CONDITION_KINDS:
        return False, "invalid-condition-kind"
    if fact.priority not in ("low", "normal", "high"):
        return False, "invalid-priority"
    if isinstance(fact.escalation_level, bool) \
            or not isinstance(fact.escalation_level, int) \
            or fact.escalation_level < 0:
        return False, "invalid-escalation-level"
    if not isinstance(fact.recovered, bool):
        return False, "invalid-recovered"
    for label, value in fact.entry_points:
        if not label or not value:
            return False, "invalid-entry-point"
    if isinstance(fact.first_seen_at, bool) \
            or not isinstance(fact.first_seen_at, int) \
            or fact.first_seen_at < 0:
        return False, "invalid-timestamp"
    if isinstance(fact.observed_at, bool) \
            or not isinstance(fact.observed_at, int) \
            or fact.observed_at < 0:
        return False, "invalid-timestamp"
    return True, "valid"


# ---- 稳定身份（M06：轮询/扫描时间不生成事件身份） --------------------------
def stable_base_key(project: str, incident_type: str, incident_id: str,
                    recipient_ref: str) -> str:
    """同一 incident（不随 kind/condition version 变化）的基础身份。"""
    raw = "\x1f".join((project or "", incident_type or "", incident_id or "",
                       recipient_ref or ""))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def stable_event_key(project: str, incident_type: str, incident_id: str,
                     kind: str, condition_version: int,
                     recipient_ref: str) -> str:
    """事件身份：project + incident + kind + condition version + 收件范围。"""
    raw = "\x1f".join((project or "", incident_type or "", incident_id or "",
                       kind or "", str(int(condition_version)),
                       recipient_ref or ""))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def identity_fingerprint(kind: str, ref: str, value: str) -> str:
    """收件/发件绑定的确定性冻结摘要（不存敏感地址原文）。

    outbox 行冻结该摘要；配置变化（地址/发件身份/ref 变化）会导致运行时
    指纹一致，从而拒绝用新配置证明旧 unknown 回执或发送（M10/M12）。
    """
    raw = "\x1f".join((kind or "", ref or "", value or ""))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


# ---- 事件选择（M05/M06/M07） ----------------------------------------------
def decide_event(fact: IncidentFact, *, rows: list[dict]) -> dict:
    """由 incident 事实与已有 outbox 行推导应建立的事件。

    ``rows`` = 同一 base_key 的既有行（含状态/condition_version/kind）。

    返回：
      event: None | KIND_*（None 表示不投递新邮件）
      reason: 稳定原因码
      expire_unsubmitted: 是否先抑制“尚未可能提交”的首封/提醒
      prior_possible: 是否已有前置提醒被标记为可能已提交
    """
    if not isinstance(rows, list):
        rows = []
    reminders = [r for r in rows if r.get("kind") in REMINDER_KINDS]
    prior_possible = any(
        r.get("possible_submitted_at") is not None
        or r.get("status") in POSSIBLY_SUBMITTED_STATUSES
        for r in reminders)
    max_version = max((int(r.get("condition_version") or 0)
                       for r in reminders), default=0)
    max_escalation = max((int(r.get("escalation_level") or 0)
                          for r in reminders), default=0)
    has_recovery = any(r.get("kind") == KIND_RECOVERY for r in rows)

    if fact.recovered:
        if not prior_possible:
            return {"event": None,
                    "reason": "recovered-before-possible-submit",
                    "expire_unsubmitted": True, "prior_possible": False}
        if has_recovery:
            return {"event": None, "reason": "duplicate-recovery",
                    "expire_unsubmitted": False, "prior_possible": True}
        return {"event": KIND_RECOVERY, "reason": "recovered",
                "expire_unsubmitted": False, "prior_possible": True}

    if fact.condition_kind == CONDITION_PAUSE:
        return {"event": None, "reason": "normal-pause-no-nag",
                "expire_unsubmitted": False, "prior_possible": prior_possible}
    if fact.condition_kind == CONDITION_UNKNOWN:
        return {"event": None, "reason": "condition-unknown",
                "expire_unsubmitted": False, "prior_possible": prior_possible}
    if fact.condition_kind != CONDITION_HUMAN:
        return {"event": None, "reason": "condition-unknown",
                "expire_unsubmitted": False, "prior_possible": prior_possible}

    if not reminders:
        return {"event": KIND_FIRST, "reason": "first-human-action",
                "expire_unsubmitted": False, "prior_possible": False}
    if fact.condition_version > max_version:
        return {"event": KIND_UPDATE, "reason": "condition-changed",
                "expire_unsubmitted": False, "prior_possible": prior_possible}
    if fact.escalation_level > max_escalation:
        return {"event": KIND_ESCALATION, "reason": "escalation-point",
                "expire_unsubmitted": False, "prior_possible": prior_possible}
    return {"event": None, "reason": "duplicate-observation",
            "expire_unsubmitted": False, "prior_possible": prior_possible}


# ---- 摘要内容（M08，确定性 + 脱敏） ----------------------------------------
def redact_text(text: str, *, limit: int = MAX_FIELD_LEN) -> str:
    """逐字段脱敏：控制字符/换行替换 → 清除凭据形态 → 截断。确定性。"""
    value = str(text or "")
    value = CONTROL_CHARACTER_RE.sub(" ", value)
    value = value[: limit * 3]
    for pattern in _CREDENTIAL_PATTERNS:
        value = pattern.sub("<redacted>", value)
    return value[:limit]


def redact_fields(data: dict, *, allowed: frozenset[str] = ALLOWED_DIGEST_KEYS
                  ) -> dict:
    """白名单过滤字段（fail-closed）：非白名单键、敏感键名一律丢弃。"""
    out: dict = {}
    if not isinstance(data, dict):
        return out
    for key, value in data.items():
        if not isinstance(key, str) or key not in allowed:
            continue
        lowered = key.lower()
        if any(hint in lowered for hint in _SENSITIVE_KEY_HINTS):
            continue
        out[key] = value
    return out


_KIND_LABELS = {
    KIND_FIRST: "需要人工",
    KIND_UPDATE: "条件变化",
    KIND_ESCALATION: "已升级",
    KIND_RECOVERY: "已恢复",
}


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc).isoformat()


def digest(fact: IncidentFact, *, event_id: str, kind: str | None = None) -> dict:
    """从结构化事实确定性组装发件内容（subject/body）+ 内容哈希。

    内容只含白名单脱敏字段与换行清洗；**subject 确定性携带完整事件 ID**
    （防同 incident 的 update/recovery 主题碰撞）；不含原始台账/聊天/凭据/
    收件地址。所有身份字段（project/incident/condition/收件范围）与自由文本
    统一过 ``redact_text``。
    ``kind`` 缺省按恢复/人工推导（与服务层事件选择保持一致即可）。
    """
    if kind is None:
        kind = KIND_RECOVERY if fact.recovered else KIND_FIRST
    kind_label = _KIND_LABELS.get(kind, "提醒")
    project = redact_text(fact.project)
    incident_type = redact_text(fact.incident_type)
    incident_id = redact_text(fact.incident_id)
    subject = (f"[oc-orch:{project}] {kind_label}: "
               f"{incident_type}#{incident_id} [{event_id}]")[:MAX_SUBJECT_LEN]

    lines: list[str] = []
    lines.append(f"事件 ID: {event_id}")
    lines.append(f"项目: {project}")
    lines.append(f"事件: {incident_type} / {incident_id} ({kind})")
    lines.append(f"条件: {redact_text(fact.condition_key)} "
                 f"(v{fact.condition_version})")
    lines.append(f"优先级: {redact_text(fact.priority)}")
    if fact.recovered:
        lines.append("状态: 已恢复")
        if fact.recovered_evidence:
            lines.append(f"恢复证据: {redact_text(fact.recovered_evidence)}")
    else:
        lines.append("状态: 需要人工")
        if fact.blocked_reason:
            lines.append(f"阻塞原因: {redact_text(fact.blocked_reason)}")
        if fact.impact:
            lines.append(f"影响: {redact_text(fact.impact)}")
        if fact.tried:
            lines.append("已尝试: "
                         + "；".join(redact_text(t) for t in fact.tried))
        if fact.decision_needed:
            lines.append(f"所需决定/下一动作: {redact_text(fact.decision_needed)}")
    if fact.entry_points:
        lines.append("定位入口: "
                     + "；".join(f"{redact_text(l, limit=80)}={redact_text(v)}"
                                 for l, v in fact.entry_points))
    lines.append(f"收件范围: {redact_text(fact.recipient_ref)}")
    lines.append(f"首次观察: {_iso(fact.first_seen_at)}")
    lines.append(f"最近观察: {_iso(fact.observed_at)}")
    if fact.escalation_level:
        lines.append(f"升级级别: {fact.escalation_level}")
    body = "\n".join(lines)
    content_hash = hashlib.sha256(
        f"{subject}\x00{body}".encode("utf-8")).hexdigest()
    return {"subject": subject, "body": body, "content_hash": content_hash}


def backoff_ms(attempt: int, *, retry_min_ms: int, retry_max_ms: int) -> int:
    """已证未提交瞬态的指数退避（跨重启持久保存在行内，不重新累计预算）。"""
    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
        attempt = 1
    if retry_min_ms is None or retry_max_ms is None \
            or retry_min_ms < 1 or retry_max_ms < retry_min_ms:
        retry_min_ms, retry_max_ms = 60_000, 3_600_000
    return min(retry_max_ms, retry_min_ms * (2 ** (attempt - 1)))


# ---- 导出 ---------------------------------------------------------------
__all__ = [
    "MAX_CLOCK_MS",
    "CONDITION_HUMAN", "CONDITION_PAUSE", "CONDITION_UNKNOWN", "CONDITION_KINDS",
    "KIND_FIRST", "KIND_UPDATE", "KIND_ESCALATION", "KIND_RECOVERY",
    "EVENT_KINDS", "REMINDER_KINDS",
    "POLICY_DISABLED", "POLICY_MISSING_SCOPE", "POLICY_ENABLED",
    "STATUS_PENDING_AUTH", "STATUS_PENDING", "STATUS_CLAIMED",
    "STATUS_SUBMIT_UNKNOWN", "STATUS_ACCEPTED", "STATUS_RETRY_WAIT",
    "STATUS_RATE_LIMITED", "STATUS_AUTH_BLOCKED", "STATUS_CHANNEL_BLOCKED",
    "STATUS_PERMANENT_FAILED", "STATUS_EXPIRED_SUPPRESSED",
    "CLAIMABLE_STATUSES", "POSSIBLY_SUBMITTED_STATUSES", "TERMINAL_STATUSES",
    "Recipient", "Sender", "EmailPolicy", "IncidentFact",
    "policy_state", "authorize", "query_authorized", "validate_fact",
    "stable_base_key", "stable_event_key", "identity_fingerprint",
    "decide_event",
    "redact_text", "redact_fields", "digest", "backoff_ms",
]