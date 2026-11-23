"""组合根：装配 SQLite、OpenCode 与文件证据，连接由调用方作用域持有。"""

from contextlib import contextmanager

from .application.services import Orchestration
from .adapters.clock import SystemClock
from .adapters.opencode import Client, load_service
from .adapters.config import load_config
from .adapters.sqlite_db import connect, init
from .adapters.sqlite_ledger import Ledger, deliverable_kind
from .adapters.input_ledger import (
    ADMISSION_PREPARED,
    ADMISSION_POSSIBLE,
    ADMISSION_ADMITTED,
    ADMISSION_DELIVERED,
    ADMISSION_UNKNOWN,
    ADMISSION_CANCELLED,
    ADMISSION_RELEASED,
)
from .adapters.target_gate import PROBE_FAILURE_KEY_PREFIX, TargetGateStateError
from .adapters.results import collect_attempt, verify_attempt
from .util import ensure_home, short_id


def make_target_gate(conn, clock):
    """运行时共享同一台账连接与时钟；闸门不执行网络请求。"""
    from .adapters.target_gate import TargetGate

    return TargetGate(conn, clock)


def make_wake_admission(ledger, target_gate):
    """项目/回合控制、provider探测与原输入possible标记共享短事务。"""
    from .adapters.wake_admission import WakeAdmission

    return WakeAdmission(ledger, target_gate)


def make_coordinator_control(conn, clock=None):
    """独立项目意图/回合屏障；入口与运行时共用连接、时钟和应用规则。

    不推断真人来源、不访问服务，也不清除 provider gate。明确的本地控制
    操作由共享用例校验；底座模块惰性加载，避免无关只读入口触发装配。
    """
    from .adapters.coordinator_control import CoordinatorControlLedger
    from .application.coordinator_control import CoordinatorControlService

    actual_clock = clock or SystemClock()
    return CoordinatorControlService(
        CoordinatorControlLedger(conn, actual_clock), actual_clock)


def make_project_control_view(conn, clock=None):
    """精确作用域控制/历史回执，同一WAL只读快照，无Client。"""
    from .application.project_control_view import ProjectControlView
    from .adapters.release_identity import runtime_identity
    from .adapters.sqlite_db import read_snapshot

    return ProjectControlView(make_coordinator_control(conn, clock),
                              read_snapshot=lambda: read_snapshot(conn),
                              identity=lambda: runtime_identity(conn))


def make_delivery_policy(ledger, clock=None):
    """显式策略与只读覆盖；仅独立delivery写契约已安装时启用运行守卫。"""
    from .adapters.delivery_policy import DeliveryPolicyStore
    from .adapters.delivery_policy_view import DeliveryPolicyViews
    from .application.delivery_policy import DeliveryPolicyService

    actual_clock = clock or ledger.clock or SystemClock()
    return DeliveryPolicyService(DeliveryPolicyStore(ledger.conn, actual_clock),
                                 views=DeliveryPolicyViews(ledger, actual_clock))


def make_prototype(ledger, clock=None):
    from .adapters.prototype import PrototypeStore

    return PrototypeStore(ledger, clock or ledger.clock or SystemClock())


def make_turn_observation(conn, clock):
    from .adapters.coordinator_observation import CoordinatorTurnObservation

    return CoordinatorTurnObservation(conn, clock)


def make_registered_turn_observer(ledger, *, client, clock=None, window=None):
    from .application.registered_turn import RegisteredTurnObserver
    from .adapters.results import read_message_window
    from .adapters.operator_turn import OperatorTurnStore

    actual = clock or ledger.clock or SystemClock()
    return RegisteredTurnObserver(ledger, make_coordinator_control(ledger.conn, actual),
        make_turn_observation(ledger.conn, actual), actual, client=client,
        window=window or (lambda sid, **kw: read_message_window(client(), sid, max_pages=20, page_size=50, **kw)),
        finish=OperatorTurnStore(ledger, actual).finish_registered)


def read_session_window(client, session_id, **kw):
    """entrypoints 使用的有界消息窗口读取；经 composition 隔离 adapters 依赖。"""
    from .adapters.results import read_message_window

    return read_message_window(client, session_id, **kw)


def make_operator_turn(ledger, *, client, clock=None):
    from .application.operator_turn import OperatorTurnService
    from .adapters.operator_turn import OperatorTurnStore

    actual = clock or ledger.clock or SystemClock()
    return OperatorTurnService(OperatorTurnStore(ledger, actual),
        make_registered_turn_observer(ledger, client=client, clock=actual), client=client, clock=actual)


def make_resource_authorization(ledger, clock=None):
    from .adapters.resource_authorization import ResourceAuthorizationStore
    from .application.resource_authorization import ResourceAuthorizationService

    # 底座无Client/网络；授权是独立版本事实，不从模式/模型目录自动补造。
    return ResourceAuthorizationService(ResourceAuthorizationStore(
        ledger.conn, clock or ledger.clock or SystemClock()))


def make_dependency_wait(ledger, clock=None, *, store=None):
    from .application.dependency_wait import DependencyWaitService

    return DependencyWaitService(
        ledger, clock or SystemClock(), store=store or make_responsibilities(ledger))


def make_continuity_status(ledger, clock=None):
    """无Client、无邮件执行器、默认不推进游标的共享业务健康用例。"""
    from .application.continuity_status import ContinuityStatusService
    from .adapters.release_identity import runtime_identity
    from .adapters.sqlite_db import read_snapshot

    actual_clock = clock or SystemClock()
    return ContinuityStatusService(
        make_dependency_wait(ledger, actual_clock),
        make_coordinator_control(ledger.conn, actual_clock),
        make_target_gate(ledger.conn, actual_clock), actual_clock,
        read_snapshot=lambda: read_snapshot(ledger.conn),
        identity=lambda: runtime_identity(ledger.conn))


def make_operator_email(conn, clock=None, *, policy=None, transport=None):
    """独立执行器的本地装配：默认关闭，不发现账号、不登录、不探测邮箱。

    真实通道须由可信装配层另行注入已批准策略与已验证传输；默认Agently
    没有sender binding，即使误传enabled策略也在调用CLI之前拒绝。
    """
    from .adapters.agently_mail import AgentlyMailTransport
    from .adapters.email_outbox import EmailOutbox
    from .application.operator_email import OperatorEmailService
    from .domain.operator_email import EmailPolicy
    from .adapters.operator_incident import IncidentStore

    actual_clock = clock or SystemClock()
    source_store = IncidentStore(conn, actual_clock,
                                 base=Ledger(conn, "", clock=actual_clock))
    return OperatorEmailService(
        EmailOutbox(conn, actual_clock, source_validator=source_store.make_source_validator(),
                    require_source_binding=True),
        transport if transport is not None else AgentlyMailTransport(), actual_clock,
        policy if policy is not None else EmailPolicy())


def inspect_email_config(home):
    """只读检查本机邮件配置；错误返回结构化，不抛出到 entrypoints。"""
    from .adapters.email_config import EmailConfigError, load_email_runtime

    try:
        runtime = load_email_runtime(home)
    except EmailConfigError as exc:
        return {"ok": False, "error": str(exc), "runtime": None}
    return {"ok": True, "error": None, "runtime": runtime}


def make_email_executor(home, clock=None):
    """按本机 email-policy.json 装配独立邮件 executor；关闭/坏配置不发送。"""
    from .adapters.email_config import EmailConfigError, load_email_runtime
    from .adapters.agently_mail import AgentlyMailTransport
    from .runtime.email_executor import OperatorEmailExecutor

    runtime = load_email_runtime(home)
    transport = None
    policy = None
    if runtime.enabled:
        policy = runtime.policy
        transport = AgentlyMailTransport(sender_binding=runtime.sender_binding)
    return OperatorEmailExecutor(home, clock=clock, policy=policy,
                                 transport=transport), runtime


def make_configured_operator_incident(ledger, *, home, clock=None):
    """带显式邮件策略的 incident 服务；配置不可用时回退为本地待办（不发送）。"""
    from .adapters.email_config import EmailConfigError, load_email_runtime
    from .adapters.agently_mail import AgentlyMailTransport

    try:
        runtime = load_email_runtime(home)
    except EmailConfigError:
        runtime = None
    if runtime is not None and runtime.enabled:
        return make_operator_incident(
            ledger, clock, policy=runtime.policy,
            transport=AgentlyMailTransport(sender_binding=runtime.sender_binding),
            recipient_refs=runtime.recipient_refs)
    return make_operator_incident(ledger, clock)


def make_operator_incident(ledger, clock=None, *, policy=None, transport=None,
                           recipient_refs=()):
    """默认只有本地人工待办；显式范围/策略可登记outbox，但此用例从不发送。"""
    from .adapters.operator_incident import IncidentStore
    from .application.operator_incident import OperatorIncidentService

    actual_clock = clock or SystemClock()
    return OperatorIncidentService(
        IncidentStore(ledger.conn, actual_clock, base=ledger), actual_clock,
        email=(make_operator_email(ledger.conn, actual_clock, policy=policy,
                                   transport=transport) if recipient_refs else None),
        recipient_refs=recipient_refs)


def make_failover_email(conn, *, clock=None, policy=None, transport=None):
    """提供商故障知会：FailoverLedger validator + 独立 outbox + 复用发送/回执。

    - outbox 源校验用 failover 事实 validator（N08：事实知会不因换代失效）；
    - 发送/回执/过期扫描完全复用 ``OperatorEmailService``（dispatch/verify/claim）；
    - policy 缺省 disabled（fail-closed）；显式传入或调用方从 email-policy.json 加载。
    """
    from .adapters.agently_mail import AgentlyMailTransport
    from .adapters.email_outbox import EmailOutbox
    from .adapters.failover_ledger import FailoverLedger
    from .application.failover_email import FailoverEmailService
    from .application.operator_email import OperatorEmailService
    from .domain.operator_email import EmailPolicy

    actual_clock = clock or SystemClock()
    failover = FailoverLedger(conn, clock=actual_clock.now_ms)
    outbox = EmailOutbox(
        conn, actual_clock,
        source_validator=failover.make_source_validator(),
        require_source_binding=True)
    email = OperatorEmailService(
        outbox,
        transport if transport is not None else AgentlyMailTransport(),
        actual_clock,
        policy if policy is not None else EmailPolicy())
    return FailoverEmailService(email, actual_clock)


def make_configured_failover_email(conn, home, *, clock=None):
    """按本机邮件策略装配故障知会；未启用时不创建传输器。"""
    from .adapters.email_config import load_email_runtime

    runtime = load_email_runtime(home)
    if not runtime.enabled:
        return None

    from .adapters.agently_mail import AgentlyMailTransport

    return make_failover_email(
        conn, clock=clock, policy=runtime.policy,
        transport=AgentlyMailTransport(sender_binding=runtime.sender_binding))


def make_failover_snapshot(conn, *, task_id=None, project=None, clock=None):
    """组合只读状态查询使用的持久端口；应用层只接触结构化读接口。"""
    from .adapters.email_outbox import EmailOutbox
    from .adapters.failover_ledger import FailoverLedger
    from .application.failover_status import failover_snapshot

    actual_clock = clock or SystemClock()
    failover = FailoverLedger(conn, clock=actual_clock.now_ms)
    outbox = EmailOutbox(
        conn, actual_clock, source_validator=failover.make_source_validator(),
        require_source_binding=True)
    return failover_snapshot(failover, outbox, task_id=task_id, project=project)


def make_failover_control(conn, ledger, *, clock=None):
    """显式的 operator 用例，屏蔽 CLI 对存储和契约安装适配器的依赖。"""
    from .adapters.failover_guard import install_failover_guard
    from .adapters.failover_ledger import FailoverLedger
    from .application.failover_control import FailoverControl

    actual_clock = clock or SystemClock()
    store = FailoverLedger(conn, clock=actual_clock.now_ms, audit=ledger.audit)
    return FailoverControl(
        store,
        install_guard=lambda actor: install_failover_guard(conn, actor=actor),
        task_project=lambda task_id: ledger.get_task(task_id).get("project"))


def make_responsibilities(ledger):
    """S4 后续责任存储：与 Ledger 共用连接/时钟，不自动 DDL（schema v3）。"""
    from .adapters.responsibility_ledger import ResponsibilityStore

    return ResponsibilityStore(ledger)


def make_fault_recovery(conn, home, *, client, clock=None):
    """提供商故障恢复用例：FailoverLedger + 资源授权 + 共享派单的编排装配。

    - ``failover``：meta 命名空间持久层（episode/operation/策略/路线阻断）。
    - ``dispatch``：复用共享 Orchestration.dispatch 做切换确认后的后继 Attempt。
    - ``authorize``：包装独立资源授权；拒绝/未配置一律保守 `allowed=False`
      （F08：工作模式与价格目录不产生授权，无授权不自动切换）。
    """
    from .adapters.failover_ledger import FailoverLedger
    from .application.fault_recovery import FaultRecoveryService

    actual_clock = clock or SystemClock()
    ledger = Ledger(conn, home, clock=actual_clock)
    failover = FailoverLedger(conn, clock=actual_clock.now_ms,
                              audit=ledger.audit)
    guard = failover.guard_valid()
    if not guard.get("valid"):
        from .domain.errors import LedgerError
        raise LedgerError("FAILOVER_GUARD_INVALID",
                          "provider failover 写契约未安装或无效；自动恢复保持关闭",
                          guard)
    orch = application(ledger, client, load_config(home), clock=actual_clock)
    resource = make_resource_authorization(ledger, actual_clock)

    def recovery_dispatch(**kwargs):
        from .adapters.failover_guard import authorized_failover_operation
        from .adapters.failover_ledger import FailoverLedger

        operation_id = kwargs.pop("_failover_operation_id", None)
        episode_id = kwargs.pop("_failover_episode_id", None)
        if not operation_id or not episode_id:
            from .domain.errors import LedgerError
            raise LedgerError("FAILOVER_OPERATION_REQUIRED",
                              "恢复派单缺少持久化 episode/operation 身份")
        episode = FailoverLedger(conn, clock=actual_clock.now_ms).get_record(
            "episode", episode_id)
        metadata = (episode or {}).get("metadata") or {}
        if (not episode or episode.get("status") != "continuation_requested"
                or metadata.get("operation_id") != operation_id
                or episode.get("session_id") != kwargs.get("session")):
            from .domain.errors import LedgerError
            raise LedgerError("FAILOVER_OPERATION_MISMATCH",
                              "恢复派单身份与持久化故障链不匹配")
        failover_context = {
            "episode_id": episode_id,
            "operation_id": operation_id,
            "old_attempt_id": str(episode.get("old_attempt_id") or ""),
            "failed_route": str(episode.get("route") or ""),
        }
        with authorized_failover_operation(conn, operation_id):
            return orch.dispatch(**kwargs, _provider_failover=failover_context)

    def authorize(**kw):
        try:
            out = resource.authorize(scope=kw["scope"], scope_id=kw["scope_id"],
                                     model=kw["model"], preset=kw["preset"])
            return {"allowed": bool(out.get("allowed"))}
        except Exception:
            return {"allowed": False}

    return FaultRecoveryService(
        ledger=ledger, failover=failover, client=client(), clock=actual_clock,
        dispatch=recovery_dispatch, authorize=authorize)


def make_managed_inputs(conn, home, *, client, clock=None, responsibilities=None):
    """显式输入用例；惰性客户端确保 record/列表/空白名单不访问服务。

    ``responsibilities`` 可选注入共享 ResponsibilityStore；托管发送在 POST 前
    登记后续责任，CLI/Web 与 Supervisor 使用同一短连接语义。
    """
    from .adapters.managed_input_ledger import ManagedInputLedger
    from .application.managed_inputs import ManagedInputs

    actual_clock = clock or SystemClock()
    base_ledger = Ledger(conn, home, clock=actual_clock)
    actual_resp = responsibilities
    if actual_resp is None:
        actual_resp = make_responsibilities(base_ledger)
    return ManagedInputs(
        ManagedInputLedger(conn, base_ledger), client=client, clock=actual_clock,
        target_gate=make_target_gate(conn, actual_clock),
        responsibilities=actual_resp)


@contextmanager
def open_responsibilities(home):
    """CLI/Web 后续责任作用域；只装配 Ledger + ResponsibilityStore。"""
    actual_home = ensure_home(home)
    conn = init(actual_home)
    try:
        yield make_responsibilities(Ledger(conn, actual_home,
                                           clock=SystemClock()))
    finally:
        conn.close()


@contextmanager
def open_managed_inputs(home, *, client_factory=None):
    """CLI/Web 显式输入共享连接作用域，退出即关闭；不修改进程环境。"""
    actual_home = ensure_home(home)
    conn = init(actual_home)
    client_instance = None

    def get_client():
        nonlocal client_instance
        if client_instance is None:
            client_instance = (client_factory or Client)()
        return client_instance

    try:
        yield make_managed_inputs(conn, actual_home, client=get_client)
    finally:
        conn.close()


class Results:
    def __init__(self, ledger, client, cfg, *, clock=None):
        self.ledger, self.client, self.cfg = ledger, client, cfg
        self.clock = clock

    def collect(self, attempt_id, *, partial=False, carry_manifest=None):
        return collect_attempt(self.ledger, self.client(), attempt_id, self.cfg,
                               auto=False, partial=partial,
                               carry_manifest=carry_manifest,
                               clock=self.clock.now_ms if self.clock else None)

    def scan_baseline(self, attempt_id):
        from .adapters.results import scan_deliverable_baseline  # 组合根装配
        return scan_deliverable_baseline(
            self.ledger, attempt_id,
            budgets={"deliverable_scan_max_entries": self.cfg.get(
                "deliverable_scan_max_entries", 10000),
                "deliverable_scan_max_bytes": self.cfg.get(
                    "deliverable_scan_max_bytes", 512 * 1024 * 1024)},
            captured_at=(self.clock.now_ms() if self.clock else None))

    def message_window(self, session_id, *, anchor_id=None, since_ms=None, until_ms=None):
        """执行对账与正式收集共享分页覆盖契约；运行时经组合根使用文件/HTTP 适配。"""
        from .adapters.results import read_message_window
        return read_message_window(
            self.client(), session_id,
            max_pages=self.cfg.get("result_scan_max_pages", 20),
            page_size=self.cfg.get("result_max_messages", 100),
            anchor_id=anchor_id, since_ms=since_ms, until_ms=until_ms)

    def verify(self, attempt_id, **intent):
        return verify_attempt(self.ledger, attempt_id, **intent)


def application(ledger, client, cfg, *, clock=None):
    actual_clock = clock or SystemClock()
    from .adapters.failover_ledger import FailoverLedger

    return Orchestration(ledger, client=client,
                         results=Results(ledger, client, cfg, clock=actual_clock),
                         clock=actual_clock, new_id=short_id,
                         route_blocks=FailoverLedger(
                             ledger.conn, clock=actual_clock.now_ms))


@contextmanager
def open_application(home, *, client_factory=None):
    """每个 Web 动作创建并关闭自己的连接；不改变进程级环境变量。"""
    conn = init(ensure_home(home))
    client = None

    def get_client():
        nonlocal client
        if client is None:
            client = (client_factory or Client)()
        return client

    try:
        yield application(Ledger(conn, home), get_client, load_config(home))
    finally:
        conn.close()
