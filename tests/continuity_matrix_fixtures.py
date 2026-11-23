"""R4 组合迁移/故障验收矩阵的核心夹具（仅供 test_continuity_release_matrix 使用）。

本模块只用**真实 API** 在临时 SQLite 台账上构造合法的控制/责任/outbox 事实：

- 控制维度：CoordinatorControlService.pause/resume + hold_session/begin_user_turn/
  finish_user_turn（真实租约 + 稳定 operation 索引）；
- provider 维度：TargetGate.observe_error / claim_probe（真实冷却）；
- 依赖维度：DependencyWaitService.register_condition_wait + reconcile，其中
  satisfied 必须是**真实 collect_attempt + verify_attempt** 产出的有效验收；
- 邮件维度：OperatorEmailService.enqueue_incident / dispatch_once（FakeTransport
  只在“外部通道”边界注入，outbox/回执均走真实适配器）；
- 发布门：install_writer_guard 显式安装 + 一致 backup_to / init 恢复。

约定：零网络、零真实服务；FakeClock 受控；所有故障矩阵只作用于临时库。
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO))

from oc_orch.adapters.sqlite_db import init, meta_get  # noqa: E402
from oc_orch.adapters.sqlite_ledger import Ledger  # noqa: E402
from oc_orch.adapters.coordinator_control import CoordinatorControlLedger  # noqa: E402
from oc_orch.application.coordinator_control import (  # noqa: E402
    CoordinatorControlService,
)
from oc_orch.adapters.target_gate import (  # noqa: E402
    HALF_OPEN_TIMEOUT_MS,
    PROBE_OBSERVATION_WINDOW_MS,
    TargetGate,
)
from oc_orch.adapters.responsibility_ledger import (  # noqa: E402
    STATE_AWAITING_REVIEW,
    STATE_WAITING_DEPENDENCY,
    STATE_WAITING_HUMAN,
    ResponsibilityStore,
)
from oc_orch.application.dependency_wait import DependencyWaitService  # noqa: E402
from oc_orch.adapters.email_outbox import EmailOutbox  # noqa: E402
from oc_orch.application.mail_port import MailUnknownError  # noqa: E402
from oc_orch.application.operator_email import OperatorEmailService  # noqa: E402
from oc_orch.domain.operator_email import (  # noqa: E402
    CONDITION_HUMAN,
    KIND_FIRST,
    STATUS_ACCEPTED,
    STATUS_CLAIMED,
    STATUS_PENDING,
    STATUS_PENDING_AUTH,
    STATUS_SUBMIT_UNKNOWN,
    EmailPolicy,
    IncidentFact,
    Recipient,
    Sender,
)
from tests.fakes import FakeClock, FakeClient  # noqa: E402

T0 = 1_800_000_000_000

# 维度投影里使用的稳定坐标。
PROJ_CTRL = "/tmp/opencode/matrix-ctrl"
PROJ_RUN = "/tmp/opencode/matrix-run"
SID = "ses_matrix_coord"
SID_FINISHED = "ses_matrix_finished"
SID_COMBINED = "ses_matrix_comb"
GATE_COOLDOWN_SID = "ses_gate_cooldown"
GATE_PROBE_SID = "ses_gate_probe"
GATE_MANUAL_SID = "ses_gate_manual"


# ---------------------------------------------------------------------------
# 依赖条件 / 邮件策略 / incident 事实 / fake 传输（纯数据，真实 API 消费它们）
# ---------------------------------------------------------------------------
def condition(**over):
    base = {"type": "task_state", "require": "accepted", "mode": "all",
            "task_ids": ["T-up"], "result_version": None, "revision": 1,
            "successor": None}
    base.update(over)
    return base


def enabled_policy(**overrides) -> EmailPolicy:
    kwargs = dict(
        enabled=True,
        sender=Sender("oc-ops", "ops@example.test"),
        recipients=(Recipient("op-primary", "operator@example.test"),),
        projects=frozenset({"proj-a"}),
        event_kinds=frozenset(("first", "update", "escalation","recovery")),
        unattended=True,
        receipt_queries_authorized=True,
        retry_min_ms=10_000,
        retry_max_ms=80_000,
        max_attempts=3,
        receipt_check_limit=3,
    )
    kwargs.update(overrides)
    return EmailPolicy(**kwargs)


def incident_fact(*, incident_id: str, observed_at: int = T0,
                  condition_version: int = 1) -> IncidentFact:
    return IncidentFact(
        incident_id=incident_id,
        project="proj-a",
        incident_type="manual-step",
        condition_key="waiting-human",
        condition_version=condition_version,
        recipient_ref="op-primary",
        condition_kind=CONDITION_HUMAN,
        blocked_reason="需要人工决策",
        impact="等待决策",
        decision_needed="选A或B",
        first_seen_at=T0,
        observed_at=observed_at,
    )


class FakeTransport:
    """可注入结果/异常的窄传输桩；永不接触真实邮箱/网络（外部通道边界）。"""

    def __init__(self):
        self.sent: list[dict] = []
        self.searches: list[dict] = []
        self.send_result: object = {"queued": True}
        self.search_result: object = []

    def send(self, **kw):
        self.sent.append(dict(kw))
        if isinstance(self.send_result, Exception):
            raise self.send_result
        return self.send_result

    def search_receipt(self, **kw):
        self.searches.append(dict(kw))
        if isinstance(self.search_result, Exception):
            raise self.search_result
        return list(self.search_result)


# ---------------------------------------------------------------------------
# 真实 collect/verify 产出的有效验收（唯一合法 satisfied 证据）
# ---------------------------------------------------------------------------
def real_verified_upstream(ledger: Ledger, home: str, clock: FakeClock,
                           task_id: str, attempt_id: str, site_dir: str) -> str:
    """复现 RealVerifyAcceptanceTests：真实 collect_attempt + verify_attempt。

    返回 attempt_id；调用后任务 T-up 处于 accepted 且绑定当前结果版本（非 force），
    是依赖 satisfied 的唯一合法证据来源。
    """
    from oc_orch.adapters.results import (  # noqa: PLC0415
        collect_attempt,
        scan_deliverable_baseline,
        verify_attempt,
    )

    os.makedirs(site_dir, exist_ok=True)
    with open(os.path.join(site_dir, "README.md"), "w", encoding="utf-8") as fh:
        fh.write("baseline")
    ledger.create_attempt(
        task_id, attempt_id=attempt_id, directory=site_dir, write_scope=[site_dir],
        status="dispatched", dispatch_state="confirmed", session_id="ses_real",
        dispatch_msg_id="msg_real_in", deliverables=["README.md"],
        takeover_confirmed=True)
    t0 = clock.now_ms()
    ref = scan_deliverable_baseline(ledger, attempt_id, captured_at=t0)
    ledger.seal_execution(attempt_id, mode="new", session_before={}, baseline=ref)
    ledger.record_execution_started(attempt_id, started_at=t0,
                                    event_id="evt-mx-start")
    ledger.record_terminal(
        attempt_id, outcome="succeeded", source="sse", event_id="evt-mx-end",
        candidate={"outcome": "succeeded", "source": "sse", "ended_at": t0 + 10,
                   "session_id": "ses_real", "input_in_inbox": False})
    client = FakeClient()
    client.append_message("ses_real", {
        "id": "msg_real_in", "type": "user", "text": "go",
        "time": {"created": t0}})
    client.append_message("ses_real", {
        "id": "msg_real_done", "type": "assistant", "time": {"created": t0 + 1},
        "content": [{"type": "text", "text": "任务完成"}]})
    with open(os.path.join(site_dir, "README.md"), "w", encoding="utf-8") as fh:
        fh.write("collected-v1")
    info = collect_attempt(
        ledger, client, attempt_id,
        cfg={"result_max_messages": 50, "result_store_limit": 20000}, auto=False)
    assert info["status_after"] == "result_collected", info
    out = verify_attempt(ledger, attempt_id, result="pass")
    assert out["result"] == "pass", out
    return attempt_id


# ---------------------------------------------------------------------------
# 全量表事实摘要（临时库专用；等价 scripts/check_continuity_release.py）
# ---------------------------------------------------------------------------
def digest_query(conn, sql, params=()):
    digest = hashlib.sha256()
    count = 0
    for row in conn.execute(sql, params):
        values = [dict(blob_hex=v.hex()) if isinstance(v, bytes) else v for v in row]
        digest.update(json.dumps(values, ensure_ascii=False,
                                 separators=(",", ":")).encode())
        digest.update(b"\n")
        count += 1
    return {"count": count, "sha256": digest.hexdigest()}


def table_facts(conn):
    out = {}
    for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' "
                            "AND name NOT LIKE 'sqlite_%' ORDER BY name"):
        name = row[0]
        cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{name}")')]
        order = ",".join('"' + c.replace('"', '""') + '"' for c in cols)
        out[name] = digest_query(conn, f'SELECT * FROM "{name}" ORDER BY {order}')
    return out


def meta_map(conn) -> dict:
    return dict(conn.execute("SELECT key, value FROM meta").fetchall())


# ---------------------------------------------------------------------------
# 组合合法状态构造
# ---------------------------------------------------------------------------
@dataclass
class MatrixState:
    home: str
    conn: sqlite3.Connection
    clock: FakeClock
    ledger: Ledger
    control: CoordinatorControlService
    control_ledger: CoordinatorControlLedger
    gate: TargetGate
    store: ResponsibilityStore
    dep: DependencyWaitService
    outbox: EmailOutbox
    transport: FakeTransport
    svc_off: OperatorEmailService
    svc_on: OperatorEmailService

    # 维度坐标 / 事实身份
    project_ctrl: str = PROJ_CTRL
    project_run: str = PROJ_RUN
    sid: str = SID
    sid_finished: str = SID_FINISHED
    sid_combined: str = SID_COMBINED
    completed_watermark: int = 0

    gate_cooldown_sid: str = GATE_COOLDOWN_SID
    gate_probe_sid: str = GATE_PROBE_SID
    gate_manual_sid: str = GATE_MANUAL_SID
    probe_message_id: str = "probe-mx-1"
    probe_observation_checks: int = 0
    probe_observation_deadline: int = 0

    dep_wait_rid: str = ""
    dep_sat_rid: str = ""
    dep_human_rid: str = ""
    dep_deadline: int = 0
    up_verified_aid: str = ""
    result_version: str = ""

    biz_task_id: str = "T-biz-mx"
    biz_attempt_id: str = "A-biz-mx"
    original_input_id: str = "msg-origin-biz"
    ack_message_id: str = "msg-ack-up1"
    ack_nid: str = ""
    acked_at: int = 0

    # 控制操作索引
    op_p1: str = "op-mx-p1"
    op_r1: str = "op-mx-r1"
    op_p2: str = "op-mx-p2"

    # 邮件事件身份（真实 enqueue_incident 生成的稳定 item_id）
    mail_items: dict = field(default_factory=dict)

    # 收集的中间事实（断言辅助）
    dep_sat_notif_count: int = 0
    mail_row_count: int = 0
    notif_total: int = 0


def build_matrix_state(tmp_dir: str) -> MatrixState:
    """在 ``tmp_dir`` 内用真实 API 构造全部合法维度组合状态。

    顺序约束：probe-unknown 需要推进时钟（半开超时），因此放在最后；冷却门在
    推进时钟之后重新登记，避免被时间推进误“解除”。
    """
    home = os.path.join(tmp_dir, "home")
    os.makedirs(home, exist_ok=True)
    clock = FakeClock(wall_ms=T0)
    conn = init(home)
    ledger = Ledger(conn, home, clock=clock)
    control_ledger = CoordinatorControlLedger(conn, clock)
    control = CoordinatorControlService(control_ledger, clock)
    gate = TargetGate(conn, clock)
    store = ResponsibilityStore(ledger)
    dep = DependencyWaitService(ledger, clock, store=store)
    outbox = EmailOutbox(conn, clock)
    transport = FakeTransport()
    svc_off = OperatorEmailService(outbox, transport, clock, EmailPolicy())
    svc_on = OperatorEmailService(outbox, transport, clock, enabled_policy())

    st = MatrixState(
        home=home, conn=conn, clock=clock, ledger=ledger, control=control,
        control_ledger=control_ledger, gate=gate, store=store, dep=dep,
        outbox=outbox, transport=transport, svc_off=svc_off, svc_on=svc_on)

    # ---- 项目控制：paused（真实租约 + operation 索引历史） ------------
    ledger.claim_coordinator(project=st.project_ctrl, session_id=st.sid,
                             actor="matrix")
    p1 = control.pause(st.project_ctrl, session_id=st.sid,
                       operation_id=st.op_p1, reason="user-requested",
                       expected_revision=0)
    assert p1["applied"] and p1["snapshot"]["status"] == "paused", p1
    r1 = control.resume(st.project_ctrl, session_id=st.sid,
                        operation_id=st.op_r1, reason="user-resumed",
                        expected_revision=1)
    assert r1["applied"] and r1["snapshot"]["status"] == "running", r1
    p2 = control.pause(st.project_ctrl, session_id=st.sid,
                       operation_id=st.op_p2, reason="user-requested-2",
                       expected_revision=2)
    assert p2["applied"] and p2["snapshot"]["status"] == "paused", p2
    # ``project_run`` 保持 running（无控制记录 = 合法 running 默认）。

    # ---- session：完成水位 / hold+barrier --------------------------------
    fin = control.begin_user_turn(st.sid_finished, message_id="msg-f0",
                                  origin_at=T0 + 100, operation_id="op-f0",
                                  trusted=True)
    assert fin["applied"], fin
    fin2 = control.finish_user_turn(
        st.sid_finished, message_id="msg-f0", idle_id="idle-f0",
        idle_at=T0 + 101, expected_revision=fin["revision"])
    assert fin2["applied"] and fin2["snapshot"]["barrier"] is None, fin2
    st.completed_watermark = fin2["snapshot"]["watermark"]
    assert st.completed_watermark > 0

    hold = control.hold_session(st.sid_combined, event_id="ev-hold-mx",
                                origin_at=T0 + 200, reason="user-interrupted")
    assert hold["applied"], hold
    bar = control.begin_user_turn(st.sid_combined, message_id="msg-m1",
                                  origin_at=T0 + 201, operation_id="op-m1",
                                  trusted=True)
    assert bar["applied"], bar
    comb = control.get_session(st.sid_combined)
    assert comb["hold"] is not None and comb["barrier"] is not None, comb

    # ---- 业务事实：current/generation + 原输入 ID + ACK ------------------
    ledger.create_task(project=st.project_run, title="biz", task_id=st.biz_task_id,
                       coordinator_session=st.sid)
    ledger.create_attempt(st.biz_task_id, attempt_id=st.biz_attempt_id,
                          directory=os.path.join(home, "site-biz"),
                          session_id="ses_worker", generation=1, status="running",
                          dispatch_msg_id=st.original_input_id,
                          takeover_confirmed=True)
    biz = ledger.get_attempt(st.biz_attempt_id)
    assert biz["dispatch_msg_id"] == st.original_input_id

    # ---- 依赖维度 ------------------------------------------------------
    ledger.create_task(project=st.project_run, title="down-wait",
                       task_id="T-wait", coordinator_session=st.sid)
    ledger.create_task(project=st.project_run, title="down-sat",
                       task_id="T-sat", coordinator_session=st.sid)
    ledger.create_task(project=st.project_run, title="down-human",
                       task_id="T-human", coordinator_session=st.sid)
    ledger.create_task(project=st.project_run, title="up-wait",
                       task_id="T-up-w", coordinator_session=st.sid)
    ledger.create_task(project=st.project_run, title="up-sat",
                       task_id="T-up-s", coordinator_session=st.sid)

    # 下游等待任务各自的真实 attempt（责任身份校验要求 attempt 隶属 task）
    for tid, aid in (("T-wait", "A-wait"), ("T-sat", "A-sat"),
                     ("T-human", "A-human")):
        ledger.create_attempt(
            tid, attempt_id=aid, directory=os.path.join(home, f"site-{aid}"),
            session_id="ses_worker", generation=1, status="running",
            takeover_confirmed=True)

    dep.register_condition_wait(
        "coord:matrix", st.project_run, "T-wait",
        condition(task_ids=["T-up-w"]), attempt_id="A-wait", generation=1,
        target_session=st.sid, next_action="wait upstream w")
    dep.register_condition_wait(
        "coord:matrix", st.project_run, "T-sat",
        condition(task_ids=["T-up-s"]), attempt_id="A-sat", generation=1,
        target_session=st.sid, next_action="review upstream s")
    dep.register_condition_wait(
        "coord:matrix", st.project_run, "T-human",
        condition(task_ids=["T-up-missing"]), attempt_id="A-human", generation=1,
        target_session=st.sid, next_action="human must decide")

    st.dep_wait_rid = st.store.list_open_responsibilities(
        task_id="T-wait")[0]["id"]
    st.dep_sat_rid = st.store.list_open_responsibilities(task_id="T-sat")[0]["id"]
    st.dep_human_rid = st.store.list_open_responsibilities(
        task_id="T-human")[0]["id"]
    st.dep_deadline = st.store.get_responsibility(st.dep_wait_rid)["deadline"]

    # 真实 collect/verify 上游（唯一有效验收证据）
    site_s = os.path.join(home, "site-up-s")
    st.up_verified_aid = real_verified_upstream(
        ledger, home, clock, "T-up-s", "A-up-s", site_s)
    att = ledger.get_attempt("A-up-s")
    st.result_version = att["metadata"]["result"]["version"]
    verif = att["verification"]
    assert verif["result_snapshot"]["result_version"] == st.result_version
    assert verif["forced"] is False

    out = dep.reconcile(limit=10)
    assert out["waiting"] >= 1 and out["ready"] >= 1 and out["needs_human"] >= 1, out
    sat_row = st.store.get_responsibility(st.dep_sat_rid)
    assert sat_row["state"] == STATE_AWAITING_REVIEW, sat_row
    human_row = st.store.get_responsibility(st.dep_human_rid)
    assert human_row["state"] == STATE_WAITING_HUMAN, human_row

    # 原 ACK：依赖复核通知实际经 claim→sent→ack 成为已 ACK 事实（原 input 身份
    # = message_id，原 ACK = acked_at）。
    notifs = ledger.list_notifications(target_session=st.sid, state=None, limit=100)
    sat_notifs = [n for n in notifs if n["kind"] == "continuation_ready"]
    assert len(sat_notifs) == 1, sat_notifs
    st.ack_nid = sat_notifs[0]["id"]
    st.dep_sat_notif_count = len(sat_notifs)
    claimed = ledger.claim_notifications([st.ack_nid], owner="matrix")
    assert len(claimed) == 1, claimed
    ledger.mark_notification_sent(st.ack_nid, message_id=st.ack_message_id,
                                  next_attempt_at=T0, owner="matrix")
    acked = ledger.ack_notifications([st.ack_nid], actor="coordinator")
    assert acked == 1
    n = ledger.get_notification(st.ack_nid)
    st.acked_at = n["acked_at"]
    assert n["state"] == "acked" and n["message_id"] == st.ack_message_id

    # ---- 邮件维度（真实 enqueue/dispatch；外部通道用 FakeTransport） ----
    auth = svc_off.enqueue_incident(incident_fact(incident_id="inc-auth"))
    assert auth["result"] == "pending-auth", auth
    st.mail_items["pending-auth"] = auth["item_id"]

    pend = svc_on.enqueue_incident(incident_fact(incident_id="inc-pending"))
    assert pend["result"] == "enqueued", pend
    st.mail_items["pending"] = pend["item_id"]
    assert outbox.get_item(pend["item_id"])["status"] == STATUS_PENDING

    claim = svc_on.enqueue_incident(incident_fact(incident_id="inc-claimed"))
    st.mail_items["claimed"] = claim["item_id"]
    c = outbox.claim(claim["item_id"], owner="matrix", now=clock.now_ms())
    assert c["allowed"] and outbox.get_item(claim["item_id"])["status"] == \
        STATUS_CLAIMED, c

    unk = svc_on.enqueue_incident(incident_fact(incident_id="inc-unknown"))
    st.mail_items["submit-unknown"] = unk["item_id"]
    transport.send_result = MailUnknownError("send-timeout")
    r_unk = svc_on.dispatch_once(item_id=unk["item_id"], owner="matrix")
    assert r_unk["result"] == "submit-unknown", r_unk
    assert outbox.get_item(unk["item_id"])["status"] == STATUS_SUBMIT_UNKNOWN

    acc = svc_on.enqueue_incident(incident_fact(incident_id="inc-acc"))
    st.mail_items["accepted"] = acc["item_id"]
    transport.send_result = {"queued": True}
    r_acc = svc_on.dispatch_once(item_id=acc["item_id"], owner="matrix")
    assert r_acc["result"] == "accepted", r_acc
    assert outbox.get_item(acc["item_id"])["status"] == STATUS_ACCEPTED

    st.mail_row_count = len(outbox.list_all())
    assert st.mail_row_count == 5
    st.notif_total = conn.execute("SELECT COUNT(*) FROM notifications").fetchone()[0]

    # ---- provider 维度：probe-unknown（需推进时钟，放最后） ------------
    gate.observe_error(GATE_PROBE_SID, {"category": "quota", "status": 429,
                                        "message": "limit"},
                       evidence_key="mx-e-probe", observed_at=clock.now_ms())
    gate.observe_error(GATE_MANUAL_SID, {"category": "quota", "status": 429},
                       evidence_key="mx-e-manual-before",
                       observed_at=clock.now_ms())
    gate.block_manual(GATE_MANUAL_SID, reason="historical-manual")
    assert gate.get(GATE_MANUAL_SID)["state"] == "manual-blocked"

    # 冷却窗口过后才能进入半开探测；随后推进超过半开超时 → probe-unknown。
    clock.advance(wall=60 * 60 * 1000)  # 越过 blocked_until（quota 冷却 ≤ 1h）
    probe = gate.claim_probe(GATE_PROBE_SID, owner="matrix",
                             message_id=st.probe_message_id)
    assert probe["allowed"], probe
    clock.advance(wall=HALF_OPEN_TIMEOUT_MS + 1000)
    probe_view = gate.get(GATE_PROBE_SID)
    assert probe_view["state"] == "probe-unknown", probe_view
    obs = gate.claim_probe_observation(
        GATE_PROBE_SID, message_id=st.probe_message_id,
        expected_revision=probe_view["revision"],
        idle_at=clock.now_ms())
    assert obs["allowed"], obs
    obs_view = gate.get(GATE_PROBE_SID)
    st.probe_observation_checks = obs_view["probe_observation"]["checks"]
    st.probe_observation_deadline = obs_view["probe_observation"]["deadline_at"]
    assert st.probe_observation_checks == 1
    assert st.probe_observation_deadline > 0
    assert st.probe_observation_deadline == min(
        max(obs_view["probe_observation"]["started_at"], 0)
        + PROBE_OBSERVATION_WINDOW_MS, 8 * 10 ** 12)

    # 冷却门在推进时钟后再登记，保证快照时刻仍在冷却窗口内。
    cooldown = gate.observe_error(
        GATE_COOLDOWN_SID, {"category": "quota", "status": 429,
                            "message": "cooldown"},
        evidence_key="mx-e-cooldown", observed_at=clock.now_ms())
    assert cooldown["changed"], cooldown
    cd_view = gate.get(GATE_COOLDOWN_SID)
    assert cd_view["state"] == "blocked", cd_view

    return st


# ---------------------------------------------------------------------------
# 覆盖表（机器可读）：dimension × state → 真实构造 API + 断言方法
# ---------------------------------------------------------------------------
COVERAGE_TABLE = [
    # (dimension, state, api_reference, asserted_by_test_method)
    ("project", "running", "CoordinatorControlService.get_project(default)", "test_legal_state_construction_coverage"),
    ("project", "paused", "CoordinatorControlService.pause(lease+CAS+op-index)", "test_legal_state_construction_coverage"),
    ("session", "hold", "CoordinatorControlService.hold_session", "test_legal_state_construction_coverage"),
    ("session", "barrier", "CoordinatorControlService.begin_user_turn(trusted=True)", "test_legal_state_construction_coverage"),
    ("session", "completion-watermark", "CoordinatorControlService.finish_user_turn", "test_legal_state_construction_coverage"),
    ("provider-gate", "cooldown", "TargetGate.observe_error -> blocked", "test_legal_state_construction_coverage"),
    ("provider-gate", "probe-unknown", "TargetGate.claim_probe + clock advance + claim_probe_observation", "test_legal_state_construction_coverage"),
    ("provider-gate", "legacy-manual", "TargetGate.block_manual -> manual-blocked", "test_legal_state_construction_coverage"),
    ("dependency", "waiting", "DependencyWaitService.register_condition_wait + reconcile", "test_legal_state_construction_coverage"),
    ("dependency", "satisfied-awaiting_review", "real collect_attempt+verify_attempt + reconcile", "test_legal_state_construction_coverage"),
    ("dependency", "waiting-human", "DependencyWaitService.reconcile(condition-missing)", "test_legal_state_construction_coverage"),
    ("mail", "pending-auth", "OperatorEmailService.enqueue_incident(disabled-policy)", "test_legal_state_construction_coverage"),
    ("mail", "pending", "OperatorEmailService.enqueue_incident(enabled-policy)", "test_legal_state_construction_coverage"),
    ("mail", "claimed", "EmailOutbox.claim(owner)", "test_legal_state_construction_coverage"),
    ("mail", "submit-unknown", "OperatorEmailService.dispatch_once(MailUnknownError)", "test_legal_state_construction_coverage"),
    ("mail", "accepted", "OperatorEmailService.dispatch_once(queued=True)", "test_legal_state_construction_coverage"),
]

FAILURE_TABLE = [
    ("install-rollback", "install_writer_guard 注入坏 trigger SQL 整体回滚", "test_guard_install_failure_rolls_back_whole_transaction"),
    ("bad-json", "控制/闸门/outbox 记录坏 JSON fail-closed", "test_bad_json_control_fail_closed"),
    ("bad-schema", "control/gate/mail schema 键不兼容 fail-closed", "test_schema_mismatch_fail_closed_across_dimensions"),
    ("missing-fields", "缺字段/缺回执证据 fail-closed", "test_missing_fields_fail_closed_for_outbox_invariants"),
    ("old-conn-rejected", "旧 schema3 连接（无契约函数）12 表 × 3 操作拒写", "test_old_connection_write_refused_all_guarded_tables"),
    ("old-impl-rejected", "真实旧实现子进程试写拒绝（OC_ORCH_MATRIX_OLD_SOURCE 可选项）", "test_old_implementation_refused_when_old_source_provided"),
    ("retained-new-facts", "新事实保留后恢复不倒旧快照", "test_recovery_reuses_current_account_not_old_snapshot"),
    ("late-idle", "旧 idle/旧中断不得清除更新屏障或重建 hold", "test_late_idle_and_late_hold_cannot_clear_barrier_or_rebuild_hold"),
    ("late-ack", "晚到 ACK/发送确认不得回退 current 或重复计数", "test_late_ack_and_late_dispatch_confirm_no_op_or_no_regress"),
    ("late-receipt", "晚到邮件回执不得覆盖新事件/已受理证据", "test_late_receipt_not_overriding_new_event_or_accepted_evidence"),
]

REQUIRED_STATES = {
    "project": {"running", "paused"},
    "session": {"hold", "barrier", "completion-watermark"},
    "provider-gate": {"cooldown", "probe-unknown", "legacy-manual"},
    "dependency": {"waiting", "satisfied-awaiting_review", "waiting-human"},
    "mail": {"pending-auth", "pending", "claimed", "submit-unknown", "accepted"},
}

REQUIRED_FAILURES = {row[0] for row in FAILURE_TABLE}


def coverage_report() -> dict:
    """机器可读覆盖报告（临时库矩阵；测试与脚本 --report 共用同一口径）。"""
    by_dim: dict[str, list[str]] = {}
    for dim, state, api, method in COVERAGE_TABLE:
        by_dim.setdefault(dim, []).append(
            {"state": state, "api": api, "asserted_by": method})
    failures = [{"kind": k, "api": a, "test": m}
                for k, a, m in FAILURE_TABLE]
    covered = {d: {r["state"] for r in rows}
               for d, rows in by_dim.items()}
    missing = {d: sorted(REQUIRED_STATES[d] - covered[d])
               for d in REQUIRED_STATES if REQUIRED_STATES[d] - covered[d]}
    return {
        "schema": "oc-orch.r4-matrix-coverage.v1",
        "legal_states": by_dim,
        "failure_modes": failures,
        "missing_states": missing,
        "complete": not missing,
        "complete_meaning": ("complete=True 仅表示覆盖表枚举完整（REQUIRED_STATES/"
                             "REQUIRED_FAILURES 全含），不代表全笛卡尔组合或 36 场景/"
                             "功能整体完成"),
        "scope": "R4 临时库组合矩阵：16 个合法状态维度 + 10 个失败模式（显式集合），"
                 "非 openspec 36 场景/功能全量验收",
        "exhaustive_cartesian": False,
    }