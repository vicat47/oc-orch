"""验收用例（application 层）：事务外验收准备的业务判定与 verify 编排。

职责边界：
- 本模块只做业务判定与编排，不接触结果文件、子进程、SQLite 或网络；
  application 层不允许依赖 util/adapters（scripts/check_boundaries.py 守卫）。
- 窄 I/O 依赖由调用方注入：
  * ledger       —— 只读快照（verification_snapshot/get_attempt）与统一提交
                    入口（submit_verification）的端口；
  * read_result  —— 读取并核验 collect 结果产物，返回 (snapshot, error)；
  * run_checks_io—— 执行 command 类型验收命令并写证据（subprocess I/O）；
  * clock        —— wall 时钟（毫秒），用于证据快照时间点；
  * prepare      —— 可选注入“准备函数”端口：默认绑定本模块 prepare_verification，
                    上层兼容门面可用它保持对旧入口的 mock.patch 拦截能力，
                    编排逻辑仍只在本层实现一次。
"""

from __future__ import annotations

from typing import Any, Callable, Iterable, Protocol

from ..domain import results as _result_rules
from ..domain.errors import LedgerError
from ..domain.rules import VERIFIABLE_STATES, verification_gate_rows


class VerificationLedger(Protocol):
    """验收流程所需的台账能力（application 端口，不暴露连接/游标/SQL）。"""

    def get_attempt(self, attempt_id: str) -> dict: ...

    def verification_snapshot(self, attempt_id: str) -> dict: ...

    def submit_verification(self, attempt_id: str, *, verification: dict,
                            result: str, reason: str = "", force: bool = False,
                            actor: str = "coordinator") -> dict: ...


def check_preparation_state(att: dict, task: dict, *, force: bool) -> dict:
    """在执行检查命令前拒绝已知的旧尝试/人工控制；提交时仍须事务内复核。"""
    if task["current_attempt_id"] != att["id"]:
        raise LedgerError("NOT_CURRENT", "旧尝试不能提交当前任务的验收")
    if task["status"] in ("cancelled", "paused", "accepted") or task.get("closed_at") is not None:
        raise LedgerError("TASK_PROTECTED", "任务已被人工控制或关闭，不能验收")
    if att["status"] not in VERIFIABLE_STATES:
        raise LedgerError("NOT_READY", f"尝试当前状态 {att['status']} 不能验收")
    side = {k: att["metadata"][k] for k in ("active_shells", "active_children")
            if att["metadata"].get(k)}
    if side and not force:
        raise LedgerError("SIDE_EFFECTS_ACTIVE", "仍有后台 shell/子 session；需确认停止或有因 force", side)
    return side


def prepare_verification(
    attempt_id: str,
    *,
    result: str,
    reason: str = "",
    evidence: Iterable[str] = (),
    notes: str = "",
    force: bool = False,
    run_checks: bool = False,
    check_timeout: int = 120,
    actor: str = "coordinator",
    ledger: VerificationLedger,
    read_result: Callable[[dict, dict], tuple[dict | None, str | None]],
    run_checks_io: Callable[[str, int], None],
    clock: Callable[[], int],
    changed_check: Callable[[dict, dict], list[str]] | None = None,
) -> dict:
    """验收证据准备（事务外）：参数校验、运行检查、读取/核验结果产物与证据快照。

    只读台账与结果文件；run_checks（业务参数）为 True 时经 run_checks_io 执行
    验收命令并更新交付物证据事实（deliverables 状态），但绝不写验收结论、不推进
    attempt/task 状态。返回可直接交给 ledger.submit_verification 的 verification
    载荷（含 guard 指纹、result_snapshot 与未满足项 unmet，后二者随 submit 落库）。
    """
    if result not in ("pass", "fail"):
        raise LedgerError("INVALID_RESULT", f"验收结果必须是 pass/fail：{result!r}")
    if result == "fail" and not (reason or "").strip():
        raise LedgerError("REASON_REQUIRED", "验收失败需要原因（--reason）")
    if force and not (reason or "").strip():
        raise LedgerError("FORCE_REASON_REQUIRED",
                          "人工例外（force）必须提供非空原因并保存未满足项")
    snap = ledger.verification_snapshot(attempt_id)
    att, task = snap["attempt"], snap["task"]
    side = check_preparation_state(att, task, force=force)
    if run_checks:
        run_checks_io(attempt_id, check_timeout)
        snap = ledger.verification_snapshot(attempt_id)
        att, task = snap["attempt"], snap["task"]
        side = check_preparation_state(att, task, force=force)
    gate = verification_gate_rows(snap["deliverables"])
    # ---- 来源/检查证据门（F4/D10）：事务外准备与台账提交共用规则 --------
    # provenance 摘要中的必需 unchanged/unknown 项不能经普通 verify；
    # command 检查证据必须绑定当前结果的 content_fingerprint，
    # 旧输入的检查不能判新输入（行集缺失 = legacy 旧台账，不在准备层阻断）。
    provenance_rows = (att["metadata"] or {}).get("deliverable_provenance") or []
    stale_src = _result_rules.stale_provenance_rows(provenance_rows)
    current_fp = ((att["metadata"] or {}).get("result") or {}).get(
        "content_fingerprint")
    invalid_checks = _result_rules.command_checks_bound(snap["deliverables"],
                                                        current_fp)
    collected_marks = (type(att["result_collected_at"]) is int
                       and att["result_collected_at"] > 0 and bool(att["result_path"]))
    result_snapshot: dict[str, Any] | None = None
    result_error: str | None = "collect_incomplete"
    changed_targets: list[str] = []
    if collected_marks:
        result_snapshot, result_error = read_result(att, task)
        if (result_error is None and changed_check is not None
                and result == "pass"):
            # T35：--run-checks 之后/验收准备时声明产物内容变化 → RESULT_CHANGED
            changed_targets = changed_check(att, result_snapshot or {})
            if changed_targets:
                raise LedgerError(
                    "RESULT_CHANGED",
                    "声明产物内容在收集后又发生变化："
                    + ", ".join(changed_targets)
                    + "；需重新 collect 后验收",
                    {"changed": changed_targets},
                )
    if result == "pass" and not force:
        if not collected_marks:
            raise LedgerError(
                "NOT_READY",
                "结果收集未完成（缺失 result_collected_at/result_path）；"
                "正常通过需先 collect 或显式 --force",
                {"result_collected_at": att["result_collected_at"],
                 "result_path": att["result_path"]},
            )
        if result_error is not None:
            raise LedgerError(
                "NOT_READY",
                f"结果产物无效：{result_error}；需重新 collect/检查或 --force",
                {"result_path": att["result_path"], "error": result_error},
            )
        if stale_src["stale"] or stale_src["unknown"]:
            # F4：未声明沿用（unchanged）与来源不可判定（unknown）同门；
            # 与 submit_verification 的 STALE_GATE_FAIL 保持一致。
            raise LedgerError(
                "STALE_GATE_FAIL",
                "存在未声明沿用的旧产物或来源未知交付物（stale/unknown）；"
                "需显式 carry、产物更新后重新 collect，或显式 --force 记录原因",
                {"stale": stale_src["stale"], "unknown": stale_src["unknown"]},
            )
        unverified_targets = ([d["target"] for d in gate["unverified"]]
                              + invalid_checks)
        missing_targets = [d["target"] for d in gate["missing"]]
        if missing_targets or unverified_targets:
            # 与 submit_verification 的 AUTH_GATE_FAIL 门保持一致：
            # 缺失（missing）与未验证（含绑定旧输入失效）都在准备层拒绝。
            raise LedgerError(
                "AUTH_GATE_FAIL",
                "必需交付物缺失或已声明检查未通过（含绑定旧输入的检查失效）；"
                "不能正常通过",
                {"missing": missing_targets,
                 "unverified": unverified_targets,
                 "check_input_mismatch": invalid_checks},
            )
    evidence_list = list(evidence)
    unmet: list[dict[str, Any]] = []
    unmet += [{"code": "deliverable_missing", "target": d["target"]}
              for d in gate["missing"]]
    unmet += [{"code": "check_unverified", "target": d["target"]}
              for d in gate["unverified"]]
    if stale_src["stale"] or stale_src["unknown"]:
        unmet.append({"code": "deliverable_stale",
                      "targets": stale_src["stale"] + stale_src["unknown"],
                      "unknown": stale_src["unknown"]})
    if invalid_checks:
        unmet.append({"code": "check_input_mismatch",
                      "targets": invalid_checks,
                      "content_fingerprint": current_fp})
    if side:
        unmet.append({"code": "side_effects", **side})
    if result == "pass":
        if not collected_marks:
            unmet.append({"code": "collect_incomplete",
                          "status": att["status"],
                          "result_collected_at": att["result_collected_at"],
                          "result_path": att["result_path"]})
        elif result_error is not None:
            unmet.append({"code": "result_invalid",
                          "path": att["result_path"],
                          "error": result_error})
    prepared = {
        "schema": "oc-orch.verification.v1",
        "attempt_id": attempt_id,
        "task_id": task["id"],
        "generation": att["generation"],
        "result": result,
        "verified_at": clock(),
        "actor": actor,
        "evidence": evidence_list,
        "notes": notes or "",
        "forced": bool(force),
        "force_reason": reason if (force and result == "pass") else None,
        "missing": [d["target"] for d in gate["missing"]],
        "unverified": [d["target"] for d in gate["unverified"]] + invalid_checks,
        "stale_deliverables": stale_src["stale"] + stale_src["unknown"],
        "check_input_mismatch": invalid_checks or None,
        "deliverables": [{"target": d["target"], "kind": d["kind"],
                          "status": d["status"]} for d in gate["deliverables"]],
        "collected_at": att["result_collected_at"],
        "result_path": att["result_path"],
        "result_summary": (att.get("result_summary") or "")[:2000],
        "result_snapshot": result_snapshot,
        "result_error": result_error,
        "side_effects": side or None,
        "unmet": unmet,
        "guard": {"fingerprint": snap["fingerprint"],
                  "snapshot_at": snap["snapshot_at"]},
    }
    return {"prepared": prepared, "attempt": att, "task": task,
            "gate": gate, "snapshot": snap}


def _bind_prepare(ledger: VerificationLedger,
                  read_result: Callable[[dict, dict], tuple[dict | None, str | None]],
                  run_checks_io: Callable[[str, int], None],
                  clock: Callable[[], int]) -> Callable[..., dict]:
    """把窄 I/O 依赖绑定到本模块 prepare_verification，返回可重入的 prepare 端口。"""

    def prepare(attempt_id: str, *, result: str, reason: str = "",
                evidence: Iterable[str] = (), notes: str = "", force: bool = False,
                run_checks: bool = False, check_timeout: int = 120,
                actor: str = "coordinator") -> dict:
        return prepare_verification(
            attempt_id, result=result, reason=reason, evidence=evidence, notes=notes,
            force=force, run_checks=run_checks, check_timeout=check_timeout, actor=actor,
            ledger=ledger, read_result=read_result, run_checks_io=run_checks_io, clock=clock,
        )

    return prepare


def verify_attempt(
    attempt_id: str,
    *,
    result: str,
    reason: str = "",
    evidence: Iterable[str] = (),
    notes: str = "",
    force: bool = False,
    run_checks: bool = False,
    check_timeout: int = 120,
    actor: str = "coordinator",
    ledger: VerificationLedger,
    read_result: Callable[[dict, dict], tuple[dict | None, str | None]],
    run_checks_io: Callable[[str, int], None],
    clock: Callable[[], int],
    prepare: Callable[..., dict] | None = None,
) -> dict:
    """统一验收用例：事务外 prepare（检查/证据快照）→ 单事务 submit。

    既有 accepted/verification_failed 终态直接委托 submit 做幂等只读或受保护
    拒绝；不为幂等读取重复运行检查或重新验证结果文件。prepare 端口可注入，
    默认绑定本模块的 prepare_verification（编排只在本层实现一次）。
    """
    att = ledger.get_attempt(attempt_id)
    if att["status"] in ("accepted", "verification_failed"):
        return ledger.submit_verification(
            attempt_id, verification={}, result=result, reason=reason,
            force=force, actor=actor,
        )
    prep_fn = prepare if prepare is not None else _bind_prepare(
        ledger, read_result, run_checks_io, clock)
    prep = prep_fn(attempt_id, result=result, reason=reason, evidence=evidence,
                   notes=notes, force=force, run_checks=run_checks,
                   check_timeout=check_timeout, actor=actor)
    return ledger.submit_verification(
        attempt_id, verification=prep["prepared"], result=result, reason=reason,
        force=force, actor=actor,
    )