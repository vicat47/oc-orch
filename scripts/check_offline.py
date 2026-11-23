#!/usr/bin/env python3
"""在隔离台账与服务配置下运行分层离线回归，不连接真实 OpenCode。

用法：
  python3 scripts/check_offline.py [--layer {unit,integration,contract,all}] [--runner {unittest,pytest}]

分层由测试文件名集合指定（`all` 使用 tests/test_*.py 自动发现，新增文件自动纳入且不漏测）：

- unit        -- 纯单元/状态机测试
- integration -- 真 SQLite 并发、应用用例与 Supervisor 离线剧本
- contract    -- CLI/Web/适配器契约与架构边界
- all         -- tests/ 下全部 test_*.py（默认）

分层清单中的文件缺失时报错，防止遗漏约定用例却报告通过。
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
TESTS = ROOT / "tests"

# 分层 → 测试文件名（相对 tests/）。`all` 不在此表，使用自动发现。
LAYERS: dict[str, list[str]] = {
    "unit": [
        "test_unit.py",
        "test_notification_policy.py",  # 唤醒通知有效性纯规则
        "test_notification_policy_safety.py", # 独立坏形状与未知副作用拒绝性回归
        "test_quota_classification.py", # 订阅限额与普通429分类
        "test_managed_input_policy.py", # S4输入意图与回执纯规则
        "test_responsibility_policy.py", # S4后续责任连续性纯规则
        "test_dependency_wait.py",       # 结构化依赖条件与业务健康纯规则
        "test_operator_email.py",        # 邮件事件选择、授权与最小摘要规则
        "test_conversation_control.py",  # 显式本地对话控制入口拒绝性契约
        "test_delivery_policy.py",      # 交付策略严格值与动态指令纯投影
        "test_route_context.py",        # 上下文/cache/估计的未知与过期投影
        "test_delivery_metrics.py",     # 固定cohort/窗口/force/未结及切换度量
    ],
    "integration": [
        "test_claim_atomicity.py",      # F1 写权认领原子性
        "test_verification_atomicity.py",  # F2 验收门禁原子性
        "test_application.py",          # 应用用例与真实台账契约
        "test_dispatch_recovery.py",    # 恢复确认的 current/人工控制交错
        "test_result_boundary.py",      # 证据端口与事务外验收准备
        "test_supervisor_integration.py",  # 任务 3.2
        "test_notification_integration.py",  # 任务 3.3
        "test_wake_delivery_acceptance.py", # P01–P07 独立正确行为验收
        "test_wake_delivery_races.py",      # 目标发送/事件归属/取消竞态
        "test_wake_nudge_acceptance.py",    # 有限nudge、冷却不烧预算、pending不得催办
        "test_wake_runtime.py",             # C 组：S1 claim 组装、unknown 观察、foreign 队头
        "test_target_gate.py",              # 目标冷却持久化与单半开探测
        "test_target_gate_runtime.py",      # 目标冷却的自动发送与观察接线
        "test_probe_unknown_recovery.py",   # 原probe晚到成功、持久补证预算与CAS
        "test_turn_interruption_resume.py", # 回合中断 vs 项目暂停的恢复边界
        "test_coordinator_control.py",     # 项目意图、CAS与持久控制底座
        "test_delivery_policy_ledger.py",   # 未启用策略草案的版本/CAS/原子性/WAL
        "test_resource_authorization.py",  # 独立资源授权、撤销/过期/CAS/WAL/幂等
        "test_delivery_action.py",         # 单次许可、全部发送入口/切换线性化/unknown
        "test_delivery_guard.py",          # 独立delivery契约与旧v2连接拒写
        "test_operator_turn.py",           # 显式补充指令/原ID/完整结束共用观察器
        "test_prototype.py",               # 原型缺口CAS/幂等/正常修复证据/恢复
        "test_coordinator_turn_barrier.py", # 用户回合屏障与中断版本守卫
        "test_continuity_runtime.py",      # 父协调真实控制台账与派单/唤醒接线
        "test_dispatch_authority.py",      # 旧holder/过期/停用/同holder ABA拒发
        "test_continuity_status.py",       # CLI/runtime共享业务健康只读投影
        "test_continuity_parent_safety.py", # 子路独立审查父接线的WAL/覆盖反例
        "test_wake_control_admission.py",  # 控制/provider探测/input发送权同事务
        "test_dependency_continuity.py",   # 依赖满足后的原子待办与有界等待
        "test_email_outbox.py",            # 邮件持久认领、未知补证和重启预算
        "test_email_executor.py",          # 独立邮件tick、默认关闭与主循环隔离
        "test_operator_incident.py",       # 业务事实/稳定incident及来源重验
        "test_incident_email_bridge.py",   # incident→outbox发送前源绑定
        "test_incident_runtime.py",        # 父级工厂/主tick/独立执行器接线
        "test_incident_source_safety.py",  # 恢复事务/损坏门禁/真实归属/只读范围独立反例
        "test_continuity_release_matrix.py", # 控制/依赖/outbox组合备份与故障矩阵
        "test_notification_validity_ledger.py", # S1台账通知有效性
        "test_notification_scope_acceptance.py", # 台账与纯规则的缺归属一致性
        "test_input_ledger.py",             # S2原输入/批次/目标未决槽位
        "test_input_owner_acceptance.py",   # 新输入/nudge发送权不得由NULL授予
        "test_managed_input_acceptance.py", # S4意图/人工操作/过期规则
        "test_managed_input_runtime_acceptance.py", # 周期观察与本地过期
        "test_managed_input_expiry_acceptance.py", # 逐输入到期与公平调度
        "test_managed_input_safety_acceptance.py", # 取消/未知与撤销安全
        "test_managed_input_observation_acceptance.py", # 持久诊断与告警幂等
        "test_managed_input_ledger.py", # S4输入回执与操作CAS
        "test_managed_inputs.py", # S4共享输入用例
        "test_managed_tracking.py", # 默认托管与写权边界
        "test_responsibility_ledger.py", # 责任登记/转交/发布原子性
        "test_responsibility_migration.py", # schema3迁移与回退拒绝
        "test_writer_guard.py",          # 发布写契约、旧schema3拒写与保留新事实
        "test_writer_guard_upgrade.py",  # 显式v1→v2写契约升级及旧UDF连接拒写
        "test_continuation_runtime.py", # 执行后待办及缺失责任检查
        "test_s4_continuity_acceptance.py", # 父协调独立事务/ACK验收
        "test_s4_end_to_end_acceptance.py", # collect后新输入的执行接续
        "test_input_migration.py",          # S2 schema升级与回退演练
        "test_completion_integrity.py",    # 修复 attempt 完成完整性（事故闭环）
        "test_collection_atomicity.py",    # 收集/发布事务与并发崩溃
        "test_collection_remediation.py",  # 收集前置守卫、CAS 与 legacy 恢复
        "test_result_publication.py",      # 结果版本化发布与 ACK/重启
        "test_provenance_remediation.py",  # 来源/完整扫描/验收准备拒绝性回归
        "test_scan_integrity.py",         # 递归目录/glob、枚举预算及链接边界
        "test_collection_acceptance.py",  # 独立应用交错、partial、分页与重试验收
        "test_execution_acceptance.py",   # SSE 事实冲突、输入身份及无 turn 基线
        "test_publication_acceptance.py", # 旧代次发布、ACK 交错及 >300 路由责任
        "test_completion_migration.py",    # SQLite 一致备份与新旧版本混用演练
    ],
    "contract": [
        "test_verification_cli.py",     # F2 CLI 契约
        "test_web.py",                  # Web 控制面契约
        "test_coordinator_web.py",      # coordinator-first 响应式 Web 契约（排序/归属/深链/只读）
        "test_web_assets.py",           # 随包资产白名单/manifest/构建契约
        "test_continuity_web.py",       # 健康/人工待办及pause/resume共享Web用例
        "test_project_control_view.py", # 插件精确控制视图/只读历史回执
        "test_continuity_web_safety.py", # 独立类型/CAS/分页/草稿/响应归属反例
        "test_client_contract.py",      # 任务 5.1 HTTP/SSE 契约
        "test_entrypoint_equivalence.py",  # CLI/Web 等价契约
        "test_completion_cli_contract.py",  # 完成协议展示、显式 ACK 与预算配置
        "test_protocol_remediation.py",  # 严格消息分页与 V2 fake 语义
        "test_input_protocol.py",      # 输入调度/park/FIFO及严格观察
        "test_input_wire.py",          # PATCH/DELETE真实loopback HTTP契约
        "test_managed_input_cli.py", # S4托管CLI与旧send兼容
        "test_managed_input_web.py", # S4Web输入及责任诊断
        "test_offline_discovery.py",   # 自动发现与分层清单恰好覆盖、不运行真实E2E
        "test_boundaries.py",           # 架构边界回归（静态契约/单元均可）
        "test_manual_gate_unlock.py",   # manual-gate 显式解锁 CLI 契约
        "test_manual_turn_recovery.py", # 回合 hold 显式解除/完成登记应用用例
        "test_email_config.py",         # 本机邮件策略严格配置与默认关闭CLI
        "test_work_mode.py",           # 提示词工作模式（默认fast/别名/prompt/Web返工）
        "test_turn_hold_web.py",        # 回合 hold Web 展示/候选/登记/强制解除
        "test_coordinator_control_cli.py", # 显式对话控制 CLI 与旧入口兼容
        "test_delivery_policy_entrypoints.py", # CLI/Web严格切换/覆盖投影/禁自动授权
        "test_operator_turn_web.py",       # 显式用户输入API/CLI/只读/伪来源拒绝
        "test_prototype_web.py",           # 原型CLI/Web共享记录与历史只读对账
        "test_agently_mail_contract.py", # 邮件CLI适配器离线契约，无真实邮件
        "test_compatibility_facades.py",  # 旧导入与 mock.patch 契约
    ],
}


def resolve_layer(layer: str) -> list[str]:
    """返回该分层实际存在的测试文件路径（绝对路径列表）。"""
    return [str(TESTS / name) for name in LAYERS[layer] if (TESTS / name).exists()]


def runner_command(runner: str, layer: str, files: list[str]) -> list[str]:
    if runner == "pytest":
        cmd = [sys.executable, "-B", "-m", "pytest"]
        return cmd + (files if layer != "all" else [str(TESTS)]) + ["-v"]
    # unittest（默认，与历史基准确认行为一致）
    if layer == "all":
        return [sys.executable, "-B", "-m", "unittest", "discover",
                "-s", "tests", "-p", "test_*.py", "-v"]
    return [sys.executable, "-B", "-m", "unittest"] + files + ["-v"]


def skip_notes(layer: str, files: list[str]) -> list[str]:
    """列出分层集合中尚未落地、本轮被跳过的文件。"""
    landed = {Path(f).name for f in files}
    return [name for name in LAYERS.get(layer, [])
            if name not in landed]


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="oc-orch 分层离线回归入口")
    p.add_argument("--layer", choices=sorted(set(LAYERS) | {"all"}), default="all")
    p.add_argument("--runner", choices=["unittest", "pytest"], default="unittest",
                   help="运行器；pytest 为已声明开发工具（需先安装 dev 依赖并锁定）")
    args = p.parse_args(argv)

    files = resolve_layer(args.layer) if args.layer != "all" else []
    skipped = skip_notes(args.layer, files) if args.layer != "all" else []
    if skipped:
        p.error("分层清单缺少测试文件：" + ", ".join(skipped))
    name_map = {"unit": "单元", "integration": "集成", "contract": "契约", "all": "全部"}

    preferred = Path("/tmp/opencode")
    base = str(preferred) if preferred.is_dir() else None
    with tempfile.TemporaryDirectory(prefix="oc-orch-offline-", dir=base) as tmp:
        # 只占用、不监听：避免测试误连碰巧运行在固定端口上的真实服务。
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reserved:
            reserved.bind(("127.0.0.1", 0))
            service = Path(tmp) / "service.json"
            service.write_text(json.dumps({
                "hostname": "127.0.0.1",
                "port": reserved.getsockname()[1],
                "password": "",
            }), encoding="utf-8")
            service.chmod(0o600)
            env = os.environ.copy()
            env.update({
                "OC_ORCH_SERVICE_CONFIG": str(service),
                "OC_ORCH_HOME": str(Path(tmp) / "state"),
                "TMPDIR": tmp,
                "PYTHONDONTWRITEBYTECODE": "1",
                "NO_PROXY": "127.0.0.1,localhost,::1",
                "no_proxy": "127.0.0.1,localhost,::1",
            })
            print(f"离线分层[{name_map[args.layer]}]（{args.runner}）：临时 SQLite + "
                  f"loopback Web；不包含真实 E2E。", flush=True)
            command = runner_command(args.runner, args.layer, files)
            return subprocess.run(command, cwd=ROOT, env=env, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
