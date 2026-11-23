#!/usr/bin/env python3
"""托管发送示例（S4）：默认只预览，--apply 才真正执行。

只作为用法演示：显式调用 oc-orch 顶层 ``send``（默认 ``send_tracked``）入口，
业务与全部外部 I/O 都走共享 application 用例。本脚本不等同于真实外部
production 脚本已经接入；真实执行与读取生产账本需另行获得用户授权。

安全约束：
- 默认 dry-run：只打印将执行的 CLI 命令与参数，不触网、不落库。
- 必须同时提供 ``--home``（显式演练/临时台账目录）与 ``--apply`` 才会真正执行。
- ``work_kind`` 未声明（None）时不猜只读：封存/释放的目标会由 CLI/用例拒绝，
  需显式 ``observe``/``read_only``，或经 rework 重新认领写范围。
- ``read_only`` 与 ``observe`` 语义相同；CLI ``--work-kind`` 已支持 read_only，
  脚本按用户输入原样透传，不再映射。
- 显式 ``--untracked`` 是纯通信：不登记责任/不授予写权，并展示未托管边界。
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="managed_input_send",
        description="托管发送示例：默认 dry-run 预览，--apply 才真正执行。")
    p.add_argument("--home", required=True,
                   help="显式台账目录（演练/临时 home；不要传生产目录）")
    p.add_argument("--apply", action="store_true",
                   help="真正执行（默认仅预览命令与参数）")
    p.add_argument("--session", required=True)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--text")
    g.add_argument("--file")
    p.add_argument("--message-id",
                   help="原 message ID（重试对账必须复用；不换身份重投）")
    p.add_argument("--delivery", choices=["queue", "steer"], default="queue")
    p.add_argument("--owner", help="发起责任人；可推断时省略")
    p.add_argument("--task", help="已托管任务归属（歧义时必须显式）")
    p.add_argument("--attempt", help="当前尝试归属（歧义时必须显式）")
    p.add_argument("--continuation-target",
                   help="执行结束后的接续目标 session（通常为协调者）")
    p.add_argument("--deadline", type=int,
                   help="责任处理期限（毫秒时间戳）；跨重启保存")
    p.add_argument("--material", action="append",
                   help="可定位材料引用（可重复）；只保存引用，不读取内容")
    p.add_argument("--work-kind", choices=["observe", "read_only", "write"],
                   default=None,
                   help="显式工作性质；缺省不猜只读/写。封存/释放目标需 "
                        "observe/read_only 或 rework")
    p.add_argument("--untracked", action="store_true",
                   help="显式纯通信：不登记责任/不授予写权")
    return p


def _invoke_cli(argv: list[str]) -> int:
    env = dict(os.environ)
    src = str(_repo_root() / "src")
    env["PYTHONPATH"] = src + (os.pathsep + env["PYTHONPATH"]
                               if env.get("PYTHONPATH") else "")
    return subprocess.run([sys.executable, "-B", "-m", "oc_orch.cli", *argv],
                          env=env).returncode


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    if args.untracked and args.work_kind == "write":
        print("错误：--untracked 纯通信不得声明 --work-kind write（不授予写权）",
              file=sys.stderr)
        return 2

    cli_args = ["--home", args.home, "send", "--session", args.session,
                "--delivery", args.delivery]
    if args.text:
        cli_args += ["--text", args.text]
    if args.file:
        cli_args += ["--file", args.file]
    for flag, value in (("--message-id", args.message_id),
                        ("--owner", args.owner), ("--task", args.task),
                        ("--attempt", args.attempt),
                        ("--continuation-target", args.continuation_target),
                        ("--deadline", args.deadline)):
        if value is not None:
            cli_args += [flag, str(value)]
    for material in args.material or ():
        cli_args += ["--material", material]
    if args.work_kind is not None:
        cli_args += ["--work-kind", args.work_kind]
    if args.untracked:
        cli_args += ["--untracked"]

    print("预览：python -m oc_orch.cli " + " ".join(cli_args), file=sys.stderr)
    if not args.apply:
        print("（dry-run：未执行任何发送。加 --apply 才会真正提交，且 --home "
              "必须是显式演练目录）", file=sys.stderr)
        return 0

    code = _invoke_cli(cli_args)
    print("（示例执行完成：受理/回执不代表已投递/执行/业务 ACK；请用 "
          "`oc-orch input list/show` 与 `oc-orch responsibility list` 核对）",
          file=sys.stderr)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
