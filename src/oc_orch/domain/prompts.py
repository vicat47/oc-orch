"""派单提示值的构造；仅消费已确定的任务与尝试身份。

工作模式只写入提示词提示，不改变模型/资源/费用授权；默认 latency-first。
"""

from .work_mode import render_work_mode


def build_prompt(task: dict, attempt_id: str, generation: int, directory: str,
                 scope: list[str], deliverables: list[str],
                 user_prompt: str | None, *,
                 work_mode: str = "latency-first") -> str:
    header = (
        f"你是执行 session（任务 {task['id']} / 尝试 {attempt_id} / "
        f"第 {generation} 次尝试）。\n"
        f"工作目录：{directory}\n"
        f"允许写入范围：{', '.join(scope) if scope else directory}\n"
        f"任务目标：{task['objective'] or task['title']}\n"
        f"验收标准：{'；'.join(task['acceptance']) if task['acceptance'] else '由主协调定义'}\n"
        f"预期交付物：{', '.join(deliverables) if deliverables else '按任务说明'}\n"
    )
    body = user_prompt.strip() if user_prompt else (
        "请完成上述任务，并遵守项目规范与修改边界。\n"
        "只在授权范围内修改；需要扩大范围、改变公共接口或遇到阻塞时，先报告。\n"
    )
    footer = (
        "\n---\n"
        "回报格式（供主协调验收）：完成摘要、修改文件、验证命令及结果、"
        "未解决问题、交付物路径、可供后续任务使用的接口或结论。\n"
        "验收由主协调执行；不要仅凭本回合成功就声称任务已验收通过。\n"
        f"任务标识：{task['id']}；尝试标识：{attempt_id}。请在回报中保留这两个标识。\n"
    )
    mode = render_work_mode(work_mode)
    return header + "\n" + mode + "\n\n" + body + footer
