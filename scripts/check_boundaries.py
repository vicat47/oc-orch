#!/usr/bin/env python3
"""标准库 AST 依赖守卫：阻止业务层反向依赖传输/SQLite 和外层直接 SQL。"""

from __future__ import annotations

import ast
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1] / "src" / "oc_orch"
LAYERS = {"domain", "application", "adapters", "runtime", "entrypoints"}
ALLOWED = {
    "domain": {"domain"},
    "application": {"domain", "application"},
    "adapters": {"domain", "application", "adapters", "util"},
    "runtime": {"domain", "application", "runtime", "composition", "util"},
    "entrypoints": {"domain", "application", "entrypoints", "runtime", "composition", "util"},
}
IO_MODULES = {"sqlite3", "urllib", "http", "httpx", "httpx_sse", "subprocess", "socket"}
PACKAGE_CONSTANTS = {"__version__", "SCHEMA_VERSION"}


def violations(source: str, module: str) -> list[str]:
    layer = module.split(".")[1] if "." in module else ""
    if layer not in LAYERS:
        return []
    tree = ast.parse(source)
    errors = []
    for node in ast.walk(tree):
        names = []
        if isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                parts = module.split(".")[:-node.level]
                name = ".".join(parts + ([node.module] if node.module else []))
                names = [name] if node.module else [name + "." + a.name for a in node.names]
            else:
                names = (["oc_orch." + a.name for a in node.names]
                         if node.module == "oc_orch" else [node.module or ""])
        for name in names:
            if name.startswith("oc_orch."):
                target = name.split(".")[1]
                if target not in ALLOWED[layer] and target not in PACKAGE_CONSTANTS:
                    errors.append(f"{module}:{node.lineno}: {layer} -> {name}")
            elif layer in ("domain", "application") and name.split(".")[0] in IO_MODULES:
                errors.append(f"{module}:{node.lineno}: {layer} 不得使用 I/O 实现 {name}")
        if layer != "adapters" and isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute) and node.func.attr in ("execute", "executemany", "executescript", "cursor"):
                errors.append(f"{module}:{node.lineno}: SQL 只能位于 adapters")
    return errors


def check(root: Path = ROOT) -> list[str]:
    errors = []
    for path in sorted(root.rglob("*.py")):
        module = "oc_orch." + ".".join(path.relative_to(root).with_suffix("").parts)
        errors.extend(violations(path.read_text(encoding="utf-8"), module))
    return errors


def main() -> int:
    errors = check()
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print("依赖方向与 SQL 边界检查通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
