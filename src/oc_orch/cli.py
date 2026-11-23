"""oc-orch.cli 兼容门面：实际实现迁移至 oc_orch.entrypoints.cli。

本模块用 sys.modules 替换保持一致身份：`import oc_orch.cli` 与
`import oc_orch.entrypoints.cli` 得到同一模块对象，monkeypatch 与
`python -m oc_orch.cli` 均指向权威实现。
"""

import sys as _sys
from oc_orch.entrypoints import cli as _cli

_sys.modules[__name__] = _cli

if __name__ == "__main__":
    raise SystemExit(_cli.main())