"""oc-orch.web 兼容门面：实际实现迁移至 oc_orch.entrypoints.web。

与 oc_orch.entrypoints.web 保持同一模块身份，import 路径与 monkeypatch
均指向权威实现。
"""

import sys as _sys
from oc_orch.entrypoints import web as _web

_sys.modules[__name__] = _web