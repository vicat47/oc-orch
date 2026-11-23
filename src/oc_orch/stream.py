"""oc-orch.stream 兼容门面：实际实现迁移至 oc_orch.runtime.stream。

与 oc_orch.runtime.stream 保持同一模块身份，import 路径与 monkeypatch
均指向权威实现。
"""

import sys as _sys

import oc_orch.runtime.stream as _implementation

_sys.modules[__name__] = _implementation