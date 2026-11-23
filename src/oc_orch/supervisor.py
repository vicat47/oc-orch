"""oc-orch.supervisor 兼容门面：实际实现迁移至 oc_orch.runtime.supervisor。

用 sys.modules 替换保持一致身份：`import oc_orch.supervisor` 与
`import oc_orch.runtime.supervisor` 得到同一模块对象，monkeypatch 与
supervisor_status 读取均指向权威实现。
"""

import sys as _sys

import oc_orch.runtime.supervisor as _implementation

_sys.modules[__name__] = _implementation