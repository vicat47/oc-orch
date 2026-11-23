"""OpenCode V2 客户端兼容门面（adapter facade）。

`oc_orch.client` 通过 sys.modules 别名指向 `oc_orch.adapters.opencode` 实现模块，
保证既有 `import oc_orch.client` / `from oc_orch.client import ...` 与
`mock.patch("oc_orch.client.X")` 指向同一模块对象；实现、错误分类
（ApiError/TransportError/StreamError）与 HTTP/SSE 契约均位于 adapters/opencode.py。
本门面不反向被 adapters 导入（依赖方向由 scripts/check_boundaries.py 守卫）。
"""

from __future__ import annotations

import sys as _sys

import oc_orch.adapters.opencode as _implementation

_sys.modules[__name__] = _implementation