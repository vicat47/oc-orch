"""兼容旧导入路径；结果收集/检查 I/O 与验收兼容入口位于 adapters.results。"""

import sys
from .adapters import results as _implementation

sys.modules[__name__] = _implementation