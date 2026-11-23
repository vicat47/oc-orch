"""兼容旧导入路径；分类逻辑唯一实现位于 domain.classify。"""

import sys
from .domain import classify as _implementation

sys.modules[__name__] = _implementation