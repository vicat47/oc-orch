"""兼容旧导入路径；配置加载唯一实现位于 adapters.config。"""

import sys
from .adapters import config as _implementation

sys.modules[__name__] = _implementation