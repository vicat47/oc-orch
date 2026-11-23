"""兼容旧导入路径；唤醒抑制策略唯一实现位于 domain.wake_policy。"""

import sys
from .domain import wake_policy as _implementation

sys.modules[__name__] = _implementation