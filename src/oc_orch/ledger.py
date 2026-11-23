"""兼容旧导入路径；台账唯一实现位于 adapters.sqlite_ledger。"""

import sys
from .adapters import sqlite_ledger as _implementation

sys.modules[__name__] = _implementation
