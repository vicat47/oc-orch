"""兼容旧导入路径；SQLite 台账连接/事务唯一实现位于 adapters.sqlite_db。"""

import sys
from .adapters import sqlite_db as _implementation

sys.modules[__name__] = _implementation