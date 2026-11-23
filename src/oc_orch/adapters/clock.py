"""真实系统时钟实现：默认装配由组合根引入。

wall = time.time()*1000（重启后可解释，用于持久 deadline）；
monotonic = time.monotonic()*1000（单进程等待间隔）；二者不得混用。
"""

from __future__ import annotations

import time


class SystemClock:
    def now_ms(self) -> int:
        return int(time.time() * 1000)

    def monotonic(self) -> int:
        return int(time.monotonic() * 1000)

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)