"""通用工具：时间、JSON、路径、原子写、脱敏、指纹。"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import time


def now_ms() -> int:
    return int(time.time() * 1000)


def now_s() -> int:
    return int(time.time())


def iso(ts_ms: int | None) -> str | None:
    if not ts_ms:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(ts_ms / 1000))


def json_dumps(value, **kwargs) -> str:
    kwargs.setdefault("ensure_ascii", False)
    kwargs.setdefault("separators", (",", ":"))
    kwargs.setdefault("sort_keys", True)
    return json.dumps(value, **kwargs)


def json_loads(text: str | None, default=None):
    if not text:
        return default
    try:
        return json.loads(text)
    except (TypeError, ValueError):
        return default


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_file(path: str, chunk: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            data = f.read(chunk)
            if not data:
                break
            h.update(data)
    return h.hexdigest()


def atomic_write(path: str, data: str | bytes, mode: int = 0o600) -> None:
    """同目录临时文件 + os.replace，避免读到半截内容。"""
    os.makedirs(os.path.dirname(os.path.abspath(path)), mode=0o700, exist_ok=True)
    binary = isinstance(data, bytes)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=os.path.dirname(os.path.abspath(path)))
    try:
        with os.fdopen(fd, "wb" if binary else "w", encoding=None if binary else "utf-8") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def default_home() -> str:
    env = os.environ.get("OC_ORCH_HOME")
    if env:
        return os.path.abspath(os.path.expanduser(env))
    base = os.environ.get("XDG_DATA_HOME")
    if not base:
        base = os.path.join(os.path.expanduser("~"), ".local", "share")
    return os.path.join(base, "opencode-orchestrator")


def ensure_home(path: str) -> str:
    os.makedirs(path, mode=0o700, exist_ok=True)
    try:
        os.chmod(path, 0o700)
    except OSError:
        pass
    return path


def service_config_path() -> str:
    return os.environ.get(
        "OC_ORCH_SERVICE_CONFIG",
        os.path.expanduser("~/.config/opencode/service.json"),
    )


def normalize_path(path: str, base: str | None = None) -> str:
    path = os.path.expanduser(path)
    if not os.path.isabs(path):
        path = os.path.join(base or os.getcwd(), path)
    return os.path.realpath(os.path.normpath(path))


def path_prefix(path: str) -> str:
    """把 glob 的前缀部分转为真实路径，用于写入范围冲突判断。"""
    path = os.path.expanduser(path)
    for marker in ("*", "?", "["):
        idx = path.find(marker)
        if idx >= 0:
            path = path[:idx]
    path = path.rstrip(os.sep) or os.sep
    return os.path.realpath(os.path.normpath(path))


def paths_overlap(a: str, b: str) -> bool:
    """目录/文件前缀冲突：相同或一方位于另一方之下。"""
    a = path_prefix(os.path.abspath(a))
    b = path_prefix(os.path.abspath(b))
    if a == b:
        return True
    try:
        return a.startswith(b + os.sep) or b.startswith(a + os.sep)
    except ValueError:  # 不同盘符等
        return False


_SECRET_PATTERNS = [
    re.compile(r"(?i)(authorization\s*:\s*basic\s+)[A-Za-z0-9+/=]+"),
    re.compile(r"(?i)(password\"?\s*[:=]\s*\")[^\"]+"),
    re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._\-]+"),
]


def redact(text: str, limit: int = 4000) -> str:
    if text is None:
        return ""
    out = text
    for pat in _SECRET_PATTERNS:
        out = pat.sub(r"\1<REDACTED>", out)
    if len(out) > limit:
        out = out[:limit] + f"...<truncated {len(out) - limit} chars>"
    return out


def short_id(prefix: str, length: int = 12) -> str:
    return prefix + os.urandom(length // 2 + 1).hex()[:length]


def utc_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
