"""oc-orch 控制面前端静态资源（随 wheel/sdist 分发，运行期无 Node/npm/CDN）。

生成物由 `web/build.mjs` 输出到本包，与 `manifest.json`（含文件 sha256）一起随包
打包。读取必须走固定白名单 + importlib.resources：严禁任意路径遍历；文件系统
布局下显式拒绝 symlink 越界；浏览器只允许 `HTTP_WHITELIST`（app.js/app.css）。
`index.html` 由 `/` 经 `HTML_PAGE` 直接提供，`manifest.json` 与许可证文本不对外
暴露为浏览器资源。
"""

from __future__ import annotations

import hashlib
import json
import os
from importlib import resources

# 浏览器可访问的固定白名单（相对文件名，不含路径分隔符）。
HTTP_WHITELIST: tuple[str, ...] = ("app.js", "app.css")
# 随包分发的全部文件（含页面外壳、清单与许可证）。
MANIFESTED: tuple[str, ...] = (
    "app.js",
    "app.css",
    "index.html",
    "THIRD_PARTY_LICENSES.txt",
    "manifest.json",
)

_HTTP_TO_TYPE: dict[str, str] = {
    "app.js": "text/javascript; charset=utf-8",
    "app.css": "text/css; charset=utf-8",
}


def _check(name: str) -> None:
    """白名单 + 形状校验：拒绝路径分隔符/穿越/隐藏文件/空名。"""
    if (name not in MANIFESTED or name in (".", "..", "")
            or "/" in name or "\\" in name or "\x00" in name):
        raise ValueError("web asset 不在白名单: %r" % (name,))


def _base(name: str):
    return resources.files("oc_orch.entrypoints.web_assets").joinpath(name)


def read_bytes(name: str) -> bytes:
    """读取白名单内资源字节；缺失或 symlink 越界一律拒绝。"""
    _check(name)
    target = _base(name)
    try:
        is_file = target.is_file()
    except (OSError, ValueError):
        is_file = False
    if not is_file:
        raise FileNotFoundError("web asset 缺失: %s" % name)
    # 文件系统布局下显式拒绝 symlink（压缩包内无 fspath/符号链接语义）。
    try:
        if os.path.islink(os.fspath(target)):
            raise ValueError("web asset symlink 拒绝: %s" % name)
    except (OSError, TypeError):
        pass
    return target.read_bytes()


def read_text(name: str) -> str:
    return read_bytes(name).decode("utf-8")


def content_type(name: str) -> str:
    return _HTTP_TO_TYPE[name]


def index_html() -> str:
    """页面外壳（与 / 响应的 HTML_PAGE 同一内容）；缺失时由调用方决定兜底。"""
    return read_text("index.html")


def manifest() -> dict:
    return json.loads(read_text("manifest.json"))


def digest(name: str) -> str:
    return hashlib.sha256(read_bytes(name)).hexdigest()


def iter_names() -> tuple[str, ...]:
    """包内存放的全部随包文件；供打包/一致性检查使用。"""
    return MANIFESTED