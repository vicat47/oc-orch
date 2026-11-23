"""oc-orch Web 控制面页面（Lit 增量版：本地资源外壳）。

页面本体由 `web/build.mjs` 构建为独立前端资源并提交到本包
（`web_assets/`：app.js / app.css / index.html / 许可证 / manifest.json），
随 wheel/sdist 分发；运行期无 Node/npm/CDN 依赖。

本模块只保留兼容入口 `HTML_PAGE`：即随包分发的 `index.html` 外壳
（相对原内联版大幅变小，仅引用同源 `/assets/app.css` 与 `/assets/app.js`）。
`HTML_PAGE` 的内容与 `web_assets.index_html()` 完全一致，保证开发期与
wheel 安装后页面相同。

资源契约：产物缺失视为打包缺陷，导入即抛错（失败快、不进入“加载中”挂起），
禁止用宽泛 except 兜底掩盖漏包。
"""

from __future__ import annotations

from . import web_assets

__all__ = ["HTML_PAGE"]


def _load_shell() -> str:
    try:
        return web_assets.index_html()
    except FileNotFoundError as e:
        raise ImportError(
            "oc-orch 控制面前端资源缺失：%s。请在仓库 web/ 下执行 "
            "`npm ci --ignore-scripts && npm run build`，并确保产物随包分发。" % e
        ) from e


HTML_PAGE = _load_shell()
