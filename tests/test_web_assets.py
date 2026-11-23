#!/usr/bin/env python3
"""前端随包静态资源契约：白名单读取、loopback /assets/* 服务、manifest 可复现、
HTML 外壳一致性与 wheel 仓库外烟测。

安全要求：
- 读取走 importlib.resources + 固定白名单，严禁任意路径遍历/symlink；
  浏览器只暴露 app.js/app.css（index/manifest/license 不对外提供字节）。
- /assets/* 经 Host 守卫、nosniff、no-store 与正确 MIME/Content-Length 返回。
- HTML_PAGE 与打包 index.html 完全一致（页面外壳随 wheel 分发，无 CDN）。
- manifest.json 不含自包含摘要与时间戳，sha256 与真实文件逐字节一致。

wheel 仓库外烟测默认跳过（需要网络装构建工具），设置 OC_ORCH_WEB_WHEEL_SMOKE=1
启用；离线回归不受影响。
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import urllib.error
import urllib.request
from pathlib import Path

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from oc_orch.entrypoints import web_assets  # noqa: E402
from oc_orch.entrypoints.web_page import HTML_PAGE  # noqa: E402
from oc_orch.web import WebServer  # noqa: E402


def _package_dir() -> Path:
    return Path(web_assets.__file__).resolve().parent


class AssetLoaderContract(unittest.TestCase):
    def test_manifested_files_exist_and_not_symlink(self):
        pkg = _package_dir()
        for name in web_assets.MANIFESTED:
            p = pkg / name
            self.assertTrue(p.is_file(), "%s 缺失" % name)
            self.assertFalse(p.is_symlink(), "%s 不得为 symlink" % name)

    def test_whitelist_rejects_traversal_and_foreign_names(self):
        # 禁止任意路径遍历/别名；未随包分发的名字一律拒绝
        bad = ["..", ".", "", "../web_page.py", "../../web.py", "a/b",
               "a\\b", "web_page.py", "package-lock.json",
               "\x00app.js", "app.js\x00"]
        for name in bad:
            with self.assertRaises(ValueError, msg=name):
                web_assets.read_bytes(name)
        # 随包文件（含外壳/清单/许可证）可被内部读取（HTML_PAGE/manifest 校验用）
        for name in web_assets.MANIFESTED:
            self.assertTrue(web_assets.read_bytes(name), name)

    def test_http_whitelist_is_app_js_and_app_css(self):
        self.assertEqual(tuple(web_assets.HTTP_WHITELIST), ("app.js", "app.css"))
        # 浏览器只暴露 app.js/app.css；index/manifest/license 不提供字节给浏览器
        for name in web_assets.MANIFESTED:
            if name not in web_assets.HTTP_WHITELIST:
                self.assertNotIn(name, web_assets.HTTP_WHITELIST)
                with self.assertRaises(KeyError, msg=name):
                    web_assets.content_type(name)  # 未入浏览器白名单即无 MIME

    def test_readable_bytes_match_disk(self):
        for name in ("app.js", "app.css"):
            disk = (_package_dir() / name).read_bytes()
            self.assertEqual(web_assets.read_bytes(name), disk)
            self.assertEqual(web_assets.content_type(name), (
                "text/javascript; charset=utf-8" if name.endswith(".js")
                else "text/css; charset=utf-8"))

    def test_html_page_equals_packaged_index_html(self):
        self.assertEqual(HTML_PAGE, web_assets.index_html())
        self.assertIn("/assets/app.js", HTML_PAGE)
        self.assertIn("/assets/app.css", HTML_PAGE)
        self.assertNotIn("src=\"http", HTML_PAGE, "不得引用外部脚本")
        self.assertNotIn("href=\"http", HTML_PAGE, "不得引用外部样式")

    def test_manifest_sha256_matches_bytes_no_self_hash_no_timestamp(self):
        m = web_assets.manifest()
        self.assertEqual(m["schema"], "oc-orch.web-assets.v1")
        self.assertIsInstance(m["version"], str)
        self.assertNotIn("built_at", m, "manifest 不得含时间戳（保持可复现）")
        self.assertNotIn("manifest.json", m["files"], "manifest 不得自包含摘要")
        for name, meta in m["files"].items():
            self.assertIn(name, ("app.js", "app.css", "index.html",
                                 "THIRD_PARTY_LICENSES.txt"))
            self.assertEqual(meta["sha256"],
                             hashlib.sha256(web_assets.read_bytes(name)).hexdigest(),
                             "%s 哈希不符" % name)
            self.assertEqual(meta["size"], len(web_assets.read_bytes(name)),
                             "%s size 不符" % name)
            self.assertNotIn("/", name, "manifest 路径必须相对可复现")

    def test_packaging_declares_assets(self):
        # wheel 经 package-data、sdist 经 MANIFEST.in 携带资源
        pyproject = (Path(ROOT) / "pyproject.toml").read_text(encoding="utf-8")
        self.assertIn("oc_orch.entrypoints.web_assets", pyproject)
        self.assertIn("*.js", pyproject)
        self.assertIn("*.json", pyproject)
        manifest_in = Path(ROOT) / "MANIFEST.in"
        self.assertTrue(manifest_in.is_file(), "sdist 需要 MANIFEST.in")
        self.assertIn("web_assets", manifest_in.read_text(encoding="utf-8"))


class AssetServingContract(unittest.TestCase):
    def setUp(self):
        import os as _os
        self.tmp = tempfile.mkdtemp(prefix="web-assets-", dir="/tmp/opencode")
        self.addCleanup(lambda: shutil.rmtree(self.tmp, ignore_errors=True))
        self.server = WebServer(self.tmp, port=0)
        self.port = self.server.start()
        _os.makedirs(_os.path.join(self.tmp, "state"), exist_ok=True)

    def tearDown(self):
        self.server.stop()

    def req(self, path, headers=None):
        h = dict(headers or {})
        r = urllib.request.Request(
            "http://127.0.0.1:%d%s" % (self.port, path), headers=h)
        try:
            with urllib.request.urlopen(r, timeout=15) as resp:
                return resp.status, resp.read(), dict(resp.headers)
        except urllib.error.HTTPError as e:
            return e.code, e.read(), dict(e.headers)

    def test_page_and_assets_served_with_security_headers(self):
        st, body, headers = self.req("/")
        self.assertEqual(st, 200)
        self.assertEqual(body.decode("utf-8"), HTML_PAGE)
        self.assertEqual(headers["Cache-Control"], "no-store")
        self.assertEqual(headers["X-Content-Type-Options"], "nosniff")
        for name, ctype in (("app.js", "text/javascript; charset=utf-8"),
                            ("app.css", "text/css; charset=utf-8")):
            st, body, headers = self.req("/assets/" + name)
            self.assertEqual(st, 200, name)
            self.assertEqual(headers["Content-Type"], ctype, name)
            self.assertEqual(int(headers["Content-Length"]), len(body), name)
            self.assertEqual(headers["Cache-Control"], "no-store", name)
            self.assertEqual(headers["X-Content-Type-Options"], "nosniff", name)
            self.assertEqual(web_assets.read_bytes(name), body, name)

    def test_traversal_and_foreign_paths_rejected(self):
        for path in ("/assets/../web_page.py", "/assets/app.js/..",
                     "/assets/%2e%2e/web_page.py", "/assets/%2E%2E%2Fweb_page.py",
                     "/assets/web_page.py", "/assets/package-lock.json",
                     "/assets/manifest.json", "/assets/index.html",
                     "/assets/THIRD_PARTY_LICENSES.txt", "/assets/../.git/config"):
            st, body, _ = self.req(path)
            self.assertIn(st, (400, 404), path)
            self.assertNotIn(b"<!doctype", body[:128], "不得回退到页面")
            payload = json.loads(body.decode("utf-8"))
            self.assertFalse(payload["ok"], path)

    def test_host_guard_for_assets(self):
        st, _, _ = self.req("/assets/app.js", headers={"Host": "evil.example"})
        self.assertEqual(st, 403)

    def test_favicon_and_root_no_assets_leak(self):
        st, body, _ = self.req("/assets/")
        self.assertEqual(st, 404)

    def test_packaged_bundle_has_no_unsafe_html(self):
        bundle = web_assets.read_bytes("app.js").decode("utf-8")
        self.assertNotIn("unsafeHTML", bundle, "禁止 unsafeHTML 字符串式渲染")
        self.assertIn("refreshLight", bundle, "打包含单飞刷新导出")
        self.assertIn("hashchange", bundle, "打包含 hash 路由")

    def test_bundle_includes_views_not_placeholder_only(self):
        bundle = web_assets.read_bytes("app.js").decode("utf-8")
        for needle in ("renderOverview", "renderCoordinator", "renderTask",
                       "renderDiagnostics", "renderTasksView"):
            self.assertIn(needle, bundle, "视图必须静态内联进 bundle")


def _pip_network_env():
    """隔离构建使用显式 PyPI 索引，避免继承本机代理配置。"""
    env = os.environ.copy()
    for key in ("http_proxy", "https_proxy", "all_proxy", "HTTP_PROXY",
                "HTTPS_PROXY", "ALL_PROXY"):
        env.pop(key, None)
    return env


PIP_INDEX = "https://pypi.org/simple"


class WheelSmoke(unittest.TestCase):
    """仓库外 wheel + sdist 烟测：临时 venv 装构建工具→构建 wheel/sdist→干净 venv
    安装→importlib.resources 与 /assets 资源 GET 可达。

    默认跳过（需要网络与磁盘构建）；**显式设置 OC_ORCH_WEB_WHEEL_SMOKE=1 后任何
    真实构建/安装/服务失败都是测试失败**（网络失败也记为失败），不把失败当 skip
    造成假绿；失败消息保留完整 pip/build 输出，便于复现与核对版本。
    """

    @unittest.skipUnless(os.environ.get("OC_ORCH_WEB_WHEEL_SMOKE"),
                         "设置 OC_ORCH_WEB_WHEEL_SMOKE=1 运行 wheel/sdist 仓库外烟测")
    def test_wheel_and_sdist_install_outside_repo_and_serve_assets(self):
        base = Path("/tmp/opencode") if Path("/tmp/opencode").exists() else Path(
            tempfile.gettempdir())
        build_venv = Path(tempfile.mkdtemp(prefix="wheel-build-", dir=str(base)))
        target_venv = Path(tempfile.mkdtemp(prefix="wheel-target-", dir=str(base)))
        self.addCleanup(lambda: shutil.rmtree(build_venv, ignore_errors=True))
        self.addCleanup(lambda: shutil.rmtree(target_venv, ignore_errors=True))

        def run(cmd, **kw):
            kw.setdefault("env", _pip_network_env())
            proc = subprocess.run(cmd, text=True, capture_output=True,
                                  timeout=420, **kw)
            if proc.returncode != 0:
                self.fail("命令失败：%s\n--- stdout ---\n%s\n--- stderr ---\n%s\n"
                          "（显式启用 OC_ORCH_WEB_WHEEL_SMOKE=1 后不可 skip，此为真实失败）"
                          % (" ".join(cmd), proc.stdout, proc.stderr))
            return proc

        p1 = build_venv / "bin" / "python"
        run([sys.executable, "-m", "venv", str(build_venv)])
        run([str(p1), "-m", "pip", "-q", "install", "-U",
             "-i", PIP_INDEX, "setuptools>=68", "wheel", "build"])
        whl_dir = build_venv / "dist"
        # wheel：仓库外构建 + 安装 + 资源服务
        run([str(p1), "-m", "pip", "wheel", "--no-deps", "--no-build-isolation",
             "-w", str(whl_dir), "."], cwd=ROOT)
        wheels = list(whl_dir.glob("oc_orch-*.whl"))
        self.assertEqual(len(wheels), 1, "应恰好产出一个 wheel")
        # sdist：真实构建并检查前端资源随包
        run([str(p1), "-m", "build", "--sdist", "--no-isolation",
             "--outdir", str(whl_dir), "."], cwd=ROOT)
        sdists = list(whl_dir.glob("oc_orch-*.tar.gz"))
        self.assertEqual(len(sdists), 1, "应恰好产出一个 sdist")
        self._assert_sdist_has_assets(sdists[0], wheels[0])

        p2 = target_venv / "bin" / "python"
        run([sys.executable, "-m", "venv", str(target_venv)])
        run([str(p2), "-m", "pip", "-q", "install", "--no-deps", str(wheels[0])],
            cwd=str(target_venv))
        probe = r"""
import sys
from oc_orch.entrypoints import web_assets
from oc_orch.entrypoints.web_page import HTML_PAGE
for name in web_assets.MANIFESTED:
    data = web_assets.read_bytes(name)
    assert data, "empty " + name
assert "app.js" in HTML_PAGE and "app.css" in HTML_PAGE
m = web_assets.manifest()
for name, meta in m["files"].items():
    import hashlib
    assert meta["sha256"] == hashlib.sha256(web_assets.read_bytes(name)).hexdigest()
print("WHEEL-SMOKE-OK")
"""
        proc = subprocess.run([str(p2), "-c", probe], text=True,
                              capture_output=True, timeout=120,
                              cwd=str(target_venv))
        self.assertEqual(proc.returncode, 0, "wheel 仓库外读取失败：\n%s"
                         % (proc.stdout + proc.stderr))
        self.assertIn("WHEEL-SMOKE-OK", proc.stdout)
        # 仓库外启动 Web 并 GET /assets（校验 MIME / Content-Length / HTML 外壳一致性）
        web_probe = r"""
import json, tempfile, urllib.request, shutil
from oc_orch.entrypoints import web_assets
from oc_orch.entrypoints.web_page import HTML_PAGE
from oc_orch.web import WebServer
tmp = tempfile.mkdtemp(prefix="wheel-web-", dir="/tmp/opencode")
try:
    server = WebServer(tmp, port=0)
    port = server.start()
    try:
        with urllib.request.urlopen("http://127.0.0.1:%d/" % port, timeout=15) as r:
            assert r.read().decode("utf-8") == HTML_PAGE
        for name, ct in (("app.js", "text/javascript; charset=utf-8"),
                         ("app.css", "text/css; charset=utf-8")):
            with urllib.request.urlopen("http://127.0.0.1:%d/assets/%s" % (port, name),
                                        timeout=15) as r:
                assert r.headers["Content-Type"] == ct, (name, r.headers["Content-Type"])
                assert int(r.headers["Content-Length"]) == len(web_assets.read_bytes(name))
                assert r.headers["X-Content-Type-Options"] == "nosniff"
    finally:
        server.stop()
finally:
    shutil.rmtree(tmp, ignore_errors=True)
print("WHEEL-WEB-OK")
"""
        proc = subprocess.run([str(p2), "-c", web_probe], text=True,
                              capture_output=True, timeout=120,
                              cwd=str(target_venv))
        self.assertEqual(proc.returncode, 0, "wheel 仓库外 Web/asset 失败：\n%s"
                         % (proc.stdout + proc.stderr))
        self.assertIn("WHEEL-WEB-OK", proc.stdout)

    @staticmethod
    def _assert_sdist_has_assets(sdist: Path, wheel: Path):
        import tarfile
        import zipfile
        want = {f"oc_orch/entrypoints/web_assets/{n}" for n in (
            "__init__.py", "app.js", "app.css", "index.html",
            "THIRD_PARTY_LICENSES.txt", "manifest.json")}
        with tarfile.open(sdist, "r:gz") as tf:
            # setuptools sdist 为 <pkg>-<version>/[src/]oc_orch/entrypoints/...：
            # 归一化定位到 oc_orch/entrypoints 后再比较
            names = set()
            for n in tf.getnames():
                marker = "oc_orch/entrypoints"
                idx = n.find(marker)
                if idx >= 0:
                    names.add(n[idx:])
        missing = want - names
        assert not missing, "sdist 缺少随包资源：%s" % sorted(missing)
        with zipfile.ZipFile(wheel) as zf:
            names = set(zf.namelist())
        missing = want - names
        assert not missing, "wheel 缺少随包资源：%s" % sorted(missing)


if __name__ == "__main__":
    unittest.main(verbosity=2)