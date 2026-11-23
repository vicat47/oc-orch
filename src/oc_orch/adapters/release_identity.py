"""仅报告实际加载源码的来源；不以版本号、外部入口或Git HEAD冒充同版。"""

import hashlib
import json
from pathlib import Path

from .. import __version__, SCHEMA_VERSION
from .writer_guard import GUARD_KEY, check_writer_guard


def runtime_identity(conn, *, root: Path | None = None) -> dict:
    source_root = root if root is not None else Path(__file__).resolve().parents[3]
    result = {"package_version": __version__, "schema_version": SCHEMA_VERSION,
              "release_id": None, "manifest_verified": False, "source": "development-or-unmanifested",
              "writer_contract": None, "writer_guard_valid": False,
              "delivery_policy_guard_valid": False,
              "native_trusted_chat_enabled": False, "email_default_enabled": False}
    try:
        marker = conn.execute("SELECT value FROM meta WHERE key=?", (GUARD_KEY,)).fetchone()
        result["writer_contract"] = marker[0] if marker else None
        result["writer_guard_valid"] = check_writer_guard(conn)
    except Exception:
        result["writer_guard_valid"] = False
    try:
        from .delivery_guard import check_delivery_guard

        result["delivery_policy_guard_valid"] = check_delivery_guard(conn)
    except Exception:
        result["delivery_policy_guard_valid"] = False
    manifest_path = source_root / "RELEASE.json"
    if not manifest_path.is_file():
        return result
    result["source"] = "release-manifest-unverified"
    try:
        manifest = json.loads(manifest_path.read_text())
        files = manifest["runtime_files"]
        if not isinstance(files, dict) or not files or len(files) > 1000:
            return result
        for name, sha in files.items():
            path = source_root / name
            if (not isinstance(name, str) or Path(name).is_absolute()
                    or ".." in Path(name).parts or not path.resolve().is_relative_to(source_root.resolve())
                    or not isinstance(sha, str) or hashlib.sha256(path.read_bytes()).hexdigest() != sha):
                return result
        identity = hashlib.sha256(json.dumps(files, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        if identity != manifest.get("runtime_manifest_sha256"):
            return result
        release_id = manifest.get("release_id")
        if not isinstance(release_id, str) or not release_id.strip():
            return result
        result.update(source="verified-release-manifest", release_id=release_id,
                      manifest_verified=True,
                      email_default_enabled=manifest.get("email_enabled") is True)
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return result
