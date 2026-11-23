"""D5 扫描整合回归：递归内容、glob 稳定性、实际枚举预算和链接边界。"""
import os
import json
from pathlib import Path
from unittest.mock import patch

from tests.test_provenance_remediation import RemediationBase, CFG
from tests.fakes import complete_execution
from oc_orch.adapters import results
from oc_orch.ledger import LedgerError


class ScanIntegrityTests(RemediationBase):
    def scan(self, target, kind="dir", entries=10000):
        return results._scan_artifact_manifest(
            str(Path(self.site) / target), kind,
            results._scan_budget_state({"deliverable_scan_max_entries": entries}))

    def test_nested_content_changes_provenance_and_verify_rechecks_it(self):
        _, att, out = self._collected(
            deliverables=[{"kind": "dir", "target": "tree"}],
            baseline_contents={"tree/inner/deep.txt": "before"},
            final_contents={"tree/inner/deep.txt": "after"})
        self.assertEqual(self.provenance_of(att["id"], "tree"), "changed")
        self.assertTrue(out["ready_for_verification"])
        self._write("tree/inner/deep.txt", "changed after collect")
        with self.assertRaises(LedgerError) as cm:
            results.verify_attempt(self.ledger, att["id"], result="pass")
        self.assertEqual(cm.exception.code, "RESULT_CHANGED")

    def test_recursive_glob_includes_zero_levels_and_nested_files(self):
        self._write("tree/root.txt", "root")
        self._write("tree/inner/deep.txt", "deep")
        self._write("tree/.private.txt", "hidden")
        manifest, complete, reasons = self.scan("tree/**/*.txt", "glob")
        self.assertTrue(complete, reasons)
        self.assertEqual({Path(i["path"]).relative_to(self.site).as_posix()
                          for i in manifest}, {"tree/root.txt", "tree/inner/deep.txt"})

    def test_glob_changes_during_hash_are_incomplete(self):
        self._write("tree/a.txt", "a")
        real_lstat = os.lstat
        changed = False

        def add_file(path, *args, **kwargs):
            nonlocal changed
            if str(path).endswith("/a.txt") and not changed:
                changed = True
                self._write("tree/b.txt", "b")
            return real_lstat(path, *args, **kwargs)

        with patch.object(results.os, "lstat", side_effect=add_file):
            _, complete, reasons = self.scan("tree/*.txt", "glob")
        self.assertFalse(complete)
        self.assertTrue(any("unstable" in reason for reason in reasons), reasons)

    def test_nonmatching_entries_still_consume_enumeration_budget(self):
        for i in range(30):
            self._write(f"tree/f{i:02}.bin", "x")
        original = os.scandir
        reads = []

        class CountingDirectory:
            def __init__(self, path):
                self.iterator = original(path)

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                self.iterator.close()

            def __iter__(self):
                return self

            def __next__(self):
                item = next(self.iterator)
                reads.append(item.name)
                return item

        with patch.object(results.os, "scandir", CountingDirectory):
            manifest, complete, reasons = self.scan("tree/*.txt", "glob", entries=5)
        self.assertFalse(complete, reasons)
        self.assertEqual(manifest, [])
        # 最多多读一个条目以证明超预算，不能先物化全部目录再宣布截断。
        self.assertLessEqual(len(reads), 6)

    def test_missing_glob_baseline_can_prove_new_file(self):
        _, att, out = self._collected(
            deliverables=[{"kind": "glob", "target": "tree/*.txt"}],
            final_contents={"tree/new.txt": "new"}, texts=())
        self.assertEqual(self.provenance_of(att["id"], "tree/*.txt"), "new")
        self.assertTrue(out["ready_for_verification"])

    def test_directory_links_are_recorded_without_following(self):
        self._write("tree/a.txt", "a")
        os.symlink(".", Path(self.site) / "tree/back")
        manifest, complete, reasons = self.scan("tree/**", "glob", entries=10)
        self.assertTrue(complete, reasons)
        self.assertLessEqual(len(manifest), 3)
        link = next(i for i in manifest if Path(i["path"]).name == "back")
        self.assertEqual(link["type"], "link")

    def test_inaccessible_literal_parent_is_unknown_not_missing(self):
        if os.geteuid() == 0:
            self.skipTest("需要非 root 权限验证")
        self._write("blocked/deep/a.txt", "a")
        root = Path(self.site) / "blocked"
        root.chmod(0)
        self.addCleanup(root.chmod, 0o700)
        _, att = self._attempt(deliverables=[
            {"kind": "glob", "target": "blocked/deep/*.txt"}, "blocked/deep/a.txt"])
        baseline = self._seal_baseline(att["id"])
        self.assertFalse(baseline["coverage"]["complete"])
        self.assertTrue(all(item["status"] == "unknown" for item in baseline["items"]))

    def test_existing_baseline_is_reused_before_rescanning(self):
        self._write("out.txt", "before")
        _, att = self._attempt(deliverables=["out.txt"])
        first = self._seal_baseline(att["id"])
        self._write("out.txt", "after")
        with patch.object(results, "_scan_file_snapshot", side_effect=AssertionError("must not rescan")):
            again = self._seal_baseline(att["id"])
        self.assertEqual(first["baseline_sha256"], again["baseline_sha256"])

    def test_corrupt_existing_baseline_cannot_be_recaptured(self):
        _, att = self._attempt(deliverables=["out.txt"])
        first = self._seal_baseline(att["id"])
        path = Path(first["path"])
        path.write_text("{broken")
        with self.assertRaises(LedgerError) as cm:
            self._seal_baseline(att["id"])
        self.assertEqual(cm.exception.code, "BASELINE_INVALID")
        self.assertEqual(path.read_text(), "{broken")

    def test_pinned_baseline_missing_or_changed_rejects_collect(self):
        self._write("out.txt", "original")
        _, att = self._attempt(deliverables=["out.txt"])
        baseline = self._seal_baseline(att["id"])
        self.ledger.seal_execution(att["id"], mode="new", session_before={}, baseline=baseline)
        self._messages("ses_w", ["完成"], "msg_in", 1000)
        complete_execution(self.ledger, att["id"], input_at=1000, ended_at=2000)
        path = Path(baseline["path"])
        doc = json.loads(path.read_text())
        doc["items"][0].update(status="missing", sha256=None)
        path.write_text(json.dumps(doc))
        for case in ("changed", "missing"):
            with self.subTest(case=case):
                if case == "missing":
                    path.unlink()
                before = self.ledger.get_attempt(att["id"])
                with self.assertRaises(LedgerError) as cm:
                    results.collect_attempt(self.ledger, self.client, att["id"], CFG)
                self.assertEqual(cm.exception.code, "BASELINE_INVALID")
                self.assertEqual(self.ledger.get_attempt(att["id"]), before)
                self.assertEqual(self.ledger.list_notifications(), [])

    def test_concurrent_baseline_capture_does_not_replace_first_published(self):
        self._write("out.txt", "first observation")
        _, att = self._attempt(deliverables=["out.txt"])
        serialize = results.json.dumps
        fired = False
        published = {}

        def interleave(value, *args, **kwargs):
            nonlocal fired
            encoded = serialize(value, *args, **kwargs)
            if not fired and isinstance(value, dict) and value.get("schema") == "oc-orch.baseline.v1":
                fired = True
                self._write("out.txt", "second observation")
                published.update(self._seal_baseline(att["id"]))
            return encoded

        with patch.object(results.json, "dumps", side_effect=interleave):
            older = self._seal_baseline(att["id"])
        self.assertEqual(older["baseline_sha256"], published["baseline_sha256"])
        self.assertEqual(results.load_baseline(self.ledger, att["id"])["items"], published["items"])
