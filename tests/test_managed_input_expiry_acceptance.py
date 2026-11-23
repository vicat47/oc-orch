"""独立复现E审查：到期告警逐责任完整、处理窗无饥饿、白名单可显式重登记。"""
import tempfile
import unittest

from oc_orch.adapters.managed_input_ledger import ManagedInputLedger
from oc_orch.db import init
from oc_orch.ledger import Ledger
from tests.fakes import FakeClock


class ManagedInputExpiryAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="managed-expiry-acceptance-", dir="/tmp/opencode")
        self.addCleanup(self.temp.cleanup)
        self.conn = init(self.temp.name)
        self.addCleanup(self.conn.close)
        self.clock = FakeClock()
        self.base = Ledger(self.conn, self.temp.name, clock=self.clock)
        self.repo = ManagedInputLedger(self.conn, self.base)

    def park(self, key, *, owner="operator", expires_in=1000):
        return self.repo.prepare_managed_input("ses_" + key, intent="park", text=key,
            owner=owner, request_id=key, project="offline", now=self.clock.now_ms(),
            expires_at=self.clock.now_ms() + expires_in,
            flush_condition="explicit-human-confirmation")["input"]

    def alerted(self, input_id):
        return any(input_id in a["detail"] for a in self.base.list_alerts(state="new", limit=500))

    def test_same_owner_each_expired_input_has_its_own_visible_responsibility(self):
        first = self.park("first")
        second = self.park("second")
        self.clock.advance(seconds=2)
        self.repo.expire_managed_parks(now=self.clock.now_ms())
        self.assertTrue(self.alerted(first["input_id"]))
        self.assertTrue(self.alerted(second["input_id"]),
                        "同owner不能把第二个input标成已告警却不包含其责任")
        before = len(self.base.list_alerts(state="new", limit=500))
        self.repo.expire_managed_parks(now=self.clock.now_ms())
        self.assertEqual(len(self.base.list_alerts(state="new", limit=500)), before)

    def test_already_alerted_prefix_cannot_starve_later_expired_input(self):
        first = self.park("first", owner="first-owner")
        second = self.park("second", owner="second-owner")
        self.clock.advance(seconds=2)
        self.repo.expire_managed_parks(now=self.clock.now_ms(), limit=1)
        self.repo.expire_managed_parks(now=self.clock.now_ms(), limit=1)
        self.assertTrue(self.alerted(first["input_id"]))
        self.assertTrue(self.alerted(second["input_id"]),
                        "已告警的最早一条不能永久占据limit=1的候选窗")

    def test_not_expired_prefix_cannot_starve_expired_input(self):
        self.park("not-due", owner="first-owner", expires_in=1000000)
        expired = self.park("due", owner="second-owner")
        self.clock.advance(seconds=2)
        self.repo.expire_managed_parks(now=self.clock.now_ms(), limit=1)
        self.assertTrue(self.alerted(expired["input_id"]),
                        "未到期前缀不能挤掉真正到期责任")

    def test_reregister_same_parameters_explicitly_reenables_watch(self):
        args = {"owner": "operator", "project": "offline", "max_pending_age_ms": 1000}
        created = self.repo.register_managed_watch("ses_watch", now=self.clock.now_ms(), **args)
        initial = created.get("watch", created)
        disabled = self.repo.unregister_managed_watch("ses_watch", owner="operator",
            expected_revision=initial["revision"], now=self.clock.now_ms())
        old = disabled.get("watch", disabled)
        self.assertFalse(old["enabled"])
        self.repo.register_managed_watch("ses_watch", now=self.clock.now_ms(), **args)
        current = self.repo.get_managed_watch("ses_watch")
        self.assertTrue(current["enabled"])
        self.assertGreater(current["revision"], old["revision"])
