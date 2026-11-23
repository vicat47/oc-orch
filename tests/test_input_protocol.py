"""输入协议的离线契约：显式调度、FIFO 与写请求不重放。"""
import unittest
from unittest.mock import patch

from oc_orch.client import Client, MessageProtocolError, Service, TransportError
from tests.fakes import FakeClient


class InputProtocolTests(unittest.TestCase):
    def setUp(self):
        self.client = Client(service=Service("http://127.0.0.1:1", "127.0.0.1", 1, ""))

    def test_resume_false_and_true_are_serialized_without_guessing(self):
        for resume in (False, True, None):
            with self.subTest(resume=resume), patch.object(
                self.client, "_post", return_value={"data": {"id": "msg_a"}}
            ) as post:
                self.client.prompt("ses_a", message_id="msg_a", text="x", resume=resume)
                body = post.call_args.args[1]
                if resume is None:
                    self.assertNotIn("resume", body)
                else:
                    self.assertIs(body["resume"], resume)

    def test_steer_calls_patch_once_and_keeps_original_identity(self):
        with patch.object(self.client, "_request", side_effect=TransportError("/x", "lost")) as req:
            with self.assertRaises(TransportError):
                self.client.update_inbox_delivery("ses_a", "msg_old", delivery="steer")
        req.assert_called_once_with("PATCH", "/api/session/ses_a/inbox/msg_old",
                                    {"delivery": "steer"}, retry=False)

    def test_cancel_calls_delete_once_without_claiming_delivery(self):
        with patch.object(self.client, "_request", return_value=None) as req:
            self.assertIsNone(self.client.cancel_inbox("ses_a", "msg_old"))
        req.assert_called_once_with("DELETE", "/api/session/ses_a/inbox/msg_old", retry=False)

    def test_broken_inbox_response_is_not_an_empty_queue(self):
        for value in (None, {}, {"data": None}, {"data": {}}, {"data": ["bad"]}):
            with self.subTest(value=value), patch.object(self.client, "_get", return_value=value):
                with self.assertRaises(MessageProtocolError):
                    self.client.inbox("ses_a")
        with patch.object(self.client, "_get", return_value={"data": []}):
            self.assertEqual(self.client.inbox("ses_a"), [])

    def test_broken_active_response_is_not_an_idle_target(self):
        for value in (None, {}, {"data": None}, {"data": []}):
            with self.subTest(value=value), patch.object(self.client, "_get", return_value=value):
                with self.assertRaises(MessageProtocolError):
                    self.client.active_sessions()
        with patch.object(self.client, "_get", return_value={"data": {}}):
            self.assertEqual(self.client.active_sessions(), {})

    def test_park_stays_pending_until_next_scheduled_execution_fifo(self):
        fake = FakeClient()
        fake.set_session("ses_a")
        fake.prompt("ses_a", message_id="msg_park", text="old", resume=False)
        self.assertNotIn("ses_a", fake.scheduled_sessions)
        self.assertEqual(fake.run_scheduled("ses_a", at=10), [])
        fake.prompt("ses_a", message_id="msg_new", text="new", resume=True)
        self.assertIn("ses_a", fake.scheduled_sessions)
        delivered = fake.run_scheduled("ses_a", at=20)
        self.assertEqual([m["id"] for m in delivered], ["msg_park", "msg_new"])
        self.assertEqual(fake.inbox("ses_a"), [])

    def test_steer_schedules_original_input_and_cancel_noop_is_not_delivery(self):
        fake = FakeClient()
        fake.set_session("ses_a")
        fake.prompt("ses_a", message_id="msg_old", text="old", resume=False)
        fake.update_inbox_delivery("ses_a", "msg_old", delivery="steer")
        self.assertEqual([m["id"] for m in fake.run_scheduled("ses_a", at=20)], ["msg_old"])
        before = list(fake.inbox_lifecycle_events)
        self.assertIsNone(fake.cancel_inbox("ses_a", "msg_old"))
        self.assertEqual(fake.inbox_lifecycle_events, before)
        fake.prompt("ses_a", message_id="msg_cancel", text="cancel", resume=False)
        fake.cancel_inbox("ses_a", "msg_cancel")
        self.assertIsNone(fake.get_message("ses_a", "msg_cancel"))
        self.assertEqual(fake.inbox("ses_a"), [])


if __name__ == "__main__":
    unittest.main()
