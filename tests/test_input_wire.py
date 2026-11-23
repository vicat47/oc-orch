"""新 inbox 窄端口的真实 loopback HTTP 契约，不访问 OpenCode。"""
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import threading
import unittest
from unittest.mock import patch

from oc_orch.client import ApiError, Client, MessageProtocolError, Service, TransportError


class _InputHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self.handle_input()

    def do_PATCH(self):
        self.handle_input()

    def do_DELETE(self):
        self.handle_input()

    def handle_input(self):
        size = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(size)
        self.server.seen.append((self.command, self.path, body))
        status, payload = self.server.response
        if status is None:
            self.close_connection = True
            self.connection.close()
            return
        self.send_response(status)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        if payload:
            self.wfile.write(payload)


@contextmanager
def input_wire(status=204, body=None):
    server = ThreadingHTTPServer(("127.0.0.1", 0), _InputHandler)
    server.seen = []
    server.response = (status, b"" if body is None else json.dumps(body).encode())
    thread = threading.Thread(target=server.serve_forever,
                              kwargs={"poll_interval": 0.02}, daemon=True)
    thread.start()
    port = server.server_port
    client = Client(service=Service(f"http://127.0.0.1:{port}", "127.0.0.1", port, ""))
    try:
        with patch.dict(os.environ, {"NO_PROXY": "127.0.0.1,localhost,::1",
                                     "no_proxy": "127.0.0.1,localhost,::1"}):
            yield client, server.seen
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


class InputWireTests(unittest.TestCase):
    def test_patch_204_original_path_and_body(self):
        with input_wire() as (client, seen):
            self.assertIsNone(client.update_inbox_delivery("ses_a", "msg_old", delivery="steer"))
        self.assertEqual(len(seen), 1)
        verb, path, body = seen[0]
        self.assertEqual((verb, path), ("PATCH", "/api/session/ses_a/inbox/msg_old"))
        self.assertEqual(json.loads(body), {"delivery": "steer"})

    def test_delete_204_has_no_implied_delivery_receipt(self):
        with input_wire() as (client, seen):
            self.assertIsNone(client.cancel_inbox("ses_a", "msg_old"))
        self.assertEqual(seen, [("DELETE", "/api/session/ses_a/inbox/msg_old", b"")])

    def test_write_rejection_is_preserved_and_not_retried(self):
        for status in (400, 401, 404, 409):
            with self.subTest(status=status), input_wire(
                status, {"_tag": "ConflictError", "message": "not pending"}
            ) as (client, seen):
                with self.assertRaises(ApiError) as error:
                    client.update_inbox_delivery("ses_a", "msg_old", delivery="steer")
                self.assertEqual(error.exception.status, status)
                self.assertEqual(error.exception.tag, "ConflictError")
                self.assertEqual(len(seen), 1)

    def test_connection_loss_does_not_replay_patch(self):
        with input_wire(None) as (client, seen):
            with self.assertRaises(TransportError):
                client.update_inbox_delivery("ses_a", "msg_old", delivery="steer")
            self.assertEqual(len(seen), 1)

    def test_connection_loss_does_not_replay_delete(self):
        with input_wire(None) as (client, seen):
            with self.assertRaises(TransportError):
                client.cancel_inbox("ses_a", "msg_old")
            self.assertEqual(len(seen), 1)

    def test_inbox_bad_shape_is_unknown_not_empty(self):
        with input_wire(200, {"data": None}) as (client, seen):
            with self.assertRaises(MessageProtocolError):
                client.inbox("ses_a")
            self.assertEqual(len(seen), 1)

    def test_active_bad_shape_is_unknown_not_inactive(self):
        with input_wire(200, {"data": []}) as (client, seen):
            with self.assertRaises(MessageProtocolError):
                client.active_sessions()
            self.assertEqual(len(seen), 1)


if __name__ == "__main__":
    unittest.main()
