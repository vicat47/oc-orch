#!/usr/bin/env python3
"""P2 协议降级修复回归（收口复核“分页协议降级错误”红→绿）。

对应 docs/review/20261108-completion-integrity-closeout.md 主协调者补充：
adapters/opencode.py 旧版把缺 cursor、坏 data、坏 cursor、非对象响应统一
规整为空数据/null 游标，read_message_window 随后报告 coverage.complete=true
（见 evidence/2026-11-08-completion-integrity-review/protocol-coverage.json
四类 incorrectly_complete=true 观测）。本文件的断言是该反例的逐条翻转：

- 协议违规 → MessageProtocolError（code=MESSAGE_PROTOCOL，窄 reason），
  与 HTTP 层 ApiError、真实 404（get_message → None）三者可区分；
- 消费者边界：read_message_window 收到 MessageProtocolError → coverage
  complete=false 并保留 error 原因（供 C 的分页消费者转换为 coverage
  incomplete / 覆盖不足，不猜测结果）；
- FakeClient 对齐同一契约：消息排序（asc/desc）、游标分页（cursor 与 order
  不组合）、inbox 排队→投递后可见（V2 queue 语义）、idle 消息形状、
  get_message 样本、fail(None) 注入被忽略、坏 cursor 按真实服务抛 400。

全程不访问真实服务：HTTP 契约使用 tests.test_client_contract.ScriptedServer
（loopback 假 HTTP）；fake 测试使用内存 FakeClient。服务配置经
OC_ORCH_SERVICE_CONFIG 隔离（_ContractBase 底座保证）。
"""

from __future__ import annotations

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))

import tests.test_client_contract as contract  # noqa: E402
from oc_orch.adapters.results import read_message_window  # noqa: E402
from oc_orch.client import (  # noqa: E402
    ApiError,
    Client,
    MessageProtocolError,
    OrchError,
    Service,
)
from tests.fakes import FakeClient  # noqa: E402


# ---------------------------------------------------------------- P2 矩阵

class TestProtocolDegradeRemediation(contract._ContractBase):
    """复核反例四类 + 消费者转换：协议错误不被降级为“完整覆盖”。"""

    def _client_returning(self, payload, status: int = 200) -> tuple[Client, object]:
        step = {"status": status, "payload": payload}
        srv = self.server([step])
        return self.client_to(srv.port), srv

    def test_cursor_missing_raises_protocol_error(self):
        # 反例 case=cursor_missing：缺 cursor 字段曾被规整为空游标并 complete=true。
        client, srv = self._client_returning(
            self.load_bytes("http", "message_page_cursor_missing.json"))
        with self.assertRaises(MessageProtocolError) as cm:
            client.message_page("ses_r", order="asc")
        self.assertEqual(cm.exception.reason, "cursor_missing")
        self.assertEqual(cm.exception.path, "/api/session/ses_r/message")
        self.assertIsInstance(cm.exception, OrchError)

    def test_data_wrong_type_raises_protocol_error(self):
        # 反例 case=data_wrong_type：data 非 list 曾被规整为空列表。
        client, _ = self._client_returning(
            self.load_bytes("http", "message_page_data_wrong_type.json"))
        with self.assertRaises(MessageProtocolError) as cm:
            client.message_page("ses_r", order="asc")
        self.assertEqual(cm.exception.reason, "data_wrong_type")

    def test_cursor_wrong_type_raises_protocol_error(self):
        # 反例 case=cursor_wrong_type：cursor 非对象曾被规整为 null 游标。
        client, _ = self._client_returning(
            self.load_bytes("http", "message_page_cursor_wrong_type.json"))
        with self.assertRaises(MessageProtocolError) as cm:
            client.message_page("ses_r", order="asc")
        self.assertEqual(cm.exception.reason, "cursor_wrong_type")

    def test_body_not_object_raises_protocol_error(self):
        # 反例 case=body_not_object：非对象 2xx 响应曾被规整为空页。
        client, _ = self._client_returning(
            self.load_bytes("http", "message_page_body_not_object.txt"))
        with self.assertRaises(MessageProtocolError) as cm:
            client.message_page("ses_r", order="asc")
        self.assertEqual(cm.exception.reason, "body_not_object")

    def test_all_four_review_cases_report_incomplete_coverage(self):
        """复核脚本四类输入全部从 coverage.complete=true 翻转为 false。"""
        payloads = [
            self.load_bytes("http", "message_page_cursor_missing.json"),
            self.load_bytes("http", "message_page_data_wrong_type.json"),
            self.load_bytes("http", "message_page_cursor_wrong_type.json"),
            self.load_bytes("http", "message_page_body_not_object.txt"),
        ]
        for payload in payloads:
            with self.subTest(payload=payload[:40]):
                client, _ = self._client_returning(payload)
                messages, coverage = read_message_window(
                    client, "ses_r", max_pages=5, page_size=50)
                self.assertFalse(coverage["complete"],
                                 "协议错误不得被报告为完整覆盖")
                # 消费者保留可诊断原因：MESSAGE_PROTOCOL 必须可检索
                self.assertIn("MESSAGE_PROTOCOL", coverage.get("error", ""))

    def test_valid_shape_still_reports_complete_coverage(self):
        # 修复不收紧合法路径：有效形状 + 显式终页（next=null）仍完整。
        valid = self.load_bytes("http", "message_page_valid.json").decode()
        final = self.load_bytes("http", "message_page_empty.json").decode()
        srv = self.server([{"payload": valid}, {"payload": final}])
        client = self.client_to(srv.port)
        messages, coverage = read_message_window(
            client, "ses_r", max_pages=5, page_size=50)
        self.assertEqual(coverage["complete"], True)
        self.assertEqual(coverage["pages"], 2)
        self.assertEqual([m["id"] for m in messages],
                         ["msg_user_1", "msg_idle_1"])
        # 第二页沿 cursor 翻页：不携带 order（服务契约）
        self.assertNotIn("order=", srv.seen[1][1])
        self.assertIn("cursor=", srv.seen[1][1])

    def test_max_pages_budget_is_bounded(self):
        # 服务端重复同一 next 游标（default 步重复同页）：消费者必须有界发现
        # 并停止——complete=False、pages ≤ max_pages，绝不能无限翻页或假完整。
        # 页数不锁定具体值：翻页实现（重复游标检测/预算耗尽）属消费者策略。
        valid = self.load_bytes("http", "message_page_valid.json").decode()
        srv = self.server(default={"payload": valid})
        client = self.client_to(srv.port)
        _, coverage = read_message_window(client, "ses_r",
                                          max_pages=3, page_size=50)
        self.assertFalse(coverage["complete"])
        self.assertLessEqual(coverage["pages"], 3)
        self.assertGreaterEqual(coverage["pages"], 1)


# ---------------------------------------------------------------- FakeClient 契约

class FakeClientBase(unittest.TestCase):
    def make_client(self, msgs: list[tuple[int, str, str]]) -> FakeClient:
        """按 (created, id, type) 布置消息；同刻按 id 稳定排序。"""
        client = FakeClient()
        for created, mid, mtype in msgs:
            client.append_message("ses_a", {
                "id": mid, "type": mtype,
                "time": {"created": created},
                **({"text": f"内容-{mid}"} if mtype == "user" else
                   {"outcome": "succeeded"} if mtype == "idle" else {}),
            })
        return client


class TestFakeClientMessageContract(FakeClientBase):
    """FakeClient 与真实 V2 消息契约对齐（排队/投递/排序/分页/形状）。"""

    def test_prompt_only_queues_until_delivered(self):
        # V2 queue 语义：prompt 只入 inbox；未投递前消息列表/定点读取不可见。
        client = FakeClient()
        client.prompt("ses_a", message_id="msg_q1", text="执行任务",
                      delivery="queue")
        inbox = client.inbox("ses_a")
        self.assertEqual([i["id"] for i in inbox], ["msg_q1"])
        self.assertIn("text", inbox[0]["payload"], "inbox 条目含 payload")
        self.assertEqual(inbox[0]["payload"]["text"], "执行任务")
        self.assertIn("delivery", inbox[0])
        self.assertEqual(client.messages("ses_a"), [],
                         "inbox 排队不等于已执行：消息尚未可见")
        self.assertIsNone(client.get_message("ses_a", "msg_q1"))

    def test_deliver_inbox_produces_v2_user_message(self):
        # 投递后：固封 Session.Message.User 形状（正文在 text 字段）。
        client = FakeClient()
        client.prompt("ses_a", message_id="msg_q1", text="执行任务",
                      delivery="queue",
                      metadata={"oc_orch_task": "T-x"})
        out = client.deliver_inbox("ses_a", at=1794008072447)
        self.assertEqual(out[0]["id"], "msg_q1")
        self.assertEqual(client.inbox("ses_a"), [])
        msgs = client.messages("ses_a", order="asc")
        self.assertEqual(len(msgs), 1)
        self.assertEqual(msgs[0]["type"], "user")
        self.assertEqual(msgs[0]["text"], "执行任务")
        self.assertEqual(msgs[0]["time"]["created"], 1794008072447)
        self.assertEqual(msgs[0]["metadata"]["oc_orch_task"], "T-x")
        self.assertEqual(client.get_message("ses_a", "msg_q1"), msgs[0])

    def test_deliver_inbox_maps_legacy_texts_inbox_items(self):
        # 兼容旧夹具手写 inbox 条目（texts/顶层 text）→ 投递后统一为 text。
        client = FakeClient()
        client.set_inbox("ses_a", [{"id": "msg_old", "texts": "旧夹具输入"}])
        out = client.deliver_inbox("ses_a", at=42)
        self.assertEqual(out[0]["text"], "旧夹具输入")
        client.set_inbox("ses_a", [{"id": "msg_top", "text": "顶层文本"}])
        out = client.deliver_inbox("ses_a", at=43)
        self.assertEqual(out[0]["text"], "顶层文本")

    def test_messages_ordered_by_created_then_id(self):
        client = self.make_client([
            (300, "m_c", "user"), (100, "m_a", "user"), (200, "m_b", "user"),
        ])
        asc = client.messages("ses_a", order="asc")
        self.assertEqual([m["id"] for m in asc], ["m_a", "m_b", "m_c"])
        desc = client.messages("ses_a", order="desc")
        self.assertEqual([m["id"] for m in desc], ["m_c", "m_b", "m_a"])
        # 同刻消息按 id 稳定排序
        client.append_message("ses_a", {"id": "m_a2", "type": "user",
                                        "text": "x", "time": {"created": 100}})
        asc = client.messages("ses_a", order="asc")
        self.assertEqual([m["id"] for m in asc][:2], ["m_a", "m_a2"])

    def test_message_page_asc_pagination_via_cursor(self):
        client = self.make_client([
            (100, "m_a", "user"), (200, "m_b", "user"), (300, "m_c", "user"),
            (400, "m_d", "user"), (500, "m_e", "user"),
        ])
        page1 = client.message_page("ses_a", limit=2, order="asc")
        self.assertEqual([m["id"] for m in page1["data"]], ["m_a", "m_b"])
        self.assertIsNotNone(page1["cursor"]["next"])
        page2 = client.message_page("ses_a", limit=2,
                                    cursor=page1["cursor"]["next"])
        self.assertEqual([m["id"] for m in page2["data"]], ["m_c", "m_d"])
        page3 = client.message_page("ses_a", limit=2,
                                    cursor=page2["cursor"]["next"])
        self.assertEqual([m["id"] for m in page3["data"]], ["m_e"])
        self.assertIsNone(page3["cursor"]["next"], "终页 next=null 为有效语义")

    def test_message_page_desc_first_page_and_same_direction_cursor(self):
        client = self.make_client([
            (100, "m_a", "user"), (200, "m_b", "user"), (300, "m_c", "user"),
        ])
        page1 = client.message_page("ses_a", limit=2, order="desc")
        self.assertEqual([m["id"] for m in page1["data"]], ["m_c", "m_b"])
        # 沿 cursor 翻页：fake 按 token 内方向继续（desc），不依赖 order 参数
        page2 = client.message_page("ses_a", limit=2,
                                    cursor=page1["cursor"]["next"])
        self.assertEqual([m["id"] for m in page2["data"]], ["m_a"])
        self.assertIsNone(page2["cursor"]["next"])

    def test_message_page_empty_session_is_valid(self):
        client = FakeClient()
        page = client.message_page("ses_a", order="asc")
        self.assertEqual(page["data"], [])
        self.assertIsNone(page["cursor"]["next"])
        self.assertIsNone(page["cursor"]["previous"])

    def test_message_page_scan_cap_truncates_visible_history(self):
        # 覆盖不足模拟：可见历史上限之外的消息翻页不可达（next=null）。
        client = self.make_client([
            (100, "m_old", "user"), (200, "m_mid", "user"),
            (300, "m_new", "user"),
        ])
        client.message_scan_cap = 2
        page = client.message_page("ses_a", limit=10, order="asc")
        self.assertEqual([m["id"] for m in page["data"]], ["m_mid", "m_new"])
        self.assertIsNone(page["cursor"]["next"])

    def test_message_page_bad_cursor_raises_api_error_400(self):
        # 坏 cursor 按真实服务语义显式 400，不静默返回空页。
        client = self.make_client([(100, "m_a", "user")])
        for bad in ("junk", "asc:notanumber:x", "sideways:100:m_a"):
            with self.subTest(cursor=bad):
                with self.assertRaises(ApiError) as cm:
                    client.message_page("ses_a", cursor=bad)
                self.assertEqual(cm.exception.status, 400)
                self.assertEqual(cm.exception.tag, "InvalidCursor")

    def test_add_idle_and_visibility(self):
        client = FakeClient()
        msg = client.add_idle("ses_a", outcome="succeeded", at=99,
                              msg_id="msg_idle_x")
        self.assertEqual(msg["type"], "idle")
        self.assertEqual(msg["outcome"], "succeeded")
        again = client.get_message("ses_a", "msg_idle_x")
        self.assertEqual(again["outcome"], "succeeded")
        self.assertEqual(client.messages("ses_a", type_="idle"),
                         [client.get_message("ses_a", "msg_idle_x")])

    def test_get_message_hit_and_miss(self):
        client = self.make_client([(100, "m_a", "user")])
        hit = client.get_message("ses_a", "m_a")
        self.assertEqual(hit["id"], "m_a")
        self.assertEqual(hit["text"], "内容-m_a")
        self.assertIsNone(client.get_message("ses_a", "m_missing"),
                          "真实不存在（fake 无该消息）等价 404 → None")

    def test_fail_none_is_ignored(self):
        # fail(method, None) 不入队：不破坏注入队列、不在调用处 raise None。
        client = FakeClient()
        client.fail("prompt", None)
        client.fail("message_page", None)
        client.prompt("ses_a", message_id="msg_q1", text="ok",
                      delivery="queue")
        page = client.message_page("ses_a", order="asc")
        self.assertEqual(page["data"], [])
        # 后续正常注入不受影响
        client.fail("message_page", ApiError(500, "/m", None, "boom"))
        with self.assertRaises(ApiError):
            client.message_page("ses_a", order="asc")


class TestServiceFacadeShape(unittest.TestCase):
    """真实 Client / FakeClient 共享的最小服务形状（窄端口 service 属性）。"""

    def test_fake_service_public_shape(self):
        client = FakeClient()
        public = client.service.public()
        self.assertEqual(
            sorted(public), sorted(["base", "host", "port", "scheme", "config"]))

    def test_real_service_dataclass_defaults(self):
        svc = Service(base="http://127.0.0.1:1", host="127.0.0.1", port=1,
                      password="x")
        self.assertEqual(svc.scheme, "http")


if __name__ == "__main__":
    unittest.main(verbosity=2)
