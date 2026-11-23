"""显式插件控制的精确只读视图/历史回执；不发现真实 OpenCode。"""

from tests.test_continuity_web import ContinuityWebBase


class ProjectControlViewTests(ContinuityWebBase):
    def view(self, suffix=""):
        return self.get("/api/coordinator/control?project=p&session_id=ses_coord" + suffix)

    def body(self, action, revision=0):
        return dict(project="p", session_id="ses_coord", operation_id="plugin-" + action,
                    reason="explicit-plugin-test", expected_revision=revision,
                    expected_epoch=self.lease["epoch"], confirm="p")

    def test_exact_view_without_task_pagination_or_client(self):
        before = list(self.conn.iterdump())
        status, data = self.view()
        self.assertEqual(status, 200, data)
        self.assertEqual(data["schema"], "oc-orch.project-control.v1")
        self.assertEqual(data["project"], "p")
        self.assertEqual(data["session_id"], "ses_coord")
        self.assertEqual(data["project_control"]["status"], "running")
        self.assertEqual(data["project_control"]["revision"], 0)
        self.assertEqual(data["lease"]["holder_session"], "ses_coord")
        self.assertEqual(data["lease"]["epoch"], self.lease["epoch"])
        self.assertIs(data["read_only"], False)
        self.assertIn("manifest_verified", data["runtime"])
        self.assertEqual(list(self.conn.iterdump()), before)
        self.assertFalse(self.client.called)

    def test_historical_receipt_and_current_are_distinct(self):
        status, _ = self.post("/api/coordinator/pause", self.body("pause"))
        self.assertEqual(status, 200)
        _, paused = self.view()
        revision = paused["project_control"]["revision"]
        status, _ = self.post("/api/coordinator/resume", self.body("resume", revision))
        self.assertEqual(status, 200)
        before = list(self.conn.iterdump())
        status, data = self.view("&operation_id=plugin-pause")
        self.assertEqual(status, 200, data)
        self.assertEqual(data["operation"]["action"], "pause")
        self.assertEqual(data["operation"]["revision_after"], revision)
        self.assertEqual(data["project_control"]["status"], "running")
        self.assertGreater(data["project_control"]["revision"], revision)
        self.assertNotIn("reason", data["operation"])
        self.assertEqual(list(self.conn.iterdump()), before)

    def test_missing_operation_is_unknown_not_unexecuted(self):
        status, data = self.view("&operation_id=never-submitted")
        self.assertEqual(status, 200, data)
        self.assertIsNone(data["operation"])

    def test_receipt_cannot_be_read_under_another_project_or_session(self):
        self.post("/api/coordinator/pause", self.body("pause"))
        for query in ("project=other&session_id=ses_coord", "project=p&session_id=ses_other"):
            status, data = self.get("/api/coordinator/control?" + query + "&operation_id=plugin-pause")
            self.assertEqual(status, 200, data)
            self.assertIsNone(data["operation"])

    def test_ambiguous_missing_and_blank_scopes_fail_closed(self):
        for query in ("", "project=p", "project=&session_id=ses_coord",
                      "project=p&project=other&session_id=ses_coord",
                      "project=p&session_id=ses_coord&trusted=true",
                      "project=p&session_id=ses_coord&operation_id="):
            status, data = self.get("/api/coordinator/control?" + query)
            self.assertEqual(status, 400, (query, data))

    def test_read_only_view_exposes_the_write_gate(self):
        self.server.read_only = True
        status, data = self.view()
        self.assertEqual(status, 200, data)
        self.assertIs(data["read_only"], True)
