"""Focused regressions for defects found during independent contract review."""
import unittest
from unittest.mock import patch

from controller.gateway import CCSGateway, CCSRequestError
from controller.remote import FakeRemoteRunner


class ReviewedContractTests(unittest.TestCase):
    def test_workspace_file_read_requires_path_before_remote_routing(self):
        for extra in ({}, {"path": None}, {"path": ""}, {"path": "."}):
            with self.subTest(extra=extra):
                remote = FakeRemoteRunner()
                gateway = CCSGateway(remote, audit_path=":memory:")
                request = {
                    "schema_version": 1,
                    "request_id": "contract-review",
                    "actor": {"id": "reviewer", "kind": "agent"},
                    "action": "workspace.execute",
                    "dry_run": True,
                    "payload": {"request": {
                        "action": "inspect", "mode": "read",
                        "project": "/home/example/project", **extra,
                    }},
                }
                with patch.object(remote, "compatibility", wraps=remote.compatibility) as compatibility:
                    with self.assertRaises(CCSRequestError) as raised:
                        gateway.execute(request)
                    self.assertEqual(raised.exception.execution_state, "not_sent")
                    self.assertTrue(any(item["path"] == "payload.request.path"
                                        for item in raised.exception.field_errors))
                    compatibility.assert_not_called()


if __name__ == "__main__":
    unittest.main()
