import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from researchflow.contracts import ResearchContractError, audit_claim_ledger, audit_project


class ResearchFlowContractTests(unittest.TestCase):
    def test_project_template_is_valid_but_not_ready_until_author_inputs_exist(self):
        fixture_root = Path(__file__).resolve().parent / "fixtures" / "researchflow"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for relative in (
                "research-project.json",
                "config/worker-pool.json",
                "config/literature-profile.yaml",
            ):
                target = root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes((fixture_root / relative).read_bytes())
            project_path = root / "research-project.json"
            report = audit_project(project_path, workspace_root=root)
            self.assertTrue(report.valid)
            self.assertFalse(report.ready)
            self.assertEqual(report.summary["expected_workers"], ["lab118", "ubuntu-second"])
            self.assertEqual(report.issues, ("unresolved placeholder at $.title",))

            project = json.loads(project_path.read_text(encoding="utf-8"))
            project["title"] = "A bounded contract fixture"
            project_path.write_text(json.dumps(project), encoding="utf-8")
            self.assertTrue(audit_project(project_path, workspace_root=root).ready)

    def test_supported_claim_requires_verified_workspace_relative_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            evidence = root / "research" / "evidence" / "result.json"
            evidence.parent.mkdir(parents=True)
            evidence.write_text('{"metric":0.91}', encoding="utf-8")
            digest = hashlib.sha256(evidence.read_bytes()).hexdigest()
            ledger = {
                "schema_version": 1,
                "study_id": "study_example",
                "claims": [{
                    "id": "claim-primary",
                    "text": "The candidate improves the preregistered primary outcome.",
                    "boundary": "Restricted to the declared held-out split and compute budget.",
                    "status": "supported",
                    "evidence": [{
                        "path": "research/evidence/result.json",
                        "sha256": digest,
                        "role": "primary"
                    }],
                    "limitations": ["External validation has not been performed."]
                }]
            }
            ledger_path = root / "ledger.json"
            ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
            report = audit_claim_ledger(ledger_path, workspace_root=root)
            self.assertTrue(report.ready)
            self.assertEqual(report.summary["verified_evidence_count"], 1)

    def test_evidence_path_cannot_escape_workspace(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ledger = {
                "schema_version": 1,
                "study_id": "study_example",
                "claims": [{
                    "id": "claim-primary",
                    "text": "A bounded claim.",
                    "boundary": "A bounded scope.",
                    "status": "supported",
                    "evidence": [{"path": "../outside.json", "sha256": "0" * 64, "role": "primary"}],
                    "limitations": []
                }]
            }
            ledger_path = root / "ledger.json"
            ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
            with self.assertRaises(ResearchContractError):
                audit_claim_ledger(ledger_path, workspace_root=root)


if __name__ == "__main__":
    unittest.main()
