import json
import os
import sys
import time
import unittest
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "sdk"))
from ontoflow_sdk import OntoFlowClient  # noqa: E402

API = os.getenv("ONTOFLOW_API_URL", "http://localhost:8000").rstrip("/")
ES = os.getenv("ONTOFLOW_ES_URL", "http://localhost:9200").rstrip("/")


def _put(index, doc_id, doc):
    r = requests.put(
        f"{ES}/{index}/_doc/{doc_id}",
        params={"refresh": "wait_for"},
        json=doc,
        timeout=10,
    )
    r.raise_for_status()


def _wait(url, timeout=60):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=3)
            if r.ok:
                return
            last = f"HTTP {r.status_code}: {r.text[:200]}"
        except Exception as exc:  # pragma: no cover - diagnostic path
            last = repr(exc)
        time.sleep(1)
    raise RuntimeError(f"Timed out waiting for {url}: {last}")


class ApiIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _wait(f"{API}/health")
        cls.prefix = f"IT-{int(time.time())}"
        cls.machine = f"{cls.prefix}-M1"
        cls.batch = f"{cls.prefix}-B1"
        cls.finding = f"{cls.prefix}-QF1"
        cls.document = f"{cls.prefix}-DOC1"

        _put(
            "ontology_objects",
            cls.machine,
            {
                "object_type": "Machine",
                "machine_id": cls.machine,
                "name": "Integration Robot",
                "status": "running",
                "production_line": "Integration Line",
                "dq_score": 1.0,
                "valid": True,
            },
        )
        _put(
            "ontology_objects",
            cls.batch,
            {
                "object_type": "Batch",
                "batch_id": cls.batch,
                "machine_id": cls.machine,
                "product": "Integration Vehicle",
                "dq_score": 1.0,
                "valid": True,
            },
        )
        _put(
            "ontology_objects",
            cls.finding,
            {
                "object_type": "QualityFinding",
                "finding_id": cls.finding,
                "batch_id": cls.batch,
                "machine_id": cls.machine,
                "title": "Integration dimensional deviation",
                "description": "Fixture wear caused a dimensional deviation.",
                "severity": "high",
                "status": "open",
                "timestamp": "2026-10-05T08:00:00Z",
                "dq_score": 1.0,
                "valid": True,
            },
        )
        _put(
            "documents",
            cls.document,
            {
                "object_type": "EngineeringDocument",
                "document_id": cls.document,
                "title": f"QA report for {cls.batch}",
                "text": f"Batch {cls.batch} on machine {cls.machine} had fixture wear and dimensional deviation.",
                "machine_id": cls.machine,
                "batch_id": cls.batch,
                "source_system": "INTEGRATION_TEST",
                "dq_score": 0.99,
                "valid": True,
            },
        )

    def test_rest_get_and_reverse_link(self):
        r = requests.get(f"{API}/objects/Batch/{self.batch}", timeout=10)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["machine_id"], self.machine)

        r = requests.get(
            f"{API}/objects/Batch/{self.batch}/links/quality_findings", timeout=10
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual([x["finding_id"] for x in r.json()], [self.finding])

    def test_graphql_relationship_traversal(self):
        query = """
        query($id: String!) {
          batch(id: $id) {
            batchId
            machine { machineId name }
            qualityFindings { findingId severity status }
          }
        }
        """
        r = requests.post(
            f"{API}/graphql",
            json={"query": query, "variables": {"id": self.batch}},
            timeout=10,
        )
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertNotIn("errors", body, body)
        batch = body["data"]["batch"]
        self.assertEqual(batch["machine"]["machineId"], self.machine)
        self.assertEqual(batch["qualityFindings"][0]["findingId"], self.finding)

    def test_sdk_and_action_audit(self):
        client = OntoFlowClient(API)
        batch = client.Batch.get(self.batch)
        self.assertEqual(batch.machine_id, self.machine)
        findings = client.QualityFinding.where(batch_id=self.batch).where(severity="high").all()
        self.assertEqual(len(findings), 1)
        updated = findings[0].actions.acknowledge(user="integration-test")
        self.assertEqual(updated.status, "acknowledged")

        audit = requests.get(
            f"{ES}/action_audit/_search",
            params={"q": f"object_id:{self.finding}"},
            timeout=10,
        )
        audit.raise_for_status()
        self.assertGreaterEqual(audit.json()["hits"]["total"]["value"], 1)

    def test_rag_returns_quality_approved_document(self):
        r = requests.post(
            f"{API}/rag",
            json={"query": f"Why did batch {self.batch} have a dimensional deviation?", "dq_min": 0.8},
            timeout=30,
        )
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        citation_ids = {c["id"] for c in body["citations"]}
        self.assertIn(self.document, citation_ids)
        self.assertTrue(body["answer"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
