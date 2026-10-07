import json
import os
import time
import unittest

import requests
from kafka import KafkaProducer

API = os.getenv("ONTOFLOW_API_URL", "http://localhost:8000").rstrip("/")
KAFKA = os.getenv("ONTOFLOW_KAFKA", "localhost:9092")
ES = os.getenv("ONTOFLOW_ES_URL", "http://localhost:9200").rstrip("/")


def wait_for(getter, description, timeout=240, interval=3):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            value = getter()
            if value:
                return value
            last = value
        except Exception as exc:  # pragma: no cover - diagnostic path
            last = repr(exc)
        time.sleep(interval)
    raise AssertionError(f"Timed out waiting for {description}; last={last}")


class FullPipelineE2E(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        wait_for(
            lambda: requests.get(f"{API}/health", timeout=3).ok,
            "API health",
            timeout=90,
        )
        suffix = str(int(time.time()))
        cls.machine = f"E2E-M-{suffix}"
        cls.part = f"E2E-P-{suffix}"
        cls.order = f"E2E-O-{suffix}"
        cls.batch = f"E2E-B-{suffix}"
        cls.finding = f"E2E-QF-{suffix}"
        cls.maintenance = f"E2E-MA-{suffix}"
        cls.document = f"E2E-DOC-{suffix}"
        cls.invalid_finding = f"E2E-BAD-{suffix}"

        producer = KafkaProducer(
            bootstrap_servers=KAFKA,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        records = [
            ("raw.events", {"object_type": "Machine", "machine_id": cls.machine, "name": "E2E Welding Robot", "status": "running", "production_line": "E2E Line"}),
            ("raw.events", {"object_type": "Part", "part_id": cls.part, "name": "E2E reinforcement", "revision": "A"}),
            ("raw.events", {"object_type": "Order", "order_id": cls.order, "product": "E2E Vehicle", "status": "in_production"}),
            ("raw.events", {"object_type": "Batch", "batch_id": cls.batch, "machine_id": cls.machine, "order_id": cls.order, "product": "E2E Vehicle", "started_at": "2026-10-05T08:00:00Z"}),
            ("raw.events", {"object_type": "QualityFinding", "finding_id": cls.finding, "batch_id": cls.batch, "part_id": cls.part, "machine_id": cls.machine, "title": "E2E dimensional deviation", "description": "Fixture wear caused the deviation", "severity": "high", "status": "open", "timestamp": "2026-10-05T08:05:00Z"}),
            ("raw.events", {"object_type": "MaintenanceAction", "maintenance_id": cls.maintenance, "machine_id": cls.machine, "action_type": "replace_tool", "description": "Replaced worn fixture", "timestamp": "2026-10-05T08:10:00Z"}),
            ("raw.documents", {"object_type": "EngineeringDocument", "document_id": cls.document, "title": f"E2E QA report {cls.batch}", "text": f"Batch {cls.batch} on machine {cls.machine} failed because fixture wear affected part {cls.part}. Maintenance {cls.maintenance} replaced the tool.", "batch_id": cls.batch, "machine_id": cls.machine, "part_id": cls.part, "source_system": "E2E_QA", "dq_score": 0.98, "valid": True}),
            # Deliberately invalid: missing required title.
            ("raw.events", {"object_type": "QualityFinding", "finding_id": cls.invalid_finding, "batch_id": cls.batch, "severity": "high", "status": "open", "timestamp": "2026-10-05T08:06:00Z"}),
        ]
        for topic, record in records:
            producer.send(topic, record).get(timeout=20)
        producer.flush()
        producer.close()

        def batch_visible():
            r = requests.get(f"{API}/objects/Batch/{cls.batch}", timeout=5)
            return r.json() if r.status_code == 200 else None

        wait_for(batch_visible, "Kafka -> Spark -> Gold -> Elasticsearch batch", timeout=300)

        def document_visible():
            r = requests.get(f"{API}/objects/EngineeringDocument/{cls.document}", timeout=5)
            return r.json() if r.status_code == 200 else None

        wait_for(document_visible, "document indexing", timeout=180)

    def test_graphql_traverses_streamed_ontology(self):
        query = """
        query($id: String!) {
          batch(id: $id) {
            batchId
            machine { machineId }
            qualityFindings { findingId severity affectedPart { partId } }
            documents { documentId dqScore valid }
          }
        }
        """
        r = requests.post(
            f"{API}/graphql",
            json={"query": query, "variables": {"id": self.batch}},
            timeout=15,
        )

        print("\n========== TRAVERSAL RESPONSE ==========")
        print("HTTP:", r.status_code)

        try:
            body = r.json()
            print(json.dumps(body, indent=2))
        except Exception:
            print(r.text)
            raise

        print("==================================\n")

        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertNotIn("errors", body, body)
        b = body["data"]["batch"]
        self.assertEqual(b["machine"]["machineId"], self.machine)
        self.assertEqual(b["qualityFindings"][0]["findingId"], self.finding)
        self.assertEqual(b["qualityFindings"][0]["affectedPart"]["partId"], self.part)
        self.assertEqual(b["documents"][0]["documentId"], self.document)

    def test_rag_uses_streamed_document_and_ontology_context(self):
        r = requests.post(
            f"{API}/rag",
            json={"query": f"Why did batch {self.batch} fail quality inspection?", "dq_min": 0.75},
            timeout=30,
        )
        print("\n========== RAG RESPONSE ==========")
        print("HTTP:", r.status_code)

        try:
            body = r.json()
            print(json.dumps(body, indent=2))
        except Exception:
            print(r.text)
            raise

        print("==================================\n")

        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertIn(self.document, {c["id"] for c in body["citations"]})
        context_ids = {f"{x.get('object_type')}:{x.get('id')}" for x in body["context"]}
        self.assertTrue(any(self.batch in x for x in context_ids), context_ids)

    def test_action_round_trip(self):
        r = requests.post(
            f"{API}/actions/acknowledge_finding/{self.finding}",
            json={"inputs": {"user": "e2e-test"}},
            timeout=10,
        )
        print("\n========== FINDING RESPONSE ==========")
        print("HTTP:", r.status_code)

        try:
            body = r.json()
            print(json.dumps(body, indent=2))
        except Exception:
            print(r.text)
            raise

        print("==================================\n")

        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["status"], "acknowledged")

    def test_invalid_event_reaches_quarantine(self):
        def found():
            r = requests.get(
                f"{ES}/quarantine/_search",
                params={"q": self.invalid_finding},
                timeout=5,
            )
            print("HTTP:", r.status_code)

            try:
                body = r.json()
                print(json.dumps(body, indent=2))
            except Exception:
                print(r.text)
                raise

            print("==================================\n")
            
            if not r.ok:
                return False
            return r.json()["hits"]["total"]["value"] > 0

        self.assertTrue(wait_for(found, "invalid event in quarantine", timeout=180))


if __name__ == "__main__":
    unittest.main(verbosity=2)
