import json
import os
import sys
import time
from pathlib import Path

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ApiError

ES_URL = os.getenv('ELASTICSEARCH_URL', 'http://elasticsearch:9200')
MAPPINGS_DIR = Path(os.getenv('ES_MAPPINGS_DIR', '/es'))
INDICES = ['ontology_objects', 'documents', 'quarantine', 'action_audit']


def wait_for_es(client: Elasticsearch, attempts: int = 30, delay: float = 2.0) -> None:
    last_error = None
    for _ in range(attempts):
        try:
            if client.ping():
                return
        except Exception as exc:  # pragma: no cover - startup diagnostics
            last_error = exc
        time.sleep(delay)
    raise RuntimeError(f'Elasticsearch did not become reachable at {ES_URL}: {last_error}')


def main() -> int:
    client = Elasticsearch(ES_URL, request_timeout=30)
    wait_for_es(client)

    for name in INDICES:
        mapping_path = MAPPINGS_DIR / f'{name}.json'
        if not mapping_path.exists():
            raise FileNotFoundError(f'Missing mapping file: {mapping_path}')

        if client.indices.exists(index=name):
            print(f'Index {name} already exists; skipping')
            continue

        payload = json.loads(mapping_path.read_text(encoding='utf-8'))
        try:
            client.indices.create(index=name, **payload)
            print(f'Created index {name}')
        except ApiError as exc:
            print(f'Failed creating {name}: {exc}', file=sys.stderr)
            if getattr(exc, 'body', None):
                print(json.dumps(exc.body, indent=2), file=sys.stderr)
            raise

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
