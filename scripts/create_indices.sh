#!/usr/bin/env bash
set -e
until curl -sf http://localhost:9200 >/dev/null; do sleep 2; done
curl -sS -X PUT localhost:9200/engineering_events -H 'Content-Type: application/json' -d @es/engineering_events.json || true
curl -sS -X PUT localhost:9200/documents -H 'Content-Type: application/json' -d @es/documents.json || true
curl -sS -X PUT localhost:9200/quarantine -H 'Content-Type: application/json' -d @es/quarantine.json || true
echo 'Indices ready.'
