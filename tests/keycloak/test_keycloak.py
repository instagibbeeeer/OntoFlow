from pathlib import Path
import sys
import os
import requests
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from api.security import validate_token
issuer = os.getenv(
    "OIDC_ISSUER",
    "http://localhost:8080/realms/ontoflow-ci"
)
def test_keycloak_jwks():
    response = requests.post(
        f"{issuer}/protocol/openid-connect/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "ci-client",
            "client_secret": "ci-test-secret",
        },
        timeout=10,
    )

    response.raise_for_status()
    token = response.json()["access_token"]

    claims = validate_token(token, ("objects:read",))

    assert "objects:read" in claims["scope"].split()