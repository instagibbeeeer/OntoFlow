from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from api.security import validate_token
response = requests.post(
    f"{issuer}/protocol/openid-connect/token",
    data={
        "grant_type": "client_credentials",
        "client_id": "ci-client",
        "client_secret": "ci-test-secret",
    },
)
response.raise_for_status()
token = response.json()["access_token"]

# Verify it using OntoFlow's real JWKS validation
claims = validate_token(token, ("objects:read",))

assert "objects:read" in claims["scope"].split()