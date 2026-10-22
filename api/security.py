
import os
from functools import lru_cache

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import PyJWKClient

bearer = HTTPBearer(auto_error=False)


def validate_token(token: str, required_scopes=()):
    issuer, audience, _ = _oidc_settings()
    try:
        signing_key = _jwks_client().get_signing_key_from_jwt(token)
        claims = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            audience=audience,
            issuer=issuer,
            leeway=30,
            options={"require": ["exp", "iat", "iss", "sub", "aud"]},
        )
    except (jwt.PyJWTError, jwt.PyJWKClientError, ValueError) as exc:
        raise _unauthorized() from exc

    if claims.get("token_use") not in (None, "access"):
        raise _unauthorized("An OAuth access token is required")

    scopes = set(claims.get("scope", "").split())
    scp = claims.get("scp", [])
    scopes.update(scp.split() if isinstance(scp, str) else scp)
    if not set(required_scopes).issubset(scopes):
        raise HTTPException(status_code=403, detail="Insufficient OAuth scopes")
    return claims



