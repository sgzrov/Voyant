import logging
import os
import time
from typing import Any, Dict, Optional

import requests
from fastapi import HTTPException, Request
from jose import JWTError, jwt

logger = logging.getLogger(__name__)

_JWKS_CACHE: Optional[Dict[str, Any]] = None
_JWKS_CACHE_TS: float = 0.0
_JWKS_TTL_SECONDS: int = 300


# Reads Clerk JWT configuration from env and derives issuer from the JWKS URL
def _get_auth_config() -> tuple[str, Optional[str], str]:
    jwks_url = os.getenv("CLERK_JWKS_URL")
    if not jwks_url:
        raise HTTPException(status_code=500, detail="CLERK_JWKS_URL is not configured.")

    audience = os.getenv("CLERK_AUDIENCE") or None
    issuer = jwks_url.split("/.well-known/")[0]
    return jwks_url, audience, issuer


# Fetches JWKS keys (cached for a short TTL) so we can validate incoming JWT signatures
def get_jwks():
    global _JWKS_CACHE, _JWKS_CACHE_TS
    jwks_url, _, _ = _get_auth_config()
    now = time.time()
    if _JWKS_CACHE is not None and (now - _JWKS_CACHE_TS) < _JWKS_TTL_SECONDS:
        return _JWKS_CACHE.get("keys", [])
    try:
        response = requests.get(jwks_url, timeout=3.0)
        response.raise_for_status()
        data = response.json()
        _JWKS_CACHE = data
        _JWKS_CACHE_TS = now
        return data.get("keys", [])
    except Exception as e:
        if _JWKS_CACHE is not None:
            return _JWKS_CACHE.get("keys", [])
        raise HTTPException(status_code=503, detail=f"Unable to fetch JWKS: {str(e)}")


# Finds the JWK that matches the JWT header `kid` so `jwt.decode()` can verify the signature
def get_public_key(token):
    jwks = get_jwks()
    unverified_header = jwt.get_unverified_header(token)
    for key in jwks:
        if key["kid"] == unverified_header["kid"]:
            return key
    raise HTTPException(status_code=401, detail="Public key not found.")


# Verifies the Clerk bearer token from the Authorization header and returns decoded JWT claims
def verify_clerk_jwt(request: Request):
    _, audience, issuer = _get_auth_config()
    if not audience:
        logger.warning("CLERK_AUDIENCE is not set; audience claim will not be checked.")

    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid token.")

    token = auth_header.split(" ")[1]
    if token.count(".") != 2:
        raise HTTPException(status_code=401, detail="Token is not a valid JWT.")

    try:
        key = get_public_key(token)
        payload = jwt.decode(
            token,
            key,
            algorithms=["RS256"],
            audience=audience,
            issuer=issuer,
            options={"verify_aud": False} if not audience else {},
        )
        return payload
    except JWTError as e:
        raise HTTPException(status_code=401, detail=f"Token verification failed: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Token verification error: {str(e)}")


