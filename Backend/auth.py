import os
import time
import requests
from dotenv import load_dotenv
from fastapi import Request, HTTPException
from jose import jwt, JWTError
from typing import cast, Any, Dict, Optional

load_dotenv()

JWKS_URL = os.getenv("CLERK_JWKS_URL")
if not JWKS_URL:
    raise RuntimeError("CLERK_JWKS_URL is not set in the environment.")

AUDIENCE = os.getenv("CLERK_AUDIENCE")
if not AUDIENCE:
    print("[WARNING] CLERK_AUDIENCE is not set in the environment. Audience claim will not be checked.")

CLERK_ISSUER = JWKS_URL.split("/.well-known/")[0]

_JWKS_CACHE: Optional[Dict[str, Any]] = None
_JWKS_CACHE_TS: float = 0.0
_JWKS_TTL_SECONDS: int = 300


def get_jwks():
    global _JWKS_CACHE, _JWKS_CACHE_TS
    now = time.time()
    if _JWKS_CACHE is not None and (now - _JWKS_CACHE_TS) < _JWKS_TTL_SECONDS:
        return _JWKS_CACHE.get("keys", [])
    try:
        response = requests.get(cast(str, JWKS_URL), timeout = 3.0)
        response.raise_for_status()
        data = response.json()
        _JWKS_CACHE = data
        _JWKS_CACHE_TS = now
        return data.get("keys", [])
    except Exception as e:
        if _JWKS_CACHE is not None:
            return _JWKS_CACHE.get("keys", [])
        raise HTTPException(status_code = 503, detail = f"Unable to fetch JWKS: {str(e)}")


def get_public_key(token):
    jwks = get_jwks()
    unverified_header = jwt.get_unverified_header(token)
    for key in jwks:
        if key["kid"] == unverified_header["kid"]:
            return key
    raise HTTPException(status_code=401, detail="Public key not found.")


def verify_clerk_jwt(request: Request):
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
            audience=AUDIENCE,
            issuer=CLERK_ISSUER,
            options={"verify_aud": False} if not AUDIENCE else {}
        )
        return payload
    except JWTError as e:
        raise HTTPException(status_code=401, detail=f"Token verification failed: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Token verification error: {str(e)}")


