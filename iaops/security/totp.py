from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
import time
from urllib.parse import quote


def generate_base32_secret(length: int = 20) -> str:
    raw = secrets.token_bytes(length)
    return base64.b32encode(raw).decode("ascii").replace("=", "")


def provisioning_uri(*, issuer: str, account_name: str, secret: str) -> str:
    issuer_q = quote(issuer)
    account_q = quote(account_name)
    return (
        f"otpauth://totp/{issuer_q}:{account_q}"
        f"?secret={secret}&issuer={issuer_q}&algorithm=SHA1&digits=6&period=30"
    )


def _normalize_secret(secret: str) -> bytes:
    normalized = secret.strip().upper().replace(" ", "")
    padding = "=" * ((8 - len(normalized) % 8) % 8)
    return base64.b32decode(normalized + padding, casefold=True)


def _totp_at(secret: str, timestamp: int, digits: int = 6, period: int = 30) -> str:
    key = _normalize_secret(secret)
    counter = int(timestamp // period).to_bytes(8, "big")
    digest = hmac.new(key, counter, hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    code_int = ((digest[offset] & 0x7F) << 24) | ((digest[offset + 1] & 0xFF) << 16) | ((digest[offset + 2] & 0xFF) << 8) | (digest[offset + 3] & 0xFF)
    return str(code_int % (10**digits)).zfill(digits)


def verify_totp(secret: str, code: str, *, window: int = 1, timestamp: int | None = None) -> bool:
    normalized = "".join(ch for ch in str(code) if ch.isdigit())
    if len(normalized) != 6:
        return False
    now = int(timestamp if timestamp is not None else time.time())
    for offset in range(-window, window + 1):
        candidate = _totp_at(secret, now + (offset * 30))
        if hmac.compare_digest(candidate, normalized):
            return True
    return False


def generate_current_totp(secret: str, *, timestamp: int | None = None) -> str:
    now = int(timestamp if timestamp is not None else time.time())
    return _totp_at(secret, now)
