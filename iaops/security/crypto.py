from __future__ import annotations

import base64
import hashlib
import os

from cryptography.fernet import Fernet


def _derive_key_material(secret_text: str) -> bytes:
    digest = hashlib.sha256(secret_text.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest)


def _get_fernet() -> Fernet:
    raw = os.getenv("IAOPS_CRYPTO_KEY")
    if raw:
        return Fernet(raw.encode("utf-8"))
    dev_fallback = "iaops-dev-only-change-me"
    return Fernet(_derive_key_material(dev_fallback))


def encrypt_text(value: str) -> str:
    token = _get_fernet().encrypt(value.encode("utf-8"))
    return token.decode("utf-8")


def decrypt_text(value: str) -> str:
    payload = _get_fernet().decrypt(value.encode("utf-8"))
    return payload.decode("utf-8")

