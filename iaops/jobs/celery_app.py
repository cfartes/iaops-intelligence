from __future__ import annotations

import os

try:
    from celery import Celery
except Exception:  # pragma: no cover
    Celery = None


def build_celery() -> "Celery | None":
    if Celery is None:
        return None
    broker_url = os.getenv("IAOPS_CELERY_BROKER_URL") or os.getenv("CELERY_BROKER_URL") or "redis://redis:6379/0"
    backend_url = os.getenv("IAOPS_CELERY_RESULT_BACKEND") or os.getenv("CELERY_RESULT_BACKEND") or broker_url
    app = Celery("iaops_jobs", broker=broker_url, backend=backend_url)
    app.conf.update(
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        task_track_started=True,
        timezone="UTC",
        enable_utc=True,
    )
    return app


celery_app = build_celery()

