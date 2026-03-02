from __future__ import annotations

import json
import os
import time
from typing import Any

try:
    from psycopg import connect
except Exception:  # pragma: no cover
    connect = None

from .celery_app import celery_app
from .pipeline import run_billing_cycle, run_housekeeping, run_ingest_metadata, run_monitor_scan, run_rag_rebuild


def _run_job_payload(job_kind: str, payload: dict[str, Any]) -> dict[str, Any]:
    if job_kind == "ingest_metadata":
        return run_ingest_metadata(payload)
    if job_kind == "rag_rebuild":
        return run_rag_rebuild(payload)
    if job_kind == "monitor_scan":
        return run_monitor_scan(payload)
    if job_kind == "billing_cycle":
        return run_billing_cycle(payload)
    if job_kind == "housekeeping":
        return run_housekeeping(payload)
    time.sleep(0.05)
    return {"status": "ok", "details": f"job {job_kind} executed"}


def _db_enabled() -> bool:
    return bool((os.getenv("IAOPS_DB_DSN") or "").strip() and connect is not None)


def _mark_job_started(job_id: int, payload: dict[str, Any]) -> None:
    if not _db_enabled():
        return
    schema = os.getenv("IAOPS_DB_SCHEMA") or "iaops_gov"
    dsn = os.getenv("IAOPS_DB_DSN") or ""
    sql = f"""
        UPDATE {schema}.async_job_run
           SET status = 'running',
               started_at = COALESCE(started_at, NOW()),
               result_json = COALESCE(result_json, '{{}}'::jsonb) || %(extra)s::jsonb
         WHERE id = %(job_id)s
    """
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(sql, {"job_id": int(job_id), "extra": json.dumps(payload or {}, ensure_ascii=True)})
        conn.commit()


def _mark_job_retry(job_id: int, error_text: str, attempt: int, next_delay_sec: int) -> None:
    if not _db_enabled():
        return
    schema = os.getenv("IAOPS_DB_SCHEMA") or "iaops_gov"
    dsn = os.getenv("IAOPS_DB_DSN") or ""
    sql = f"""
        UPDATE {schema}.async_job_run
           SET status = 'retrying',
               error_text = %(error_text)s,
               result_json = COALESCE(result_json, '{{}}'::jsonb) || %(extra)s::jsonb
         WHERE id = %(job_id)s
    """
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            sql,
            {
                "job_id": int(job_id),
                "error_text": (error_text or "")[:2000],
                "extra": json.dumps(
                    {"attempt": int(attempt), "next_retry_delay_sec": int(max(1, next_delay_sec))},
                    ensure_ascii=True,
                ),
            },
        )
        conn.commit()


def _mark_job_finished(job_id: int, result: dict[str, Any]) -> None:
    if not _db_enabled():
        return
    schema = os.getenv("IAOPS_DB_SCHEMA") or "iaops_gov"
    dsn = os.getenv("IAOPS_DB_DSN") or ""
    sql = f"""
        UPDATE {schema}.async_job_run
           SET status = 'done',
               result_json = %(result)s::jsonb,
               error_text = NULL,
               finished_at = NOW()
         WHERE id = %(job_id)s
    """
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(sql, {"job_id": int(job_id), "result": json.dumps(result or {}, ensure_ascii=True)})
        conn.commit()


def _mark_job_dead_letter(job_id: int, error_text: str, attempt: int) -> None:
    if not _db_enabled():
        return
    schema = os.getenv("IAOPS_DB_SCHEMA") or "iaops_gov"
    dsn = os.getenv("IAOPS_DB_DSN") or ""
    sql = f"""
        UPDATE {schema}.async_job_run
           SET status = 'dead_letter',
               error_text = %(error_text)s,
               result_json = COALESCE(result_json, '{{}}'::jsonb) || %(extra)s::jsonb,
               finished_at = NOW()
         WHERE id = %(job_id)s
    """
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            sql,
            {
                "job_id": int(job_id),
                "error_text": (error_text or "")[:2000],
                "extra": json.dumps({"attempt": int(attempt), "dead_letter": True}, ensure_ascii=True),
            },
        )
        conn.commit()


if celery_app is not None:
    @celery_app.task(bind=True, name="iaops.jobs.execute", max_retries=3)
    def execute_job(self, job_id: int, job_kind: str, payload: dict[str, Any]) -> dict[str, Any]:
        attempt = int(self.request.retries or 0) + 1
        _mark_job_started(int(job_id), {"runner": "celery", "attempt": attempt})
        try:
            result = _run_job_payload(job_kind, payload or {})
            _mark_job_finished(int(job_id), result)
            return result
        except Exception as exc:
            retries = int(self.request.retries or 0)
            if retries < int(self.max_retries):
                delay = min(60, 2 ** retries)
                _mark_job_retry(int(job_id), str(exc), attempt, delay)
                raise self.retry(exc=exc, countdown=delay)
            _mark_job_dead_letter(int(job_id), str(exc), attempt)
            raise
else:
    def execute_job(job_id: int, job_kind: str, payload: dict[str, Any]) -> dict[str, Any]:
        _ = job_id
        return _run_job_payload(job_kind, payload or {})
