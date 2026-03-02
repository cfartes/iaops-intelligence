from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Any

try:
    from psycopg import connect
except Exception:  # pragma: no cover
    connect = None

from .celery_app import celery_app
from .tasks import execute_job


@dataclass
class JobQueue:
    dsn: str | None
    schema: str = "iaops_gov"
    use_celery: bool = False

    def enqueue(self, *, tenant_id: int | None, job_kind: str, payload: dict[str, Any]) -> dict[str, Any]:
        job_id = self._insert_job_run(tenant_id=tenant_id, job_kind=job_kind, payload=payload)
        if self.use_celery and celery_app is not None:
            task = execute_job.delay(job_id, job_kind, payload)  # type: ignore[attr-defined]
            self._attach_dispatch_metadata(job_id=job_id, payload={"task_id": str(task.id), "runner": "celery"})
            return {"job_id": job_id, "status": "queued", "runner": "celery", "task_id": str(task.id)}

        thread = threading.Thread(target=self._run_sync_job, args=(job_id, job_kind, payload), daemon=True)
        thread.start()
        return {"job_id": job_id, "status": "queued", "runner": "thread"}

    def list_jobs(self, *, tenant_id: int | None, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
        if not self._db_enabled():
            return []
        sql = f"""
            SELECT id, tenant_id, job_kind, status, payload_json, result_json, error_text, started_at, finished_at, created_at
            FROM {self.schema}.async_job_run
            WHERE (%(tenant_id)s::bigint IS NULL OR tenant_id = %(tenant_id)s)
            ORDER BY id DESC
            LIMIT %(limit)s
            OFFSET %(offset)s
        """
        with connect(self.dsn) as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "tenant_id": tenant_id,
                    "limit": max(1, min(limit, 200)),
                    "offset": max(0, int(offset or 0)),
                },
            )
            rows = cur.fetchall()
        items = []
        for row in rows:
            items.append(
                {
                    "id": int(row[0]),
                    "tenant_id": int(row[1]) if row[1] is not None else None,
                    "job_kind": str(row[2]),
                    "status": str(row[3]),
                    "payload": row[4] or {},
                    "result": row[5] or {},
                    "error_text": row[6],
                    "started_at": row[7].isoformat() if row[7] else None,
                    "finished_at": row[8].isoformat() if row[8] else None,
                    "created_at": row[9].isoformat() if row[9] else None,
                }
            )
        return items

    def retry_job(self, *, tenant_id: int | None, job_id: int) -> dict[str, Any]:
        if not self._db_enabled():
            raise ValueError("fila de jobs indisponivel")
        sql = f"""
            SELECT id, tenant_id, job_kind, payload_json, status
            FROM {self.schema}.async_job_run
            WHERE id = %(job_id)s
              AND (%(tenant_id)s::bigint IS NULL OR tenant_id = %(tenant_id)s)
            LIMIT 1
        """
        with connect(self.dsn) as conn, conn.cursor() as cur:
            cur.execute(sql, {"job_id": int(job_id), "tenant_id": tenant_id})
            row = cur.fetchone()
        if not row:
            raise ValueError("job nao encontrado")
        source_status = str(row[4] or "").strip().lower()
        if source_status not in {"failed", "dead_letter"}:
            raise ValueError("somente jobs failed/dead_letter podem ser reprocessados")
        source_tenant_id = int(row[1]) if row[1] is not None else None
        new_payload = dict(row[3] or {})
        if source_tenant_id is not None:
            new_payload.setdefault("tenant_id", source_tenant_id)
        replay = self.enqueue(
            tenant_id=source_tenant_id,
            job_kind=str(row[2]),
            payload=new_payload,
        )
        replay["retried_from_job_id"] = int(job_id)
        return replay

    def _run_sync_job(self, job_id: int, job_kind: str, payload: dict[str, Any]) -> None:
        max_retries = int(os.getenv("IAOPS_SYNC_JOB_MAX_RETRIES") or 3)
        attempt = 0
        while True:
            attempt += 1
            self._mark_job_started(job_id=job_id, payload={"runner": "thread", "attempt": attempt})
            try:
                result = execute_job(job_id, job_kind, payload)  # fallback local
                self._mark_job_finished(job_id=job_id, result=result)
                return
            except Exception as exc:  # pragma: no cover
                if attempt <= max_retries:
                    delay = min(30, 2 ** (attempt - 1))
                    self._mark_job_retrying(job_id=job_id, error_text=str(exc), attempt=attempt, next_delay_sec=delay)
                    time.sleep(delay)
                    continue
                self._mark_job_dead_letter(job_id=job_id, error_text=str(exc), attempt=attempt)
                return

    def _insert_job_run(self, *, tenant_id: int | None, job_kind: str, payload: dict[str, Any]) -> int:
        if not self._db_enabled():
            return int(time.time())
        sql = f"""
            INSERT INTO {self.schema}.async_job_run (
                tenant_id, job_kind, status, payload_json
            )
            VALUES (
                %(tenant_id)s, %(job_kind)s, 'queued', %(payload_json)s::jsonb
            )
            RETURNING id
        """
        with connect(self.dsn) as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "tenant_id": tenant_id,
                    "job_kind": job_kind,
                    "payload_json": json.dumps(payload or {}, ensure_ascii=True),
                },
            )
            row = cur.fetchone()
            conn.commit()
        return int(row[0])

    def _mark_job_started(self, *, job_id: int, payload: dict[str, Any]) -> None:
        if not self._db_enabled():
            return
        sql = f"""
            UPDATE {self.schema}.async_job_run
               SET status = 'running',
                   started_at = NOW(),
                   result_json = COALESCE(result_json, '{{}}'::jsonb) || %(extra)s::jsonb
             WHERE id = %(job_id)s
        """
        with connect(self.dsn) as conn, conn.cursor() as cur:
            cur.execute(sql, {"job_id": job_id, "extra": json.dumps(payload or {}, ensure_ascii=True)})
            conn.commit()

    def _attach_dispatch_metadata(self, *, job_id: int, payload: dict[str, Any]) -> None:
        if not self._db_enabled():
            return
        sql = f"""
            UPDATE {self.schema}.async_job_run
               SET result_json = COALESCE(result_json, '{{}}'::jsonb) || %(extra)s::jsonb
             WHERE id = %(job_id)s
        """
        with connect(self.dsn) as conn, conn.cursor() as cur:
            cur.execute(sql, {"job_id": job_id, "extra": json.dumps(payload or {}, ensure_ascii=True)})
            conn.commit()

    def _mark_job_finished(self, *, job_id: int, result: dict[str, Any]) -> None:
        if not self._db_enabled():
            return
        sql = f"""
            UPDATE {self.schema}.async_job_run
               SET status = 'done',
                   result_json = %(result)s::jsonb,
                   finished_at = NOW()
             WHERE id = %(job_id)s
        """
        with connect(self.dsn) as conn, conn.cursor() as cur:
            cur.execute(sql, {"job_id": job_id, "result": json.dumps(result or {}, ensure_ascii=True)})
            conn.commit()

    def _mark_job_retrying(self, *, job_id: int, error_text: str, attempt: int, next_delay_sec: int) -> None:
        if not self._db_enabled():
            return
        sql = f"""
            UPDATE {self.schema}.async_job_run
               SET status = 'retrying',
                   error_text = %(error_text)s,
                   result_json = COALESCE(result_json, '{{}}'::jsonb) || %(extra)s::jsonb
             WHERE id = %(job_id)s
        """
        with connect(self.dsn) as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "job_id": job_id,
                    "error_text": error_text[:2000],
                    "extra": json.dumps(
                        {"attempt": int(attempt), "next_retry_delay_sec": int(max(1, next_delay_sec))},
                        ensure_ascii=True,
                    ),
                },
            )
            conn.commit()

    def _mark_job_dead_letter(self, *, job_id: int, error_text: str, attempt: int) -> None:
        if not self._db_enabled():
            return
        sql = f"""
            UPDATE {self.schema}.async_job_run
               SET status = 'dead_letter',
                   error_text = %(error_text)s,
                   result_json = COALESCE(result_json, '{{}}'::jsonb) || %(extra)s::jsonb,
                   finished_at = NOW()
             WHERE id = %(job_id)s
        """
        with connect(self.dsn) as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "job_id": job_id,
                    "error_text": error_text[:2000],
                    "extra": json.dumps({"attempt": int(attempt), "dead_letter": True}, ensure_ascii=True),
                },
            )
            conn.commit()

    def _db_enabled(self) -> bool:
        return bool(self.dsn and connect is not None)


_DEFAULT_QUEUE: JobQueue | None = None


def get_job_queue(dsn: str | None, schema: str = "iaops_gov") -> JobQueue:
    global _DEFAULT_QUEUE
    if _DEFAULT_QUEUE is None:
        use_celery = str(os.getenv("IAOPS_USE_CELERY") or "1").lower() not in {"0", "false", "no"}
        _DEFAULT_QUEUE = JobQueue(dsn=dsn, schema=schema, use_celery=use_celery)
    return _DEFAULT_QUEUE
