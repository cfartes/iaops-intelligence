from __future__ import annotations

import hashlib
import json
import os
import urllib.error
import urllib.request
from collections import defaultdict
from typing import Any

try:
    from psycopg import connect
except Exception:  # pragma: no cover
    connect = None

try:
    import pyodbc
except Exception:  # pragma: no cover
    pyodbc = None

try:
    import pymysql
except Exception:  # pragma: no cover
    pymysql = None

try:
    import oracledb
except Exception:  # pragma: no cover
    oracledb = None

from iaops.security.crypto import decrypt_text

_EMBED_CACHE: dict[str, list[float]] = {}


def run_ingest_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    tenant_id = int(payload.get("tenant_id") or 0)
    data_source_id = payload.get("data_source_id")
    if tenant_id <= 0:
        return {"status": "error", "message": "tenant_id obrigatorio", "ingested_objects": 0}
    if connect is None:
        return {"status": "error", "message": "psycopg indisponivel", "ingested_objects": 0}
    dsn = os.getenv("IAOPS_DB_DSN")
    schema = os.getenv("IAOPS_DB_SCHEMA") or "iaops_gov"
    if not dsn:
        return {"status": "error", "message": "IAOPS_DB_DSN nao configurado", "ingested_objects": 0}

    with connect(dsn) as conn, conn.cursor() as cur:
        sources = _load_data_sources(cur=cur, schema=schema, tenant_id=tenant_id, data_source_id=data_source_id)
        ingested_tables = 0
        ingested_columns = 0
        docs_upserted = 0
        errors: list[str] = []
        for source in sources:
            try:
                metadata = _extract_source_metadata(source)
                table_map = _upsert_monitored_metadata(
                    cur=cur,
                    schema=schema,
                    tenant_id=tenant_id,
                    data_source_id=int(source["id"]),
                    metadata=metadata,
                )
                ingested_tables += len(table_map)
                ingested_columns += sum(len(item.get("columns") or []) for item in metadata)
                docs_upserted += _upsert_rag_documents(
                    cur=cur,
                    schema=schema,
                    tenant_id=tenant_id,
                    data_source_id=int(source["id"]),
                    source_type=str(source["source_type"]),
                    metadata=metadata,
                )
            except Exception as exc:  # pragma: no cover
                errors.append(f"source#{source['id']}: {exc}")
        conn.commit()
    return {
        "status": "ok" if not errors else "partial",
        "ingested_tables": ingested_tables,
        "ingested_columns": ingested_columns,
        "docs_upserted": docs_upserted,
        "errors": errors,
    }


def run_rag_rebuild(payload: dict[str, Any]) -> dict[str, Any]:
    tenant_id = int(payload.get("tenant_id") or 0)
    if tenant_id <= 0:
        return {"status": "error", "message": "tenant_id obrigatorio", "indexed_documents": 0}
    if connect is None:
        return {"status": "error", "message": "psycopg indisponivel", "indexed_documents": 0}
    dsn = os.getenv("IAOPS_DB_DSN")
    schema = os.getenv("IAOPS_DB_SCHEMA") or "iaops_gov"
    if not dsn:
        return {"status": "error", "message": "IAOPS_DB_DSN nao configurado", "indexed_documents": 0}
    with connect(dsn) as conn, conn.cursor() as cur:
        sql = f"""
            SELECT rd.id, rd.content_text
            FROM {schema}.rag_document rd
            WHERE rd.tenant_id = %(tenant_id)s
        """
        cur.execute(sql, {"tenant_id": tenant_id})
        rows = cur.fetchall()
        for row in rows:
            emb = _text_embedding(str(row[1] or ""))
            cur.execute(
                f"UPDATE {schema}.rag_document SET embedding_json = %(emb)s::jsonb, updated_at = NOW() WHERE id = %(id)s",
                {"id": int(row[0]), "emb": json.dumps(emb, ensure_ascii=True)},
            )
        conn.commit()
    return {"status": "ok", "indexed_documents": len(rows)}


def run_monitor_scan(payload: dict[str, Any]) -> dict[str, Any]:
    tenant_id = int(payload.get("tenant_id") or 0)
    if tenant_id <= 0:
        return {"status": "error", "message": "tenant_id obrigatorio", "alerts_triggered": 0}
    if connect is None:
        return {"status": "error", "message": "psycopg indisponivel", "alerts_triggered": 0}
    dsn = os.getenv("IAOPS_DB_DSN")
    schema = os.getenv("IAOPS_DB_SCHEMA") or "iaops_gov"
    if not dsn:
        return {"status": "error", "message": "IAOPS_DB_DSN nao configurado", "alerts_triggered": 0}
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) FROM {schema}.monitored_table WHERE tenant_id = %(tenant_id)s AND is_active = TRUE",
            {"tenant_id": tenant_id},
        )
        monitored = int((cur.fetchone() or [0])[0] or 0)
    return {"status": "ok", "alerts_triggered": 0, "monitored_tables": monitored}


def run_billing_cycle(payload: dict[str, Any]) -> dict[str, Any]:
    if connect is None:
        return {"status": "error", "message": "psycopg indisponivel", "installments_created": 0}
    dsn = os.getenv("IAOPS_DB_DSN")
    schema = os.getenv("IAOPS_DB_SCHEMA") or "iaops_gov"
    if not dsn:
        return {"status": "error", "message": "IAOPS_DB_DSN nao configurado", "installments_created": 0}
    created = 0
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT s.id, p.monthly_price_cents
            FROM {schema}.billing_subscription s
            JOIN {schema}.billing_plan p ON p.id = s.plan_id
            WHERE s.status = 'active'
            """
        )
        rows = cur.fetchall()
        for row in rows:
            sub_id = int(row[0])
            amount = int(row[1])
            cur.execute(
                f"""
                INSERT INTO {schema}.billing_installment (subscription_id, due_date, amount_cents, status)
                SELECT %(sub_id)s, (date_trunc('month', NOW()) + INTERVAL '1 month')::date, %(amount)s, 'open'
                WHERE NOT EXISTS (
                    SELECT 1 FROM {schema}.billing_installment i
                    WHERE i.subscription_id = %(sub_id)s
                      AND i.due_date = (date_trunc('month', NOW()) + INTERVAL '1 month')::date
                )
                """,
                {"sub_id": sub_id, "amount": amount},
            )
            if cur.rowcount > 0:
                created += 1
        conn.commit()
    return {"status": "ok", "installments_created": created}


def run_housekeeping(payload: dict[str, Any]) -> dict[str, Any]:
    if connect is None:
        return {"status": "error", "message": "psycopg indisponivel"}
    dsn = os.getenv("IAOPS_DB_DSN")
    schema = os.getenv("IAOPS_DB_SCHEMA") or "iaops_gov"
    if not dsn:
        return {"status": "error", "message": "IAOPS_DB_DSN nao configurado"}
    retention_days = int(payload.get("retention_days") or 90)
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {schema}.mcp_call_log WHERE requested_at < NOW() - (%(days)s::text || ' days')::interval",
            {"days": retention_days},
        )
        deleted_calls = int(cur.rowcount or 0)
        cur.execute(
            f"""
            DELETE FROM {schema}.async_job_run
            WHERE finished_at IS NOT NULL
              AND finished_at < NOW() - (%(days)s::text || ' days')::interval
              AND status IN ('done', 'failed', 'dead_letter')
            """,
            {"days": retention_days},
        )
        deleted_jobs = int(cur.rowcount or 0)
        conn.commit()
    return {"status": "ok", "retention_days": retention_days, "deleted_calls": deleted_calls, "deleted_jobs": deleted_jobs}


def run_hub_intake_retry(payload: dict[str, Any]) -> dict[str, Any]:
    if connect is None:
        return {"status": "error", "message": "psycopg indisponivel"}
    dsn = os.getenv("IAOPS_DB_DSN")
    schema = os.getenv("IAOPS_DB_SCHEMA") or "iaops_gov"
    if not dsn:
        return {"status": "error", "message": "IAOPS_DB_DSN nao configurado"}

    client_id = int(payload.get("client_id") or 0)
    limit = max(1, min(int(payload.get("limit") or 20), 100))
    retry_delay_sec = max(60, int(payload.get("retry_delay_sec") or 1800))
    _ensure_hub_intake_pending_table(dsn=dsn, schema=schema)

    with connect(dsn) as conn, conn.cursor() as cur:
        if client_id > 0:
            cur.execute(
                f"""
                SELECT id, client_id, attempts
                FROM {schema}.hub_intake_pending
                WHERE client_id = %(client_id)s::bigint
                  AND status = 'pending'
                LIMIT 1
                """,
                {"client_id": client_id},
            )
        else:
            cur.execute(
                f"""
                SELECT id, client_id, attempts
                FROM {schema}.hub_intake_pending
                WHERE status = 'pending'
                  AND next_retry_at <= NOW()
                ORDER BY next_retry_at ASC, id ASC
                LIMIT %(limit)s
                """,
                {"limit": limit},
            )
        rows = cur.fetchall()

    if not rows:
        return {"status": "ok", "processed": 0, "sent": 0, "failed": 0}

    cfg = _load_hub_intake_config(dsn=dsn, schema=schema)
    intake_key = str(cfg.get("intake_api_key") or "").strip()
    intake_url = str(cfg.get("intake_endpoint_url") or "").strip()

    sent = 0
    failed = 0
    failed_clients: list[int] = []
    last_error = None
    for row in rows:
        pending_id = int(row[0])
        item_client_id = int(row[1])
        attempts = int(row[2] or 0)

        if not intake_key or not intake_url:
            failed += 1
            failed_clients.append(item_client_id)
            last_error = "intake_nao_configurado"
            _upsert_hub_intake_pending_failure(
                dsn=dsn,
                schema=schema,
                client_id=item_client_id,
                attempts=attempts,
                reason="intake_api_key_or_url_not_configured",
                status_code=None,
                payload_json=None,
                retry_delay_sec=retry_delay_sec,
            )
            continue

        payload_obj = _build_hub_intake_client_payload(dsn=dsn, schema=schema, client_id=item_client_id)
        if not payload_obj:
            failed += 1
            last_error = "payload_nao_encontrado"
            _upsert_hub_intake_pending_failure(
                dsn=dsn,
                schema=schema,
                client_id=item_client_id,
                attempts=attempts,
                reason="client_payload_not_found",
                status_code=None,
                payload_json=None,
                retry_delay_sec=retry_delay_sec,
            )
            continue

        send_result = _send_hub_intake_payload(
            intake_url=intake_url,
            intake_key=intake_key,
            payload_obj=payload_obj,
        )
        if bool(send_result.get("sent")):
            sent += 1
            _mark_hub_intake_pending_sent(
                dsn=dsn,
                schema=schema,
                pending_id=pending_id,
                payload_json=payload_obj,
                status_code=send_result.get("status_code"),
            )
            continue

        failed += 1
        failed_clients.append(item_client_id)
        last_error = str(send_result.get("reason") or "send_failed")
        _upsert_hub_intake_pending_failure(
            dsn=dsn,
            schema=schema,
            client_id=item_client_id,
            attempts=attempts,
            reason=last_error,
            status_code=send_result.get("status_code"),
            payload_json=payload_obj,
            retry_delay_sec=retry_delay_sec,
        )

    # Reagenda somente os clientes que ainda falharam.
    if failed_clients:
        try:
            from iaops.jobs.queue import get_job_queue

            queue = get_job_queue(dsn, schema)
            for cid in sorted(set(failed_clients)):
                queue.enqueue(
                    tenant_id=None,
                    job_kind="hub_intake_retry",
                    payload={"client_id": int(cid), "retry_delay_sec": retry_delay_sec},
                    delay_seconds=retry_delay_sec,
                )
        except Exception:
            pass

    return {
        "status": "ok" if failed == 0 else ("partial" if sent > 0 else "error"),
        "processed": len(rows),
        "sent": sent,
        "failed": failed,
        "last_error": last_error,
    }


def search_rag_documents(*, tenant_id: int, query_text: str, limit: int = 8) -> list[dict[str, Any]]:
    if connect is None:
        return []
    dsn = os.getenv("IAOPS_DB_DSN")
    schema = os.getenv("IAOPS_DB_SCHEMA") or "iaops_gov"
    if not dsn:
        return []
    qv = _text_embedding(query_text or "")
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, doc_kind, doc_key, content_text, metadata_json, embedding_json
            FROM {schema}.rag_document
            WHERE tenant_id = %(tenant_id)s
            """,
            {"tenant_id": tenant_id},
        )
        rows = cur.fetchall()
    scored: list[tuple[float, dict[str, Any]]] = []
    for row in rows:
        emb = row[5] or []
        if not isinstance(emb, list):
            continue
        sim = _cosine_similarity(qv, [float(x) for x in emb if isinstance(x, (int, float))])
        scored.append(
            (
                sim,
                {
                    "id": int(row[0]),
                    "doc_kind": row[1],
                    "doc_key": row[2],
                    "content_text": row[3],
                    "metadata": row[4] or {},
                    "score": sim,
                },
            )
        )
    scored.sort(key=lambda item: item[0], reverse=True)
    return [item for _, item in scored[: max(1, min(limit, 50))]]


def _ensure_hub_intake_pending_table(*, dsn: str, schema: str) -> None:
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.hub_intake_pending (
                id BIGSERIAL PRIMARY KEY,
                client_id BIGINT NOT NULL UNIQUE REFERENCES {schema}.client(id) ON DELETE CASCADE,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                last_status_code INTEGER,
                payload_json JSONB,
                next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                sent_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_hub_intake_pending_status_next_retry
                ON {schema}.hub_intake_pending (status, next_retry_at)
            """
        )
        conn.commit()


def _load_hub_intake_config(*, dsn: str, schema: str) -> dict[str, str]:
    env_key = str(
        os.getenv("IAOPS_HUB_INTAKE_API_KEY")
        or os.getenv("HUB_APP_KEY")
        or ""
    ).strip()
    env_url = str(
        os.getenv("IAOPS_HUB_INTAKE_URL")
        or ""
    ).strip()
    if env_key and env_url:
        return {"intake_api_key": env_key, "intake_endpoint_url": env_url}
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT intake_api_key_enc, intake_endpoint_url
            FROM {schema}.app_hub_integration_config
            WHERE id = 1
            LIMIT 1
            """
        )
        row = cur.fetchone()
    db_key = ""
    db_url = ""
    if row:
        enc_key = str(row[0] or "").strip()
        if enc_key:
            try:
                db_key = decrypt_text(enc_key)
            except Exception:
                db_key = ""
        db_url = str(row[1] or "").strip()
    return {
        "intake_api_key": env_key or db_key,
        "intake_endpoint_url": env_url or db_url,
    }


def _build_hub_intake_client_payload(*, dsn: str, schema: str, client_id: int) -> dict[str, Any] | None:
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                c.id,
                c.fantasy_name,
                c.legal_name,
                c.cnpj,
                COALESCE(c.contact_email, c.access_email),
                COALESCE(c.financial_email, c.contact_email, c.access_email),
                COALESCE(c.nf_email, c.notification_email, c.financial_email, c.contact_email, c.access_email),
                c.contact_phone,
                c.address_text,
                c.bairro,
                c.cidade,
                c.uf,
                c.cep,
                c.created_at::date AS created_date,
                c.status,
                c.data_liberado,
                COALESCE(bp.code, p.code, '-') AS plan_code,
                COALESCE(bp.monthly_price_cents, p.monthly_price_cents, 0) AS monthly_price_cents
            FROM {schema}.client c
            LEFT JOIN LATERAL (
                SELECT bs.plan_id
                FROM {schema}.billing_subscription bs
                WHERE bs.client_id = c.id
                  AND bs.status = 'active'
                ORDER BY bs.id DESC
                LIMIT 1
            ) bs ON TRUE
            LEFT JOIN {schema}.billing_plan bp ON bp.id = bs.plan_id
            LEFT JOIN LATERAL (
                SELECT s.plan_id
                FROM {schema}.subscription s
                WHERE s.client_id = c.id
                  AND s.status = 'active'
                ORDER BY s.id DESC
                LIMIT 1
            ) s ON TRUE
            LEFT JOIN {schema}.plan p ON p.id = s.plan_id
            WHERE c.id = %(client_id)s::bigint
            LIMIT 1
            """,
            {"client_id": int(client_id)},
        )
        row = cur.fetchone()
    if not row:
        return None
    status_raw = str(row[14] or "").strip().lower()
    status_access = "active" if status_raw in {"active", "ativo"} else ("blocked" if status_raw in {"blocked", "bloqueado"} else "inactive")
    address_parts = [str(row[8] or "").strip(), str(row[9] or "").strip(), str(row[10] or "").strip(), str(row[11] or "").strip(), str(row[12] or "").strip()]
    full_address = ", ".join([part for part in address_parts if part])
    return {
        "external_client_id": str(row[0]),
        "nome": row[1] or row[2] or f"Cliente {row[0]}",
        "cpf_cnpj": row[3],
        "email_contato": row[4],
        "email_financeiro": row[5] or row[4],
        "email_nf": row[6] or row[5] or row[4],
        "documento": row[3],
        "telefone": row[7],
        "endereco": full_address or row[8],
        "plano": row[16],
        "valor_cents": int(row[17] or 0),
        "data_cadastro": row[13].isoformat() if row[13] else None,
        "status_acesso": status_access,
        "data_liberado": row[15].isoformat() if row[15] else None,
    }


def _send_hub_intake_payload(*, intake_url: str, intake_key: str, payload_obj: dict[str, Any]) -> dict[str, Any]:
    request = urllib.request.Request(
        url=str(intake_url),
        method="POST",
        data=json.dumps(payload_obj).encode("utf-8"),
        headers={"Content-Type": "application/json", "X-HUB-APP-KEY": str(intake_key)},
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return {"sent": True, "status_code": int(getattr(response, "status", 200) or 200)}
    except urllib.error.HTTPError as exc:
        details = ""
        try:
            details = exc.read().decode("utf-8", errors="ignore")
        except Exception:
            details = ""
        return {"sent": False, "status_code": int(exc.code), "reason": details or str(exc.reason)}
    except Exception as exc:
        return {"sent": False, "reason": str(exc)}


def _upsert_hub_intake_pending_failure(
    *,
    dsn: str,
    schema: str,
    client_id: int,
    attempts: int,
    reason: str,
    status_code: Any,
    payload_json: dict[str, Any] | None,
    retry_delay_sec: int,
) -> None:
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {schema}.hub_intake_pending (
                client_id, status, attempts, last_error, last_status_code, payload_json, next_retry_at, updated_at
            )
            VALUES (
                %(client_id)s, 'pending', %(attempts)s, %(last_error)s, %(last_status_code)s, %(payload_json)s::jsonb,
                NOW() + (%(retry_delay_sec)s::text || ' seconds')::interval,
                NOW()
            )
            ON CONFLICT (client_id)
            DO UPDATE SET
                status = 'pending',
                attempts = GREATEST(1, {schema}.hub_intake_pending.attempts + 1),
                last_error = EXCLUDED.last_error,
                last_status_code = EXCLUDED.last_status_code,
                payload_json = COALESCE(EXCLUDED.payload_json, {schema}.hub_intake_pending.payload_json),
                next_retry_at = NOW() + (%(retry_delay_sec)s::text || ' seconds')::interval,
                updated_at = NOW()
            """,
            {
                "client_id": int(client_id),
                "attempts": max(1, int(attempts or 0) + 1),
                "last_error": str(reason or "")[:2000],
                "last_status_code": int(status_code) if status_code not in (None, "") else None,
                "payload_json": json.dumps(payload_json or {}, ensure_ascii=True),
                "retry_delay_sec": int(max(60, retry_delay_sec)),
            },
        )
        conn.commit()


def _mark_hub_intake_pending_sent(
    *,
    dsn: str,
    schema: str,
    pending_id: int,
    payload_json: dict[str, Any] | None,
    status_code: Any,
) -> None:
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {schema}.hub_intake_pending
               SET status = 'sent',
                   sent_at = NOW(),
                   next_retry_at = NOW(),
                   last_error = NULL,
                   last_status_code = COALESCE(%(last_status_code)s, last_status_code),
                   payload_json = COALESCE(%(payload_json)s::jsonb, payload_json),
                   updated_at = NOW()
             WHERE id = %(pending_id)s
            """,
            {
                "pending_id": int(pending_id),
                "last_status_code": int(status_code) if status_code not in (None, "") else None,
                "payload_json": json.dumps(payload_json or {}, ensure_ascii=True),
            },
        )
        conn.commit()


def _load_data_sources(*, cur, schema: str, tenant_id: int, data_source_id: Any) -> list[dict]:
    sql = f"""
        SELECT id, source_type, conn_secret_ref
        FROM {schema}.data_source
        WHERE tenant_id = %(tenant_id)s
          AND is_active = TRUE
          AND (%(data_source_id)s::bigint IS NULL OR id = %(data_source_id)s)
        ORDER BY id
    """
    cur.execute(sql, {"tenant_id": tenant_id, "data_source_id": int(data_source_id) if data_source_id not in (None, "") else None})
    rows = cur.fetchall()
    return [{"id": int(row[0]), "source_type": str(row[1]), "conn_secret_ref": str(row[2] or "")} for row in rows]


def _extract_source_metadata(source: dict[str, Any]) -> list[dict[str, Any]]:
    source_type = str(source["source_type"]).lower().strip()
    profile = _parse_secret_profile(str(source.get("conn_secret_ref") or ""))
    if source_type in {"postgres", "postgresql"}:
        return _read_postgres_metadata(profile)
    if source_type in {"mysql"}:
        return _read_mysql_metadata(profile)
    if source_type in {"sqlserver", "sql_server", "mssql"}:
        return _read_sqlserver_metadata(profile)
    if source_type in {"oracle"}:
        return _read_oracle_metadata(profile)
    if source_type in {"power_bi", "powerbi"}:
        return _read_powerbi_metadata(profile)
    if source_type in {"microsoft_fabric", "fabric"}:
        return _read_fabric_metadata(profile)
    return []


def _parse_secret_profile(conn_secret_ref: str) -> dict[str, Any]:
    raw = str(conn_secret_ref or "").strip()
    if raw.startswith("json:"):
        parsed = json.loads(raw[5:])
        return parsed if isinstance(parsed, dict) else {}
    if raw.startswith("enc:"):
        parsed = json.loads(decrypt_text(raw[4:]))
        return parsed if isinstance(parsed, dict) else {}
    if raw.startswith("{") and raw.endswith("}"):
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _read_postgres_metadata(profile: dict[str, Any]) -> list[dict[str, Any]]:
    if connect is None:
        return []
    dsn = str(profile.get("dsn") or "").strip()
    if not dsn:
        host = str(profile.get("host") or "").strip()
        port = str(profile.get("port") or "5432").strip()
        dbname = str(profile.get("dbname") or profile.get("database") or "").strip()
        user = str(profile.get("user") or "").strip()
        password = str(profile.get("password") or "").strip()
        if not host or not dbname or not user:
            return []
        dsn = f"host={host} port={port} dbname={dbname} user={user}"
        if password:
            dsn += f" password={password}"
    sql = """
        SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name, ordinal_position
    """
    grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(sql)
        for schema_name, table_name, column_name, data_type in cur.fetchall():
            grouped[(str(schema_name), str(table_name))].append({"column_name": str(column_name), "data_type": str(data_type)})
    return _grouped_to_metadata(grouped)


def _read_mysql_metadata(profile: dict[str, Any]) -> list[dict[str, Any]]:
    if pymysql is None:
        return []
    host = str(profile.get("host") or "").strip()
    port = int(profile.get("port") or 3306)
    database = str(profile.get("database") or profile.get("dbname") or "").strip()
    user = str(profile.get("user") or profile.get("username") or "").strip()
    password = str(profile.get("password") or "").strip()
    if not host or not user:
        return []
    sql = """
        SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA NOT IN ('information_schema','mysql','performance_schema','sys')
        ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
    """
    if database:
        sql = """
            SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s
            ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
        """
    grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    with pymysql.connect(host=host, port=port, user=user, password=password, database=database or None) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (database,) if database else ())
            for schema_name, table_name, column_name, data_type in cur.fetchall():
                grouped[(str(schema_name), str(table_name))].append({"column_name": str(column_name), "data_type": str(data_type)})
    return _grouped_to_metadata(grouped)


def _read_sqlserver_metadata(profile: dict[str, Any]) -> list[dict[str, Any]]:
    if pyodbc is None:
        return []
    host = str(profile.get("host") or profile.get("server") or "").strip()
    port = int(profile.get("port") or 1433)
    database = str(profile.get("database") or profile.get("dbname") or "master").strip()
    user = str(profile.get("user") or profile.get("username") or "").strip()
    password = str(profile.get("password") or "").strip()
    if not host or not user:
        return []
    driver = str(profile.get("driver") or "").strip()
    if not driver:
        available = list(pyodbc.drivers())
        preferred = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server"]
        driver = next((item for item in preferred if item in available), (available[0] if available else "SQL Server"))
    conn_str = (
        f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={database};UID={user};PWD={password};"
        "Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=8;"
    )
    sql = """
        SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'sys')
        ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
    """
    grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    with pyodbc.connect(conn_str, timeout=8) as conn:
        cur = conn.cursor()
        cur.execute(sql)
        for schema_name, table_name, column_name, data_type in cur.fetchall():
            grouped[(str(schema_name), str(table_name))].append({"column_name": str(column_name), "data_type": str(data_type)})
    return _grouped_to_metadata(grouped)


def _read_oracle_metadata(profile: dict[str, Any]) -> list[dict[str, Any]]:
    if oracledb is None:
        return []
    host = str(profile.get("host") or profile.get("server") or "").strip()
    port = int(profile.get("port") or 1521)
    user = str(profile.get("user") or profile.get("username") or "").strip()
    password = str(profile.get("password") or "").strip()
    service_name = str(profile.get("service_name") or "").strip()
    owner_filter = str(profile.get("owner") or user).upper().strip()
    if not host or not user:
        return []
    dsn = str(profile.get("dsn") or "").strip()
    if not dsn:
        dsn = f"{host}:{port}/{service_name or 'XEPDB1'}"
    sql = """
        SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = :owner
        ORDER BY OWNER, TABLE_NAME, COLUMN_ID
    """
    grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    with oracledb.connect(user=user, password=password, dsn=dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, owner=owner_filter)
            for schema_name, table_name, column_name, data_type in cur.fetchall():
                grouped[(str(schema_name), str(table_name))].append({"column_name": str(column_name), "data_type": str(data_type)})
    return _grouped_to_metadata(grouped)


def _read_powerbi_metadata(profile: dict[str, Any]) -> list[dict[str, Any]]:
    token = str(profile.get("access_token") or "").strip()
    if not token:
        return []
    url = str(profile.get("api_url") or "https://api.powerbi.com/v1.0/myorg/groups?$top=20").strip()
    req = urllib.request.Request(url=url, method="GET", headers={"Authorization": f"Bearer {token}", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=12) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    values = data.get("value") if isinstance(data, dict) else None
    if not isinstance(values, list):
        return []
    metadata: list[dict[str, Any]] = []
    for item in values:
        name = str((item or {}).get("name") or (item or {}).get("id") or "workspace")
        metadata.append(
            {
                "schema_name": "powerbi",
                "table_name": name.replace(" ", "_").lower(),
                "columns": [{"column_name": "id", "data_type": "string"}, {"column_name": "name", "data_type": "string"}],
            }
        )
    return metadata


def _read_fabric_metadata(profile: dict[str, Any]) -> list[dict[str, Any]]:
    token = str(profile.get("access_token") or "").strip()
    if not token:
        return []
    url = str(profile.get("api_url") or "https://api.fabric.microsoft.com/v1/workspaces?top=20").strip()
    req = urllib.request.Request(url=url, method="GET", headers={"Authorization": f"Bearer {token}", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=12) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    values = data.get("value") if isinstance(data, dict) else None
    if not isinstance(values, list):
        return []
    metadata: list[dict[str, Any]] = []
    for item in values:
        name = str((item or {}).get("displayName") or (item or {}).get("name") or (item or {}).get("id") or "workspace")
        metadata.append(
            {
                "schema_name": "fabric",
                "table_name": name.replace(" ", "_").lower(),
                "columns": [{"column_name": "id", "data_type": "string"}, {"column_name": "name", "data_type": "string"}],
            }
        )
    return metadata


def _grouped_to_metadata(grouped: dict[tuple[str, str], list[dict[str, str]]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (schema_name, table_name), columns in grouped.items():
        rows.append({"schema_name": schema_name, "table_name": table_name, "columns": columns})
    return rows


def _upsert_monitored_metadata(*, cur, schema: str, tenant_id: int, data_source_id: int, metadata: list[dict[str, Any]]) -> dict[str, int]:
    table_map: dict[str, int] = {}
    for item in metadata:
        schema_name = str(item.get("schema_name") or "").strip() or "public"
        table_name = str(item.get("table_name") or "").strip()
        if not table_name:
            continue
        cur.execute(
            f"""
            INSERT INTO {schema}.monitored_table (
                tenant_id, data_source_id, schema_name, table_name, is_active
            )
            VALUES (
                %(tenant_id)s, %(data_source_id)s, %(schema_name)s, %(table_name)s, TRUE
            )
            ON CONFLICT (tenant_id, data_source_id, schema_name, table_name)
            DO UPDATE SET is_active = TRUE
            RETURNING id
            """,
            {
                "tenant_id": tenant_id,
                "data_source_id": data_source_id,
                "schema_name": schema_name,
                "table_name": table_name,
            },
        )
        mt_id = int(cur.fetchone()[0])
        table_map[f"{schema_name}.{table_name}"] = mt_id
        for column in item.get("columns") or []:
            col_name = str(column.get("column_name") or "").strip()
            if not col_name:
                continue
            cur.execute(
                f"""
                INSERT INTO {schema}.monitored_column (
                    tenant_id, monitored_table_id, column_name, data_type
                )
                VALUES (
                    %(tenant_id)s, %(monitored_table_id)s, %(column_name)s, %(data_type)s
                )
                ON CONFLICT (monitored_table_id, column_name)
                DO UPDATE SET data_type = EXCLUDED.data_type
                """,
                {
                    "tenant_id": tenant_id,
                    "monitored_table_id": mt_id,
                    "column_name": col_name,
                    "data_type": str(column.get("data_type") or "") or None,
                },
            )
    return table_map


def _upsert_rag_documents(
    *,
    cur,
    schema: str,
    tenant_id: int,
    data_source_id: int,
    source_type: str,
    metadata: list[dict[str, Any]],
) -> int:
    count = 0
    for item in metadata:
        schema_name = str(item.get("schema_name") or "").strip() or "public"
        table_name = str(item.get("table_name") or "").strip()
        if not table_name:
            continue
        cols = item.get("columns") or []
        col_text = ", ".join(f"{col.get('column_name')}:{col.get('data_type')}" for col in cols if col.get("column_name"))
        content = f"Fonte {source_type}. Tabela {schema_name}.{table_name}. Colunas: {col_text}"
        emb = _text_embedding(content)
        cur.execute(
            f"""
            INSERT INTO {schema}.rag_document (
                tenant_id, data_source_id, doc_kind, doc_key, content_text, metadata_json, embedding_json, updated_at
            )
            VALUES (
                %(tenant_id)s, %(data_source_id)s, 'table_schema', %(doc_key)s, %(content_text)s, %(metadata_json)s::jsonb, %(embedding)s::jsonb, NOW()
            )
            ON CONFLICT (tenant_id, doc_kind, doc_key)
            DO UPDATE SET
                content_text = EXCLUDED.content_text,
                metadata_json = EXCLUDED.metadata_json,
                embedding_json = EXCLUDED.embedding_json,
                updated_at = NOW()
            """,
            {
                "tenant_id": tenant_id,
                "data_source_id": data_source_id,
                "doc_key": f"{data_source_id}:{schema_name}.{table_name}",
                "content_text": content,
                "metadata_json": json.dumps(
                    {
                        "source_type": source_type,
                        "schema_name": schema_name,
                        "table_name": table_name,
                        "columns": [col.get("column_name") for col in cols if col.get("column_name")],
                    },
                    ensure_ascii=True,
                ),
                "embedding": json.dumps(emb, ensure_ascii=True),
            },
        )
        count += 1
    return count


def _text_embedding(text: str, dimensions: int = 48) -> list[float]:
    norm_text = str(text or "").strip()
    cache_key = hashlib.sha256(norm_text.encode("utf-8")).hexdigest()
    cached = _EMBED_CACHE.get(cache_key)
    if cached is not None:
        return cached

    provider = str(os.getenv("IAOPS_EMBEDDING_PROVIDER") or "deterministic").strip().lower()
    vector: list[float] | None = None
    if provider in {"openai", "openai_compat", "azure_openai"}:
        vector = _embedding_openai_compatible(norm_text)
    elif provider in {"ollama"}:
        vector = _embedding_ollama(norm_text)
    if not vector:
        vector = _embedding_deterministic(norm_text, dimensions=dimensions)
    _EMBED_CACHE[cache_key] = vector
    return vector


def _embedding_deterministic(text: str, dimensions: int = 48) -> list[float]:
    vec = [0.0 for _ in range(dimensions)]
    for token in str(text or "").lower().split():
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        for i in range(dimensions):
            vec[i] += float(digest[i % len(digest)]) / 255.0
    norm = sum(v * v for v in vec) ** 0.5
    if norm <= 1e-9:
        return vec
    return [round(v / norm, 6) for v in vec]


def _embedding_openai_compatible(text: str) -> list[float] | None:
    endpoint = str(os.getenv("IAOPS_EMBEDDING_ENDPOINT") or "https://api.openai.com/v1/embeddings").strip()
    model = str(os.getenv("IAOPS_EMBEDDING_MODEL") or "text-embedding-3-small").strip()
    api_key = str(os.getenv("IAOPS_EMBEDDING_API_KEY") or os.getenv("OPENAI_API_KEY") or "").strip()
    if not endpoint or not model or not api_key:
        return None
    timeout = float(os.getenv("IAOPS_EMBEDDING_TIMEOUT_SEC") or 12)
    body = {"model": model, "input": text}
    req = urllib.request.Request(
        url=endpoint,
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        data=json.dumps(body, ensure_ascii=True).encode("utf-8"),
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list) or not data:
            return None
        emb = data[0].get("embedding") if isinstance(data[0], dict) else None
        if not isinstance(emb, list):
            return None
        return [float(x) for x in emb if isinstance(x, (int, float))]
    except Exception:  # pragma: no cover
        return None


def _embedding_ollama(text: str) -> list[float] | None:
    base = str(os.getenv("IAOPS_OLLAMA_ENDPOINT") or "http://ollama:11434").strip().rstrip("/")
    endpoint = str(os.getenv("IAOPS_EMBEDDING_ENDPOINT") or f"{base}/api/embeddings").strip()
    model = str(os.getenv("IAOPS_OLLAMA_MODEL") or os.getenv("IAOPS_EMBEDDING_MODEL") or "nomic-embed-text").strip()
    if not endpoint or not model:
        return None
    timeout = float(os.getenv("IAOPS_EMBEDDING_TIMEOUT_SEC") or 12)
    body = {"model": model, "prompt": text}
    req = urllib.request.Request(
        url=endpoint,
        method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        data=json.dumps(body, ensure_ascii=True).encode("utf-8"),
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        emb = payload.get("embedding") if isinstance(payload, dict) else None
        if not isinstance(emb, list):
            return None
        return [float(x) for x in emb if isinstance(x, (int, float))]
    except Exception:  # pragma: no cover
        return None


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    if not a or not b:
        return 0.0
    size = min(len(a), len(b))
    dot = sum(float(a[i]) * float(b[i]) for i in range(size))
    na = sum(float(a[i]) * float(a[i]) for i in range(size)) ** 0.5
    nb = sum(float(b[i]) * float(b[i]) for i in range(size)) ** 0.5
    if na <= 1e-9 or nb <= 1e-9:
        return 0.0
    return float(dot / (na * nb))
