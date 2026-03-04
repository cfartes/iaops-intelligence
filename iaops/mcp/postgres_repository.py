from __future__ import annotations

import datetime as dt
import os
from typing import Any

from psycopg import connect
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from iaops.security.crypto import decrypt_text, encrypt_text
from iaops.security.totp import generate_base32_secret, provisioning_uri, verify_totp

from .models import ToolPolicy
from .repository import DEFAULT_LLM_MODELS, MCPRepository


DEFAULT_TOOL_POLICIES = {
    "tenant.list_client": ToolPolicy("tenant.list_client", "viewer", True, 1000, 120, True, None),
    "tenant.get_limits": ToolPolicy("tenant.get_limits", "viewer", True, None, 120, True, None),
    "tenant.create": ToolPolicy("tenant.create", "owner", True, None, 60, True, None),
    "tenant.update_status": ToolPolicy("tenant.update_status", "owner", True, None, 120, True, None),
    "tenant.update_identity": ToolPolicy("tenant.update_identity", "owner", True, None, 120, True, None),
    "inventory.list_tables": ToolPolicy("inventory.list_tables", "viewer", True, 1000, 120, True, None),
    "inventory.list_columns": ToolPolicy("inventory.list_columns", "viewer", True, 1000, 120, True, None),
    "access.list_users": ToolPolicy("access.list_users", "admin", True, 1000, 120, True, None),
    "security.mfa.get_status": ToolPolicy("security.mfa.get_status", "viewer", True, None, 120, True, None),
    "security.mfa.begin_setup": ToolPolicy("security.mfa.begin_setup", "viewer", True, None, 30, True, None),
    "security.mfa.enable": ToolPolicy("security.mfa.enable", "viewer", True, None, 30, True, None),
    "security.mfa.disable_self": ToolPolicy("security.mfa.disable_self", "viewer", True, None, 30, True, None),
    "security.mfa.admin_reset": ToolPolicy("security.mfa.admin_reset", "admin", True, None, 60, True, None),
    "pref.get_user_tenant": ToolPolicy("pref.get_user_tenant", "viewer", True, None, 120, True, None),
    "pref.update_user_tenant": ToolPolicy("pref.update_user_tenant", "viewer", True, None, 60, True, None),
    "tenant_llm.get_config": ToolPolicy("tenant_llm.get_config", "viewer", True, None, 120, True, None),
    "tenant_llm.update_config": ToolPolicy("tenant_llm.update_config", "admin", True, None, 60, True, None),
    "tenant_llm.list_providers": ToolPolicy("tenant_llm.list_providers", "viewer", True, 200, 120, True, None),
    "tenant_llm.list_models": ToolPolicy("tenant_llm.list_models", "viewer", True, 500, 120, True, None),
    "llm_admin.list_providers": ToolPolicy("llm_admin.list_providers", "owner", True, 200, 120, True, None),
    "llm_admin.list_models": ToolPolicy("llm_admin.list_models", "owner", True, 500, 120, True, None),
    "llm_admin.get_app_config": ToolPolicy("llm_admin.get_app_config", "owner", True, None, 120, True, None),
    "llm_admin.update_app_config": ToolPolicy("llm_admin.update_app_config", "owner", True, None, 60, True, None),
    "channel.list_user_tenants": ToolPolicy("channel.list_user_tenants", "viewer", True, 100, 120, True, None),
    "channel.set_active_tenant": ToolPolicy("channel.set_active_tenant", "viewer", True, None, 120, True, None),
    "channel.get_active_tenant": ToolPolicy("channel.get_active_tenant", "viewer", True, None, 120, True, None),
    "channel.binding.list": ToolPolicy("channel.binding.list", "admin", True, 500, 120, True, None),
    "channel.binding.upsert": ToolPolicy("channel.binding.upsert", "admin", True, None, 120, True, None),
    "channel.binding.delete": ToolPolicy("channel.binding.delete", "admin", True, None, 120, True, None),
    "inventory.list_tenant_tables": ToolPolicy("inventory.list_tenant_tables", "viewer", True, 1000, 120, True, None),
    "inventory.register_table": ToolPolicy("inventory.register_table", "admin", True, None, 120, True, None),
    "inventory.delete_table": ToolPolicy("inventory.delete_table", "admin", True, None, 120, True, None),
    "inventory.list_table_columns": ToolPolicy("inventory.list_table_columns", "viewer", True, 1000, 120, True, None),
    "inventory.register_column": ToolPolicy("inventory.register_column", "admin", True, None, 120, True, None),
    "inventory.delete_column": ToolPolicy("inventory.delete_column", "admin", True, None, 120, True, None),
    "source.list_catalog": ToolPolicy("source.list_catalog", "viewer", True, 1000, 120, True, None),
    "source.list_tenant": ToolPolicy("source.list_tenant", "viewer", True, 1000, 120, True, None),
    "source.register": ToolPolicy("source.register", "admin", True, None, 60, True, None),
    "source.update_status": ToolPolicy("source.update_status", "admin", True, None, 120, True, None),
    "source.update": ToolPolicy("source.update", "admin", True, None, 120, True, None),
    "source.delete": ToolPolicy("source.delete", "admin", True, None, 60, True, None),
    "query.execute_safe_sql": ToolPolicy("query.execute_safe_sql", "admin", True, 200, 30, True, ["public", "analytics"]),
    "incident.create": ToolPolicy("incident.create", "admin", True, None, 60, True, None),
    "incident.list": ToolPolicy("incident.list", "viewer", True, 200, 120, True, None),
    "incident.update_status": ToolPolicy("incident.update_status", "admin", True, None, 120, True, None),
    "events.list": ToolPolicy("events.list", "viewer", True, 200, 120, True, None),
    "audit.list_calls": ToolPolicy("audit.list_calls", "admin", True, 200, 120, True, None),
    "security_sql.get_policy": ToolPolicy("security_sql.get_policy", "viewer", True, 200, 120, True, None),
    "security_sql.update_policy": ToolPolicy("security_sql.update_policy", "admin", True, 200, 120, True, None),
    "security_mcp.list_policies": ToolPolicy("security_mcp.list_policies", "viewer", True, 500, 120, True, None),
    "security_mcp.update_policy": ToolPolicy("security_mcp.update_policy", "admin", True, None, 120, True, None),
    "mcp_client.list_connections": ToolPolicy("mcp_client.list_connections", "viewer", True, 200, 120, True, None),
    "mcp_client.upsert_connection": ToolPolicy("mcp_client.upsert_connection", "admin", True, None, 120, True, None),
    "mcp_client.update_status": ToolPolicy("mcp_client.update_status", "admin", True, None, 120, True, None),
    "ops.get_health_summary": ToolPolicy("ops.get_health_summary", "viewer", True, None, 120, True, None),
    "setup.get_progress": ToolPolicy("setup.get_progress", "admin", True, None, 120, True, None),
    "setup.upsert_progress": ToolPolicy("setup.upsert_progress", "admin", True, None, 120, True, None),
}

DEFAULT_SOURCE_CATALOG = [
    {"code": "postgres", "name": "PostgreSQL", "category": "relational", "is_supported": True, "notes": None},
    {"code": "mysql", "name": "MySQL", "category": "relational", "is_supported": True, "notes": None},
    {"code": "sqlserver", "name": "SQL Server", "category": "relational", "is_supported": True, "notes": None},
    {"code": "oracle", "name": "Oracle", "category": "relational", "is_supported": True, "notes": None},
    {"code": "mongodb", "name": "MongoDB", "category": "nosql", "is_supported": True, "notes": None},
    {"code": "cassandra", "name": "Cassandra", "category": "nosql", "is_supported": True, "notes": None},
    {"code": "dynamodb", "name": "DynamoDB", "category": "nosql", "is_supported": True, "notes": None},
    {"code": "snowflake", "name": "Snowflake", "category": "warehouse", "is_supported": True, "notes": None},
    {"code": "bigquery", "name": "BigQuery", "category": "warehouse", "is_supported": True, "notes": None},
    {"code": "redshift", "name": "Redshift", "category": "warehouse", "is_supported": True, "notes": None},
    {"code": "s3", "name": "AWS S3", "category": "lake_storage", "is_supported": True, "notes": None},
    {"code": "azure_blob", "name": "Azure Blob Storage", "category": "lake_storage", "is_supported": True, "notes": None},
    {"code": "gcs", "name": "Google Cloud Storage", "category": "lake_storage", "is_supported": True, "notes": None},
    {"code": "power_bi", "name": "Power BI", "category": "bi_semantic", "is_supported": True, "notes": None},
    {"code": "fabric", "name": "Microsoft Fabric", "category": "lakehouse_semantic", "is_supported": True, "notes": None},
]


class PostgresMCPRepository(MCPRepository):
    """Repositorio MCP persistente em PostgreSQL (schema iaops_gov)."""

    def __init__(self, dsn: str, schema: str = "iaops_gov") -> None:
        self.dsn = dsn
        self.schema = schema
        self._monitored_column_meta_ready = False
        self._data_source_rag_meta_ready = False
        self._plan_source_limits_ready = False
        self._channel_tables_ready = False

    def _ensure_monitored_column_meta(self) -> None:
        if self._monitored_column_meta_ready:
            return
        ddl_statements = [
            f"ALTER TABLE {self.schema}.monitored_column ADD COLUMN IF NOT EXISTS source_description_text TEXT",
            f"ALTER TABLE {self.schema}.monitored_column ADD COLUMN IF NOT EXISTS llm_description_suggested TEXT",
            f"ALTER TABLE {self.schema}.monitored_column ADD COLUMN IF NOT EXISTS llm_classification_suggested TEXT",
            f"ALTER TABLE {self.schema}.monitored_column ADD COLUMN IF NOT EXISTS llm_confidence_score NUMERIC(5,2)",
            f"ALTER TABLE {self.schema}.monitored_column ADD COLUMN IF NOT EXISTS llm_description_confirmed BOOLEAN NOT NULL DEFAULT FALSE",
            f"ALTER TABLE {self.schema}.monitored_column ADD COLUMN IF NOT EXISTS llm_confirmed_at TIMESTAMPTZ",
            f"ALTER TABLE {self.schema}.monitored_column ADD COLUMN IF NOT EXISTS llm_confirmed_by_user_id BIGINT",
        ]
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            for ddl in ddl_statements:
                cur.execute(ddl)
            conn.commit()
        self._monitored_column_meta_ready = True

    def _ensure_data_source_rag_meta(self) -> None:
        if self._data_source_rag_meta_ready:
            return
        ddl_statements = [
            f"ALTER TABLE {self.schema}.data_source ADD COLUMN IF NOT EXISTS rag_enabled BOOLEAN NOT NULL DEFAULT FALSE",
            f"ALTER TABLE {self.schema}.data_source ADD COLUMN IF NOT EXISTS rag_context_text TEXT",
        ]
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            for ddl in ddl_statements:
                cur.execute(ddl)
            conn.commit()
        self._data_source_rag_meta_ready = True

    def _ensure_plan_source_limits(self) -> None:
        if self._plan_source_limits_ready:
            return
        ddl_statements = [
            f"ALTER TABLE {self.schema}.plan ADD COLUMN IF NOT EXISTS max_data_sources_per_client INTEGER NOT NULL DEFAULT 10",
            f"ALTER TABLE {self.schema}.plan ADD COLUMN IF NOT EXISTS max_data_sources_per_tenant INTEGER NOT NULL DEFAULT 5",
        ]
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            for ddl in ddl_statements:
                cur.execute(ddl)
            conn.commit()
        self._plan_source_limits_ready = True

    def _ensure_channel_tables(self) -> None:
        if self._channel_tables_ready:
            return
        ddl_statements = [
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.channel_user_binding (
                id BIGSERIAL PRIMARY KEY,
                client_id BIGINT NOT NULL REFERENCES {self.schema}.client(id),
                tenant_id BIGINT REFERENCES {self.schema}.tenant(id),
                user_id BIGINT REFERENCES {self.schema}.app_user(id),
                channel_type TEXT NOT NULL CHECK (channel_type IN ('telegram', 'whatsapp')),
                external_user_key TEXT NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (channel_type, external_user_key)
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.channel_tenant_context (
                id BIGSERIAL PRIMARY KEY,
                client_id BIGINT NOT NULL REFERENCES {self.schema}.client(id),
                user_id BIGINT NOT NULL REFERENCES {self.schema}.app_user(id),
                channel_type TEXT NOT NULL CHECK (channel_type IN ('telegram', 'whatsapp')),
                conversation_key TEXT NOT NULL,
                active_tenant_id BIGINT REFERENCES {self.schema}.tenant(id),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (channel_type, conversation_key)
            )
            """,
            f"ALTER TABLE {self.schema}.channel_user_binding ADD COLUMN IF NOT EXISTS tenant_id BIGINT REFERENCES {self.schema}.tenant(id)",
            f"ALTER TABLE {self.schema}.channel_user_binding ALTER COLUMN user_id DROP NOT NULL",
            f"CREATE INDEX IF NOT EXISTS idx_channel_binding_client_user ON {self.schema}.channel_user_binding(client_id, user_id)",
            f"CREATE INDEX IF NOT EXISTS idx_channel_binding_client_tenant ON {self.schema}.channel_user_binding(client_id, tenant_id)",
            f"CREATE INDEX IF NOT EXISTS idx_channel_context_client_user ON {self.schema}.channel_tenant_context(client_id, user_id)",
        ]
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            for ddl in ddl_statements:
                cur.execute(ddl)
            conn.commit()
        self._channel_tables_ready = True

    def _ensure_channel_binding_tenant_column(self) -> None:
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(
                f"ALTER TABLE {self.schema}.channel_user_binding ADD COLUMN IF NOT EXISTS tenant_id BIGINT REFERENCES {self.schema}.tenant(id)"
            )
            cur.execute(f"ALTER TABLE {self.schema}.channel_user_binding ALTER COLUMN user_id DROP NOT NULL")
            conn.commit()

    def is_tenant_operational(self, client_id: int, tenant_id: int) -> bool:
        if int(tenant_id) <= 0:
            return True
        sql = f"""
            SELECT EXISTS (
                SELECT 1
                FROM {self.schema}.tenant t
                JOIN {self.schema}.client c ON c.id = t.client_id
                LEFT JOIN {self.schema}.v_client_billing_delinquency d ON d.client_id = t.client_id
                WHERE t.id = %(tenant_id)s
                  AND t.client_id = %(client_id)s
                  AND t.status = 'active'
                  AND c.status = 'active'
                  AND d.client_id IS NULL
            ) AS ok
        """
        fallback_sql = f"""
            SELECT EXISTS (
                SELECT 1
                FROM {self.schema}.tenant t
                JOIN {self.schema}.client c ON c.id = t.client_id
                WHERE t.id = %(tenant_id)s
                  AND t.client_id = %(client_id)s
                  AND t.status = 'active'
                  AND c.status = 'active'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {self.schema}.installment inst
                      JOIN {self.schema}.invoice inv ON inv.id = inst.invoice_id
                      LEFT JOIN {self.schema}.subscription sub
                        ON sub.client_id = t.client_id
                       AND sub.status = 'active'
                       AND (sub.ends_at IS NULL OR sub.ends_at >= NOW())
                      LEFT JOIN {self.schema}.plan p ON p.id = sub.plan_id
                      WHERE inv.client_id = t.client_id
                        AND inst.status IN ('open', 'overdue')
                        AND inst.due_date < CURRENT_DATE - COALESCE(p.late_tolerance_days, 0)
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {self.schema}.billing_installment bi
                      JOIN {self.schema}.billing_subscription bs ON bs.id = bi.subscription_id
                      WHERE bs.client_id = t.client_id
                        AND bs.status = 'active'
                        AND bi.status IN ('open', 'overdue')
                        AND bi.due_date < CURRENT_DATE - COALESCE(bs.tolerance_days, 5)
                  )
            ) AS ok
        """
        basic_sql = f"""
            SELECT EXISTS (
                SELECT 1
                FROM {self.schema}.tenant t
                JOIN {self.schema}.client c ON c.id = t.client_id
                WHERE t.id = %(tenant_id)s
                  AND t.client_id = %(client_id)s
                  AND t.status = 'active'
                  AND c.status = 'active'
            ) AS ok
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            try:
                cur.execute(sql, {"client_id": client_id, "tenant_id": tenant_id})
            except Exception:
                conn.rollback()
                try:
                    cur.execute(fallback_sql, {"client_id": client_id, "tenant_id": tenant_id})
                except Exception:
                    conn.rollback()
                    cur.execute(basic_sql, {"client_id": client_id, "tenant_id": tenant_id})
            row = cur.fetchone()
        return bool(row and row["ok"])

    def get_user_role(self, tenant_id: int, user_id: int) -> str | None:
        sql = f"""
            SELECT role
            FROM {self.schema}.tenant_user_role
            WHERE tenant_id = %(tenant_id)s
              AND user_id = %(user_id)s
            LIMIT 1
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(sql, {"tenant_id": tenant_id, "user_id": user_id})
            row = cur.fetchone()
        return row["role"] if row else None

    def is_superadmin(self, user_id: int) -> bool:
        sql = f"""
            SELECT COALESCE(is_superadmin, FALSE) AS is_superadmin
            FROM {self.schema}.app_user
            WHERE id = %(user_id)s
            LIMIT 1
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(sql, {"user_id": user_id})
            row = cur.fetchone()
        return bool(row and row["is_superadmin"])

    def list_client_tenants(self, client_id: int) -> list[dict[str, Any]]:
        sql = f"""
            SELECT id, client_id, name, slug, status, created_at
            FROM {self.schema}.tenant
            WHERE client_id = %(client_id)s
            ORDER BY id
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(sql, {"client_id": client_id})
            rows = cur.fetchall()
        return rows

    def get_client_tenant_limits(self, client_id: int, tenant_id: int | None = None) -> dict[str, Any]:
        billing_limits_sql = f"""
            SELECT
                COALESCE(bp.max_tenants, 1) AS max_tenants,
                COALESCE(bp.max_data_sources_per_client, 10) AS max_data_sources_per_client,
                COALESCE(bp.max_data_sources_per_tenant, 5) AS max_data_sources_per_tenant
            FROM {self.schema}.billing_subscription bs
            JOIN {self.schema}.billing_plan bp ON bp.id = bs.plan_id
            WHERE bs.client_id = %(client_id)s
              AND bs.status = 'active'
              AND (bs.ends_at IS NULL OR bs.ends_at >= NOW())
            ORDER BY bs.id DESC
            LIMIT 1
        """
        legacy_limits_sql = f"""
            SELECT
                COALESCE(p.max_tenants, 1) AS max_tenants,
                COALESCE(p.max_data_sources_per_client, 10) AS max_data_sources_per_client,
                COALESCE(p.max_data_sources_per_tenant, 5) AS max_data_sources_per_tenant
            FROM {self.schema}.subscription s
            JOIN {self.schema}.plan p ON p.id = s.plan_id
            WHERE s.client_id = %(client_id)s
              AND s.status = 'active'
              AND (s.ends_at IS NULL OR s.ends_at >= NOW())
            ORDER BY s.id DESC
            LIMIT 1
        """
        counters_sql = f"""
            SELECT
                (
                    SELECT COUNT(*)
                    FROM {self.schema}.tenant t
                    WHERE t.client_id = %(client_id)s
                      AND t.status = 'active'
                ) AS active_tenants,
                (
                    SELECT COUNT(*)
                    FROM {self.schema}.data_source ds
                    JOIN {self.schema}.tenant t ON t.id = ds.tenant_id
                    WHERE t.client_id = %(client_id)s
                ) AS total_data_sources,
                (
                    SELECT COUNT(*)
                    FROM {self.schema}.data_source ds
                    JOIN {self.schema}.tenant t ON t.id = ds.tenant_id
                    WHERE t.client_id = %(client_id)s
                      AND t.status = 'active'
                      AND ds.is_active = TRUE
                ) AS active_data_sources,
                (
                    SELECT COUNT(*)
                    FROM {self.schema}.data_source ds
                    JOIN {self.schema}.tenant t ON t.id = ds.tenant_id
                    WHERE t.client_id = %(client_id)s
                      AND ds.tenant_id = %(tenant_id)s
                ) AS total_data_sources_tenant,
                (
                    SELECT COUNT(*)
                    FROM {self.schema}.data_source ds
                    JOIN {self.schema}.tenant t ON t.id = ds.tenant_id
                    WHERE t.client_id = %(client_id)s
                      AND ds.tenant_id = %(tenant_id)s
                      AND ds.is_active = TRUE
                ) AS active_data_sources_tenant
        """
        limit_row: dict[str, Any] | None = None
        tenant_scope_id = int(tenant_id) if tenant_id is not None else -1
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            try:
                cur.execute(billing_limits_sql, {"client_id": client_id})
                limit_row = cur.fetchone()
            except Exception:
                conn.rollback()
                limit_row = None
            if not limit_row:
                try:
                    self._ensure_plan_source_limits()
                    cur.execute(legacy_limits_sql, {"client_id": client_id})
                    limit_row = cur.fetchone()
                except Exception:
                    conn.rollback()
                    limit_row = None
            cur.execute(counters_sql, {"client_id": client_id, "tenant_id": tenant_scope_id})
            counters = cur.fetchone() or {}
        max_tenants = int(limit_row["max_tenants"]) if limit_row else 1
        active_tenants = int(counters.get("active_tenants", 0) or 0)
        max_data_sources_per_client = int(limit_row["max_data_sources_per_client"]) if limit_row else 10
        max_data_sources_per_tenant = int(limit_row["max_data_sources_per_tenant"]) if limit_row else 5
        total_data_sources = int(counters.get("total_data_sources", 0) or 0)
        total_data_sources_tenant = int(counters.get("total_data_sources_tenant", 0) or 0)
        active_data_sources = int(counters.get("active_data_sources", 0) or 0)
        active_data_sources_tenant = int(counters.get("active_data_sources_tenant", 0) or 0)
        return {
            "active_tenants": active_tenants,
            "max_tenants": max_tenants,
            "can_create": active_tenants < max_tenants,
            "total_data_sources": total_data_sources,
            "active_data_sources": active_data_sources,
            "max_data_sources_per_client": max_data_sources_per_client,
            "can_create_data_source_client": total_data_sources < max_data_sources_per_client,
            "total_data_sources_tenant": total_data_sources_tenant,
            "active_data_sources_tenant": active_data_sources_tenant,
            "max_data_sources_per_tenant": max_data_sources_per_tenant,
            "can_create_data_source_tenant": total_data_sources_tenant < max_data_sources_per_tenant,
        }

    def create_tenant(self, client_id: int, *, name: str, slug: str) -> dict[str, Any]:
        limits = self.get_client_tenant_limits(client_id)
        if not limits["can_create"]:
            raise ValueError("limite de tenants ativos do plano foi atingido")
        check_slug_sql = f"""
            SELECT id
            FROM {self.schema}.tenant
            WHERE client_id = %(client_id)s
              AND slug = %(slug)s
            LIMIT 1
        """
        insert_sql = f"""
            INSERT INTO {self.schema}.tenant (
                client_id,
                name,
                slug,
                status
            )
            VALUES (
                %(client_id)s,
                %(name)s,
                %(slug)s,
                'active'
            )
            RETURNING id, client_id, name, slug, status, created_at
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(check_slug_sql, {"client_id": client_id, "slug": slug})
            if cur.fetchone():
                raise ValueError("slug ja utilizado para este cliente")
            cur.execute(insert_sql, {"client_id": client_id, "name": name, "slug": slug})
            row = cur.fetchone()
            conn.commit()
        return row

    def update_tenant_status(self, client_id: int, tenant_id: int, status: str) -> dict[str, Any]:
        if status not in {"active", "disabled"}:
            raise ValueError("status invalido")
        select_sql = f"""
            SELECT id, client_id, name, slug, status, created_at
            FROM {self.schema}.tenant
            WHERE id = %(tenant_id)s
              AND client_id = %(client_id)s
            LIMIT 1
        """
        update_sql = f"""
            UPDATE {self.schema}.tenant
               SET status = %(status)s
             WHERE id = %(tenant_id)s
               AND client_id = %(client_id)s
         RETURNING id, client_id, name, slug, status, created_at
        """
        disable_sources_sql = f"""
            UPDATE {self.schema}.data_source
               SET is_active = FALSE
             WHERE tenant_id = %(tenant_id)s
               AND is_active = TRUE
        """
        disable_tables_sql = f"""
            UPDATE {self.schema}.monitored_table
               SET is_active = FALSE
             WHERE tenant_id = %(tenant_id)s
               AND is_active = TRUE
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(select_sql, {"tenant_id": tenant_id, "client_id": client_id})
            current = cur.fetchone()
            if not current:
                raise ValueError("tenant nao encontrado")
            if status == "active" and current["status"] != "active":
                limits = self.get_client_tenant_limits(client_id)
                if not limits["can_create"]:
                    raise ValueError("limite de tenants ativos do plano foi atingido")
            cur.execute(
                update_sql,
                {
                    "tenant_id": tenant_id,
                    "client_id": client_id,
                    "status": status,
                },
            )
            row = cur.fetchone()
            if status == "disabled":
                cur.execute(disable_sources_sql, {"tenant_id": tenant_id})
                cur.execute(disable_tables_sql, {"tenant_id": tenant_id})
            conn.commit()
        return row

    def list_tenant_users(self, tenant_id: int) -> list[dict[str, Any]]:
        sql = f"""
            SELECT
                u.id AS user_id,
                u.email,
                u.full_name,
                tur.role,
                u.is_active,
                COALESCE(umc.is_enabled, FALSE) AS mfa_enabled
            FROM {self.schema}.tenant_user_role tur
            JOIN {self.schema}.app_user u ON u.id = tur.user_id
            LEFT JOIN {self.schema}.user_mfa_config umc ON umc.user_id = u.id
            WHERE tur.tenant_id = %(tenant_id)s
            ORDER BY
                CASE tur.role
                    WHEN 'owner' THEN 3
                    WHEN 'admin' THEN 2
                    ELSE 1
                END DESC,
                u.email
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(sql, {"tenant_id": tenant_id})
            rows = cur.fetchall()
        return rows

    def get_user_mfa_status(self, tenant_id: int, user_id: int) -> dict[str, Any]:
        role = self.get_user_role(tenant_id, user_id)
        if role is None:
            raise ValueError("usuario fora do escopo do tenant")
        sql = f"""
            SELECT
                COALESCE(cfg.is_enabled, FALSE) AS enabled,
                cfg.enabled_at,
                pnd.expires_at AS pending_expires_at
            FROM {self.schema}.app_user u
            LEFT JOIN {self.schema}.user_mfa_config cfg ON cfg.user_id = u.id
            LEFT JOIN {self.schema}.user_mfa_pending_setup pnd ON pnd.user_id = u.id
            WHERE u.id = %(user_id)s
            LIMIT 1
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(sql, {"user_id": user_id})
            row = cur.fetchone()
        if not row:
            raise ValueError("usuario nao encontrado")
        return {
            "enabled": bool(row["enabled"]),
            "enabled_at": row["enabled_at"].isoformat() if row["enabled_at"] else None,
            "has_pending_setup": bool(row["pending_expires_at"]),
            "pending_expires_at": row["pending_expires_at"].isoformat() if row["pending_expires_at"] else None,
        }

    def begin_user_mfa_setup(self, tenant_id: int, user_id: int, issuer: str) -> dict[str, Any]:
        role = self.get_user_role(tenant_id, user_id)
        if role is None:
            raise ValueError("usuario fora do escopo do tenant")
        user_sql = f"""
            SELECT email
            FROM {self.schema}.app_user
            WHERE id = %(user_id)s
            LIMIT 1
        """
        upsert_sql = f"""
            INSERT INTO {self.schema}.user_mfa_pending_setup (
                user_id,
                pending_secret_ciphertext,
                expires_at,
                created_at,
                updated_at
            )
            VALUES (
                %(user_id)s,
                %(pending_secret_ciphertext)s,
                NOW() + INTERVAL '10 minutes',
                NOW(),
                NOW()
            )
            ON CONFLICT (user_id)
            DO UPDATE SET
                pending_secret_ciphertext = EXCLUDED.pending_secret_ciphertext,
                expires_at = NOW() + INTERVAL '10 minutes',
                updated_at = NOW()
            RETURNING expires_at
        """
        secret = generate_base32_secret()
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(user_sql, {"user_id": user_id})
            user_row = cur.fetchone()
            if not user_row:
                raise ValueError("usuario nao encontrado")
            cur.execute(
                upsert_sql,
                {
                    "user_id": user_id,
                    "pending_secret_ciphertext": encrypt_text(secret),
                },
            )
            pending_row = cur.fetchone()
            conn.commit()
        return {
            "secret": secret,
            "provisioning_uri": provisioning_uri(issuer=issuer, account_name=user_row["email"], secret=secret),
            "expires_at": pending_row["expires_at"].isoformat(),
        }

    def enable_user_mfa(self, tenant_id: int, user_id: int, otp_code: str) -> dict[str, Any]:
        role = self.get_user_role(tenant_id, user_id)
        if role is None:
            raise ValueError("usuario fora do escopo do tenant")
        pending_sql = f"""
            SELECT pending_secret_ciphertext, expires_at
            FROM {self.schema}.user_mfa_pending_setup
            WHERE user_id = %(user_id)s
            LIMIT 1
        """
        upsert_cfg_sql = f"""
            INSERT INTO {self.schema}.user_mfa_config (
                user_id,
                method,
                totp_secret_ciphertext,
                is_enabled,
                enabled_at,
                disabled_at,
                updated_at
            )
            VALUES (
                %(user_id)s,
                'totp',
                %(totp_secret_ciphertext)s,
                TRUE,
                NOW(),
                NULL,
                NOW()
            )
            ON CONFLICT (user_id)
            DO UPDATE SET
                method = 'totp',
                totp_secret_ciphertext = EXCLUDED.totp_secret_ciphertext,
                is_enabled = TRUE,
                enabled_at = NOW(),
                disabled_at = NULL,
                updated_at = NOW()
            RETURNING enabled_at
        """
        delete_pending_sql = f"DELETE FROM {self.schema}.user_mfa_pending_setup WHERE user_id = %(user_id)s"
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(pending_sql, {"user_id": user_id})
            pending_row = cur.fetchone()
            if not pending_row:
                raise ValueError("setup MFA nao iniciado")
            if pending_row["expires_at"] < dt.datetime.now(dt.timezone.utc):
                cur.execute(delete_pending_sql, {"user_id": user_id})
                conn.commit()
                raise ValueError("setup MFA expirado")
            secret = decrypt_text(pending_row["pending_secret_ciphertext"])
            if not verify_totp(secret, otp_code):
                raise ValueError("codigo TOTP invalido")
            cur.execute(
                upsert_cfg_sql,
                {
                    "user_id": user_id,
                    "totp_secret_ciphertext": encrypt_text(secret),
                },
            )
            cfg_row = cur.fetchone()
            cur.execute(delete_pending_sql, {"user_id": user_id})
            conn.commit()
        return {"enabled": True, "enabled_at": cfg_row["enabled_at"].isoformat()}

    def disable_user_mfa(self, tenant_id: int, user_id: int, otp_code: str) -> dict[str, Any]:
        role = self.get_user_role(tenant_id, user_id)
        if role is None:
            raise ValueError("usuario fora do escopo do tenant")
        select_sql = f"""
            SELECT totp_secret_ciphertext, is_enabled
            FROM {self.schema}.user_mfa_config
            WHERE user_id = %(user_id)s
            LIMIT 1
        """
        disable_sql = f"""
            UPDATE {self.schema}.user_mfa_config
               SET is_enabled = FALSE,
                   totp_secret_ciphertext = NULL,
                   disabled_at = NOW(),
                   updated_at = NOW()
             WHERE user_id = %(user_id)s
            RETURNING disabled_at
        """
        delete_pending_sql = f"DELETE FROM {self.schema}.user_mfa_pending_setup WHERE user_id = %(user_id)s"
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(select_sql, {"user_id": user_id})
            cfg = cur.fetchone()
            if not cfg or not bool(cfg["is_enabled"]):
                raise ValueError("MFA nao esta habilitado")
            secret_cipher = cfg["totp_secret_ciphertext"]
            if not secret_cipher:
                raise ValueError("segredo MFA indisponivel")
            secret = decrypt_text(secret_cipher)
            if not verify_totp(secret, otp_code):
                raise ValueError("codigo TOTP invalido")
            cur.execute(disable_sql, {"user_id": user_id})
            row = cur.fetchone()
            cur.execute(delete_pending_sql, {"user_id": user_id})
            conn.commit()
        return {"enabled": False, "disabled_at": row["disabled_at"].isoformat()}

    def admin_reset_user_mfa(self, tenant_id: int, target_user_id: int, reset_by_user_id: int) -> dict[str, Any]:
        if self.get_user_role(tenant_id, target_user_id) is None:
            raise ValueError("usuario alvo fora do escopo do tenant")
        if self.get_user_role(tenant_id, reset_by_user_id) is None:
            raise ValueError("usuario executor fora do escopo do tenant")
        upsert_sql = f"""
            INSERT INTO {self.schema}.user_mfa_config (
                user_id,
                method,
                totp_secret_ciphertext,
                is_enabled,
                enabled_at,
                disabled_at,
                last_reset_by_user_id,
                last_reset_at,
                updated_at
            )
            VALUES (
                %(target_user_id)s,
                'totp',
                NULL,
                FALSE,
                NULL,
                NOW(),
                %(reset_by_user_id)s,
                NOW(),
                NOW()
            )
            ON CONFLICT (user_id)
            DO UPDATE SET
                totp_secret_ciphertext = NULL,
                is_enabled = FALSE,
                enabled_at = NULL,
                disabled_at = NOW(),
                last_reset_by_user_id = EXCLUDED.last_reset_by_user_id,
                last_reset_at = NOW(),
                updated_at = NOW()
            RETURNING last_reset_at
        """
        delete_pending_sql = f"DELETE FROM {self.schema}.user_mfa_pending_setup WHERE user_id = %(target_user_id)s"
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(
                upsert_sql,
                {
                    "target_user_id": target_user_id,
                    "reset_by_user_id": reset_by_user_id,
                },
            )
            row = cur.fetchone()
            cur.execute(delete_pending_sql, {"target_user_id": target_user_id})
            conn.commit()
        return {
            "target_user_id": target_user_id,
            "enabled": False,
            "reset_at": row["last_reset_at"].isoformat(),
        }

    def get_user_tenant_preference(self, tenant_id: int, user_id: int) -> dict[str, Any]:
        if self.get_user_role(tenant_id, user_id) is None:
            raise ValueError("usuario fora do escopo do tenant")
        sql = f"""
            SELECT language_code, theme_code, chat_response_mode
            FROM {self.schema}.user_tenant_preference
            WHERE tenant_id = %(tenant_id)s
              AND user_id = %(user_id)s
            LIMIT 1
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(sql, {"tenant_id": tenant_id, "user_id": user_id})
            row = cur.fetchone()
        return {
            "tenant_id": tenant_id,
            "user_id": user_id,
            "language_code": row["language_code"] if row else "pt-BR",
            "theme_code": row["theme_code"] if row else "light",
            "chat_response_mode": row["chat_response_mode"] if row else "executive",
        }

    def upsert_user_tenant_preference(
        self,
        tenant_id: int,
        user_id: int,
        *,
        language_code: str | None = None,
        theme_code: str | None = None,
        chat_response_mode: str | None = None,
    ) -> dict[str, Any]:
        if self.get_user_role(tenant_id, user_id) is None:
            raise ValueError("usuario fora do escopo do tenant")
        current = self.get_user_tenant_preference(tenant_id, user_id)
        mode = str(chat_response_mode or current["chat_response_mode"]).strip().lower()
        if mode not in {"executive", "detailed"}:
            raise ValueError("chat_response_mode invalido")
        upsert_sql = f"""
            INSERT INTO {self.schema}.user_tenant_preference (
                tenant_id,
                user_id,
                language_code,
                theme_code,
                chat_response_mode,
                created_at
            )
            VALUES (
                %(tenant_id)s,
                %(user_id)s,
                %(language_code)s,
                %(theme_code)s,
                %(chat_response_mode)s,
                NOW()
            )
            ON CONFLICT (tenant_id, user_id)
            DO UPDATE SET
                language_code = EXCLUDED.language_code,
                theme_code = EXCLUDED.theme_code,
                chat_response_mode = EXCLUDED.chat_response_mode
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(
                upsert_sql,
                {
                    "tenant_id": tenant_id,
                    "user_id": user_id,
                    "language_code": str(language_code or current["language_code"]).strip() or "pt-BR",
                    "theme_code": str(theme_code or current["theme_code"]).strip() or "light",
                    "chat_response_mode": mode,
                },
            )
            conn.commit()
        return self.get_user_tenant_preference(tenant_id, user_id)

    def list_supported_llm_providers(self) -> list[dict[str, Any]]:
        return [
            {"code": "openai", "name": "OpenAI"},
            {"code": "azure_openai", "name": "Azure OpenAI"},
            {"code": "anthropic", "name": "Anthropic"},
            {"code": "google_gemini", "name": "Google Gemini"},
            {"code": "mistral", "name": "Mistral"},
            {"code": "groq", "name": "Groq"},
            {"code": "ollama", "name": "Ollama (Local)"},
        ]

    def list_supported_llm_models(self, provider_name: str) -> list[dict[str, Any]]:
        key = str(provider_name or "").strip().lower()
        rows = DEFAULT_LLM_MODELS.get(key, [])
        return [dict(item) for item in rows]

    def get_app_default_llm_config(self) -> dict[str, Any] | None:
        sql = f"""
            SELECT
                id,
                name AS provider_name,
                model_code,
                endpoint_url,
                secret_ref,
                is_global_default,
                created_at
            FROM {self.schema}.llm_provider
            WHERE client_id IS NULL
              AND is_global_default = TRUE
            ORDER BY id DESC
            LIMIT 1
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(sql, {})
            row = cur.fetchone()
        return row if row else None

    def upsert_app_default_llm_config(
        self,
        *,
        provider_name: str,
        model_code: str,
        endpoint_url: str | None,
        secret_ref: str | None,
    ) -> dict[str, Any]:
        disable_old_sql = f"""
            UPDATE {self.schema}.llm_provider
               SET is_global_default = FALSE
             WHERE client_id IS NULL
               AND is_global_default = TRUE
        """
        insert_sql = f"""
            INSERT INTO {self.schema}.llm_provider (
                client_id,
                name,
                model_code,
                endpoint_url,
                secret_ref,
                is_global_default
            )
            VALUES (
                NULL,
                %(provider_name)s,
                %(model_code)s,
                %(endpoint_url)s,
                %(secret_ref)s,
                TRUE
            )
            RETURNING
                id,
                name AS provider_name,
                model_code,
                endpoint_url,
                secret_ref,
                is_global_default,
                created_at
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(disable_old_sql, {})
            cur.execute(
                insert_sql,
                {
                    "provider_name": provider_name,
                    "model_code": model_code,
                    "endpoint_url": endpoint_url,
                    "secret_ref": secret_ref,
                },
            )
            row = cur.fetchone()
            conn.commit()
        return row

    def update_tenant_identity(
        self,
        client_id: int,
        tenant_id: int,
        *,
        name: str,
        slug: str | None = None,
    ) -> dict[str, Any]:
        normalized_name = str(name or "").strip()
        normalized_slug = str(slug or "").strip().lower() or None
        if not normalized_name:
            raise ValueError("name obrigatorio")
        check_sql = f"""
            SELECT 1
            FROM {self.schema}.tenant
            WHERE client_id = %(client_id)s
              AND slug = %(slug)s
              AND id <> %(tenant_id)s
            LIMIT 1
        """
        update_sql = f"""
            UPDATE {self.schema}.tenant
            SET
                name = %(name)s,
                slug = COALESCE(%(slug)s, slug)
            WHERE id = %(tenant_id)s
              AND client_id = %(client_id)s
            RETURNING id, client_id, name, slug, status, created_at
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            if normalized_slug:
                cur.execute(check_sql, {"client_id": client_id, "slug": normalized_slug, "tenant_id": tenant_id})
                if cur.fetchone():
                    raise ValueError("slug ja utilizado para este cliente")
            cur.execute(
                update_sql,
                {
                    "tenant_id": tenant_id,
                    "client_id": client_id,
                    "name": normalized_name,
                    "slug": normalized_slug,
                },
            )
            row = cur.fetchone()
            if not row:
                raise ValueError("tenant nao encontrado")
            conn.commit()
        return row

    def get_tenant_llm_config(self, client_id: int, tenant_id: int) -> dict[str, Any]:
        cfg_sql = f"""
            SELECT
                tlc.billing_mode,
                lp.name AS provider_name,
                lp.model_code,
                lp.endpoint_url,
                lp.secret_ref
            FROM {self.schema}.tenant_llm_config tlc
            JOIN {self.schema}.llm_provider lp ON lp.id = tlc.llm_provider_id
            JOIN {self.schema}.tenant t ON t.id = tlc.tenant_id
            WHERE tlc.tenant_id = %(tenant_id)s
              AND t.client_id = %(client_id)s
            LIMIT 1
        """
        app_sql = f"""
            SELECT
                name AS provider_name,
                model_code,
                endpoint_url,
                secret_ref
            FROM {self.schema}.llm_provider
            WHERE client_id IS NULL
              AND is_global_default = TRUE
            ORDER BY id DESC
            LIMIT 1
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(cfg_sql, {"tenant_id": tenant_id, "client_id": client_id})
            row = cur.fetchone()
            cur.execute(app_sql, {})
            app_row = cur.fetchone()
        if row:
            use_app = row["billing_mode"] == "app_default_token"
            return {
                "use_app_default_llm": use_app,
                "billing_mode": row["billing_mode"],
                "provider_name": row["provider_name"],
                "model_code": row["model_code"],
                "endpoint_url": row["endpoint_url"],
                "secret_ref": row["secret_ref"],
                "app_default_available": app_row is not None,
            }
        return {
            "use_app_default_llm": False,
            "billing_mode": None,
            "provider_name": None,
            "model_code": None,
            "endpoint_url": None,
            "secret_ref": None,
            "app_default_available": app_row is not None,
        }

    def update_tenant_llm_config(
        self,
        client_id: int,
        tenant_id: int,
        *,
        use_app_default_llm: bool,
        provider_name: str | None,
        model_code: str | None,
        endpoint_url: str | None,
        secret_ref: str | None,
    ) -> dict[str, Any]:
        app_provider_sql = f"""
            SELECT id
            FROM {self.schema}.llm_provider
            WHERE client_id IS NULL
              AND is_global_default = TRUE
            ORDER BY id DESC
            LIMIT 1
        """
        tenant_provider_insert_sql = f"""
            INSERT INTO {self.schema}.llm_provider (
                client_id,
                name,
                model_code,
                endpoint_url,
                secret_ref,
                is_global_default
            )
            VALUES (
                %(client_id)s,
                %(provider_name)s,
                %(model_code)s,
                %(endpoint_url)s,
                %(secret_ref)s,
                FALSE
            )
            RETURNING id
        """
        upsert_cfg_sql = f"""
            INSERT INTO {self.schema}.tenant_llm_config (
                tenant_id,
                llm_provider_id,
                billing_mode,
                created_at
            )
            VALUES (
                %(tenant_id)s,
                %(llm_provider_id)s,
                %(billing_mode)s,
                NOW()
            )
            ON CONFLICT (tenant_id)
            DO UPDATE SET
                llm_provider_id = EXCLUDED.llm_provider_id,
                billing_mode = EXCLUDED.billing_mode
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            if use_app_default_llm:
                cur.execute(app_provider_sql, {})
                app_row = cur.fetchone()
                if not app_row:
                    raise ValueError("LLM padrao do app nao configurada pelo superadmin")
                llm_provider_id = int(app_row["id"])
                billing_mode = "app_default_token"
            else:
                if not provider_name or not model_code:
                    raise ValueError("provider_name e model_code sao obrigatorios")
                cur.execute(
                    tenant_provider_insert_sql,
                    {
                        "client_id": client_id,
                        "provider_name": provider_name,
                        "model_code": model_code,
                        "endpoint_url": endpoint_url,
                        "secret_ref": secret_ref,
                    },
                )
                inserted = cur.fetchone()
                llm_provider_id = int(inserted["id"])
                billing_mode = "tenant_provider"
            cur.execute(
                upsert_cfg_sql,
                {
                    "tenant_id": tenant_id,
                    "llm_provider_id": llm_provider_id,
                    "billing_mode": billing_mode,
                },
            )
            conn.commit()
        return self.get_tenant_llm_config(client_id, tenant_id)

    def resolve_channel_user_tenants(
        self,
        client_id: int,
        *,
        channel_type: str,
        external_user_key: str,
    ) -> dict[str, Any]:
        self._ensure_channel_tables()
        self._ensure_channel_binding_tenant_column()
        binding_sql = f"""
            SELECT
                b.id,
                b.tenant_id,
                b.user_id,
                u.email,
                u.full_name
            FROM {self.schema}.channel_user_binding b
            LEFT JOIN {self.schema}.app_user u ON u.id = b.user_id
            WHERE b.client_id = %(client_id)s
              AND b.channel_type = %(channel_type)s
              AND b.external_user_key = %(external_user_key)s
              AND b.is_active = TRUE
            LIMIT 1
        """
        tenants_sql = f"""
            SELECT
                t.id AS tenant_id,
                t.name,
                t.slug,
                t.status,
                tur.role
            FROM {self.schema}.tenant_user_role tur
            JOIN {self.schema}.tenant t ON t.id = tur.tenant_id
            WHERE tur.user_id = %(user_id)s
              AND t.client_id = %(client_id)s
            ORDER BY t.id
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(
                binding_sql,
                {
                    "client_id": client_id,
                    "channel_type": channel_type,
                    "external_user_key": external_user_key,
                },
            )
            binding_row = cur.fetchone()
            if not binding_row:
                raise ValueError("identidade do canal nao vinculada a tenant")
            tenants: list[dict[str, Any]] = []
            tenant_id = int(binding_row["tenant_id"]) if binding_row.get("tenant_id") is not None else None
            if tenant_id is not None and tenant_id > 0:
                cur.execute(
                    f"""
                    SELECT id AS tenant_id, name, slug, status
                    FROM {self.schema}.tenant
                    WHERE id = %(tenant_id)s
                      AND client_id = %(client_id)s
                    LIMIT 1
                    """,
                    {"tenant_id": tenant_id, "client_id": client_id},
                )
                tenant_row = cur.fetchone()
                if not tenant_row:
                    raise ValueError("tenant vinculado ao canal nao encontrado")
                role = "viewer"
                if binding_row.get("user_id") is not None:
                    cur.execute(
                        f"""
                        SELECT role
                        FROM {self.schema}.tenant_user_role
                        WHERE tenant_id = %(tenant_id)s
                          AND user_id = %(user_id)s
                        LIMIT 1
                        """,
                        {"tenant_id": tenant_id, "user_id": int(binding_row["user_id"])},
                    )
                    role_row = cur.fetchone()
                    if role_row and role_row.get("role"):
                        role = str(role_row["role"])
                tenants = [{**tenant_row, "role": role}]
            elif binding_row.get("user_id") is not None:
                cur.execute(
                    tenants_sql,
                    {
                        "user_id": int(binding_row["user_id"]),
                        "client_id": client_id,
                    },
                )
                tenants = cur.fetchall()
        if not tenants:
            raise ValueError("canal sem tenant vinculado")
        return {
            "user": {
                "user_id": int(binding_row["user_id"]) if binding_row.get("user_id") is not None else None,
                "email": binding_row["email"],
                "full_name": binding_row["full_name"],
            } if binding_row.get("user_id") is not None else {},
            "tenants": tenants,
        }

    def list_channel_user_bindings(self, client_id: int, *, channel_type: str | None = None) -> list[dict[str, Any]]:
        self._ensure_channel_tables()
        self._ensure_channel_binding_tenant_column()
        sql = f"""
            SELECT
                b.id,
                b.client_id,
                b.tenant_id,
                b.user_id,
                b.channel_type,
                b.external_user_key,
                b.is_active,
                b.created_at,
                b.updated_at,
                t.name AS tenant_name,
                u.email AS user_email,
                u.full_name AS user_full_name
            FROM {self.schema}.channel_user_binding b
            LEFT JOIN {self.schema}.tenant t ON t.id = b.tenant_id
            LEFT JOIN {self.schema}.app_user u ON u.id = b.user_id
            WHERE b.client_id = %(client_id)s
        """
        params: dict[str, Any] = {"client_id": int(client_id)}
        normalized_channel = str(channel_type).strip().lower() if channel_type else None
        if normalized_channel:
            sql += " AND b.channel_type = %(channel_type)s"
            params["channel_type"] = normalized_channel
        sql += " ORDER BY b.channel_type, b.external_user_key"
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return rows

    def upsert_channel_user_binding(
        self,
        client_id: int,
        *,
        tenant_id: int,
        user_id: int | None = None,
        channel_type: str,
        external_user_key: str,
        is_active: bool = True,
    ) -> dict[str, Any]:
        self._ensure_channel_tables()
        self._ensure_channel_binding_tenant_column()
        normalized_channel = str(channel_type or "").strip().lower()
        normalized_external = str(external_user_key or "").strip()
        if normalized_channel not in {"telegram", "whatsapp"}:
            raise ValueError("channel_type invalido")
        if not normalized_external:
            raise ValueError("external_user_key obrigatorio")
        tenant_sql = f"""
            SELECT id
            FROM {self.schema}.tenant
            WHERE id = %(tenant_id)s
              AND client_id = %(client_id)s
            LIMIT 1
        """
        user_sql = f"""
            SELECT id, client_id
            FROM {self.schema}.app_user
            WHERE id = %(user_id)s
            LIMIT 1
        """
        upsert_sql = f"""
            INSERT INTO {self.schema}.channel_user_binding (
                client_id,
                tenant_id,
                user_id,
                channel_type,
                external_user_key,
                is_active,
                updated_at
            )
            VALUES (
                %(client_id)s,
                %(tenant_id)s,
                %(user_id)s,
                %(channel_type)s,
                %(external_user_key)s,
                %(is_active)s,
                NOW()
            )
            ON CONFLICT (channel_type, external_user_key)
            DO UPDATE SET
                client_id = EXCLUDED.client_id,
                tenant_id = EXCLUDED.tenant_id,
                user_id = EXCLUDED.user_id,
                is_active = EXCLUDED.is_active,
                updated_at = NOW()
            RETURNING id
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(tenant_sql, {"tenant_id": int(tenant_id), "client_id": int(client_id)})
            tenant_row = cur.fetchone()
            if not tenant_row:
                raise ValueError("tenant_id invalido")
            user_row = None
            if user_id is not None:
                cur.execute(user_sql, {"user_id": int(user_id)})
                user_row = cur.fetchone()
                if not user_row:
                    raise ValueError("user_id invalido")
                if int(user_row["client_id"]) != int(client_id):
                    raise ValueError("usuario nao pertence ao cliente")
            cur.execute(
                upsert_sql,
                {
                    "client_id": int(client_id),
                    "tenant_id": int(tenant_id),
                    "user_id": int(user_id) if user_id is not None else None,
                    "channel_type": normalized_channel,
                    "external_user_key": normalized_external,
                    "is_active": bool(is_active),
                },
            )
            saved = cur.fetchone()
            conn.commit()
        rows = self.list_channel_user_bindings(client_id, channel_type=normalized_channel)
        for row in rows:
            if int(row.get("id", 0)) == int(saved["id"]):
                return row
        raise ValueError("falha ao salvar vinculo de canal")

    def delete_channel_user_binding(self, client_id: int, *, binding_id: int) -> dict[str, Any]:
        self._ensure_channel_tables()
        self._ensure_channel_binding_tenant_column()
        delete_sql = f"""
            DELETE FROM {self.schema}.channel_user_binding
            WHERE id = %(binding_id)s
              AND client_id = %(client_id)s
            RETURNING id
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(
                delete_sql,
                {
                    "binding_id": int(binding_id),
                    "client_id": int(client_id),
                },
            )
            deleted = cur.fetchone()
            if not deleted:
                raise ValueError("vinculo de canal nao encontrado")
            conn.commit()
        return {"binding_id": int(deleted["id"]), "deleted": True}

    def set_channel_active_tenant(
        self,
        client_id: int,
        *,
        channel_type: str,
        conversation_key: str,
        external_user_key: str,
        tenant_id: int,
    ) -> dict[str, Any]:
        self._ensure_channel_tables()
        resolved = self.resolve_channel_user_tenants(
            client_id,
            channel_type=channel_type,
            external_user_key=external_user_key,
        )
        allowed = {int(item["tenant_id"]) for item in resolved["tenants"]}
        if int(tenant_id) not in allowed:
            raise ValueError("tenant nao permitido para este usuario/canal")
        upsert_sql = f"""
            INSERT INTO {self.schema}.channel_tenant_context (
                client_id,
                user_id,
                channel_type,
                conversation_key,
                active_tenant_id,
                updated_at
            )
            VALUES (
                %(client_id)s,
                %(user_id)s,
                %(channel_type)s,
                %(conversation_key)s,
                %(active_tenant_id)s,
                NOW()
            )
            ON CONFLICT (channel_type, conversation_key)
            DO UPDATE SET
                client_id = EXCLUDED.client_id,
                user_id = EXCLUDED.user_id,
                active_tenant_id = EXCLUDED.active_tenant_id,
                updated_at = NOW()
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(
                upsert_sql,
                {
                    "client_id": client_id,
                    "user_id": resolved["user"]["user_id"],
                    "channel_type": channel_type,
                    "conversation_key": conversation_key,
                    "active_tenant_id": tenant_id,
                },
            )
            conn.commit()
        return {"active_tenant_id": int(tenant_id), "conversation_key": conversation_key}

    def get_channel_active_tenant(
        self,
        client_id: int,
        *,
        channel_type: str,
        conversation_key: str,
        external_user_key: str,
    ) -> dict[str, Any]:
        self._ensure_channel_tables()
        resolved = self.resolve_channel_user_tenants(
            client_id,
            channel_type=channel_type,
            external_user_key=external_user_key,
        )
        sql = f"""
            SELECT active_tenant_id
            FROM {self.schema}.channel_tenant_context
            WHERE channel_type = %(channel_type)s
              AND conversation_key = %(conversation_key)s
            LIMIT 1
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "channel_type": channel_type,
                    "conversation_key": conversation_key,
                },
            )
            row = cur.fetchone()
        active_tenant_id = int(row["active_tenant_id"]) if row and row["active_tenant_id"] else None
        if active_tenant_id is None and len(resolved["tenants"]) == 1:
            active_tenant_id = int(resolved["tenants"][0]["tenant_id"])
        return {
            "active_tenant_id": active_tenant_id,
            "conversation_key": conversation_key,
            "tenants": resolved["tenants"],
            "user": resolved["user"],
        }

    def track_app_llm_usage(
        self,
        *,
        tenant_id: int,
        feature_code: str,
        input_tokens: int,
        output_tokens: int,
    ) -> dict[str, Any] | None:
        cfg_sql = f"""
            SELECT tlc.billing_mode, tlc.llm_provider_id
            FROM {self.schema}.tenant_llm_config tlc
            WHERE tlc.tenant_id = %(tenant_id)s
            LIMIT 1
        """
        insert_sql = f"""
            INSERT INTO {self.schema}.token_usage (
                tenant_id,
                llm_provider_id,
                input_tokens,
                output_tokens,
                unit_price_per_1k_cents,
                consumed_at
            )
            VALUES (
                %(tenant_id)s,
                %(llm_provider_id)s,
                %(input_tokens)s,
                %(output_tokens)s,
                %(unit_price_per_1k_cents)s,
                NOW()
            )
            RETURNING id
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(cfg_sql, {"tenant_id": tenant_id})
            cfg = cur.fetchone()
            if not cfg or cfg["billing_mode"] != "app_default_token":
                return None
            unit_price = int(os.getenv("IAOPS_APP_LLM_PRICE_PER_1K_CENTS", "50"))
            cur.execute(
                insert_sql,
                {
                    "tenant_id": tenant_id,
                    "llm_provider_id": int(cfg["llm_provider_id"]),
                    "input_tokens": max(0, int(input_tokens)),
                    "output_tokens": max(0, int(output_tokens)),
                    "unit_price_per_1k_cents": unit_price,
                },
            )
            row = cur.fetchone()
            conn.commit()
        return {
            "usage_id": int(row["id"]),
            "tenant_id": tenant_id,
            "feature_code": feature_code,
            "input_tokens": int(input_tokens),
            "output_tokens": int(output_tokens),
        }

    def list_source_catalog(self) -> list[dict[str, Any]]:
        sql = f"""
            SELECT code, name, category, is_supported, notes
            FROM {self.schema}.data_source_catalog
            WHERE is_supported = TRUE
            ORDER BY category, name
        """
        try:
            with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
                cur.execute(sql, {})
                rows = cur.fetchall()
            if rows:
                return rows
        except Exception:
            pass
        return list(DEFAULT_SOURCE_CATALOG)

    @staticmethod
    def _is_source_type_supported(source_type: str) -> bool:
        value = str(source_type or "").strip().lower()
        return any(str(item.get("code") or "").strip().lower() == value for item in DEFAULT_SOURCE_CATALOG)

    def list_tenant_data_sources(self, tenant_id: int) -> list[dict[str, Any]]:
        self._ensure_data_source_rag_meta()
        sql = f"""
            SELECT
                ds.id,
                ds.tenant_id,
                ds.source_type,
                COALESCE(cat.name, ds.source_type) AS source_name,
                ds.conn_secret_ref,
                ds.is_active,
                ds.rag_enabled,
                ds.rag_context_text,
                ds.created_at
            FROM {self.schema}.data_source ds
            LEFT JOIN {self.schema}.data_source_catalog cat
              ON cat.code = ds.source_type
            WHERE ds.tenant_id = %(tenant_id)s
            ORDER BY ds.created_at DESC, ds.id DESC
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(sql, {"tenant_id": tenant_id})
            rows = cur.fetchall()
        return rows

    def create_tenant_data_source(
        self,
        tenant_id: int,
        *,
        source_type: str,
        conn_secret_ref: str,
        is_active: bool = True,
        rag_enabled: bool = False,
        rag_context_text: str | None = None,
    ) -> dict[str, Any]:
        self._ensure_data_source_rag_meta()
        check_catalog_sql = f"""
            SELECT code
            FROM {self.schema}.data_source_catalog
            WHERE code = %(source_type)s
              AND is_supported = TRUE
            LIMIT 1
        """
        insert_sql = f"""
            INSERT INTO {self.schema}.data_source (
                tenant_id,
                source_type,
                conn_secret_ref,
                is_active,
                rag_enabled,
                rag_context_text
            )
            VALUES (
                %(tenant_id)s,
                %(source_type)s,
                %(conn_secret_ref)s,
                %(is_active)s,
                %(rag_enabled)s,
                %(rag_context_text)s
            )
            RETURNING id
        """
        client_lookup_sql = f"""
            SELECT client_id
            FROM {self.schema}.tenant
            WHERE id = %(tenant_id)s
            LIMIT 1
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            catalog_row = None
            try:
                cur.execute(check_catalog_sql, {"source_type": source_type})
                catalog_row = cur.fetchone()
            except Exception:
                catalog_row = None
            if not catalog_row and not self._is_source_type_supported(source_type):
                raise ValueError("source_type nao suportado")
            cur.execute(client_lookup_sql, {"tenant_id": tenant_id})
            tenant_row = cur.fetchone()
            if not tenant_row:
                raise ValueError("tenant nao encontrado")
            limits = self.get_client_tenant_limits(int(tenant_row["client_id"]), tenant_id=tenant_id)
            if not limits.get("can_create_data_source_client", True):
                raise ValueError(
                    "Limite de fontes por cliente atingido no plano atual "
                    f"({limits.get('total_data_sources', 0)}/{limits.get('max_data_sources_per_client', 0)}). "
                    "Remova uma fonte ou altere o plano."
                )
            if not limits.get("can_create_data_source_tenant", True):
                raise ValueError(
                    "Limite de fontes por tenant atingido no plano atual "
                    f"({limits.get('total_data_sources_tenant', 0)}/{limits.get('max_data_sources_per_tenant', 0)}). "
                    "Remova uma fonte deste tenant ou altere o plano."
                )
            cur.execute(
                insert_sql,
                {
                    "tenant_id": tenant_id,
                    "source_type": source_type,
                    "conn_secret_ref": conn_secret_ref,
                    "is_active": is_active,
                    "rag_enabled": bool(rag_enabled),
                    "rag_context_text": str(rag_context_text or "").strip() or None,
                },
            )
            inserted = cur.fetchone()
            conn.commit()

        rows = self.list_tenant_data_sources(tenant_id)
        for row in rows:
            if int(row["id"]) == int(inserted["id"]):
                return row
        raise ValueError("falha ao recuperar data_source criada")

    def update_tenant_data_source_status(self, tenant_id: int, data_source_id: int, is_active: bool) -> dict[str, Any]:
        lookup_sql = f"""
            SELECT ds.id, ds.is_active, t.client_id
            FROM {self.schema}.data_source ds
            JOIN {self.schema}.tenant t ON t.id = ds.tenant_id
            WHERE ds.id = %(data_source_id)s
              AND ds.tenant_id = %(tenant_id)s
            LIMIT 1
        """
        update_sql = f"""
            UPDATE {self.schema}.data_source
               SET is_active = %(is_active)s
             WHERE id = %(data_source_id)s
               AND tenant_id = %(tenant_id)s
         RETURNING id
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(
                lookup_sql,
                {
                    "tenant_id": tenant_id,
                    "data_source_id": data_source_id,
                },
            )
            current = cur.fetchone()
            if not current:
                raise ValueError("fonte de dados nao encontrada")
            cur.execute(
                update_sql,
                {
                    "tenant_id": tenant_id,
                    "data_source_id": data_source_id,
                    "is_active": is_active,
                },
            )
            row = cur.fetchone()
            conn.commit()
        rows = self.list_tenant_data_sources(tenant_id)
        for item in rows:
            if int(item["id"]) == int(data_source_id):
                return item
        raise ValueError("fonte de dados nao encontrada")

    def update_tenant_data_source(
        self,
        tenant_id: int,
        data_source_id: int,
        *,
        source_type: str,
        conn_secret_ref: str,
        rag_enabled: bool | None = None,
        rag_context_text: str | None = None,
    ) -> dict[str, Any]:
        self._ensure_data_source_rag_meta()
        check_catalog_sql = f"""
            SELECT code
            FROM {self.schema}.data_source_catalog
            WHERE code = %(source_type)s
              AND is_supported = TRUE
            LIMIT 1
        """
        update_sql = f"""
            UPDATE {self.schema}.data_source
               SET source_type = %(source_type)s,
                   conn_secret_ref = %(conn_secret_ref)s,
                   rag_enabled = COALESCE(%(rag_enabled)s, rag_enabled),
                   rag_context_text = %(rag_context_text)s
             WHERE id = %(data_source_id)s
               AND tenant_id = %(tenant_id)s
         RETURNING id
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            catalog_row = None
            try:
                cur.execute(check_catalog_sql, {"source_type": source_type})
                catalog_row = cur.fetchone()
            except Exception:
                catalog_row = None
            if not catalog_row and not self._is_source_type_supported(source_type):
                raise ValueError("source_type nao suportado")
            cur.execute(
                update_sql,
                {
                    "tenant_id": tenant_id,
                    "data_source_id": data_source_id,
                    "source_type": source_type,
                    "conn_secret_ref": conn_secret_ref,
                    "rag_enabled": (bool(rag_enabled) if rag_enabled is not None else None),
                    "rag_context_text": str(rag_context_text or "").strip() or None,
                },
            )
            row = cur.fetchone()
            if not row:
                raise ValueError("fonte de dados nao encontrada")
            conn.commit()
        rows = self.list_tenant_data_sources(tenant_id)
        for item in rows:
            if int(item["id"]) == int(data_source_id):
                return item
        raise ValueError("fonte de dados nao encontrada")

    def delete_tenant_data_source(self, tenant_id: int, data_source_id: int) -> dict[str, Any]:
        select_sql = f"""
            SELECT id, source_type
            FROM {self.schema}.data_source
            WHERE id = %(data_source_id)s
              AND tenant_id = %(tenant_id)s
            LIMIT 1
        """
        list_tables_sql = f"""
            SELECT id
            FROM {self.schema}.monitored_table
            WHERE tenant_id = %(tenant_id)s
              AND data_source_id = %(data_source_id)s
        """
        delete_events_sql = f"""
            DELETE FROM {self.schema}.schema_change_event
            WHERE monitored_table_id = ANY(%(table_ids)s)
        """
        delete_columns_sql = f"""
            DELETE FROM {self.schema}.monitored_column
            WHERE monitored_table_id = ANY(%(table_ids)s)
        """
        delete_tables_sql = f"""
            DELETE FROM {self.schema}.monitored_table
            WHERE id = ANY(%(table_ids)s)
              AND tenant_id = %(tenant_id)s
        """
        delete_sql = f"""
            DELETE FROM {self.schema}.data_source
            WHERE id = %(data_source_id)s
              AND tenant_id = %(tenant_id)s
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(select_sql, {"tenant_id": tenant_id, "data_source_id": data_source_id})
            source_row = cur.fetchone()
            if not source_row:
                raise ValueError("fonte de dados nao encontrada")
            cur.execute(list_tables_sql, {"tenant_id": tenant_id, "data_source_id": data_source_id})
            table_rows = cur.fetchall() or []
            table_ids = [int(item["id"]) for item in table_rows if item.get("id") is not None]
            removed_tables = 0
            removed_columns = 0
            removed_events = 0
            if table_ids:
                cur.execute(delete_events_sql, {"table_ids": table_ids})
                removed_events = int(cur.rowcount or 0)
                cur.execute(delete_columns_sql, {"table_ids": table_ids})
                removed_columns = int(cur.rowcount or 0)
                cur.execute(delete_tables_sql, {"tenant_id": tenant_id, "table_ids": table_ids})
                removed_tables = int(cur.rowcount or 0)
            cur.execute(delete_sql, {"tenant_id": tenant_id, "data_source_id": data_source_id})
            conn.commit()
        return {
            "deleted": True,
            "id": int(source_row["id"]),
            "source_type": source_row["source_type"],
            "removed_tables": removed_tables,
            "removed_columns": removed_columns,
            "removed_events": removed_events,
        }

    def get_tool_policy(self, tenant_id: int, tool_name: str) -> ToolPolicy | None:
        sql = f"""
            SELECT
                mt.tool_name,
                mt.min_role,
                tmp.is_enabled AS is_enabled,
                tmp.max_rows,
                tmp.max_calls_per_minute,
                tmp.require_masking AS require_masking,
                tmp.allowed_schema_patterns
            FROM {self.schema}.mcp_tool mt
            JOIN {self.schema}.mcp_server ms ON ms.id = mt.mcp_server_id
            LEFT JOIN {self.schema}.tenant_mcp_tool_policy tmp
              ON tmp.mcp_tool_id = mt.id
             AND tmp.tenant_id = %(tenant_id)s
            WHERE mt.tool_name = %(tool_name)s
              AND mt.is_active = TRUE
              AND ms.is_active = TRUE
            LIMIT 1
        """
        try:
            with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
                cur.execute(sql, {"tenant_id": tenant_id, "tool_name": tool_name})
                row = cur.fetchone()
        except Exception:
            return DEFAULT_TOOL_POLICIES.get(tool_name)
        if row:
            default = DEFAULT_TOOL_POLICIES.get(tool_name)
            return ToolPolicy(
                tool_name=row["tool_name"],
                min_role=str(row["min_role"] or (default.min_role if default else "viewer")),
                is_enabled=bool(row["is_enabled"]) if row["is_enabled"] is not None else bool(default.is_enabled if default else False),
                max_rows=row["max_rows"] if row["max_rows"] is not None else (default.max_rows if default else None),
                max_calls_per_minute=(
                    row["max_calls_per_minute"]
                    if row["max_calls_per_minute"] is not None
                    else (default.max_calls_per_minute if default else None)
                ),
                require_masking=(
                    bool(row["require_masking"])
                    if row["require_masking"] is not None
                    else bool(default.require_masking if default else True)
                ),
                allowed_schema_patterns=(
                    row["allowed_schema_patterns"]
                    if row["allowed_schema_patterns"] is not None
                    else (default.allowed_schema_patterns if default else None)
                ),
            )
        return DEFAULT_TOOL_POLICIES.get(tool_name)

    def list_tenant_tool_policies(self, tenant_id: int) -> list[dict[str, Any]]:
        sql = f"""
            SELECT
                mt.tool_name,
                mt.min_role,
                tmp.is_enabled AS is_enabled,
                tmp.max_rows,
                tmp.max_calls_per_minute,
                tmp.require_masking AS require_masking,
                COALESCE(tmp.allowed_schema_patterns, '[]'::jsonb) AS allowed_schema_patterns
            FROM {self.schema}.mcp_tool mt
            JOIN {self.schema}.mcp_server ms ON ms.id = mt.mcp_server_id
            LEFT JOIN {self.schema}.tenant_mcp_tool_policy tmp
              ON tmp.mcp_tool_id = mt.id
             AND tmp.tenant_id = %(tenant_id)s
            WHERE mt.is_active = TRUE
              AND ms.is_active = TRUE
            ORDER BY mt.tool_name
        """
        try:
            with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
                cur.execute(sql, {"tenant_id": tenant_id})
                rows = cur.fetchall()
        except Exception:
            rows = []
        materialized = []
        for row in rows:
            default = DEFAULT_TOOL_POLICIES.get(str(row["tool_name"] or ""))
            materialized.append(
                {
                    "tool_name": row["tool_name"],
                    "min_role": str(row["min_role"] or (default.min_role if default else "viewer")),
                    "is_enabled": bool(row["is_enabled"]) if row["is_enabled"] is not None else bool(default.is_enabled if default else False),
                    "max_rows": row["max_rows"] if row["max_rows"] is not None else (default.max_rows if default else None),
                    "max_calls_per_minute": (
                        row["max_calls_per_minute"]
                        if row["max_calls_per_minute"] is not None
                        else (default.max_calls_per_minute if default else None)
                    ),
                    "require_masking": (
                        bool(row["require_masking"])
                        if row["require_masking"] is not None
                        else bool(default.require_masking if default else True)
                    ),
                    "allowed_schema_patterns": row["allowed_schema_patterns"] or (default.allowed_schema_patterns or [] if default else []),
                }
            )
        return materialized or [
            {
                "tool_name": policy.tool_name,
                "min_role": policy.min_role,
                "is_enabled": bool(policy.is_enabled),
                "max_rows": policy.max_rows,
                "max_calls_per_minute": policy.max_calls_per_minute,
                "require_masking": bool(policy.require_masking),
                "allowed_schema_patterns": policy.allowed_schema_patterns or [],
            }
            for policy in DEFAULT_TOOL_POLICIES.values()
        ]

    def upsert_tenant_tool_policy(
        self,
        tenant_id: int,
        *,
        tool_name: str,
        is_enabled: bool,
        max_rows: int | None,
        max_calls_per_minute: int | None,
        require_masking: bool,
        allowed_schema_patterns: list[str],
    ) -> dict[str, Any]:
        lookup_sql = f"""
            SELECT mt.id AS mcp_tool_id
            FROM {self.schema}.mcp_tool mt
            JOIN {self.schema}.mcp_server ms ON ms.id = mt.mcp_server_id
            WHERE mt.tool_name = %(tool_name)s
              AND mt.is_active = TRUE
              AND ms.is_active = TRUE
            LIMIT 1
        """
        upsert_sql = f"""
            INSERT INTO {self.schema}.tenant_mcp_tool_policy (
                tenant_id,
                mcp_tool_id,
                is_enabled,
                max_rows,
                max_calls_per_minute,
                require_masking,
                allowed_schema_patterns,
                created_at,
                updated_at
            )
            VALUES (
                %(tenant_id)s,
                %(mcp_tool_id)s,
                %(is_enabled)s,
                %(max_rows)s,
                %(max_calls_per_minute)s,
                %(require_masking)s,
                %(allowed_schema_patterns)s,
                NOW(),
                NOW()
            )
            ON CONFLICT (tenant_id, mcp_tool_id)
            DO UPDATE SET
                is_enabled = EXCLUDED.is_enabled,
                max_rows = EXCLUDED.max_rows,
                max_calls_per_minute = EXCLUDED.max_calls_per_minute,
                require_masking = EXCLUDED.require_masking,
                allowed_schema_patterns = EXCLUDED.allowed_schema_patterns,
                updated_at = NOW()
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(lookup_sql, {"tool_name": tool_name})
            row = cur.fetchone()
            if not row:
                raise ValueError("tool nao encontrada")
            cur.execute(
                upsert_sql,
                {
                    "tenant_id": tenant_id,
                    "mcp_tool_id": row["mcp_tool_id"],
                    "is_enabled": bool(is_enabled),
                    "max_rows": max_rows,
                    "max_calls_per_minute": max_calls_per_minute,
                    "require_masking": bool(require_masking),
                    "allowed_schema_patterns": Jsonb(allowed_schema_patterns),
                },
            )
            conn.commit()
        rows = self.list_tenant_tool_policies(tenant_id)
        for item in rows:
            if item["tool_name"] == tool_name:
                return item
        raise ValueError("falha ao recuperar policy")

    def list_mcp_client_connections(self, tenant_id: int) -> list[dict[str, Any]]:
        sql = f"""
            SELECT
                id,
                tenant_id,
                connection_name,
                transport_type,
                endpoint_url,
                auth_secret_ref,
                is_active,
                health_status,
                last_healthcheck_at,
                created_at
            FROM {self.schema}.mcp_client_connection
            WHERE tenant_id = %(tenant_id)s
            ORDER BY connection_name
        """
        try:
            with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
                cur.execute(sql, {"tenant_id": tenant_id})
                rows = cur.fetchall()
            return rows
        except Exception:
            return []

    def upsert_mcp_client_connection(
        self,
        tenant_id: int,
        *,
        connection_name: str,
        transport_type: str,
        endpoint_url: str | None,
        auth_secret_ref: str | None,
        is_active: bool,
    ) -> dict[str, Any]:
        if transport_type not in {"stdio", "http", "websocket"}:
            raise ValueError("transport_type invalido")
        upsert_sql = f"""
            INSERT INTO {self.schema}.mcp_client_connection (
                tenant_id,
                connection_name,
                transport_type,
                endpoint_url,
                auth_secret_ref,
                is_active
            )
            VALUES (
                %(tenant_id)s,
                %(connection_name)s,
                %(transport_type)s,
                %(endpoint_url)s,
                %(auth_secret_ref)s,
                %(is_active)s
            )
            ON CONFLICT (tenant_id, connection_name)
            DO UPDATE SET
                transport_type = EXCLUDED.transport_type,
                endpoint_url = EXCLUDED.endpoint_url,
                auth_secret_ref = EXCLUDED.auth_secret_ref,
                is_active = EXCLUDED.is_active
            RETURNING id
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(
                upsert_sql,
                {
                    "tenant_id": tenant_id,
                    "connection_name": connection_name,
                    "transport_type": transport_type,
                    "endpoint_url": endpoint_url,
                    "auth_secret_ref": auth_secret_ref,
                    "is_active": bool(is_active),
                },
            )
            row = cur.fetchone()
            conn.commit()
        rows = self.list_mcp_client_connections(tenant_id)
        for item in rows:
            if int(item["id"]) == int(row["id"]):
                return item
        raise ValueError("falha ao recuperar conexao MCP")

    def update_mcp_client_connection_status(self, tenant_id: int, connection_id: int, is_active: bool) -> dict[str, Any]:
        sql = f"""
            UPDATE {self.schema}.mcp_client_connection
               SET is_active = %(is_active)s
             WHERE id = %(connection_id)s
               AND tenant_id = %(tenant_id)s
         RETURNING id
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "tenant_id": tenant_id,
                    "connection_id": connection_id,
                    "is_active": bool(is_active),
                },
            )
            row = cur.fetchone()
            if not row:
                raise ValueError("conexao MCP nao encontrada")
            conn.commit()
        rows = self.list_mcp_client_connections(tenant_id)
        for item in rows:
            if int(item["id"]) == int(connection_id):
                return item
        raise ValueError("conexao MCP nao encontrada")

    def get_sql_security_policy(self, tenant_id: int) -> dict[str, Any]:
        sql = f"""
            SELECT
                COALESCE(tmp.is_enabled, FALSE) AS is_enabled,
                tmp.max_rows,
                tmp.max_calls_per_minute,
                COALESCE(tmp.require_masking, TRUE) AS require_masking,
                COALESCE(tmp.allowed_schema_patterns, '[]'::jsonb) AS allowed_schema_patterns
            FROM {self.schema}.mcp_tool mt
            JOIN {self.schema}.mcp_server ms ON ms.id = mt.mcp_server_id
            LEFT JOIN {self.schema}.tenant_mcp_tool_policy tmp
              ON tmp.mcp_tool_id = mt.id
             AND tmp.tenant_id = %(tenant_id)s
            WHERE mt.tool_name = 'query.execute_safe_sql'
              AND mt.is_active = TRUE
              AND ms.is_active = TRUE
            LIMIT 1
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(sql, {"tenant_id": tenant_id})
            row = cur.fetchone()
        if not row:
            default = DEFAULT_TOOL_POLICIES["query.execute_safe_sql"]
            return {
                "tool_name": default.tool_name,
                "is_enabled": default.is_enabled,
                "max_rows": default.max_rows,
                "max_calls_per_minute": default.max_calls_per_minute,
                "require_masking": default.require_masking,
                "allowed_schema_patterns": default.allowed_schema_patterns or [],
            }
        return {
            "tool_name": "query.execute_safe_sql",
            "is_enabled": bool(row["is_enabled"]),
            "max_rows": row["max_rows"],
            "max_calls_per_minute": row["max_calls_per_minute"],
            "require_masking": bool(row["require_masking"]),
            "allowed_schema_patterns": row["allowed_schema_patterns"] or [],
        }

    def update_sql_security_policy(
        self,
        tenant_id: int,
        *,
        max_rows: int | None,
        max_calls_per_minute: int | None,
        require_masking: bool,
        allowed_schema_patterns: list[str],
    ) -> dict[str, Any]:
        lookup_sql = f"""
            SELECT mt.id AS mcp_tool_id
            FROM {self.schema}.mcp_tool mt
            JOIN {self.schema}.mcp_server ms ON ms.id = mt.mcp_server_id
            WHERE mt.tool_name = 'query.execute_safe_sql'
              AND mt.is_active = TRUE
              AND ms.is_active = TRUE
            LIMIT 1
        """
        upsert_sql = f"""
            INSERT INTO {self.schema}.tenant_mcp_tool_policy (
                tenant_id,
                mcp_tool_id,
                is_enabled,
                max_rows,
                max_calls_per_minute,
                require_masking,
                allowed_schema_patterns,
                created_at,
                updated_at
            )
            VALUES (
                %(tenant_id)s,
                %(mcp_tool_id)s,
                TRUE,
                %(max_rows)s,
                %(max_calls_per_minute)s,
                %(require_masking)s,
                %(allowed_schema_patterns)s,
                NOW(),
                NOW()
            )
            ON CONFLICT (tenant_id, mcp_tool_id)
            DO UPDATE SET
                max_rows = EXCLUDED.max_rows,
                max_calls_per_minute = EXCLUDED.max_calls_per_minute,
                require_masking = EXCLUDED.require_masking,
                allowed_schema_patterns = EXCLUDED.allowed_schema_patterns,
                updated_at = NOW()
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(lookup_sql, {})
            row = cur.fetchone()
            if not row:
                raise ValueError("tool query.execute_safe_sql nao encontrada")
            cur.execute(
                upsert_sql,
                {
                    "tenant_id": tenant_id,
                    "mcp_tool_id": row["mcp_tool_id"],
                    "max_rows": max_rows,
                    "max_calls_per_minute": max_calls_per_minute,
                    "require_masking": require_masking,
                    "allowed_schema_patterns": Jsonb(allowed_schema_patterns),
                },
            )
            conn.commit()
        return self.get_sql_security_policy(tenant_id)

    def list_monitored_tables(self, tenant_id: int, schema_name: str | None = None) -> list[dict[str, Any]]:
        sql = f"""
            SELECT schema_name, table_name, is_active
            FROM {self.schema}.monitored_table
            WHERE tenant_id = %(tenant_id)s
              AND (%(schema_name)s::text IS NULL OR schema_name = %(schema_name)s)
            ORDER BY schema_name, table_name
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(sql, {"tenant_id": tenant_id, "schema_name": schema_name})
            rows = cur.fetchall()
        return rows

    def list_tenant_monitored_tables(
        self,
        tenant_id: int,
        data_source_id: int | None = None,
    ) -> list[dict[str, Any]]:
        sql = f"""
            SELECT
                mt.id,
                mt.tenant_id,
                mt.data_source_id,
                ds.source_type,
                COALESCE(cat.name, ds.source_type) AS source_name,
                mt.schema_name,
                mt.table_name,
                mt.is_active,
                mt.created_at
            FROM {self.schema}.monitored_table mt
            JOIN {self.schema}.data_source ds ON ds.id = mt.data_source_id
            LEFT JOIN {self.schema}.data_source_catalog cat ON cat.code = ds.source_type
            WHERE mt.tenant_id = %(tenant_id)s
              AND (%(data_source_id)s::bigint IS NULL OR mt.data_source_id = %(data_source_id)s)
            ORDER BY mt.schema_name, mt.table_name
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "tenant_id": tenant_id,
                    "data_source_id": data_source_id,
                },
            )
            rows = cur.fetchall()
        return rows

    def create_monitored_table(
        self,
        tenant_id: int,
        *,
        data_source_id: int,
        schema_name: str,
        table_name: str,
        is_active: bool = True,
    ) -> dict[str, Any]:
        source_check_sql = f"""
            SELECT id
            FROM {self.schema}.data_source
            WHERE id = %(data_source_id)s
              AND tenant_id = %(tenant_id)s
            LIMIT 1
        """
        exists_sql = f"""
            SELECT id
            FROM {self.schema}.monitored_table
            WHERE tenant_id = %(tenant_id)s
              AND data_source_id = %(data_source_id)s
              AND schema_name = %(schema_name)s
              AND table_name = %(table_name)s
            LIMIT 1
        """
        insert_sql = f"""
            INSERT INTO {self.schema}.monitored_table (
                tenant_id,
                data_source_id,
                schema_name,
                table_name,
                is_active
            )
            VALUES (
                %(tenant_id)s,
                %(data_source_id)s,
                %(schema_name)s,
                %(table_name)s,
                %(is_active)s
            )
            RETURNING id
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(source_check_sql, {"tenant_id": tenant_id, "data_source_id": data_source_id})
            source_row = cur.fetchone()
            if not source_row:
                raise ValueError("data_source_id nao encontrado para o tenant")
            cur.execute(
                exists_sql,
                {
                    "tenant_id": tenant_id,
                    "data_source_id": data_source_id,
                    "schema_name": schema_name,
                    "table_name": table_name,
                },
            )
            if cur.fetchone():
                raise ValueError("tabela ja cadastrada para esta fonte")
            cur.execute(
                insert_sql,
                {
                    "tenant_id": tenant_id,
                    "data_source_id": data_source_id,
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "is_active": is_active,
                },
            )
            inserted = cur.fetchone()
            conn.commit()
        rows = self.list_tenant_monitored_tables(tenant_id, data_source_id)
        for item in rows:
            if int(item["id"]) == int(inserted["id"]):
                return item
        raise ValueError("falha ao recuperar tabela monitorada criada")

    def delete_monitored_table(self, tenant_id: int, monitored_table_id: int) -> dict[str, Any]:
        select_sql = f"""
            SELECT id, schema_name, table_name
            FROM {self.schema}.monitored_table
            WHERE id = %(monitored_table_id)s
              AND tenant_id = %(tenant_id)s
            LIMIT 1
        """
        dependency_sql = f"""
            SELECT
                (SELECT COUNT(*)
                 FROM {self.schema}.monitored_column
                 WHERE monitored_table_id = %(monitored_table_id)s) AS column_count,
                (SELECT COUNT(*)
                 FROM {self.schema}.schema_change_event
                 WHERE monitored_table_id = %(monitored_table_id)s) AS event_count
        """
        delete_sql = f"""
            DELETE FROM {self.schema}.monitored_table
            WHERE id = %(monitored_table_id)s
              AND tenant_id = %(tenant_id)s
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(select_sql, {"tenant_id": tenant_id, "monitored_table_id": monitored_table_id})
            table_row = cur.fetchone()
            if not table_row:
                raise ValueError("tabela monitorada nao encontrada")
            cur.execute(dependency_sql, {"monitored_table_id": monitored_table_id})
            dep_row = cur.fetchone()
            if int(dep_row["column_count"] or 0) > 0 or int(dep_row["event_count"] or 0) > 0:
                raise ValueError("nao e permitido remover tabela monitorada com colunas/eventos vinculados")
            cur.execute(delete_sql, {"tenant_id": tenant_id, "monitored_table_id": monitored_table_id})
            conn.commit()
        return {
            "deleted": True,
            "id": int(table_row["id"]),
            "schema_name": table_row["schema_name"],
            "table_name": table_row["table_name"],
        }

    def list_monitored_columns(self, tenant_id: int, schema_name: str, table_name: str) -> list[dict[str, Any]]:
        self._ensure_monitored_column_meta()
        sql = f"""
            SELECT
                mc.column_name,
                mc.data_type,
                mc.classification,
                mc.description_text,
                mc.source_description_text,
                mc.llm_description_suggested,
                mc.llm_classification_suggested,
                mc.llm_confidence_score,
                mc.llm_description_confirmed
            FROM {self.schema}.monitored_column mc
            JOIN {self.schema}.monitored_table mt ON mt.id = mc.monitored_table_id
            WHERE mt.tenant_id = %(tenant_id)s
              AND mt.schema_name = %(schema_name)s
              AND mt.table_name = %(table_name)s
            ORDER BY mc.column_name
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                {"tenant_id": tenant_id, "schema_name": schema_name, "table_name": table_name},
            )
            rows = cur.fetchall()
        return rows

    def list_monitored_columns_by_table(
        self,
        tenant_id: int,
        monitored_table_id: int,
    ) -> list[dict[str, Any]]:
        self._ensure_monitored_column_meta()
        sql = f"""
            SELECT
                mc.id,
                mc.monitored_table_id,
                mc.column_name,
                mc.data_type,
                mc.classification,
                mc.description_text,
                mc.source_description_text,
                mc.llm_description_suggested,
                mc.llm_classification_suggested,
                mc.llm_confidence_score,
                mc.llm_description_confirmed
            FROM {self.schema}.monitored_column mc
            JOIN {self.schema}.monitored_table mt ON mt.id = mc.monitored_table_id
            WHERE mt.tenant_id = %(tenant_id)s
              AND mc.monitored_table_id = %(monitored_table_id)s
            ORDER BY mc.column_name
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "tenant_id": tenant_id,
                    "monitored_table_id": monitored_table_id,
                },
            )
            rows = cur.fetchall()
        return rows

    def create_monitored_column(
        self,
        tenant_id: int,
        *,
        monitored_table_id: int,
        column_name: str,
        data_type: str | None = None,
        classification: str | None = None,
        description_text: str | None = None,
    ) -> dict[str, Any]:
        self._ensure_monitored_column_meta()
        table_check_sql = f"""
            SELECT id
            FROM {self.schema}.monitored_table
            WHERE id = %(monitored_table_id)s
              AND tenant_id = %(tenant_id)s
            LIMIT 1
        """
        exists_sql = f"""
            SELECT id
            FROM {self.schema}.monitored_column
            WHERE monitored_table_id = %(monitored_table_id)s
              AND column_name = %(column_name)s
            LIMIT 1
        """
        insert_sql = f"""
            INSERT INTO {self.schema}.monitored_column (
                tenant_id,
                monitored_table_id,
                column_name,
                data_type,
                classification,
                description_text
            )
            VALUES (
                %(tenant_id)s,
                %(monitored_table_id)s,
                %(column_name)s,
                %(data_type)s,
                %(classification)s,
                %(description_text)s
            )
            RETURNING id
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(
                table_check_sql,
                {
                    "tenant_id": tenant_id,
                    "monitored_table_id": monitored_table_id,
                },
            )
            table_row = cur.fetchone()
            if not table_row:
                raise ValueError("monitored_table_id nao encontrado para o tenant")
            cur.execute(
                exists_sql,
                {
                    "monitored_table_id": monitored_table_id,
                    "column_name": column_name,
                },
            )
            if cur.fetchone():
                raise ValueError("coluna ja cadastrada para a tabela")
            cur.execute(
                insert_sql,
                {
                    "tenant_id": tenant_id,
                    "monitored_table_id": monitored_table_id,
                    "column_name": column_name,
                    "data_type": data_type,
                    "classification": classification,
                    "description_text": description_text,
                },
            )
            inserted = cur.fetchone()
            conn.commit()
        rows = self.list_monitored_columns_by_table(tenant_id, monitored_table_id)
        for item in rows:
            if int(item["id"]) == int(inserted["id"]):
                return item
        raise ValueError("falha ao recuperar coluna monitorada criada")

    def delete_monitored_column(
        self,
        tenant_id: int,
        monitored_column_id: int,
    ) -> dict[str, Any]:
        select_sql = f"""
            SELECT mc.id, mc.monitored_table_id, mc.column_name
            FROM {self.schema}.monitored_column mc
            JOIN {self.schema}.monitored_table mt ON mt.id = mc.monitored_table_id
            WHERE mc.id = %(monitored_column_id)s
              AND mt.tenant_id = %(tenant_id)s
            LIMIT 1
        """
        delete_sql = f"""
            DELETE FROM {self.schema}.monitored_column
            WHERE id = %(monitored_column_id)s
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(
                select_sql,
                {
                    "tenant_id": tenant_id,
                    "monitored_column_id": monitored_column_id,
                },
            )
            column_row = cur.fetchone()
            if not column_row:
                raise ValueError("coluna monitorada nao encontrada")
            cur.execute(delete_sql, {"monitored_column_id": monitored_column_id})
            conn.commit()
        return {
            "deleted": True,
            "id": int(column_row["id"]),
            "monitored_table_id": int(column_row["monitored_table_id"]),
            "column_name": column_row["column_name"],
        }

    def execute_safe_sql(self, tenant_id: int, sql_text: str, max_rows: int | None) -> dict[str, Any]:
        _ = tenant_id
        enforced_sql = sql_text
        if max_rows and " limit " not in sql_text.lower():
            enforced_sql = f"{sql_text.rstrip()} LIMIT {int(max_rows)}"

        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(enforced_sql)
            rows = cur.fetchall()
        columns = list(rows[0].keys()) if rows else []
        return {
            "rows": rows,
            "columns": columns,
            "execution_ms": None,
            "applied_masks": [],
        }

    def list_active_lgpd_rules(self, tenant_id: int) -> list[dict[str, Any]]:
        sql = f"""
            SELECT
                schema_name,
                table_name,
                column_name,
                rule_type,
                rule_config
            FROM {self.schema}.lgpd_rule
            WHERE tenant_id = %(tenant_id)s
              AND is_active = TRUE
            ORDER BY id
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(sql, {"tenant_id": tenant_id})
            rows = cur.fetchall()
        normalized: list[dict[str, Any]] = []
        for row in rows:
            normalized.append(
                {
                    "schema_name": str(row.get("schema_name") or "").strip().lower(),
                    "table_name": str(row.get("table_name") or "").strip().lower(),
                    "column_name": str(row.get("column_name") or "").strip().lower(),
                    "rule_type": str(row.get("rule_type") or "").strip().lower(),
                    "rule_config": row.get("rule_config") or {},
                }
            )
        return normalized

    def create_incident(
        self,
        tenant_id: int,
        title: str,
        severity: str,
        source_event_id: int | None,
    ) -> dict[str, Any]:
        sql = f"""
            INSERT INTO {self.schema}.incident (
                tenant_id,
                source_event_id,
                title,
                status,
                severity,
                sla_due_at
            )
            VALUES (
                %(tenant_id)s,
                %(source_event_id)s,
                %(title)s,
                'open',
                %(severity)s,
                NOW() + CASE
                    WHEN %(severity)s = 'critical' THEN INTERVAL '1 hour'
                    WHEN %(severity)s = 'high' THEN INTERVAL '4 hours'
                    WHEN %(severity)s = 'medium' THEN INTERVAL '12 hours'
                    ELSE INTERVAL '24 hours'
                END
            )
            RETURNING id AS incident_id, status, severity, source_event_id, sla_due_at
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "tenant_id": tenant_id,
                    "source_event_id": source_event_id,
                    "title": title,
                    "severity": severity,
                },
            )
            row = cur.fetchone()
            conn.commit()
        return {
            "incident_id": row["incident_id"],
            "tenant_id": tenant_id,
            "title": title,
            "severity": row["severity"],
            "source_event_id": row["source_event_id"],
            "status": row["status"],
            "sla_due_at": row["sla_due_at"].isoformat(),
        }

    def list_incidents(
        self,
        tenant_id: int,
        status: str | None = None,
        severity: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        sql = f"""
            SELECT
                i.id AS incident_id,
                i.title,
                i.status,
                i.severity,
                i.source_event_id,
                i.sla_due_at,
                i.created_at
            FROM {self.schema}.incident i
            WHERE i.tenant_id = %(tenant_id)s
              AND (%(status)s::text IS NULL OR i.status = %(status)s)
              AND (%(severity)s::text IS NULL OR i.severity = %(severity)s)
            ORDER BY i.created_at DESC
            LIMIT %(limit)s
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "tenant_id": tenant_id,
                    "status": status,
                    "severity": severity,
                    "limit": max(1, min(limit, 500)),
                },
            )
            rows = cur.fetchall()
        return rows

    def update_incident_status(
        self,
        tenant_id: int,
        incident_id: int,
        new_status: str,
    ) -> dict[str, Any]:
        transitions = {
            "open": {"ack", "resolved"},
            "ack": {"resolved", "closed"},
            "resolved": {"closed"},
            "closed": set(),
        }
        if new_status not in {"open", "ack", "resolved", "closed"}:
            raise ValueError("status invalido")
        select_sql = f"""
            SELECT status
            FROM {self.schema}.incident
            WHERE id = %(incident_id)s
              AND tenant_id = %(tenant_id)s
            LIMIT 1
        """
        update_sql = f"""
            UPDATE {self.schema}.incident
               SET status = %(new_status)s,
                   ack_at = CASE WHEN %(new_status)s = 'ack' THEN NOW() ELSE ack_at END,
                   resolved_at = CASE WHEN %(new_status)s = 'resolved' THEN NOW() ELSE resolved_at END,
                   closed_at = CASE WHEN %(new_status)s = 'closed' THEN NOW() ELSE closed_at END
             WHERE id = %(incident_id)s
               AND tenant_id = %(tenant_id)s
         RETURNING
               id AS incident_id,
               title,
               status,
               severity,
               source_event_id,
               sla_due_at,
               created_at,
               ack_at,
               resolved_at,
               closed_at
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(
                select_sql,
                {
                    "tenant_id": tenant_id,
                    "incident_id": incident_id,
                },
            )
            current = cur.fetchone()
            if not current:
                raise ValueError("incidente nao encontrado")
            current_status = current["status"]
            if new_status == current_status:
                cur.execute(
                    f"""
                    SELECT
                        id AS incident_id,
                        title,
                        status,
                        severity,
                        source_event_id,
                        sla_due_at,
                        created_at,
                        ack_at,
                        resolved_at,
                        closed_at
                    FROM {self.schema}.incident
                    WHERE id = %(incident_id)s
                      AND tenant_id = %(tenant_id)s
                    LIMIT 1
                    """,
                    {"tenant_id": tenant_id, "incident_id": incident_id},
                )
                same_row = cur.fetchone()
                return same_row
            if new_status not in transitions.get(current_status, set()):
                raise ValueError("transicao de status invalida")
            cur.execute(
                update_sql,
                {
                    "tenant_id": tenant_id,
                    "incident_id": incident_id,
                    "new_status": new_status,
                },
            )
            row = cur.fetchone()
            conn.commit()
        if not row:
            raise ValueError("incidente nao encontrado")
        return row

    def list_events(
        self,
        tenant_id: int,
        severity: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        sql = f"""
            SELECT
                e.id AS event_id,
                mt.schema_name,
                mt.table_name,
                e.change_type,
                e.severity,
                e.payload_json,
                e.detected_at
            FROM {self.schema}.schema_change_event e
            JOIN {self.schema}.monitored_table mt ON mt.id = e.monitored_table_id
            WHERE e.tenant_id = %(tenant_id)s
              AND (%(severity)s::text IS NULL OR e.severity = %(severity)s)
            ORDER BY e.detected_at DESC
            LIMIT %(limit)s
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "tenant_id": tenant_id,
                    "severity": severity,
                    "limit": max(1, min(limit, 500)),
                },
            )
            rows = cur.fetchall()
        return rows

    def list_audit_calls(
        self,
        tenant_id: int,
        tool_name: str | None = None,
        status: str | None = None,
        correlation_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        sql = f"""
            SELECT
                l.id AS call_id,
                COALESCE(mt.tool_name, l.error_code, 'unknown') AS tool_name,
                l.status,
                l.correlation_id,
                l.latency_ms,
                l.error_code,
                l.error_message,
                l.requested_at AS created_at
            FROM {self.schema}.mcp_call_log l
            LEFT JOIN {self.schema}.mcp_tool mt ON mt.id = l.mcp_tool_id
            WHERE l.tenant_id = %(tenant_id)s
              AND (%(tool_name)s::text IS NULL OR mt.tool_name = %(tool_name)s)
              AND (%(status)s::text IS NULL OR l.status = %(status)s)
              AND (%(correlation_id)s::text IS NULL OR l.correlation_id ILIKE '%%' || %(correlation_id)s || '%%')
            ORDER BY l.requested_at DESC
            LIMIT %(limit)s
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "tenant_id": tenant_id,
                    "tool_name": tool_name,
                    "status": status,
                    "correlation_id": correlation_id,
                    "limit": max(1, min(limit, 500)),
                },
            )
            rows = cur.fetchall()
        return rows

    def get_health_summary(self, tenant_id: int, window_minutes: int) -> dict[str, Any]:
        sql = f"""
            SELECT
                (SELECT COUNT(*)
                 FROM {self.schema}.incident i
                 WHERE i.tenant_id = %(tenant_id)s
                   AND i.status IN ('open', 'ack')) AS open_incidents,
                (SELECT COUNT(*)
                 FROM {self.schema}.schema_change_event e
                 WHERE e.tenant_id = %(tenant_id)s
                   AND e.severity = 'critical'
                   AND e.detected_at >= NOW() - (%(window_minutes)s::text || ' minutes')::interval) AS critical_events,
                (SELECT MAX(e.detected_at)
                 FROM {self.schema}.schema_change_event e
                 WHERE e.tenant_id = %(tenant_id)s) AS last_scan_at
        """
        channels_sql = f"""
            SELECT connection_name AS channel_type, health_status
            FROM {self.schema}.mcp_client_connection
            WHERE tenant_id = %(tenant_id)s
              AND is_active = TRUE
        """
        try:
            with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
                cur.execute(sql, {"tenant_id": tenant_id, "window_minutes": window_minutes})
                row = cur.fetchone()
                try:
                    cur.execute(channels_sql, {"tenant_id": tenant_id})
                    channels_rows = cur.fetchall()
                except Exception:
                    channels_rows = []
        except Exception:
            row = {"open_incidents": 0, "critical_events": 0, "last_scan_at": None}
            channels_rows = []

        channels_health = {item["channel_type"]: item["health_status"] for item in channels_rows}
        return {
            "open_incidents": int(row["open_incidents"] or 0),
            "critical_events": int(row["critical_events"] or 0),
            "channels_health": channels_health,
            "last_scan_at": row["last_scan_at"].isoformat() if row["last_scan_at"] else None,
        }

    def get_setup_progress(self, tenant_id: int) -> dict[str, Any] | None:
        sql = f"""
            SELECT response_payload_json
            FROM {self.schema}.mcp_call_log
            WHERE tenant_id = %(tenant_id)s
              AND status = 'success'
              AND mcp_tool_id = (
                  SELECT id
                  FROM {self.schema}.mcp_tool
                  WHERE tool_name = 'setup.upsert_progress'
                  LIMIT 1
              )
            ORDER BY requested_at DESC
            LIMIT 1
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(sql, {"tenant_id": tenant_id})
            row = cur.fetchone()
        if not row:
            return None
        payload = row.get("response_payload_json") or {}
        data = payload.get("data") if isinstance(payload, dict) else None
        progress = data.get("progress") if isinstance(data, dict) else None
        return progress if isinstance(progress, dict) else None

    def upsert_setup_progress(
        self,
        *,
        client_id: int,
        tenant_id: int,
        user_id: int,
        correlation_id: str,
        snapshot: dict[str, Any],
    ) -> dict[str, Any]:
        _ = client_id, correlation_id
        return {
            "tenant_id": tenant_id,
            "updated_by_user_id": user_id,
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "snapshot": snapshot or {},
        }

    def log_mcp_call(
        self,
        *,
        client_id: int,
        tenant_id: int,
        user_id: int,
        tool_name: str,
        status: str,
        correlation_id: str,
        request_payload: dict[str, Any],
        response_payload: dict[str, Any],
        error_code: str | None,
        error_message: str | None,
        latency_ms: int,
    ) -> None:
        tool_lookup_sql = f"""
            SELECT mt.id AS mcp_tool_id, mt.mcp_server_id
            FROM {self.schema}.mcp_tool mt
            WHERE mt.tool_name = %(tool_name)s
              AND mt.is_active = TRUE
            LIMIT 1
        """
        insert_sql = f"""
            INSERT INTO {self.schema}.mcp_call_log (
                client_id,
                tenant_id,
                user_id,
                mcp_server_id,
                mcp_tool_id,
                direction,
                correlation_id,
                request_payload_json,
                response_payload_json,
                status,
                error_code,
                error_message,
                latency_ms,
                requested_at,
                completed_at
            )
            VALUES (
                %(client_id)s,
                %(tenant_id)s,
                %(user_id)s,
                %(mcp_server_id)s,
                %(mcp_tool_id)s,
                'inbound',
                %(correlation_id)s,
                %(request_payload_json)s,
                %(response_payload_json)s,
                %(status)s,
                %(error_code)s,
                %(error_message)s,
                %(latency_ms)s,
                NOW(),
                NOW()
            )
        """
        try:
            with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
                cur.execute(tool_lookup_sql, {"tool_name": tool_name})
                tool_row = cur.fetchone()
                cur.execute(
                    insert_sql,
                    {
                        "client_id": client_id,
                        "tenant_id": tenant_id,
                        "user_id": user_id,
                        "mcp_server_id": tool_row["mcp_server_id"] if tool_row else None,
                        "mcp_tool_id": tool_row["mcp_tool_id"] if tool_row else None,
                        "correlation_id": correlation_id,
                        "request_payload_json": Jsonb(request_payload),
                        "response_payload_json": Jsonb(response_payload),
                        "status": status,
                        "error_code": error_code,
                        "error_message": error_message,
                        "latency_ms": latency_ms,
                    },
                )
                conn.commit()
        except Exception:
            # Auditoria nao pode derrubar o fluxo principal caso esquema MCP ainda nao esteja aplicado.
            return
