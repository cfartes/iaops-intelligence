from __future__ import annotations

from typing import Any

from psycopg import connect
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .models import ToolPolicy
from .repository import MCPRepository


DEFAULT_TOOL_POLICIES = {
    "inventory.list_tables": ToolPolicy("inventory.list_tables", "viewer", True, 1000, 120, True, None),
    "inventory.list_columns": ToolPolicy("inventory.list_columns", "viewer", True, 1000, 120, True, None),
    "inventory.list_tenant_tables": ToolPolicy("inventory.list_tenant_tables", "viewer", True, 1000, 120, True, None),
    "inventory.register_table": ToolPolicy("inventory.register_table", "admin", True, None, 120, True, None),
    "inventory.delete_table": ToolPolicy("inventory.delete_table", "admin", True, None, 120, True, None),
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
    "ops.get_health_summary": ToolPolicy("ops.get_health_summary", "viewer", True, None, 120, True, None),
}


class PostgresMCPRepository(MCPRepository):
    """Repositorio MCP persistente em PostgreSQL (schema iaops_gov)."""

    def __init__(self, dsn: str, schema: str = "iaops_gov") -> None:
        self.dsn = dsn
        self.schema = schema

    def is_tenant_operational(self, client_id: int, tenant_id: int) -> bool:
        sql = f"""
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
            ) AS ok
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(sql, {"client_id": client_id, "tenant_id": tenant_id})
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

    def list_source_catalog(self) -> list[dict[str, Any]]:
        sql = f"""
            SELECT code, name, category, is_supported, notes
            FROM {self.schema}.data_source_catalog
            WHERE is_supported = TRUE
            ORDER BY category, name
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(sql, {})
            rows = cur.fetchall()
        return rows

    def list_tenant_data_sources(self, tenant_id: int) -> list[dict[str, Any]]:
        sql = f"""
            SELECT
                ds.id,
                ds.tenant_id,
                ds.source_type,
                COALESCE(cat.name, ds.source_type) AS source_name,
                ds.conn_secret_ref,
                ds.is_active,
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
    ) -> dict[str, Any]:
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
                is_active
            )
            VALUES (
                %(tenant_id)s,
                %(source_type)s,
                %(conn_secret_ref)s,
                %(is_active)s
            )
            RETURNING id
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(check_catalog_sql, {"source_type": source_type})
            catalog_row = cur.fetchone()
            if not catalog_row:
                raise ValueError("source_type nao suportado")
            cur.execute(
                insert_sql,
                {
                    "tenant_id": tenant_id,
                    "source_type": source_type,
                    "conn_secret_ref": conn_secret_ref,
                    "is_active": is_active,
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
        update_sql = f"""
            UPDATE {self.schema}.data_source
               SET is_active = %(is_active)s
             WHERE id = %(data_source_id)s
               AND tenant_id = %(tenant_id)s
         RETURNING id
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(
                update_sql,
                {
                    "tenant_id": tenant_id,
                    "data_source_id": data_source_id,
                    "is_active": is_active,
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

    def update_tenant_data_source(
        self,
        tenant_id: int,
        data_source_id: int,
        *,
        source_type: str,
        conn_secret_ref: str,
    ) -> dict[str, Any]:
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
                   conn_secret_ref = %(conn_secret_ref)s
             WHERE id = %(data_source_id)s
               AND tenant_id = %(tenant_id)s
         RETURNING id
        """
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(check_catalog_sql, {"source_type": source_type})
            catalog_row = cur.fetchone()
            if not catalog_row:
                raise ValueError("source_type nao suportado")
            cur.execute(
                update_sql,
                {
                    "tenant_id": tenant_id,
                    "data_source_id": data_source_id,
                    "source_type": source_type,
                    "conn_secret_ref": conn_secret_ref,
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
        dependency_sql = f"""
            SELECT COUNT(*) AS total
            FROM {self.schema}.monitored_table
            WHERE tenant_id = %(tenant_id)s
              AND data_source_id = %(data_source_id)s
        """
        select_sql = f"""
            SELECT id, source_type
            FROM {self.schema}.data_source
            WHERE id = %(data_source_id)s
              AND tenant_id = %(tenant_id)s
            LIMIT 1
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
            cur.execute(dependency_sql, {"tenant_id": tenant_id, "data_source_id": data_source_id})
            dep_row = cur.fetchone()
            if int(dep_row["total"] or 0) > 0:
                raise ValueError("nao e permitido remover fonte com tabelas monitoradas vinculadas")
            cur.execute(delete_sql, {"tenant_id": tenant_id, "data_source_id": data_source_id})
            conn.commit()
        return {
            "deleted": True,
            "id": int(source_row["id"]),
            "source_type": source_row["source_type"],
        }

    def get_tool_policy(self, tenant_id: int, tool_name: str) -> ToolPolicy | None:
        sql = f"""
            SELECT
                mt.tool_name,
                mt.min_role,
                COALESCE(tmp.is_enabled, FALSE) AS is_enabled,
                tmp.max_rows,
                tmp.max_calls_per_minute,
                COALESCE(tmp.require_masking, TRUE) AS require_masking,
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
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(sql, {"tenant_id": tenant_id, "tool_name": tool_name})
            row = cur.fetchone()
        if row:
            return ToolPolicy(
                tool_name=row["tool_name"],
                min_role=row["min_role"],
                is_enabled=bool(row["is_enabled"]),
                max_rows=row["max_rows"],
                max_calls_per_minute=row["max_calls_per_minute"],
                require_masking=bool(row["require_masking"]),
                allowed_schema_patterns=(row["allowed_schema_patterns"] or None),
            )
        return DEFAULT_TOOL_POLICIES.get(tool_name)

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
        sql = f"""
            SELECT mc.column_name, mc.data_type, mc.classification, mc.description_text
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
              AND (%(correlation_id)s::text IS NULL OR l.correlation_id ILIKE '%' || %(correlation_id)s || '%')
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
        with connect(self.dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute(sql, {"tenant_id": tenant_id, "window_minutes": window_minutes})
            row = cur.fetchone()
            cur.execute(channels_sql, {"tenant_id": tenant_id})
            channels_rows = cur.fetchall()

        channels_health = {item["channel_type"]: item["health_status"] for item in channels_rows}
        return {
            "open_incidents": int(row["open_incidents"] or 0),
            "critical_events": int(row["critical_events"] or 0),
            "channels_health": channels_health,
            "last_scan_at": row["last_scan_at"].isoformat() if row["last_scan_at"] else None,
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
