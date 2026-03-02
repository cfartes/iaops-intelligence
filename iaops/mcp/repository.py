from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from typing import Any

from .models import ToolPolicy


class MCPRepository(ABC):
    @abstractmethod
    def is_tenant_operational(self, client_id: int, tenant_id: int) -> bool:
        raise NotImplementedError

    @abstractmethod
    def get_user_role(self, tenant_id: int, user_id: int) -> str | None:
        raise NotImplementedError

    @abstractmethod
    def get_tool_policy(self, tenant_id: int, tool_name: str) -> ToolPolicy | None:
        raise NotImplementedError

    @abstractmethod
    def list_source_catalog(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def list_tenant_data_sources(self, tenant_id: int) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def create_tenant_data_source(
        self,
        tenant_id: int,
        *,
        source_type: str,
        conn_secret_ref: str,
        is_active: bool = True,
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def update_tenant_data_source_status(self, tenant_id: int, data_source_id: int, is_active: bool) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def update_tenant_data_source(
        self,
        tenant_id: int,
        data_source_id: int,
        *,
        source_type: str,
        conn_secret_ref: str,
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def delete_tenant_data_source(self, tenant_id: int, data_source_id: int) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def get_sql_security_policy(self, tenant_id: int) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def update_sql_security_policy(
        self,
        tenant_id: int,
        *,
        max_rows: int | None,
        max_calls_per_minute: int | None,
        require_masking: bool,
        allowed_schema_patterns: list[str],
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def list_monitored_tables(self, tenant_id: int, schema_name: str | None = None) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def list_tenant_monitored_tables(
        self,
        tenant_id: int,
        data_source_id: int | None = None,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def create_monitored_table(
        self,
        tenant_id: int,
        *,
        data_source_id: int,
        schema_name: str,
        table_name: str,
        is_active: bool = True,
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def delete_monitored_table(self, tenant_id: int, monitored_table_id: int) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def list_monitored_columns(self, tenant_id: int, schema_name: str, table_name: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def execute_safe_sql(self, tenant_id: int, sql_text: str, max_rows: int | None) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def create_incident(
        self,
        tenant_id: int,
        title: str,
        severity: str,
        source_event_id: int | None,
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def list_incidents(
        self,
        tenant_id: int,
        status: str | None = None,
        severity: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def update_incident_status(
        self,
        tenant_id: int,
        incident_id: int,
        new_status: str,
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def list_events(
        self,
        tenant_id: int,
        severity: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def list_audit_calls(
        self,
        tenant_id: int,
        tool_name: str | None = None,
        status: str | None = None,
        correlation_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def get_health_summary(self, tenant_id: int, window_minutes: int) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError


class InMemoryMCPRepository(MCPRepository):
    """Repositorio de demonstracao para scaffold local."""

    def __init__(self) -> None:
        self._tenant_state = {(1, 10): True}
        self._roles = {(10, 100): "owner", (10, 101): "admin", (10, 102): "viewer"}
        self._tool_policies = {
            (10, "inventory.list_tables"): ToolPolicy("inventory.list_tables", "viewer", True, 1000, 120, True, None),
            (10, "inventory.list_columns"): ToolPolicy("inventory.list_columns", "viewer", True, 1000, 120, True, None),
            (10, "inventory.list_tenant_tables"): ToolPolicy("inventory.list_tenant_tables", "viewer", True, 1000, 120, True, None),
            (10, "inventory.register_table"): ToolPolicy("inventory.register_table", "admin", True, None, 120, True, None),
            (10, "inventory.delete_table"): ToolPolicy("inventory.delete_table", "admin", True, None, 120, True, None),
            (10, "source.list_catalog"): ToolPolicy("source.list_catalog", "viewer", True, 1000, 120, True, None),
            (10, "source.list_tenant"): ToolPolicy("source.list_tenant", "viewer", True, 1000, 120, True, None),
            (10, "source.register"): ToolPolicy("source.register", "admin", True, None, 60, True, None),
            (10, "source.update_status"): ToolPolicy("source.update_status", "admin", True, None, 120, True, None),
            (10, "source.update"): ToolPolicy("source.update", "admin", True, None, 120, True, None),
            (10, "source.delete"): ToolPolicy("source.delete", "admin", True, None, 60, True, None),
            (10, "query.execute_safe_sql"): ToolPolicy("query.execute_safe_sql", "admin", True, 200, 30, True, ["public", "analytics"]),
            (10, "incident.create"): ToolPolicy("incident.create", "admin", True, None, 60, True, None),
            (10, "incident.list"): ToolPolicy("incident.list", "viewer", True, 200, 120, True, None),
            (10, "incident.update_status"): ToolPolicy("incident.update_status", "admin", True, None, 120, True, None),
            (10, "events.list"): ToolPolicy("events.list", "viewer", True, 200, 120, True, None),
            (10, "audit.list_calls"): ToolPolicy("audit.list_calls", "admin", True, 200, 120, True, None),
            (
                10,
                "security_sql.get_policy",
            ): ToolPolicy("security_sql.get_policy", "viewer", True, 200, 120, True, ["public", "analytics"]),
            (
                10,
                "security_sql.update_policy",
            ): ToolPolicy("security_sql.update_policy", "admin", True, 200, 120, True, ["public", "analytics"]),
            (10, "ops.get_health_summary"): ToolPolicy("ops.get_health_summary", "viewer", True, None, 120, True, None),
        }
        self._tables = {
            10: [
                {"id": 501, "tenant_id": 10, "data_source_id": 1001, "schema_name": "public", "table_name": "orders", "is_active": True},
                {"id": 502, "tenant_id": 10, "data_source_id": 1001, "schema_name": "public", "table_name": "customers", "is_active": True},
                {"id": 503, "tenant_id": 10, "data_source_id": 1001, "schema_name": "analytics", "table_name": "kpi_daily", "is_active": True},
            ]
        }
        self._monitored_table_seq = 504
        self._tenant_data_sources: dict[int, list[dict[str, Any]]] = {
            10: [
                {
                    "id": 1001,
                    "tenant_id": 10,
                    "source_type": "postgresql",
                    "conn_secret_ref": "secret://tenant-10/postgres/main",
                    "is_active": True,
                    "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                }
            ]
        }
        self._data_source_seq = 1002
        self._columns = {
            (10, "public", "orders"): [
                {"column_name": "id", "data_type": "bigint", "classification": "identifier", "description_text": "PK"},
                {
                    "column_name": "customer_cpf",
                    "data_type": "text",
                    "classification": "sensitive",
                    "description_text": "Documento do titular",
                },
                {"column_name": "total_amount", "data_type": "numeric", "classification": "financial", "description_text": "Valor"},
            ]
        }
        self._incident_seq = 1
        self._incidents: list[dict[str, Any]] = []
        self._events = [
            {
                "event_id": 9001,
                "tenant_id": 10,
                "schema_name": "public",
                "table_name": "orders",
                "change_type": "column_type_changed",
                "severity": "critical",
                "detected_at": (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=20)).isoformat(),
            },
            {
                "event_id": 9002,
                "tenant_id": 10,
                "schema_name": "analytics",
                "table_name": "kpi_daily",
                "change_type": "column_added",
                "severity": "medium",
                "detected_at": (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=45)).isoformat(),
            },
        ]
        self._call_logs: list[dict[str, Any]] = []

    def is_tenant_operational(self, client_id: int, tenant_id: int) -> bool:
        return self._tenant_state.get((client_id, tenant_id), False)

    def get_user_role(self, tenant_id: int, user_id: int) -> str | None:
        return self._roles.get((tenant_id, user_id))

    def get_tool_policy(self, tenant_id: int, tool_name: str) -> ToolPolicy | None:
        return self._tool_policies.get((tenant_id, tool_name))

    def list_source_catalog(self) -> list[dict[str, Any]]:
        return [
            {"code": "postgresql", "name": "PostgreSQL", "category": "relational", "is_supported": True},
            {"code": "mysql", "name": "MySQL", "category": "relational", "is_supported": True},
            {"code": "sqlserver", "name": "SQL Server", "category": "relational", "is_supported": True},
            {"code": "oracle", "name": "Oracle", "category": "relational", "is_supported": True},
            {"code": "mongodb", "name": "MongoDB", "category": "nosql", "is_supported": True},
            {"code": "cassandra", "name": "Cassandra", "category": "nosql", "is_supported": True},
            {"code": "dynamodb", "name": "DynamoDB", "category": "nosql", "is_supported": True},
            {"code": "snowflake", "name": "Snowflake", "category": "warehouse", "is_supported": True},
            {"code": "bigquery", "name": "BigQuery", "category": "warehouse", "is_supported": True},
            {"code": "redshift", "name": "Redshift", "category": "warehouse", "is_supported": True},
            {"code": "aws_s3", "name": "AWS S3", "category": "lake_storage", "is_supported": True},
            {"code": "azure_blob", "name": "Azure Blob Storage", "category": "lake_storage", "is_supported": True},
            {"code": "gcs", "name": "Google Cloud Storage", "category": "lake_storage", "is_supported": True},
            {"code": "power_bi", "name": "Power BI", "category": "bi_semantic", "is_supported": True},
            {"code": "microsoft_fabric", "name": "Microsoft Fabric", "category": "lakehouse_semantic", "is_supported": True},
        ]

    def list_tenant_data_sources(self, tenant_id: int) -> list[dict[str, Any]]:
        rows = self._tenant_data_sources.get(tenant_id, [])
        return sorted(rows, key=lambda item: item["created_at"], reverse=True)

    def create_tenant_data_source(
        self,
        tenant_id: int,
        *,
        source_type: str,
        conn_secret_ref: str,
        is_active: bool = True,
    ) -> dict[str, Any]:
        source = {
            "id": self._data_source_seq,
            "tenant_id": tenant_id,
            "source_type": source_type,
            "conn_secret_ref": conn_secret_ref,
            "is_active": is_active,
            "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        self._data_source_seq += 1
        self._tenant_data_sources.setdefault(tenant_id, []).insert(0, source)
        return source

    def update_tenant_data_source_status(self, tenant_id: int, data_source_id: int, is_active: bool) -> dict[str, Any]:
        rows = self._tenant_data_sources.get(tenant_id, [])
        for row in rows:
            if int(row["id"]) == int(data_source_id):
                row["is_active"] = bool(is_active)
                return row
        raise ValueError("fonte de dados nao encontrada")

    def update_tenant_data_source(
        self,
        tenant_id: int,
        data_source_id: int,
        *,
        source_type: str,
        conn_secret_ref: str,
    ) -> dict[str, Any]:
        rows = self._tenant_data_sources.get(tenant_id, [])
        for row in rows:
            if int(row["id"]) == int(data_source_id):
                row["source_type"] = source_type
                row["conn_secret_ref"] = conn_secret_ref
                return row
        raise ValueError("fonte de dados nao encontrada")

    def delete_tenant_data_source(self, tenant_id: int, data_source_id: int) -> dict[str, Any]:
        rows = self._tenant_data_sources.get(tenant_id, [])
        for idx, row in enumerate(rows):
            if int(row["id"]) == int(data_source_id):
                removed = rows.pop(idx)
                return {
                    "deleted": True,
                    "id": removed["id"],
                    "source_type": removed["source_type"],
                }
        raise ValueError("fonte de dados nao encontrada")

    def get_sql_security_policy(self, tenant_id: int) -> dict[str, Any]:
        policy = self._tool_policies.get((tenant_id, "query.execute_safe_sql"))
        if not policy:
            policy = ToolPolicy("query.execute_safe_sql", "admin", True, 200, 30, True, ["public", "analytics"])
        return {
            "tool_name": policy.tool_name,
            "is_enabled": policy.is_enabled,
            "max_rows": policy.max_rows,
            "max_calls_per_minute": policy.max_calls_per_minute,
            "require_masking": policy.require_masking,
            "allowed_schema_patterns": policy.allowed_schema_patterns or [],
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
        base = self._tool_policies.get((tenant_id, "query.execute_safe_sql"))
        min_role = base.min_role if base else "admin"
        self._tool_policies[(tenant_id, "query.execute_safe_sql")] = ToolPolicy(
            "query.execute_safe_sql",
            min_role,
            True,
            max_rows,
            max_calls_per_minute,
            require_masking,
            allowed_schema_patterns,
        )
        mirror = self._tool_policies[(tenant_id, "query.execute_safe_sql")]
        return {
            "tool_name": mirror.tool_name,
            "is_enabled": mirror.is_enabled,
            "max_rows": mirror.max_rows,
            "max_calls_per_minute": mirror.max_calls_per_minute,
            "require_masking": mirror.require_masking,
            "allowed_schema_patterns": mirror.allowed_schema_patterns or [],
        }

    def list_monitored_tables(self, tenant_id: int, schema_name: str | None = None) -> list[dict[str, Any]]:
        rows = self._tables.get(tenant_id, [])
        if schema_name:
            rows = [row for row in rows if row["schema_name"] == schema_name]
        return rows

    def list_tenant_monitored_tables(
        self,
        tenant_id: int,
        data_source_id: int | None = None,
    ) -> list[dict[str, Any]]:
        rows = self._tables.get(tenant_id, [])
        if data_source_id is not None:
            rows = [row for row in rows if int(row["data_source_id"]) == int(data_source_id)]
        source_map = {int(item["id"]): item for item in self._tenant_data_sources.get(tenant_id, [])}
        enriched: list[dict[str, Any]] = []
        for row in rows:
            source = source_map.get(int(row["data_source_id"]), {})
            enriched.append(
                {
                    "id": row["id"],
                    "tenant_id": row["tenant_id"],
                    "data_source_id": row["data_source_id"],
                    "source_type": source.get("source_type"),
                    "source_name": source.get("source_type"),
                    "schema_name": row["schema_name"],
                    "table_name": row["table_name"],
                    "is_active": row["is_active"],
                }
            )
        return sorted(enriched, key=lambda item: (item["schema_name"], item["table_name"]))

    def create_monitored_table(
        self,
        tenant_id: int,
        *,
        data_source_id: int,
        schema_name: str,
        table_name: str,
        is_active: bool = True,
    ) -> dict[str, Any]:
        source_exists = any(
            int(item["id"]) == int(data_source_id)
            for item in self._tenant_data_sources.get(tenant_id, [])
        )
        if not source_exists:
            raise ValueError("data_source_id nao encontrado para o tenant")
        rows = self._tables.setdefault(tenant_id, [])
        duplicated = any(
            int(item["data_source_id"]) == int(data_source_id)
            and item["schema_name"] == schema_name
            and item["table_name"] == table_name
            for item in rows
        )
        if duplicated:
            raise ValueError("tabela ja cadastrada para esta fonte")
        row = {
            "id": self._monitored_table_seq,
            "tenant_id": tenant_id,
            "data_source_id": int(data_source_id),
            "schema_name": schema_name,
            "table_name": table_name,
            "is_active": bool(is_active),
        }
        self._monitored_table_seq += 1
        rows.append(row)
        enriched = self.list_tenant_monitored_tables(tenant_id, int(data_source_id))
        for item in enriched:
            if int(item["id"]) == int(row["id"]):
                return item
        raise ValueError("falha ao recuperar tabela monitorada criada")

    def delete_monitored_table(self, tenant_id: int, monitored_table_id: int) -> dict[str, Any]:
        rows = self._tables.get(tenant_id, [])
        for idx, row in enumerate(rows):
            if int(row["id"]) == int(monitored_table_id):
                removed = rows.pop(idx)
                return {
                    "deleted": True,
                    "id": removed["id"],
                    "schema_name": removed["schema_name"],
                    "table_name": removed["table_name"],
                }
        raise ValueError("tabela monitorada nao encontrada")

    def list_monitored_columns(self, tenant_id: int, schema_name: str, table_name: str) -> list[dict[str, Any]]:
        return self._columns.get((tenant_id, schema_name, table_name), [])

    def execute_safe_sql(self, tenant_id: int, sql_text: str, max_rows: int | None) -> dict[str, Any]:
        _ = tenant_id, sql_text
        rows = [{"metric": "open_incidents", "value": 2}, {"metric": "critical_events", "value": 1}]
        if max_rows is not None:
            rows = rows[:max_rows]
        return {
            "rows": rows,
            "columns": ["metric", "value"],
            "execution_ms": 42,
            "applied_masks": ["customer_cpf"],
        }

    def create_incident(
        self,
        tenant_id: int,
        title: str,
        severity: str,
        source_event_id: int | None,
    ) -> dict[str, Any]:
        incident_id = self._incident_seq
        self._incident_seq += 1
        sla_due_at = dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=4)
        incident = {
            "incident_id": incident_id,
            "tenant_id": tenant_id,
            "title": title,
            "severity": severity,
            "source_event_id": source_event_id,
            "status": "open",
            "sla_due_at": sla_due_at.isoformat(),
            "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        self._incidents.insert(0, incident)
        return incident

    def list_incidents(
        self,
        tenant_id: int,
        status: str | None = None,
        severity: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        rows = [item for item in self._incidents if item["tenant_id"] == tenant_id]
        if status:
            rows = [item for item in rows if item["status"] == status]
        if severity:
            rows = [item for item in rows if item["severity"] == severity]
        return rows[:limit]

    def update_incident_status(
        self,
        tenant_id: int,
        incident_id: int,
        new_status: str,
    ) -> dict[str, Any]:
        allowed = {"open", "ack", "resolved", "closed"}
        transitions = {
            "open": {"ack", "resolved"},
            "ack": {"resolved", "closed"},
            "resolved": {"closed"},
            "closed": set(),
        }
        if new_status not in allowed:
            raise ValueError("status invalido")
        for item in self._incidents:
            if item["tenant_id"] == tenant_id and item["incident_id"] == incident_id:
                current_status = item.get("status", "open")
                if new_status == current_status:
                    return item
                if new_status not in transitions.get(current_status, set()):
                    raise ValueError("transicao de status invalida")
                item["status"] = new_status
                now = dt.datetime.now(dt.timezone.utc).isoformat()
                if new_status == "ack":
                    item["ack_at"] = now
                elif new_status == "resolved":
                    item["resolved_at"] = now
                elif new_status == "closed":
                    item["closed_at"] = now
                return item
        raise ValueError("incidente nao encontrado")

    def list_events(
        self,
        tenant_id: int,
        severity: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        rows = [item for item in self._events if item["tenant_id"] == tenant_id]
        if severity:
            rows = [item for item in rows if item["severity"] == severity]
        return rows[:limit]

    def list_audit_calls(
        self,
        tenant_id: int,
        tool_name: str | None = None,
        status: str | None = None,
        correlation_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        rows = [item for item in self._call_logs if item["tenant_id"] == tenant_id]
        if tool_name:
            rows = [item for item in rows if item["tool_name"] == tool_name]
        if status:
            rows = [item for item in rows if item["status"] == status]
        if correlation_id:
            rows = [item for item in rows if correlation_id in item.get("correlation_id", "")]
        rows = sorted(rows, key=lambda x: x.get("created_at", ""), reverse=True)
        return rows[:limit]

    def get_health_summary(self, tenant_id: int, window_minutes: int) -> dict[str, Any]:
        _ = tenant_id, window_minutes
        return {
            "open_incidents": 2,
            "critical_events": 1,
            "channels_health": {"telegram": "healthy", "whatsapp": "degraded"},
            "last_scan_at": dt.datetime.now(dt.timezone.utc).isoformat(),
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
        self._call_logs.append(
            {
                "client_id": client_id,
                "tenant_id": tenant_id,
                "user_id": user_id,
                "tool_name": tool_name,
                "status": status,
                "correlation_id": correlation_id,
                "request_payload": request_payload,
                "response_payload": response_payload,
                "error_code": error_code,
                "error_message": error_message,
                "latency_ms": latency_ms,
                "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            }
        )
