from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from typing import Any

from iaops.security.totp import generate_base32_secret, provisioning_uri, verify_totp

from .models import ToolPolicy

DEFAULT_LLM_MODELS: dict[str, list[dict[str, Any]]] = {
    "openai": [
        {"code": "gpt-5", "name": "GPT-5"},
        {"code": "gpt-5-mini", "name": "GPT-5 Mini"},
        {"code": "gpt-5-nano", "name": "GPT-5 Nano"},
        {"code": "gpt-4.1", "name": "GPT-4.1"},
        {"code": "gpt-4.1-mini", "name": "GPT-4.1 Mini"},
        {"code": "gpt-4.1-nano", "name": "GPT-4.1 Nano"},
        {"code": "o4-mini", "name": "o4-mini"},
    ],
    "azure_openai": [
        {"code": "gpt-5", "name": "GPT-5"},
        {"code": "gpt-5-mini", "name": "GPT-5 Mini"},
        {"code": "gpt-5-nano", "name": "GPT-5 Nano"},
        {"code": "gpt-4.1", "name": "GPT-4.1"},
        {"code": "gpt-4.1-mini", "name": "GPT-4.1 Mini"},
        {"code": "gpt-4.1-nano", "name": "GPT-4.1 Nano"},
    ],
    "anthropic": [
        {"code": "claude-3-7-sonnet-latest", "name": "Claude 3.7 Sonnet"},
        {"code": "claude-3-5-sonnet-latest", "name": "Claude 3.5 Sonnet"},
        {"code": "claude-3-5-haiku-latest", "name": "Claude 3.5 Haiku"},
        {"code": "claude-3-opus-latest", "name": "Claude 3 Opus"},
    ],
    "google_gemini": [
        {"code": "gemini-2.5-pro", "name": "Gemini 2.5 Pro"},
        {"code": "gemini-2.5-flash", "name": "Gemini 2.5 Flash"},
        {"code": "gemini-2.5-flash-lite", "name": "Gemini 2.5 Flash Lite"},
        {"code": "gemini-3.1-pro-preview", "name": "Gemini 3.1 Pro Preview"},
        {"code": "gemini-3.1-pro-preview-customtools", "name": "Gemini 3.1 Pro Preview (Custom Tools)"},
        {"code": "gemini-3-flash-preview", "name": "Gemini 3 Flash Preview"},
    ],
    "mistral": [
        {"code": "mistral-large-latest", "name": "Mistral Large Latest"},
        {"code": "mistral-small-latest", "name": "Mistral Small Latest"},
        {"code": "ministral-8b-latest", "name": "Ministral 8B Latest"},
        {"code": "open-mistral-nemo", "name": "Open Mistral Nemo"},
    ],
    "groq": [
        {"code": "llama-3.3-70b-versatile", "name": "Llama 3.3 70B Versatile"},
        {"code": "llama-3.1-8b-instant", "name": "Llama 3.1 8B Instant"},
        {"code": "mixtral-8x7b-32768", "name": "Mixtral 8x7B 32k"},
        {"code": "gemma2-9b-it", "name": "Gemma2 9B IT"},
    ],
    "ollama": [
        {"code": "llama3.2", "name": "Llama 3.2"},
        {"code": "llama3.1", "name": "Llama 3.1"},
        {"code": "qwen2.5", "name": "Qwen 2.5"},
        {"code": "mistral", "name": "Mistral"},
        {"code": "gemma3", "name": "Gemma 3"},
    ],
}


class MCPRepository(ABC):
    @abstractmethod
    def is_tenant_operational(self, client_id: int, tenant_id: int) -> bool:
        raise NotImplementedError

    @abstractmethod
    def get_user_role(self, tenant_id: int, user_id: int) -> str | None:
        raise NotImplementedError

    @abstractmethod
    def is_superadmin(self, user_id: int) -> bool:
        raise NotImplementedError

    @abstractmethod
    def list_client_tenants(self, client_id: int) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def get_client_tenant_limits(self, client_id: int) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def create_tenant(self, client_id: int, *, name: str, slug: str) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def update_tenant_status(self, client_id: int, tenant_id: int, status: str) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def list_tenant_users(self, tenant_id: int) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def get_user_mfa_status(self, tenant_id: int, user_id: int) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def begin_user_mfa_setup(self, tenant_id: int, user_id: int, issuer: str) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def enable_user_mfa(self, tenant_id: int, user_id: int, otp_code: str) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def disable_user_mfa(self, tenant_id: int, user_id: int, otp_code: str) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def admin_reset_user_mfa(self, tenant_id: int, target_user_id: int, reset_by_user_id: int) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def get_user_tenant_preference(self, tenant_id: int, user_id: int) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def upsert_user_tenant_preference(
        self,
        tenant_id: int,
        user_id: int,
        *,
        language_code: str | None = None,
        theme_code: str | None = None,
        chat_response_mode: str | None = None,
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def get_tenant_llm_config(self, client_id: int, tenant_id: int) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def list_supported_llm_providers(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def list_supported_llm_models(self, provider_name: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def get_app_default_llm_config(self) -> dict[str, Any] | None:
        raise NotImplementedError

    @abstractmethod
    def upsert_app_default_llm_config(
        self,
        *,
        provider_name: str,
        model_code: str,
        endpoint_url: str | None,
        secret_ref: str | None,
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def resolve_channel_user_tenants(
        self,
        client_id: int,
        *,
        channel_type: str,
        external_user_key: str,
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def set_channel_active_tenant(
        self,
        client_id: int,
        *,
        channel_type: str,
        conversation_key: str,
        external_user_key: str,
        tenant_id: int,
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def get_channel_active_tenant(
        self,
        client_id: int,
        *,
        channel_type: str,
        conversation_key: str,
        external_user_key: str,
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def track_app_llm_usage(
        self,
        *,
        tenant_id: int,
        feature_code: str,
        input_tokens: int,
        output_tokens: int,
    ) -> dict[str, Any] | None:
        raise NotImplementedError

    @abstractmethod
    def get_tool_policy(self, tenant_id: int, tool_name: str) -> ToolPolicy | None:
        raise NotImplementedError

    @abstractmethod
    def list_tenant_tool_policies(self, tenant_id: int) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def list_mcp_client_connections(self, tenant_id: int) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def update_mcp_client_connection_status(self, tenant_id: int, connection_id: int, is_active: bool) -> dict[str, Any]:
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
    def list_monitored_columns_by_table(
        self,
        tenant_id: int,
        monitored_table_id: int,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def delete_monitored_column(
        self,
        tenant_id: int,
        monitored_column_id: int,
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def execute_safe_sql(self, tenant_id: int, sql_text: str, max_rows: int | None) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def list_active_lgpd_rules(self, tenant_id: int) -> list[dict[str, Any]]:
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
    def get_setup_progress(self, tenant_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    @abstractmethod
    def upsert_setup_progress(
        self,
        *,
        client_id: int,
        tenant_id: int,
        user_id: int,
        correlation_id: str,
        snapshot: dict[str, Any],
    ) -> dict[str, Any]:
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
        self._client_tenants: dict[int, list[dict[str, Any]]] = {
            1: [
                {"id": 10, "client_id": 1, "name": "Tenant Demo", "slug": "tenant-demo", "status": "active"},
            ]
        }
        self._client_plan_limits = {1: {"max_tenants": 5}}
        self._tenant_seq = 11
        self._tool_policies = {
            (10, "tenant.list_client"): ToolPolicy("tenant.list_client", "viewer", True, 1000, 120, True, None),
            (10, "tenant.get_limits"): ToolPolicy("tenant.get_limits", "viewer", True, None, 120, True, None),
            (10, "tenant.create"): ToolPolicy("tenant.create", "owner", True, None, 60, True, None),
            (10, "tenant.update_status"): ToolPolicy("tenant.update_status", "owner", True, None, 120, True, None),
            (10, "inventory.list_tables"): ToolPolicy("inventory.list_tables", "viewer", True, 1000, 120, True, None),
            (10, "inventory.list_columns"): ToolPolicy("inventory.list_columns", "viewer", True, 1000, 120, True, None),
            (10, "access.list_users"): ToolPolicy("access.list_users", "admin", True, 1000, 120, True, None),
            (10, "security.mfa.get_status"): ToolPolicy("security.mfa.get_status", "viewer", True, None, 120, True, None),
            (10, "security.mfa.begin_setup"): ToolPolicy("security.mfa.begin_setup", "viewer", True, None, 30, True, None),
            (10, "security.mfa.enable"): ToolPolicy("security.mfa.enable", "viewer", True, None, 30, True, None),
            (10, "security.mfa.disable_self"): ToolPolicy("security.mfa.disable_self", "viewer", True, None, 30, True, None),
            (10, "security.mfa.admin_reset"): ToolPolicy("security.mfa.admin_reset", "admin", True, None, 60, True, None),
            (10, "pref.get_user_tenant"): ToolPolicy("pref.get_user_tenant", "viewer", True, None, 120, True, None),
            (10, "pref.update_user_tenant"): ToolPolicy("pref.update_user_tenant", "viewer", True, None, 60, True, None),
            (10, "tenant_llm.get_config"): ToolPolicy("tenant_llm.get_config", "viewer", True, None, 120, True, None),
            (10, "tenant_llm.update_config"): ToolPolicy("tenant_llm.update_config", "admin", True, None, 60, True, None),
            (10, "tenant_llm.list_providers"): ToolPolicy("tenant_llm.list_providers", "viewer", True, 200, 120, True, None),
            (10, "tenant_llm.list_models"): ToolPolicy("tenant_llm.list_models", "viewer", True, 500, 120, True, None),
            (10, "llm_admin.list_providers"): ToolPolicy("llm_admin.list_providers", "owner", True, 200, 120, True, None),
            (10, "llm_admin.list_models"): ToolPolicy("llm_admin.list_models", "owner", True, 500, 120, True, None),
            (10, "llm_admin.get_app_config"): ToolPolicy("llm_admin.get_app_config", "owner", True, None, 120, True, None),
            (10, "llm_admin.update_app_config"): ToolPolicy("llm_admin.update_app_config", "owner", True, None, 60, True, None),
            (10, "channel.list_user_tenants"): ToolPolicy("channel.list_user_tenants", "viewer", True, 100, 120, True, None),
            (10, "channel.set_active_tenant"): ToolPolicy("channel.set_active_tenant", "viewer", True, None, 120, True, None),
            (10, "channel.get_active_tenant"): ToolPolicy("channel.get_active_tenant", "viewer", True, None, 120, True, None),
            (10, "inventory.list_tenant_tables"): ToolPolicy("inventory.list_tenant_tables", "viewer", True, 1000, 120, True, None),
            (10, "inventory.register_table"): ToolPolicy("inventory.register_table", "admin", True, None, 120, True, None),
            (10, "inventory.delete_table"): ToolPolicy("inventory.delete_table", "admin", True, None, 120, True, None),
            (10, "inventory.list_table_columns"): ToolPolicy("inventory.list_table_columns", "viewer", True, 1000, 120, True, None),
            (10, "inventory.register_column"): ToolPolicy("inventory.register_column", "admin", True, None, 120, True, None),
            (10, "inventory.delete_column"): ToolPolicy("inventory.delete_column", "admin", True, None, 120, True, None),
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
            (
                10,
                "security_mcp.list_policies",
            ): ToolPolicy("security_mcp.list_policies", "viewer", True, 500, 120, True, ["public", "analytics"]),
            (
                10,
                "security_mcp.update_policy",
            ): ToolPolicy("security_mcp.update_policy", "admin", True, 500, 120, True, ["public", "analytics"]),
            (
                10,
                "mcp_client.list_connections",
            ): ToolPolicy("mcp_client.list_connections", "viewer", True, 200, 120, True, None),
            (
                10,
                "mcp_client.upsert_connection",
            ): ToolPolicy("mcp_client.upsert_connection", "admin", True, 200, 120, True, None),
            (
                10,
                "mcp_client.update_status",
            ): ToolPolicy("mcp_client.update_status", "admin", True, 200, 120, True, None),
            (10, "ops.get_health_summary"): ToolPolicy("ops.get_health_summary", "viewer", True, None, 120, True, None),
            (10, "setup.get_progress"): ToolPolicy("setup.get_progress", "admin", True, None, 120, True, None),
            (10, "setup.upsert_progress"): ToolPolicy("setup.upsert_progress", "admin", True, None, 120, True, None),
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
                {
                    "id": 7001,
                    "monitored_table_id": 501,
                    "column_name": "id",
                    "data_type": "bigint",
                    "classification": "identifier",
                    "description_text": "PK",
                    "source_description_text": None,
                    "llm_description_suggested": "Identificador unico do registro.",
                    "llm_classification_suggested": "identifier",
                    "llm_description_confirmed": True,
                },
                {
                    "id": 7002,
                    "monitored_table_id": 501,
                    "column_name": "customer_cpf",
                    "data_type": "text",
                    "classification": "sensitive",
                    "description_text": "Documento do titular",
                    "source_description_text": None,
                    "llm_description_suggested": "CPF do cliente titular do pedido.",
                    "llm_classification_suggested": "sensitive",
                    "llm_description_confirmed": False,
                },
                {
                    "id": 7003,
                    "monitored_table_id": 501,
                    "column_name": "total_amount",
                    "data_type": "numeric",
                    "classification": "financial",
                    "description_text": "Valor",
                    "source_description_text": None,
                    "llm_description_suggested": "Valor total financeiro do pedido.",
                    "llm_classification_suggested": "financial",
                    "llm_description_confirmed": False,
                },
            ]
        }
        self._columns_by_table: dict[tuple[int, int], list[dict[str, Any]]] = {
            (10, 501): [*self._columns[(10, "public", "orders")]],
        }
        self._mcp_connections: dict[int, list[dict[str, Any]]] = {
            10: [
                {
                    "id": 1,
                    "tenant_id": 10,
                    "connection_name": "mcp-itsm",
                    "transport_type": "http",
                    "endpoint_url": "https://mcp.example.local/itsm",
                    "auth_secret_ref": "secret://tenant-10/mcp/itsm",
                    "is_active": True,
                    "health_status": "unknown",
                    "last_healthcheck_at": None,
                }
            ]
        }
        self._mcp_connection_seq = 2
        self._monitored_column_seq = 7004
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
        self._setup_progress: dict[int, dict[str, Any]] = {}
        self._users = {
            100: {"id": 100, "email": "owner@iaops.demo", "full_name": "Owner Demo", "is_active": True, "is_superadmin": True},
            101: {"id": 101, "email": "admin@iaops.demo", "full_name": "Admin Demo", "is_active": True},
            102: {"id": 102, "email": "viewer@iaops.demo", "full_name": "Viewer Demo", "is_active": True},
        }
        self._mfa_config: dict[int, dict[str, Any]] = {}
        self._mfa_pending: dict[int, dict[str, Any]] = {}
        self._user_tenant_preferences: dict[tuple[int, int], dict[str, Any]] = {
            (10, 100): {"language_code": "pt-BR", "theme_code": "light", "chat_response_mode": "executive"},
            (10, 101): {"language_code": "pt-BR", "theme_code": "light", "chat_response_mode": "detailed"},
            (10, 102): {"language_code": "pt-BR", "theme_code": "light", "chat_response_mode": "executive"},
        }
        self._supported_llm_providers = [
            {"code": "openai", "name": "OpenAI"},
            {"code": "azure_openai", "name": "Azure OpenAI"},
            {"code": "anthropic", "name": "Anthropic"},
            {"code": "google_gemini", "name": "Google Gemini"},
            {"code": "mistral", "name": "Mistral"},
            {"code": "groq", "name": "Groq"},
            {"code": "ollama", "name": "Ollama (Local)"},
        ]
        self._app_llm_config: dict[str, Any] | None = {
            "provider_name": "openai",
            "model_code": "gpt-4.1-mini",
            "endpoint_url": "https://api.openai.com/v1",
            "secret_ref": "secret://app/llm/openai",
            "is_global_default": True,
        }
        self._tenant_llm_cfg: dict[int, dict[str, Any]] = {
            10: {
                "use_app_default_llm": False,
                "billing_mode": "tenant_provider",
                "provider_name": "openai",
                "model_code": "gpt-4.1-mini",
                "endpoint_url": "https://api.openai.com/v1",
                "secret_ref": "secret://tenant-10/llm/openai",
            }
        }
        self._channel_bindings = {
            ("telegram", "tg-owner-demo"): {"client_id": 1, "user_id": 100},
            ("whatsapp", "wa-owner-demo"): {"client_id": 1, "user_id": 100},
        }
        self._channel_context: dict[tuple[str, str], dict[str, Any]] = {}

    def is_tenant_operational(self, client_id: int, tenant_id: int) -> bool:
        return self._tenant_state.get((client_id, tenant_id), False)

    def get_user_role(self, tenant_id: int, user_id: int) -> str | None:
        return self._roles.get((tenant_id, user_id))

    def is_superadmin(self, user_id: int) -> bool:
        user = self._users.get(user_id) or {}
        return bool(user.get("is_superadmin", False))

    def list_client_tenants(self, client_id: int) -> list[dict[str, Any]]:
        rows = self._client_tenants.get(client_id, [])
        return sorted(rows, key=lambda item: item["id"])

    def get_client_tenant_limits(self, client_id: int) -> dict[str, Any]:
        rows = self._client_tenants.get(client_id, [])
        active_count = sum(1 for item in rows if item["status"] == "active")
        max_tenants = int(self._client_plan_limits.get(client_id, {}).get("max_tenants", 1))
        return {
            "active_tenants": active_count,
            "max_tenants": max_tenants,
            "can_create": active_count < max_tenants,
        }

    def create_tenant(self, client_id: int, *, name: str, slug: str) -> dict[str, Any]:
        limits = self.get_client_tenant_limits(client_id)
        if not limits["can_create"]:
            raise ValueError("limite de tenants ativos do plano foi atingido")
        rows = self._client_tenants.setdefault(client_id, [])
        if any(item["slug"] == slug for item in rows):
            raise ValueError("slug ja utilizado para este cliente")
        tenant = {
            "id": self._tenant_seq,
            "client_id": client_id,
            "name": name,
            "slug": slug,
            "status": "active",
        }
        self._tenant_seq += 1
        rows.append(tenant)
        return tenant

    def update_tenant_status(self, client_id: int, tenant_id: int, status: str) -> dict[str, Any]:
        if status not in {"active", "disabled"}:
            raise ValueError("status invalido")
        rows = self._client_tenants.get(client_id, [])
        for item in rows:
            if int(item["id"]) == int(tenant_id):
                if status == "active":
                    limits = self.get_client_tenant_limits(client_id)
                    currently_active = item["status"] == "active"
                    if not currently_active and not limits["can_create"]:
                        raise ValueError("limite de tenants ativos do plano foi atingido")
                item["status"] = status
                self._tenant_state[(client_id, int(tenant_id))] = status == "active"
                return item
        raise ValueError("tenant nao encontrado")

    def list_tenant_users(self, tenant_id: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for (row_tenant_id, user_id), role in self._roles.items():
            if row_tenant_id != tenant_id:
                continue
            user = self._users.get(user_id)
            if not user:
                continue
            mfa = self._mfa_config.get(user_id) or {}
            rows.append(
                {
                    "user_id": user["id"],
                    "email": user["email"],
                    "full_name": user["full_name"],
                    "role": role,
                    "is_active": user["is_active"],
                    "mfa_enabled": bool(mfa.get("is_enabled", False)),
                }
            )
        return sorted(rows, key=lambda item: (item["role"], item["email"]))

    def get_user_mfa_status(self, tenant_id: int, user_id: int) -> dict[str, Any]:
        if self.get_user_role(tenant_id, user_id) is None:
            raise ValueError("usuario fora do escopo do tenant")
        cfg = self._mfa_config.get(user_id) or {}
        pending = self._mfa_pending.get(user_id)
        return {
            "enabled": bool(cfg.get("is_enabled", False)),
            "enabled_at": cfg.get("enabled_at"),
            "has_pending_setup": pending is not None,
            "pending_expires_at": pending.get("expires_at") if pending else None,
        }

    def begin_user_mfa_setup(self, tenant_id: int, user_id: int, issuer: str) -> dict[str, Any]:
        role = self.get_user_role(tenant_id, user_id)
        if role is None:
            raise ValueError("usuario fora do escopo do tenant")
        user = self._users.get(user_id)
        if not user:
            raise ValueError("usuario nao encontrado")
        secret = generate_base32_secret()
        expires_at = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=10)).isoformat()
        self._mfa_pending[user_id] = {"secret": secret, "expires_at": expires_at}
        return {
            "secret": secret,
            "provisioning_uri": provisioning_uri(issuer=issuer, account_name=user["email"], secret=secret),
            "expires_at": expires_at,
        }

    def enable_user_mfa(self, tenant_id: int, user_id: int, otp_code: str) -> dict[str, Any]:
        if self.get_user_role(tenant_id, user_id) is None:
            raise ValueError("usuario fora do escopo do tenant")
        pending = self._mfa_pending.get(user_id)
        if not pending:
            raise ValueError("setup MFA nao iniciado")
        expires_at = dt.datetime.fromisoformat(str(pending["expires_at"]))
        now = dt.datetime.now(dt.timezone.utc)
        if expires_at < now:
            self._mfa_pending.pop(user_id, None)
            raise ValueError("setup MFA expirado")
        if not verify_totp(str(pending["secret"]), otp_code):
            raise ValueError("codigo TOTP invalido")
        enabled_at = now.isoformat()
        self._mfa_config[user_id] = {
            "is_enabled": True,
            "secret": pending["secret"],
            "enabled_at": enabled_at,
        }
        self._mfa_pending.pop(user_id, None)
        return {
            "enabled": True,
            "enabled_at": enabled_at,
        }

    def disable_user_mfa(self, tenant_id: int, user_id: int, otp_code: str) -> dict[str, Any]:
        if self.get_user_role(tenant_id, user_id) is None:
            raise ValueError("usuario fora do escopo do tenant")
        config = self._mfa_config.get(user_id)
        if not config or not config.get("is_enabled"):
            raise ValueError("MFA nao esta habilitado")
        if not verify_totp(str(config.get("secret", "")), otp_code):
            raise ValueError("codigo TOTP invalido")
        disabled_at = dt.datetime.now(dt.timezone.utc).isoformat()
        self._mfa_config[user_id] = {
            "is_enabled": False,
            "secret": None,
            "enabled_at": None,
            "disabled_at": disabled_at,
        }
        self._mfa_pending.pop(user_id, None)
        return {"enabled": False, "disabled_at": disabled_at}

    def admin_reset_user_mfa(self, tenant_id: int, target_user_id: int, reset_by_user_id: int) -> dict[str, Any]:
        if self.get_user_role(tenant_id, target_user_id) is None:
            raise ValueError("usuario alvo fora do escopo do tenant")
        if self.get_user_role(tenant_id, reset_by_user_id) is None:
            raise ValueError("usuario executor fora do escopo do tenant")
        reset_at = dt.datetime.now(dt.timezone.utc).isoformat()
        self._mfa_config[target_user_id] = {
            "is_enabled": False,
            "secret": None,
            "enabled_at": None,
            "disabled_at": reset_at,
            "reset_by_user_id": reset_by_user_id,
        }
        self._mfa_pending.pop(target_user_id, None)
        return {"target_user_id": target_user_id, "enabled": False, "reset_at": reset_at}

    def get_user_tenant_preference(self, tenant_id: int, user_id: int) -> dict[str, Any]:
        if self.get_user_role(tenant_id, user_id) is None:
            raise ValueError("usuario fora do escopo do tenant")
        pref = self._user_tenant_preferences.get((tenant_id, user_id)) or {}
        return {
            "tenant_id": tenant_id,
            "user_id": user_id,
            "language_code": pref.get("language_code", "pt-BR"),
            "theme_code": pref.get("theme_code", "light"),
            "chat_response_mode": pref.get("chat_response_mode", "executive"),
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
        current = self._user_tenant_preferences.get((tenant_id, user_id)) or {
            "language_code": "pt-BR",
            "theme_code": "light",
            "chat_response_mode": "executive",
        }
        if language_code is not None:
            current["language_code"] = language_code
        if theme_code is not None:
            current["theme_code"] = theme_code
        if chat_response_mode is not None:
            mode = str(chat_response_mode).strip().lower()
            if mode not in {"executive", "detailed"}:
                raise ValueError("chat_response_mode invalido")
            current["chat_response_mode"] = mode
        self._user_tenant_preferences[(tenant_id, user_id)] = current
        return {
            "tenant_id": tenant_id,
            "user_id": user_id,
            "language_code": current["language_code"],
            "theme_code": current["theme_code"],
            "chat_response_mode": current["chat_response_mode"],
        }

    def list_supported_llm_providers(self) -> list[dict[str, Any]]:
        return [*self._supported_llm_providers]

    def list_supported_llm_models(self, provider_name: str) -> list[dict[str, Any]]:
        key = str(provider_name or "").strip().lower()
        rows = DEFAULT_LLM_MODELS.get(key, [])
        return [dict(item) for item in rows]

    def get_app_default_llm_config(self) -> dict[str, Any] | None:
        return dict(self._app_llm_config) if self._app_llm_config else None

    def upsert_app_default_llm_config(
        self,
        *,
        provider_name: str,
        model_code: str,
        endpoint_url: str | None,
        secret_ref: str | None,
    ) -> dict[str, Any]:
        self._app_llm_config = {
            "provider_name": provider_name,
            "model_code": model_code,
            "endpoint_url": endpoint_url,
            "secret_ref": secret_ref,
            "is_global_default": True,
        }
        return dict(self._app_llm_config)

    def get_tenant_llm_config(self, client_id: int, tenant_id: int) -> dict[str, Any]:
        cfg = self._tenant_llm_cfg.get(tenant_id)
        if cfg:
            return dict(cfg)
        return {
            "use_app_default_llm": True,
            "billing_mode": "app_default_token",
            "provider_name": self._app_llm_config.get("provider_name") if self._app_llm_config else None,
            "model_code": self._app_llm_config.get("model_code") if self._app_llm_config else None,
            "endpoint_url": self._app_llm_config.get("endpoint_url") if self._app_llm_config else None,
            "secret_ref": self._app_llm_config.get("secret_ref") if self._app_llm_config else None,
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
        _ = client_id
        if use_app_default_llm:
            self._tenant_llm_cfg[tenant_id] = {
                "use_app_default_llm": True,
                "billing_mode": "app_default_token",
                "provider_name": self._app_llm_config.get("provider_name") if self._app_llm_config else None,
                "model_code": self._app_llm_config.get("model_code") if self._app_llm_config else None,
                "endpoint_url": self._app_llm_config.get("endpoint_url") if self._app_llm_config else None,
                "secret_ref": self._app_llm_config.get("secret_ref") if self._app_llm_config else None,
            }
            return dict(self._tenant_llm_cfg[tenant_id])
        if not provider_name or not model_code:
            raise ValueError("provider_name e model_code sao obrigatorios")
        self._tenant_llm_cfg[tenant_id] = {
            "use_app_default_llm": False,
            "billing_mode": "tenant_provider",
            "provider_name": provider_name,
            "model_code": model_code,
            "endpoint_url": endpoint_url,
            "secret_ref": secret_ref,
        }
        return dict(self._tenant_llm_cfg[tenant_id])

    def resolve_channel_user_tenants(
        self,
        client_id: int,
        *,
        channel_type: str,
        external_user_key: str,
    ) -> dict[str, Any]:
        binding = self._channel_bindings.get((channel_type, external_user_key))
        if not binding or int(binding["client_id"]) != int(client_id):
            raise ValueError("identidade do canal nao vinculada a usuario")
        user_id = int(binding["user_id"])
        user = self._users.get(user_id)
        if not user:
            raise ValueError("usuario nao encontrado")
        tenants = []
        for item in self._client_tenants.get(client_id, []):
            role = self._roles.get((int(item["id"]), user_id))
            if role:
                tenants.append(
                    {
                        "tenant_id": item["id"],
                        "name": item["name"],
                        "slug": item["slug"],
                        "status": item["status"],
                        "role": role,
                    }
                )
        if not tenants:
            raise ValueError("usuario sem tenants vinculados")
        return {
            "user": {"user_id": user["id"], "email": user["email"], "full_name": user["full_name"]},
            "tenants": tenants,
        }

    def set_channel_active_tenant(
        self,
        client_id: int,
        *,
        channel_type: str,
        conversation_key: str,
        external_user_key: str,
        tenant_id: int,
    ) -> dict[str, Any]:
        resolved = self.resolve_channel_user_tenants(
            client_id,
            channel_type=channel_type,
            external_user_key=external_user_key,
        )
        allowed = [int(item["tenant_id"]) for item in resolved["tenants"]]
        if int(tenant_id) not in allowed:
            raise ValueError("tenant nao permitido para este usuario/canal")
        self._channel_context[(channel_type, conversation_key)] = {
            "client_id": client_id,
            "user_id": resolved["user"]["user_id"],
            "active_tenant_id": int(tenant_id),
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        return {
            "active_tenant_id": int(tenant_id),
            "conversation_key": conversation_key,
        }

    def get_channel_active_tenant(
        self,
        client_id: int,
        *,
        channel_type: str,
        conversation_key: str,
        external_user_key: str,
    ) -> dict[str, Any]:
        resolved = self.resolve_channel_user_tenants(
            client_id,
            channel_type=channel_type,
            external_user_key=external_user_key,
        )
        context = self._channel_context.get((channel_type, conversation_key))
        return {
            "active_tenant_id": context.get("active_tenant_id") if context else None,
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
        cfg = self._tenant_llm_cfg.get(tenant_id)
        if not cfg or not cfg.get("use_app_default_llm"):
            return None
        return {
            "tenant_id": tenant_id,
            "feature_code": feature_code,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "billing_mode": "app_default_token",
        }

    def get_tool_policy(self, tenant_id: int, tool_name: str) -> ToolPolicy | None:
        return self._tool_policies.get((tenant_id, tool_name))

    def list_tenant_tool_policies(self, tenant_id: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for (item_tenant_id, tool_name), policy in self._tool_policies.items():
            if int(item_tenant_id) != int(tenant_id):
                continue
            rows.append(
                {
                    "tool_name": tool_name,
                    "min_role": policy.min_role,
                    "is_enabled": policy.is_enabled,
                    "max_rows": policy.max_rows,
                    "max_calls_per_minute": policy.max_calls_per_minute,
                    "require_masking": policy.require_masking,
                    "allowed_schema_patterns": policy.allowed_schema_patterns or [],
                }
            )
        return sorted(rows, key=lambda item: item["tool_name"])

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
        base = self._tool_policies.get((tenant_id, tool_name))
        min_role = base.min_role if base else "admin"
        self._tool_policies[(tenant_id, tool_name)] = ToolPolicy(
            tool_name=tool_name,
            min_role=min_role,
            is_enabled=bool(is_enabled),
            max_rows=max_rows,
            max_calls_per_minute=max_calls_per_minute,
            require_masking=bool(require_masking),
            allowed_schema_patterns=list(allowed_schema_patterns or []),
        )
        policy = self._tool_policies[(tenant_id, tool_name)]
        return {
            "tool_name": policy.tool_name,
            "min_role": policy.min_role,
            "is_enabled": policy.is_enabled,
            "max_rows": policy.max_rows,
            "max_calls_per_minute": policy.max_calls_per_minute,
            "require_masking": policy.require_masking,
            "allowed_schema_patterns": policy.allowed_schema_patterns or [],
        }

    def list_mcp_client_connections(self, tenant_id: int) -> list[dict[str, Any]]:
        rows = self._mcp_connections.get(tenant_id, [])
        return sorted(rows, key=lambda item: item["connection_name"])

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
        rows = self._mcp_connections.setdefault(tenant_id, [])
        for row in rows:
            if row["connection_name"] == connection_name:
                row["transport_type"] = transport_type
                row["endpoint_url"] = endpoint_url
                row["auth_secret_ref"] = auth_secret_ref
                row["is_active"] = bool(is_active)
                return dict(row)
        row = {
            "id": self._mcp_connection_seq,
            "tenant_id": tenant_id,
            "connection_name": connection_name,
            "transport_type": transport_type,
            "endpoint_url": endpoint_url,
            "auth_secret_ref": auth_secret_ref,
            "is_active": bool(is_active),
            "health_status": "unknown",
            "last_healthcheck_at": None,
        }
        self._mcp_connection_seq += 1
        rows.append(row)
        return dict(row)

    def update_mcp_client_connection_status(self, tenant_id: int, connection_id: int, is_active: bool) -> dict[str, Any]:
        rows = self._mcp_connections.get(tenant_id, [])
        for row in rows:
            if int(row["id"]) == int(connection_id):
                row["is_active"] = bool(is_active)
                return dict(row)
        raise ValueError("conexao MCP nao encontrada")

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

    def list_monitored_columns_by_table(
        self,
        tenant_id: int,
        monitored_table_id: int,
    ) -> list[dict[str, Any]]:
        rows = self._columns_by_table.get((tenant_id, monitored_table_id), [])
        return sorted(rows, key=lambda item: item["column_name"])

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
        tables = self._tables.get(tenant_id, [])
        table_exists = any(int(item["id"]) == int(monitored_table_id) for item in tables)
        if not table_exists:
            raise ValueError("monitored_table_id nao encontrado para o tenant")
        rows = self._columns_by_table.setdefault((tenant_id, monitored_table_id), [])
        duplicated = any(item["column_name"] == column_name for item in rows)
        if duplicated:
            raise ValueError("coluna ja cadastrada para a tabela")
        row = {
            "id": self._monitored_column_seq,
            "monitored_table_id": int(monitored_table_id),
            "column_name": column_name,
            "data_type": data_type,
            "classification": classification,
            "description_text": description_text,
            "source_description_text": None,
            "llm_description_suggested": None,
            "llm_classification_suggested": None,
            "llm_description_confirmed": bool(description_text),
        }
        self._monitored_column_seq += 1
        rows.append(row)
        return row

    def delete_monitored_column(
        self,
        tenant_id: int,
        monitored_column_id: int,
    ) -> dict[str, Any]:
        for key, rows in self._columns_by_table.items():
            key_tenant_id, monitored_table_id = key
            if int(key_tenant_id) != int(tenant_id):
                continue
            for idx, row in enumerate(rows):
                if int(row["id"]) == int(monitored_column_id):
                    removed = rows.pop(idx)
                    return {
                        "deleted": True,
                        "id": removed["id"],
                        "monitored_table_id": monitored_table_id,
                        "column_name": removed["column_name"],
                    }
        raise ValueError("coluna monitorada nao encontrada")

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

    def list_active_lgpd_rules(self, tenant_id: int) -> list[dict[str, Any]]:
        _ = tenant_id
        return []

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

    def get_setup_progress(self, tenant_id: int) -> dict[str, Any] | None:
        data = self._setup_progress.get(tenant_id)
        return dict(data) if data else None

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
        payload = {
            "tenant_id": tenant_id,
            "updated_by_user_id": user_id,
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "snapshot": snapshot or {},
        }
        self._setup_progress[tenant_id] = payload
        return dict(payload)

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
