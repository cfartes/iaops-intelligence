from __future__ import annotations

import hashlib
import re
import time
from typing import Any, Callable

from .models import ROLE_ORDER, RequestContext, ToolExecutionResult
from .repository import MCPRepository


class LgpdBlockedError(ValueError):
    def __init__(self, blocked_fields: list[str]) -> None:
        self.blocked_fields = blocked_fields
        joined = ", ".join(sorted(set(blocked_fields))[:10])
        super().__init__(f"consulta bloqueada por regra LGPD nas colunas: {joined}")


class MCPGateway:
    _CLIENT_SCOPE_TOOLS = {
        "tenant.list_client",
        "tenant.get_limits",
        "tenant.create",
        "tenant.update_status",
        "channel.list_user_tenants",
        "channel.set_active_tenant",
        "channel.get_active_tenant",
        "llm_admin.list_providers",
        "llm_admin.get_app_config",
        "llm_admin.update_app_config",
    }
    _SUPERADMIN_TOOLS = {
        "llm_admin.list_providers",
        "llm_admin.get_app_config",
        "llm_admin.update_app_config",
    }
    _CHANNEL_TOOLS = {
        "channel.list_user_tenants",
        "channel.set_active_tenant",
        "channel.get_active_tenant",
    }

    def __init__(self, repository: MCPRepository) -> None:
        self.repository = repository
        self._handlers: dict[str, Callable[[RequestContext, dict[str, Any], int | None], dict[str, Any]]] = {
            "tenant.list_client": self._handle_tenant_list_client,
            "tenant.get_limits": self._handle_tenant_get_limits,
            "tenant.create": self._handle_tenant_create,
            "tenant.update_status": self._handle_tenant_update_status,
            "channel.list_user_tenants": self._handle_channel_list_user_tenants,
            "channel.set_active_tenant": self._handle_channel_set_active_tenant,
            "channel.get_active_tenant": self._handle_channel_get_active_tenant,
            "tenant_llm.get_config": self._handle_tenant_llm_get_config,
            "tenant_llm.update_config": self._handle_tenant_llm_update_config,
            "tenant_llm.list_providers": self._handle_tenant_llm_list_providers,
            "llm_admin.list_providers": self._handle_llm_admin_list_providers,
            "llm_admin.get_app_config": self._handle_llm_admin_get_app_config,
            "llm_admin.update_app_config": self._handle_llm_admin_update_app_config,
            "access.list_users": self._handle_access_list_users,
            "security.mfa.get_status": self._handle_mfa_get_status,
            "security.mfa.begin_setup": self._handle_mfa_begin_setup,
            "security.mfa.enable": self._handle_mfa_enable,
            "security.mfa.disable_self": self._handle_mfa_disable_self,
            "security.mfa.admin_reset": self._handle_mfa_admin_reset,
            "pref.get_user_tenant": self._handle_pref_get_user_tenant,
            "pref.update_user_tenant": self._handle_pref_update_user_tenant,
            "source.list_catalog": self._handle_source_list_catalog,
            "source.list_tenant": self._handle_source_list_tenant,
            "source.register": self._handle_source_register,
            "source.update_status": self._handle_source_update_status,
            "source.update": self._handle_source_update,
            "source.delete": self._handle_source_delete,
            "inventory.list_tables": self._handle_list_tables,
            "inventory.list_columns": self._handle_list_columns,
            "inventory.list_tenant_tables": self._handle_list_tenant_tables,
            "inventory.register_table": self._handle_register_table,
            "inventory.delete_table": self._handle_delete_table,
            "inventory.list_table_columns": self._handle_list_table_columns,
            "inventory.register_column": self._handle_register_column,
            "inventory.delete_column": self._handle_delete_column,
            "query.execute_safe_sql": self._handle_execute_safe_sql,
            "security_sql.get_policy": self._handle_security_sql_get_policy,
            "security_sql.update_policy": self._handle_security_sql_update_policy,
            "incident.create": self._handle_incident_create,
            "incident.list": self._handle_incident_list,
            "incident.update_status": self._handle_incident_update_status,
            "events.list": self._handle_events_list,
            "audit.list_calls": self._handle_audit_list_calls,
            "ops.get_health_summary": self._handle_health_summary,
            "setup.get_progress": self._handle_setup_get_progress,
            "setup.upsert_progress": self._handle_setup_upsert_progress,
        }

    def handle(self, payload: dict[str, Any]) -> dict[str, Any]:
        start = time.perf_counter()
        try:
            context = self._parse_context(payload)
        except ValueError as exc:
            return {
                "status": "denied",
                "tool": payload.get("tool", "unknown"),
                "correlation_id": None,
                "data": {},
                "error": {
                    "code": "invalid_context",
                    "message": str(exc),
                },
            }
        tool_name = payload.get("tool")
        tool_input = payload.get("input", {})

        if not isinstance(tool_name, str) or tool_name not in self._handlers:
            result = ToolExecutionResult("error", {}, "tool_not_found", "Tool nao registrada")
            return self._finalize_log(context, tool_name or "unknown", payload, result, start)

        tenant_ok = self.repository.is_tenant_operational(context.client_id, context.tenant_id)
        if not tenant_ok and tool_name not in self._CLIENT_SCOPE_TOOLS:
            result = ToolExecutionResult("denied", {}, "tenant_blocked", "Tenant inativo, bloqueado ou inadimplente")
            return self._finalize_log(context, tool_name, payload, result, start)

        max_rows: int | None = None
        if tool_name not in self._CHANNEL_TOOLS:
            policy = self.repository.get_tool_policy(context.tenant_id, tool_name)
            if policy is None or not policy.is_enabled:
                result = ToolExecutionResult("denied", {}, "tool_disabled", "Tool nao habilitada para o tenant")
                return self._finalize_log(context, tool_name, payload, result, start)
            max_rows = policy.max_rows

            role = self.repository.get_user_role(context.tenant_id, context.user_id)
            if role is None:
                result = ToolExecutionResult("denied", {}, "user_not_scoped", "Usuario sem escopo no tenant")
                return self._finalize_log(context, tool_name, payload, result, start)

            if ROLE_ORDER[role] < ROLE_ORDER[policy.min_role]:
                result = ToolExecutionResult("denied", {}, "insufficient_role", "Permissao insuficiente para a tool")
                return self._finalize_log(context, tool_name, payload, result, start)

        if tool_name in self._SUPERADMIN_TOOLS and not self.repository.is_superadmin(context.user_id):
            result = ToolExecutionResult("denied", {}, "superadmin_required", "Acesso restrito a superadmin")
            return self._finalize_log(context, tool_name, payload, result, start)

        try:
            data = self._handlers[tool_name](context, tool_input, max_rows)
            result = ToolExecutionResult("success", data)
        except LgpdBlockedError as exc:
            result = ToolExecutionResult(
                "denied",
                {"blocked_fields": sorted(set(exc.blocked_fields))},
                "lgpd_blocked",
                str(exc),
            )
        except ValueError as exc:
            result = ToolExecutionResult("denied", {}, "invalid_input", str(exc))
        except Exception as exc:  # pragma: no cover - scaffold fallback
            result = ToolExecutionResult("error", {}, "internal_error", f"Falha ao executar tool: {exc}")

        return self._finalize_log(context, tool_name, payload, result, start)

    def _parse_context(self, payload: dict[str, Any]) -> RequestContext:
        context = payload.get("context") or {}
        missing = [key for key in ["client_id", "tenant_id", "user_id", "correlation_id"] if key not in context]
        if missing:
            raise ValueError(f"Contexto incompleto: faltando {', '.join(missing)}")
        return RequestContext(
            client_id=int(context["client_id"]),
            tenant_id=int(context["tenant_id"]),
            user_id=int(context["user_id"]),
            correlation_id=str(context["correlation_id"]),
        )

    def _handle_list_tables(self, context: RequestContext, tool_input: dict[str, Any], max_rows: int | None) -> dict[str, Any]:
        schema_name = tool_input.get("schema_name")
        rows = self.repository.list_monitored_tables(context.tenant_id, schema_name)
        if max_rows is not None:
            rows = rows[:max_rows]
        return {"tables": rows}

    def _handle_source_list_catalog(
        self, context: RequestContext, tool_input: dict[str, Any], max_rows: int | None
    ) -> dict[str, Any]:
        _ = context, tool_input
        rows = self.repository.list_source_catalog()
        if max_rows is not None:
            rows = rows[:max_rows]
        return {"sources": rows}

    def _handle_tenant_list_client(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        _ = tool_input
        rows = self.repository.list_client_tenants(context.client_id)
        if max_rows is not None:
            rows = rows[:max_rows]
        return {"tenants": rows}

    def _handle_tenant_get_limits(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        _ = tool_input, max_rows
        return {"limits": self.repository.get_client_tenant_limits(context.client_id)}

    def _handle_tenant_create(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        _ = max_rows
        name = str(tool_input.get("name", "")).strip()
        slug = str(tool_input.get("slug", "")).strip().lower()
        if not name:
            raise ValueError("name obrigatorio")
        if not slug:
            raise ValueError("slug obrigatorio")
        tenant = self.repository.create_tenant(context.client_id, name=name, slug=slug)
        return {"tenant": tenant}

    def _handle_tenant_update_status(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        _ = max_rows
        if tool_input.get("tenant_id") is None:
            raise ValueError("tenant_id obrigatorio")
        status = str(tool_input.get("status", "")).strip().lower()
        if status not in {"active", "disabled"}:
            raise ValueError("status invalido")
        tenant = self.repository.update_tenant_status(context.client_id, int(tool_input["tenant_id"]), status)
        return {"tenant": tenant}

    def _handle_llm_admin_list_providers(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        _ = context, tool_input
        rows = self.repository.list_supported_llm_providers()
        if max_rows is not None:
            rows = rows[:max_rows]
        return {"providers": rows}

    def _handle_llm_admin_get_app_config(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        _ = context, tool_input, max_rows
        return {"config": self.repository.get_app_default_llm_config()}

    def _handle_llm_admin_update_app_config(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        _ = context, max_rows
        provider_name = str(tool_input.get("provider_name", "")).strip().lower()
        model_code = str(tool_input.get("model_code", "")).strip()
        endpoint_url = str(tool_input.get("endpoint_url", "")).strip() or None
        secret_ref = str(tool_input.get("secret_ref", "")).strip() or None
        if not provider_name:
            raise ValueError("provider_name obrigatorio")
        if not model_code:
            raise ValueError("model_code obrigatorio")
        config = self.repository.upsert_app_default_llm_config(
            provider_name=provider_name,
            model_code=model_code,
            endpoint_url=endpoint_url,
            secret_ref=secret_ref,
        )
        return {"config": config}

    def _handle_tenant_llm_list_providers(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        _ = context, tool_input
        rows = self.repository.list_supported_llm_providers()
        if max_rows is not None:
            rows = rows[:max_rows]
        return {"providers": rows}

    def _handle_tenant_llm_get_config(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        _ = tool_input, max_rows
        return {"config": self.repository.get_tenant_llm_config(context.client_id, context.tenant_id)}

    def _handle_tenant_llm_update_config(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        _ = max_rows
        use_app_default_llm = bool(tool_input.get("use_app_default_llm", False))
        provider_name = str(tool_input.get("provider_name", "")).strip().lower() or None
        model_code = str(tool_input.get("model_code", "")).strip() or None
        endpoint_url = str(tool_input.get("endpoint_url", "")).strip() or None
        secret_ref = str(tool_input.get("secret_ref", "")).strip() or None
        cfg = self.repository.update_tenant_llm_config(
            context.client_id,
            context.tenant_id,
            use_app_default_llm=use_app_default_llm,
            provider_name=provider_name,
            model_code=model_code,
            endpoint_url=endpoint_url,
            secret_ref=secret_ref,
        )
        return {"config": cfg}

    def _handle_channel_list_user_tenants(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        _ = max_rows
        channel_type = str(tool_input.get("channel_type", "")).strip().lower()
        external_user_key = str(tool_input.get("external_user_key", "")).strip()
        if channel_type not in {"telegram", "whatsapp"}:
            raise ValueError("channel_type invalido")
        if not external_user_key:
            raise ValueError("external_user_key obrigatorio")
        data = self.repository.resolve_channel_user_tenants(
            context.client_id,
            channel_type=channel_type,
            external_user_key=external_user_key,
        )
        return data

    def _handle_channel_set_active_tenant(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        _ = max_rows
        channel_type = str(tool_input.get("channel_type", "")).strip().lower()
        conversation_key = str(tool_input.get("conversation_key", "")).strip()
        external_user_key = str(tool_input.get("external_user_key", "")).strip()
        if tool_input.get("tenant_id") is None:
            raise ValueError("tenant_id obrigatorio")
        if channel_type not in {"telegram", "whatsapp"}:
            raise ValueError("channel_type invalido")
        if not conversation_key:
            raise ValueError("conversation_key obrigatorio")
        if not external_user_key:
            raise ValueError("external_user_key obrigatorio")
        result = self.repository.set_channel_active_tenant(
            context.client_id,
            channel_type=channel_type,
            conversation_key=conversation_key,
            external_user_key=external_user_key,
            tenant_id=int(tool_input["tenant_id"]),
        )
        return {"selection": result}

    def _handle_channel_get_active_tenant(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        _ = max_rows
        channel_type = str(tool_input.get("channel_type", "")).strip().lower()
        conversation_key = str(tool_input.get("conversation_key", "")).strip()
        external_user_key = str(tool_input.get("external_user_key", "")).strip()
        if channel_type not in {"telegram", "whatsapp"}:
            raise ValueError("channel_type invalido")
        if not conversation_key:
            raise ValueError("conversation_key obrigatorio")
        if not external_user_key:
            raise ValueError("external_user_key obrigatorio")
        data = self.repository.get_channel_active_tenant(
            context.client_id,
            channel_type=channel_type,
            conversation_key=conversation_key,
            external_user_key=external_user_key,
        )
        return data

    def _handle_access_list_users(self, context: RequestContext, tool_input: dict[str, Any], max_rows: int | None) -> dict[str, Any]:
        _ = tool_input
        rows = self.repository.list_tenant_users(context.tenant_id)
        if max_rows is not None:
            rows = rows[:max_rows]
        return {"users": rows}

    def _handle_mfa_get_status(self, context: RequestContext, tool_input: dict[str, Any], max_rows: int | None) -> dict[str, Any]:
        _ = tool_input, max_rows
        status = self.repository.get_user_mfa_status(context.tenant_id, context.user_id)
        return {"mfa": status}

    def _handle_mfa_begin_setup(self, context: RequestContext, tool_input: dict[str, Any], max_rows: int | None) -> dict[str, Any]:
        _ = max_rows
        issuer = str(tool_input.get("issuer", "IAOps Governance")).strip() or "IAOps Governance"
        setup = self.repository.begin_user_mfa_setup(context.tenant_id, context.user_id, issuer)
        return {"setup": setup}

    def _handle_mfa_enable(self, context: RequestContext, tool_input: dict[str, Any], max_rows: int | None) -> dict[str, Any]:
        _ = max_rows
        otp_code = str(tool_input.get("otp_code", "")).strip()
        if not otp_code:
            raise ValueError("otp_code obrigatorio")
        result = self.repository.enable_user_mfa(context.tenant_id, context.user_id, otp_code)
        return {"mfa": result}

    def _handle_mfa_disable_self(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        _ = max_rows
        otp_code = str(tool_input.get("otp_code", "")).strip()
        if not otp_code:
            raise ValueError("otp_code obrigatorio")
        result = self.repository.disable_user_mfa(context.tenant_id, context.user_id, otp_code)
        return {"mfa": result}

    def _handle_mfa_admin_reset(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        _ = max_rows
        if tool_input.get("target_user_id") is None:
            raise ValueError("target_user_id obrigatorio")
        result = self.repository.admin_reset_user_mfa(
            context.tenant_id,
            target_user_id=int(tool_input["target_user_id"]),
            reset_by_user_id=context.user_id,
        )
        return {"mfa": result}

    def _handle_pref_get_user_tenant(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        _ = tool_input, max_rows
        pref = self.repository.get_user_tenant_preference(context.tenant_id, context.user_id)
        return {"preference": pref}

    def _handle_pref_update_user_tenant(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        _ = max_rows
        pref = self.repository.upsert_user_tenant_preference(
            context.tenant_id,
            context.user_id,
            language_code=(str(tool_input.get("language_code")).strip() if tool_input.get("language_code") is not None else None),
            theme_code=(str(tool_input.get("theme_code")).strip() if tool_input.get("theme_code") is not None else None),
            chat_response_mode=(
                str(tool_input.get("chat_response_mode")).strip().lower()
                if tool_input.get("chat_response_mode") is not None
                else None
            ),
        )
        return {"preference": pref}

    def _handle_list_columns(self, context: RequestContext, tool_input: dict[str, Any], max_rows: int | None) -> dict[str, Any]:
        schema_name = str(tool_input.get("schema_name", ""))
        table_name = str(tool_input.get("table_name", ""))
        if not schema_name or not table_name:
            raise ValueError("schema_name e table_name sao obrigatorios")
        rows = self.repository.list_monitored_columns(context.tenant_id, schema_name, table_name)
        if max_rows is not None:
            rows = rows[:max_rows]
        return {"columns": rows}

    def _handle_list_tenant_tables(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        raw_source_id = tool_input.get("data_source_id")
        data_source_id = int(raw_source_id) if raw_source_id is not None else None
        rows = self.repository.list_tenant_monitored_tables(context.tenant_id, data_source_id)
        if max_rows is not None:
            rows = rows[:max_rows]
        return {"tables": rows}

    def _handle_register_table(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        _ = max_rows
        if tool_input.get("data_source_id") is None:
            raise ValueError("data_source_id obrigatorio")
        schema_name = str(tool_input.get("schema_name", "")).strip()
        table_name = str(tool_input.get("table_name", "")).strip()
        if not schema_name:
            raise ValueError("schema_name obrigatorio")
        if not table_name:
            raise ValueError("table_name obrigatorio")
        table = self.repository.create_monitored_table(
            context.tenant_id,
            data_source_id=int(tool_input["data_source_id"]),
            schema_name=schema_name,
            table_name=table_name,
            is_active=bool(tool_input.get("is_active", True)),
        )
        return {"table": table}

    def _handle_delete_table(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        _ = max_rows
        if tool_input.get("monitored_table_id") is None:
            raise ValueError("monitored_table_id obrigatorio")
        result = self.repository.delete_monitored_table(
            context.tenant_id,
            monitored_table_id=int(tool_input["monitored_table_id"]),
        )
        return {"result": result}

    def _handle_list_table_columns(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        if tool_input.get("monitored_table_id") is None:
            raise ValueError("monitored_table_id obrigatorio")
        rows = self.repository.list_monitored_columns_by_table(
            context.tenant_id,
            monitored_table_id=int(tool_input["monitored_table_id"]),
        )
        if max_rows is not None:
            rows = rows[:max_rows]
        return {"columns": rows}

    def _handle_register_column(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        _ = max_rows
        if tool_input.get("monitored_table_id") is None:
            raise ValueError("monitored_table_id obrigatorio")
        column_name = str(tool_input.get("column_name", "")).strip()
        if not column_name:
            raise ValueError("column_name obrigatorio")
        column = self.repository.create_monitored_column(
            context.tenant_id,
            monitored_table_id=int(tool_input["monitored_table_id"]),
            column_name=column_name,
            data_type=(str(tool_input.get("data_type")).strip() if tool_input.get("data_type") else None),
            classification=(str(tool_input.get("classification")).strip() if tool_input.get("classification") else None),
            description_text=(str(tool_input.get("description_text")).strip() if tool_input.get("description_text") else None),
        )
        return {"column": column}

    def _handle_delete_column(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        _ = max_rows
        if tool_input.get("monitored_column_id") is None:
            raise ValueError("monitored_column_id obrigatorio")
        result = self.repository.delete_monitored_column(
            context.tenant_id,
            monitored_column_id=int(tool_input["monitored_column_id"]),
        )
        return {"result": result}

    def _handle_source_list_tenant(
        self, context: RequestContext, tool_input: dict[str, Any], max_rows: int | None
    ) -> dict[str, Any]:
        _ = tool_input
        rows = self.repository.list_tenant_data_sources(context.tenant_id)
        if max_rows is not None:
            rows = rows[:max_rows]
        return {"sources": rows}

    def _handle_source_register(
        self, context: RequestContext, tool_input: dict[str, Any], max_rows: int | None
    ) -> dict[str, Any]:
        _ = max_rows
        source_type = str(tool_input.get("source_type", "")).strip().lower()
        conn_secret_ref = str(tool_input.get("conn_secret_ref", "")).strip()
        is_active = bool(tool_input.get("is_active", True))
        if not source_type:
            raise ValueError("source_type obrigatorio")
        if not conn_secret_ref:
            raise ValueError("conn_secret_ref obrigatorio")
        source = self.repository.create_tenant_data_source(
            context.tenant_id,
            source_type=source_type,
            conn_secret_ref=conn_secret_ref,
            is_active=is_active,
        )
        return {"source": source}

    def _handle_source_update_status(
        self, context: RequestContext, tool_input: dict[str, Any], max_rows: int | None
    ) -> dict[str, Any]:
        _ = max_rows
        if tool_input.get("data_source_id") is None:
            raise ValueError("data_source_id obrigatorio")
        source = self.repository.update_tenant_data_source_status(
            context.tenant_id,
            data_source_id=int(tool_input["data_source_id"]),
            is_active=bool(tool_input.get("is_active", True)),
        )
        return {"source": source}

    def _handle_source_update(
        self, context: RequestContext, tool_input: dict[str, Any], max_rows: int | None
    ) -> dict[str, Any]:
        _ = max_rows
        if tool_input.get("data_source_id") is None:
            raise ValueError("data_source_id obrigatorio")
        source_type = str(tool_input.get("source_type", "")).strip().lower()
        conn_secret_ref = str(tool_input.get("conn_secret_ref", "")).strip()
        if not source_type:
            raise ValueError("source_type obrigatorio")
        if not conn_secret_ref:
            raise ValueError("conn_secret_ref obrigatorio")
        source = self.repository.update_tenant_data_source(
            context.tenant_id,
            data_source_id=int(tool_input["data_source_id"]),
            source_type=source_type,
            conn_secret_ref=conn_secret_ref,
        )
        return {"source": source}

    def _handle_source_delete(
        self, context: RequestContext, tool_input: dict[str, Any], max_rows: int | None
    ) -> dict[str, Any]:
        _ = max_rows
        if tool_input.get("data_source_id") is None:
            raise ValueError("data_source_id obrigatorio")
        result = self.repository.delete_tenant_data_source(
            context.tenant_id,
            data_source_id=int(tool_input["data_source_id"]),
        )
        return {"result": result}

    def _handle_execute_safe_sql(self, context: RequestContext, tool_input: dict[str, Any], max_rows: int | None) -> dict[str, Any]:
        sql_text = str(tool_input.get("sql_text", "")).strip()
        if not sql_text:
            raise ValueError("sql_text obrigatorio")
        self._assert_safe_select(sql_text)
        sql_policy = self.repository.get_sql_security_policy(context.tenant_id)
        self._assert_allowed_schemas(sql_text, sql_policy.get("allowed_schema_patterns") or [])
        result = self.repository.execute_safe_sql(context.tenant_id, sql_text, max_rows)
        if not bool(sql_policy.get("require_masking", True)):
            return result
        rules = self.repository.list_active_lgpd_rules(context.tenant_id)
        if not rules:
            return result
        blocked = self._enforce_lgpd_block_rules(sql_text=sql_text, rows=result.get("rows") or [], columns=result.get("columns") or [], rules=rules)
        if blocked:
            raise LgpdBlockedError(blocked)
        rows = result.get("rows") or []
        columns = result.get("columns") or []
        masked_rows, applied_masks = self._apply_lgpd_masks(sql_text=sql_text, rows=rows, columns=columns, rules=rules)
        result["rows"] = masked_rows
        result["applied_masks"] = sorted(set((result.get("applied_masks") or []) + applied_masks))
        return result

    def _enforce_lgpd_block_rules(
        self,
        *,
        sql_text: str,
        rows: list[Any],
        columns: list[Any],
        rules: list[dict[str, Any]],
    ) -> list[str]:
        if not rows:
            return []
        table_refs = self._extract_sql_table_refs(sql_text)
        blocked_rules = [
            rule
            for rule in rules
            if str(rule.get("rule_type") or "").strip().lower() in {"block", "deny", "forbid", "deny_access"}
        ]
        if not blocked_rules:
            return []
        by_col: dict[str, list[dict[str, Any]]] = {}
        for rule in blocked_rules:
            col = str(rule.get("column_name") or "").strip().lower()
            if col:
                by_col.setdefault(col, []).append(rule)
        if not by_col:
            return []
        blocked_fields: list[str] = []
        normalized_columns = {str(c or "").strip().lower() for c in columns if str(c or "").strip()}
        for row in rows:
            if not isinstance(row, dict):
                continue
            candidate_cols = normalized_columns | {str(k).strip().lower() for k in row.keys()}
            for col_key in candidate_cols:
                rules_for_col = by_col.get(col_key)
                if not rules_for_col:
                    continue
                matched_rule = self._select_rule_for_sql_refs(rules_for_col, table_refs)
                if not matched_rule:
                    continue
                blocked_fields.append(
                    f"{matched_rule.get('schema_name')}.{matched_rule.get('table_name')}.{matched_rule.get('column_name')}"
                )
        return blocked_fields

    def _apply_lgpd_masks(
        self,
        *,
        sql_text: str,
        rows: list[Any],
        columns: list[Any],
        rules: list[dict[str, Any]],
    ) -> tuple[list[Any], list[str]]:
        if not rows:
            return rows, []
        table_refs = self._extract_sql_table_refs(sql_text)
        by_column: dict[str, list[dict[str, Any]]] = {}
        for rule in rules:
            col = str(rule.get("column_name") or "").strip().lower()
            if not col:
                continue
            by_column.setdefault(col, []).append(rule)
        if not by_column:
            return rows, []

        normalized_columns = [str(c or "").strip() for c in columns]
        applied: list[str] = []
        masked_rows: list[Any] = []
        for row in rows:
            if not isinstance(row, dict):
                masked_rows.append(row)
                continue
            item = dict(row)
            candidate_cols = set(item.keys()) | set(normalized_columns)
            for raw_col in candidate_cols:
                if not raw_col:
                    continue
                key = str(raw_col).strip().lower()
                rules_for_col = by_column.get(key)
                if not rules_for_col:
                    continue
                matched_rule = self._select_rule_for_sql_refs(rules_for_col, table_refs)
                if not matched_rule:
                    continue
                if raw_col not in item:
                    for existing in list(item.keys()):
                        if str(existing).strip().lower() == key:
                            raw_col = existing
                            break
                    if raw_col not in item:
                        continue
                item[raw_col] = self._mask_value(
                    value=item.get(raw_col),
                    rule_type=str(matched_rule.get("rule_type") or "").strip().lower(),
                    rule_config=matched_rule.get("rule_config") or {},
                )
                applied.append(f"{matched_rule.get('schema_name')}.{matched_rule.get('table_name')}.{key}")
            masked_rows.append(item)
        return masked_rows, applied

    def _select_rule_for_sql_refs(
        self,
        rules_for_col: list[dict[str, Any]],
        table_refs: set[tuple[str, str]],
    ) -> dict[str, Any] | None:
        if not rules_for_col:
            return None
        if not table_refs:
            return rules_for_col[0]
        for rule in rules_for_col:
            schema_name = str(rule.get("schema_name") or "").strip().lower()
            table_name = str(rule.get("table_name") or "").strip().lower()
            if (schema_name, table_name) in table_refs:
                return rule
        return None

    def _extract_sql_table_refs(self, sql_text: str) -> set[tuple[str, str]]:
        refs: set[tuple[str, str]] = set()
        pattern = re.compile(r"\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)(?:\.([a-zA-Z_][a-zA-Z0-9_]*))?", re.IGNORECASE)
        for match in pattern.finditer(sql_text):
            first = str(match.group(1) or "").strip().lower()
            second = str(match.group(2) or "").strip().lower()
            if first and second:
                refs.add((first, second))
            elif first:
                refs.add(("public", first))
        return refs

    def _mask_value(self, *, value: Any, rule_type: str, rule_config: dict[str, Any]) -> Any:
        if value is None:
            return None
        text = str(value)
        mask_char = str(rule_config.get("mask_char") or "*")
        if rule_type in {"mask", "full_mask", "anonymize"}:
            return mask_char * max(4, len(text))
        if rule_type in {"email_mask", "mask_email"}:
            return self._mask_email(text=text, mask_char=mask_char)
        if rule_type in {"hash", "sha256"}:
            return hashlib.sha256(text.encode("utf-8")).hexdigest()
        if rule_type in {"last4", "keep_last4"}:
            show_last = int(rule_config.get("show_last") or 4)
            keep = max(0, min(show_last, len(text)))
            return (mask_char * max(0, len(text) - keep)) + text[-keep:]
        if rule_type in {"cpf_mask"}:
            digits = re.sub(r"\D", "", text)
            if len(digits) >= 11:
                return "***.***.***-**"
            return mask_char * max(4, len(text))
        return mask_char * max(4, len(text))

    def _mask_email(self, *, text: str, mask_char: str) -> str:
        if "@" not in text:
            return mask_char * max(4, len(text))
        local, domain = text.split("@", 1)
        if not local:
            return f"{mask_char}@{domain}"
        if len(local) == 1:
            return f"{local}{mask_char}@{domain}"
        return f"{local[0]}{mask_char * (len(local) - 1)}@{domain}"

    def _handle_security_sql_get_policy(
        self, context: RequestContext, tool_input: dict[str, Any], max_rows: int | None
    ) -> dict[str, Any]:
        _ = tool_input, max_rows
        return {"policy": self.repository.get_sql_security_policy(context.tenant_id)}

    def _handle_security_sql_update_policy(
        self, context: RequestContext, tool_input: dict[str, Any], max_rows: int | None
    ) -> dict[str, Any]:
        _ = max_rows
        allowed_patterns = tool_input.get("allowed_schema_patterns", [])
        if not isinstance(allowed_patterns, list):
            raise ValueError("allowed_schema_patterns deve ser lista")
        normalized = [str(item).strip() for item in allowed_patterns if str(item).strip()]
        policy = self.repository.update_sql_security_policy(
            context.tenant_id,
            max_rows=int(tool_input["max_rows"]) if tool_input.get("max_rows") is not None else None,
            max_calls_per_minute=(
                int(tool_input["max_calls_per_minute"]) if tool_input.get("max_calls_per_minute") is not None else None
            ),
            require_masking=bool(tool_input.get("require_masking", True)),
            allowed_schema_patterns=normalized,
        )
        return {"policy": policy}

    def _handle_incident_create(self, context: RequestContext, tool_input: dict[str, Any], max_rows: int | None) -> dict[str, Any]:
        _ = max_rows
        title = str(tool_input.get("title", "")).strip()
        severity = str(tool_input.get("severity", "")).strip()
        source_event_id = tool_input.get("source_event_id")
        if not title:
            raise ValueError("title obrigatorio")
        if severity not in {"low", "medium", "high", "critical"}:
            raise ValueError("severity invalida")
        return self.repository.create_incident(context.tenant_id, title, severity, source_event_id)

    def _handle_incident_list(self, context: RequestContext, tool_input: dict[str, Any], max_rows: int | None) -> dict[str, Any]:
        status = tool_input.get("status")
        severity = tool_input.get("severity")
        limit = int(tool_input.get("limit", max_rows or 50))
        rows = self.repository.list_incidents(
            context.tenant_id,
            status=str(status) if status else None,
            severity=str(severity) if severity else None,
            limit=max(1, min(limit, max_rows or 500)),
        )
        return {"incidents": rows}

    def _handle_incident_update_status(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        _ = max_rows
        if "incident_id" not in tool_input:
            raise ValueError("incident_id obrigatorio")
        new_status = str(tool_input.get("new_status", "")).strip()
        if new_status not in {"open", "ack", "resolved", "closed"}:
            raise ValueError("new_status invalido")
        incident = self.repository.update_incident_status(
            context.tenant_id,
            incident_id=int(tool_input["incident_id"]),
            new_status=new_status,
        )
        return {"incident": incident}

    def _handle_events_list(self, context: RequestContext, tool_input: dict[str, Any], max_rows: int | None) -> dict[str, Any]:
        severity = tool_input.get("severity")
        limit = int(tool_input.get("limit", max_rows or 50))
        rows = self.repository.list_events(
            context.tenant_id,
            severity=str(severity) if severity else None,
            limit=max(1, min(limit, max_rows or 500)),
        )
        return {"events": rows}

    def _handle_audit_list_calls(self, context: RequestContext, tool_input: dict[str, Any], max_rows: int | None) -> dict[str, Any]:
        tool_name = tool_input.get("tool_name")
        status = tool_input.get("status")
        correlation_id = tool_input.get("correlation_id")
        limit = int(tool_input.get("limit", max_rows or 50))
        rows = self.repository.list_audit_calls(
            context.tenant_id,
            tool_name=str(tool_name) if tool_name else None,
            status=str(status) if status else None,
            correlation_id=str(correlation_id) if correlation_id else None,
            limit=max(1, min(limit, max_rows or 500)),
        )
        return {"calls": rows}

    def _handle_health_summary(self, context: RequestContext, tool_input: dict[str, Any], max_rows: int | None) -> dict[str, Any]:
        _ = max_rows
        window_minutes = int(tool_input.get("window_minutes", 60))
        return self.repository.get_health_summary(context.tenant_id, window_minutes)

    def _handle_setup_get_progress(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        _ = tool_input, max_rows
        return {"progress": self.repository.get_setup_progress(context.tenant_id)}

    def _handle_setup_upsert_progress(
        self,
        context: RequestContext,
        tool_input: dict[str, Any],
        max_rows: int | None,
    ) -> dict[str, Any]:
        _ = max_rows
        snapshot = tool_input.get("snapshot")
        if not isinstance(snapshot, dict):
            raise ValueError("snapshot deve ser objeto")
        progress = self.repository.upsert_setup_progress(
            client_id=context.client_id,
            tenant_id=context.tenant_id,
            user_id=context.user_id,
            correlation_id=context.correlation_id,
            snapshot=snapshot,
        )
        return {"progress": progress}

    def _finalize_log(
        self,
        context: RequestContext,
        tool_name: str,
        request_payload: dict[str, Any],
        result: ToolExecutionResult,
        start: float,
    ) -> dict[str, Any]:
        latency_ms = int((time.perf_counter() - start) * 1000)
        response = {
            "status": result.status,
            "tool": tool_name,
            "correlation_id": context.correlation_id,
            "data": result.data,
            "error": {
                "code": result.error_code,
                "message": result.error_message,
            }
            if result.error_code
            else None,
        }
        self.repository.log_mcp_call(
            client_id=context.client_id,
            tenant_id=context.tenant_id,
            user_id=context.user_id,
            tool_name=tool_name,
            status=result.status,
            correlation_id=context.correlation_id,
            request_payload=request_payload,
            response_payload=response,
            error_code=result.error_code,
            error_message=result.error_message,
            latency_ms=latency_ms,
        )
        return response

    @staticmethod
    def _assert_safe_select(sql_text: str) -> None:
        # Guardrail simples de scaffold; substituir por parser SQL robusto no hardening.
        stripped = re.sub(r"\s+", " ", sql_text.strip().lower())
        forbidden = [
            " insert ",
            " update ",
            " delete ",
            " drop ",
            " alter ",
            " create ",
            " truncate ",
            " grant ",
            " revoke ",
        ]
        if not stripped.startswith("select"):
            raise ValueError("somente SELECT e permitido")
        for token in forbidden:
            if token in f" {stripped} ":
                raise ValueError("comando SQL bloqueado por politica de seguranca")
        if ";" in stripped:
            raise ValueError("multiplas instrucoes nao sao permitidas")

    @staticmethod
    def _assert_allowed_schemas(sql_text: str, allowed_patterns: list[str]) -> None:
        if not allowed_patterns:
            return
        found_schemas = re.findall(r"(?:from|join)\s+([a-zA-Z_][\w]*)\.", sql_text, flags=re.IGNORECASE)
        if not found_schemas:
            return

        def _matches(schema_name: str) -> bool:
            for pattern in allowed_patterns:
                if pattern.endswith("*") and schema_name.startswith(pattern[:-1]):
                    return True
                if schema_name == pattern:
                    return True
            return False

        invalid = [schema for schema in found_schemas if not _matches(schema)]
        if invalid:
            raise ValueError(f"schema nao permitido na consulta: {', '.join(sorted(set(invalid)))}")
