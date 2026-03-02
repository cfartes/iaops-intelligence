from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from functions import handle_request


class IAOpsAPIHandler(BaseHTTPRequestHandler):
    server_version = "IAOpsAPI/0.1"

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._send_json(HTTPStatus.NO_CONTENT, {})

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._send_json(HTTPStatus.OK, {"status": "ok"})
            return
        if parsed.path == "/api/inventory/tables":
            self._handle_inventory_tables(parsed.query)
            return
        if parsed.path == "/api/data-sources/catalog":
            self._handle_data_source_catalog()
            return
        if parsed.path == "/api/data-sources":
            self._handle_data_source_list()
            return
        if parsed.path == "/api/inventory/columns":
            self._handle_inventory_columns(parsed.query)
            return
        if parsed.path == "/api/onboarding/monitored-tables":
            self._handle_onboarding_monitored_tables_list(parsed.query)
            return
        if parsed.path == "/api/onboarding/monitored-columns":
            self._handle_onboarding_monitored_columns_list(parsed.query)
            return
        if parsed.path == "/api/incidents":
            self._handle_incident_list(parsed.query)
            return
        if parsed.path == "/api/events":
            self._handle_events_list(parsed.query)
            return
        if parsed.path == "/api/operation/health":
            self._handle_operation_health(parsed.query)
            return
        if parsed.path == "/api/audit/calls":
            self._handle_audit_calls(parsed.query)
            return
        if parsed.path == "/api/security-sql/policy":
            self._handle_security_sql_policy_get()
            return
        if parsed.path == "/api/access/users":
            self._handle_access_users_list()
            return
        if parsed.path == "/api/security/mfa/status":
            self._handle_mfa_status_get()
            return
        if parsed.path == "/api/tenants":
            self._handle_tenants_list()
            return
        if parsed.path == "/api/tenants/limits":
            self._handle_tenant_limits_get()
            return
        if parsed.path == "/api/admin/llm/providers":
            self._handle_admin_llm_providers_list()
            return
        if parsed.path == "/api/admin/llm/config":
            self._handle_admin_llm_config_get()
            return
        if parsed.path == "/api/tenant-llm/providers":
            self._handle_tenant_llm_providers_list()
            return
        if parsed.path == "/api/tenant-llm/config":
            self._handle_tenant_llm_config_get()
            return
        if parsed.path == "/api/preferences/user-tenant":
            self._handle_user_tenant_preference_get()
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/mcp/call":
            self._handle_generic_call()
            return
        if parsed.path == "/api/incidents":
            self._handle_incident_create()
            return
        if parsed.path == "/api/data-sources":
            self._handle_data_source_create()
            return
        if parsed.path == "/api/incidents/status":
            self._handle_incident_update_status()
            return
        if parsed.path == "/api/data-sources/status":
            self._handle_data_source_update_status()
            return
        if parsed.path == "/api/data-sources/update":
            self._handle_data_source_update()
            return
        if parsed.path == "/api/data-sources/delete":
            self._handle_data_source_delete()
            return
        if parsed.path == "/api/onboarding/monitored-tables":
            self._handle_onboarding_monitored_table_create()
            return
        if parsed.path == "/api/onboarding/monitored-tables/delete":
            self._handle_onboarding_monitored_table_delete()
            return
        if parsed.path == "/api/onboarding/monitored-columns":
            self._handle_onboarding_monitored_column_create()
            return
        if parsed.path == "/api/onboarding/monitored-columns/delete":
            self._handle_onboarding_monitored_column_delete()
            return
        if parsed.path == "/api/security-sql/policy":
            self._handle_security_sql_policy_update()
            return
        if parsed.path == "/api/chat-bi/query":
            self._handle_chat_bi_query()
            return
        if parsed.path == "/api/security/mfa/setup":
            self._handle_mfa_setup_begin()
            return
        if parsed.path == "/api/security/mfa/enable":
            self._handle_mfa_enable()
            return
        if parsed.path == "/api/security/mfa/disable":
            self._handle_mfa_disable()
            return
        if parsed.path == "/api/security/mfa/admin-reset":
            self._handle_mfa_admin_reset()
            return
        if parsed.path == "/api/tenants":
            self._handle_tenant_create()
            return
        if parsed.path == "/api/tenants/status":
            self._handle_tenant_update_status()
            return
        if parsed.path == "/api/admin/llm/config":
            self._handle_admin_llm_config_update()
            return
        if parsed.path == "/api/tenant-llm/config":
            self._handle_tenant_llm_config_update()
            return
        if parsed.path == "/api/preferences/user-tenant":
            self._handle_user_tenant_preference_update()
            return
        if parsed.path == "/api/channel/tenants/list":
            self._handle_channel_list_tenants()
            return
        if parsed.path == "/api/channel/tenant/select":
            self._handle_channel_select_tenant()
            return
        if parsed.path == "/api/channel/tenant/active":
            self._handle_channel_get_active_tenant()
            return
        if parsed.path == "/api/channel/webhook/telegram":
            self._handle_channel_webhook("telegram")
            return
        if parsed.path == "/api/channel/webhook/whatsapp":
            self._handle_channel_webhook("whatsapp")
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def _handle_inventory_tables(self, query: str) -> None:
        qs = parse_qs(query)
        schema_name = qs.get("schema_name", [None])[0]
        payload = {
            "context": self._request_context(),
            "tool": "inventory.list_tables",
            "input": {"schema_name": schema_name},
        }
        self._dispatch_mcp(payload)

    def _handle_data_source_catalog(self) -> None:
        payload = {
            "context": self._request_context(),
            "tool": "source.list_catalog",
            "input": {},
        }
        self._dispatch_mcp(payload)

    def _handle_data_source_list(self) -> None:
        payload = {
            "context": self._request_context(),
            "tool": "source.list_tenant",
            "input": {},
        }
        self._dispatch_mcp(payload)

    def _handle_data_source_create(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "source.register",
            "input": {
                "source_type": body.get("source_type"),
                "conn_secret_ref": body.get("conn_secret_ref"),
                "is_active": body.get("is_active", True),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_data_source_update_status(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "source.update_status",
            "input": {
                "data_source_id": body.get("data_source_id"),
                "is_active": body.get("is_active", True),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_data_source_update(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "source.update",
            "input": {
                "data_source_id": body.get("data_source_id"),
                "source_type": body.get("source_type"),
                "conn_secret_ref": body.get("conn_secret_ref"),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_data_source_delete(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "source.delete",
            "input": {
                "data_source_id": body.get("data_source_id"),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_inventory_columns(self, query: str) -> None:
        qs = parse_qs(query)
        schema_name = qs.get("schema_name", [""])[0]
        table_name = qs.get("table_name", [""])[0]
        payload = {
            "context": self._request_context(),
            "tool": "inventory.list_columns",
            "input": {
                "schema_name": schema_name,
                "table_name": table_name,
            },
        }
        self._dispatch_mcp(payload)

    def _handle_onboarding_monitored_tables_list(self, query: str) -> None:
        qs = parse_qs(query)
        data_source_id_raw = qs.get("data_source_id", [None])[0]
        payload = {
            "context": self._request_context(),
            "tool": "inventory.list_tenant_tables",
            "input": {
                "data_source_id": int(data_source_id_raw) if data_source_id_raw else None,
            },
        }
        self._dispatch_mcp(payload)

    def _handle_onboarding_monitored_table_create(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "inventory.register_table",
            "input": {
                "data_source_id": body.get("data_source_id"),
                "schema_name": body.get("schema_name"),
                "table_name": body.get("table_name"),
                "is_active": body.get("is_active", True),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_onboarding_monitored_table_delete(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "inventory.delete_table",
            "input": {
                "monitored_table_id": body.get("monitored_table_id"),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_onboarding_monitored_columns_list(self, query: str) -> None:
        qs = parse_qs(query)
        monitored_table_id_raw = qs.get("monitored_table_id", [None])[0]
        payload = {
            "context": self._request_context(),
            "tool": "inventory.list_table_columns",
            "input": {
                "monitored_table_id": int(monitored_table_id_raw) if monitored_table_id_raw else None,
            },
        }
        self._dispatch_mcp(payload)

    def _handle_onboarding_monitored_column_create(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "inventory.register_column",
            "input": {
                "monitored_table_id": body.get("monitored_table_id"),
                "column_name": body.get("column_name"),
                "data_type": body.get("data_type"),
                "classification": body.get("classification"),
                "description_text": body.get("description_text"),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_onboarding_monitored_column_delete(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "inventory.delete_column",
            "input": {
                "monitored_column_id": body.get("monitored_column_id"),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_incident_create(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "incident.create",
            "input": {
                "title": body.get("title"),
                "severity": body.get("severity"),
                "source_event_id": body.get("source_event_id"),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_incident_update_status(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "incident.update_status",
            "input": {
                "incident_id": body.get("incident_id"),
                "new_status": body.get("new_status"),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_incident_list(self, query: str) -> None:
        qs = parse_qs(query)
        payload = {
            "context": self._request_context(),
            "tool": "incident.list",
            "input": {
                "status": qs.get("status", [None])[0],
                "severity": qs.get("severity", [None])[0],
                "limit": int(qs.get("limit", ["50"])[0]),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_events_list(self, query: str) -> None:
        qs = parse_qs(query)
        payload = {
            "context": self._request_context(),
            "tool": "events.list",
            "input": {
                "severity": qs.get("severity", [None])[0],
                "limit": int(qs.get("limit", ["50"])[0]),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_operation_health(self, query: str) -> None:
        qs = parse_qs(query)
        payload = {
            "context": self._request_context(),
            "tool": "ops.get_health_summary",
            "input": {
                "window_minutes": int(qs.get("window_minutes", ["60"])[0]),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_audit_calls(self, query: str) -> None:
        qs = parse_qs(query)
        payload = {
            "context": self._request_context(),
            "tool": "audit.list_calls",
            "input": {
                "tool_name": qs.get("tool_name", [None])[0],
                "status": qs.get("status", [None])[0],
                "correlation_id": qs.get("correlation_id", [None])[0],
                "limit": int(qs.get("limit", ["50"])[0]),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_security_sql_policy_get(self) -> None:
        payload = {
            "context": self._request_context(),
            "tool": "security_sql.get_policy",
            "input": {},
        }
        self._dispatch_mcp(payload)

    def _handle_access_users_list(self) -> None:
        payload = {
            "context": self._request_context(),
            "tool": "access.list_users",
            "input": {},
        }
        self._dispatch_mcp(payload)

    def _handle_mfa_status_get(self) -> None:
        payload = {
            "context": self._request_context(),
            "tool": "security.mfa.get_status",
            "input": {},
        }
        self._dispatch_mcp(payload)

    def _handle_mfa_setup_begin(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "security.mfa.begin_setup",
            "input": {
                "issuer": body.get("issuer", "IAOps Governance"),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_mfa_enable(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "security.mfa.enable",
            "input": {
                "otp_code": body.get("otp_code"),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_mfa_disable(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "security.mfa.disable_self",
            "input": {
                "otp_code": body.get("otp_code"),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_mfa_admin_reset(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "security.mfa.admin_reset",
            "input": {
                "target_user_id": body.get("target_user_id"),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_tenants_list(self) -> None:
        payload = {
            "context": self._request_context(),
            "tool": "tenant.list_client",
            "input": {},
        }
        self._dispatch_mcp(payload)

    def _handle_tenant_limits_get(self) -> None:
        payload = {
            "context": self._request_context(),
            "tool": "tenant.get_limits",
            "input": {},
        }
        self._dispatch_mcp(payload)

    def _handle_tenant_create(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "tenant.create",
            "input": {
                "name": body.get("name"),
                "slug": body.get("slug"),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_tenant_update_status(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "tenant.update_status",
            "input": {
                "tenant_id": body.get("tenant_id"),
                "status": body.get("status"),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_admin_llm_providers_list(self) -> None:
        payload = {
            "context": self._request_context(),
            "tool": "llm_admin.list_providers",
            "input": {},
        }
        self._dispatch_mcp(payload)

    def _handle_admin_llm_config_get(self) -> None:
        payload = {
            "context": self._request_context(),
            "tool": "llm_admin.get_app_config",
            "input": {},
        }
        self._dispatch_mcp(payload)

    def _handle_admin_llm_config_update(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "llm_admin.update_app_config",
            "input": {
                "provider_name": body.get("provider_name"),
                "model_code": body.get("model_code"),
                "endpoint_url": body.get("endpoint_url"),
                "secret_ref": body.get("secret_ref"),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_tenant_llm_providers_list(self) -> None:
        payload = {
            "context": self._request_context(),
            "tool": "tenant_llm.list_providers",
            "input": {},
        }
        self._dispatch_mcp(payload)

    def _handle_tenant_llm_config_get(self) -> None:
        payload = {
            "context": self._request_context(),
            "tool": "tenant_llm.get_config",
            "input": {},
        }
        self._dispatch_mcp(payload)

    def _handle_tenant_llm_config_update(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "tenant_llm.update_config",
            "input": {
                "use_app_default_llm": body.get("use_app_default_llm", False),
                "provider_name": body.get("provider_name"),
                "model_code": body.get("model_code"),
                "endpoint_url": body.get("endpoint_url"),
                "secret_ref": body.get("secret_ref"),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_user_tenant_preference_get(self) -> None:
        payload = {
            "context": self._request_context(),
            "tool": "pref.get_user_tenant",
            "input": {},
        }
        self._dispatch_mcp(payload)

    def _handle_user_tenant_preference_update(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "pref.update_user_tenant",
            "input": {
                "language_code": body.get("language_code"),
                "theme_code": body.get("theme_code"),
                "chat_response_mode": body.get("chat_response_mode"),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_channel_list_tenants(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "channel.list_user_tenants",
            "input": {
                "channel_type": body.get("channel_type"),
                "external_user_key": body.get("external_user_key"),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_channel_select_tenant(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "channel.set_active_tenant",
            "input": {
                "channel_type": body.get("channel_type"),
                "conversation_key": body.get("conversation_key"),
                "external_user_key": body.get("external_user_key"),
                "tenant_id": body.get("tenant_id"),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_channel_get_active_tenant(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "channel.get_active_tenant",
            "input": {
                "channel_type": body.get("channel_type"),
                "conversation_key": body.get("conversation_key"),
                "external_user_key": body.get("external_user_key"),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_channel_webhook(self, channel_type: str) -> None:
        body = self._read_json_body()
        external_user_key = str(
            body.get("external_user_key")
            or body.get("from")
            or body.get("user_id")
            or ""
        ).strip()
        conversation_key = str(
            body.get("conversation_key")
            or body.get("chat_id")
            or external_user_key
            or ""
        ).strip()
        message_text = str(body.get("text") or body.get("message") or "").strip()
        if not external_user_key:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {"ok": False, "error": "external_user_key obrigatorio"},
            )
            return
        if not conversation_key:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {"ok": False, "error": "conversation_key obrigatorio"},
            )
            return

        context = self._request_context()
        command = self._parse_channel_command(message_text)
        response_data = self._execute_channel_command(
            context=context,
            channel_type=channel_type,
            external_user_key=external_user_key,
            conversation_key=conversation_key,
            command=command,
            message_text=message_text,
        )
        self._send_json(HTTPStatus.OK, response_data)

    def _execute_channel_command(
        self,
        *,
        context: dict,
        channel_type: str,
        external_user_key: str,
        conversation_key: str,
        command: dict,
        message_text: str,
    ) -> dict:
        kind = command["kind"]
        if kind == "tenant_list":
            result = self._call_mcp(
                {
                    "context": context,
                    "tool": "channel.list_user_tenants",
                    "input": {
                        "channel_type": channel_type,
                        "external_user_key": external_user_key,
                    },
                }
            )
            if result["status"] != "success":
                return self._channel_error_response(result)
            data = result["data"]
            return {
                "ok": True,
                "command": "tenant_list",
                "reply_text": self._reply_tenant_list(data),
                "data": data,
            }

        if kind == "tenant_select":
            result = self._call_mcp(
                {
                    "context": context,
                    "tool": "channel.set_active_tenant",
                    "input": {
                        "channel_type": channel_type,
                        "conversation_key": conversation_key,
                        "external_user_key": external_user_key,
                        "tenant_id": command["tenant_id"],
                    },
                }
            )
            if result["status"] != "success":
                return self._channel_error_response(result)
            active = self._call_mcp(
                {
                    "context": context,
                    "tool": "channel.get_active_tenant",
                    "input": {
                        "channel_type": channel_type,
                        "conversation_key": conversation_key,
                        "external_user_key": external_user_key,
                    },
                }
            )
            active_data = active["data"] if active["status"] == "success" else {}
            return {
                "ok": True,
                "command": "tenant_select",
                "reply_text": self._reply_active_tenant(active_data),
                "data": {
                    "selection": result["data"].get("selection"),
                    "active": active_data,
                },
            }

        if kind == "tenant_active":
            result = self._call_mcp(
                {
                    "context": context,
                    "tool": "channel.get_active_tenant",
                    "input": {
                        "channel_type": channel_type,
                        "conversation_key": conversation_key,
                        "external_user_key": external_user_key,
                    },
                }
            )
            if result["status"] != "success":
                return self._channel_error_response(result)
            return {
                "ok": True,
                "command": "tenant_active",
                "reply_text": self._reply_active_tenant(result["data"]),
                "data": result["data"],
            }

        if kind == "sql_query":
            return {
                "ok": False,
                "command": "error",
                "reply_text": "Comando SQL nao e aceito. Envie a pergunta em linguagem natural.",
                "data": {},
            }

        if kind == "nl_query":
            runtime = self._resolve_channel_runtime_context(
                context=context,
                channel_type=channel_type,
                external_user_key=external_user_key,
                conversation_key=conversation_key,
            )
            if runtime.get("error"):
                return runtime["error"]
            nl_response = self._execute_nl_chat_query(runtime["context"], command["question_text"])
            if not nl_response["ok"]:
                return nl_response
            return {
                "ok": True,
                "command": "nl_query",
                "reply_text": nl_response["reply_text"],
                "data": {
                    "active_tenant": runtime["active"],
                    "query": nl_response["data"],
                },
            }

        return {
            "ok": True,
            "command": "help",
            "reply_text": self._reply_help(message_text),
            "data": {},
        }

    def _resolve_channel_runtime_context(
        self,
        *,
        context: dict,
        channel_type: str,
        external_user_key: str,
        conversation_key: str,
    ) -> dict:
        active = self._call_mcp(
            {
                "context": context,
                "tool": "channel.get_active_tenant",
                "input": {
                    "channel_type": channel_type,
                    "conversation_key": conversation_key,
                    "external_user_key": external_user_key,
                },
            }
        )
        if active["status"] != "success":
            return {"error": self._channel_error_response(active)}
        active_data = active["data"]
        active_tenant_id = active_data.get("active_tenant_id")
        user = active_data.get("user") or {}
        active_user_id = user.get("user_id")
        if active_tenant_id is None:
            return {
                "error": {
                    "ok": False,
                    "command": "error",
                    "reply_text": "Nenhum tenant ativo na conversa. Use: tenant list e tenant select <id>.",
                    "data": {"active": active_data},
                }
            }
        if active_user_id is None:
            return {
                "error": {
                    "ok": False,
                    "command": "error",
                    "reply_text": "Usuario do canal nao identificado.",
                    "data": {"active": active_data},
                }
            }
        runtime_context = {
            "client_id": int(context["client_id"]),
            "tenant_id": int(active_tenant_id),
            "user_id": int(active_user_id),
            "correlation_id": str(uuid.uuid4()),
        }
        return {"context": runtime_context, "active": active_data}

    @staticmethod
    def _parse_channel_command(message_text: str) -> dict:
        raw = message_text.strip()
        normalized = " ".join(raw.lower().split())
        if not normalized:
            return {"kind": "help"}
        if normalized in {"help", "ajuda", "/help"}:
            return {"kind": "help"}
        if normalized in {"tenant", "/tenant", "tenant list", "/tenant list", "tenants"}:
            return {"kind": "tenant_list"}
        if normalized in {"tenant active", "/tenant active"}:
            return {"kind": "tenant_active"}
        selected = re.match(r"^/?tenant\s+select\s+(\d+)$", normalized)
        if selected:
            return {"kind": "tenant_select", "tenant_id": int(selected.group(1))}
        if re.match(r"^/?sql\s+", raw, flags=re.IGNORECASE):
            return {"kind": "sql_query"}
        return {"kind": "nl_query", "question_text": raw}

    @staticmethod
    def _channel_error_response(result: dict) -> dict:
        message = (
            result.get("error", {}).get("message")
            or "Nao foi possivel processar o comando no canal."
        )
        return {
            "ok": False,
            "command": "error",
            "reply_text": f"Erro: {message}",
            "data": {"mcp": result},
        }

    @staticmethod
    def _reply_tenant_list(data: dict) -> str:
        tenants = data.get("tenants") or []
        user = data.get("user") or {}
        lines = []
        lines.append(f"Usuario: {user.get('full_name') or user.get('email') or 'n/a'}")
        lines.append("Tenants disponiveis:")
        for item in tenants:
            lines.append(
                f"- {item.get('tenant_id')}: {item.get('name')} ({item.get('status')}, role={item.get('role')})"
            )
        lines.append("Use: tenant select <id>")
        lines.append("Use: tenant active")
        return "\n".join(lines)

    @staticmethod
    def _reply_active_tenant(data: dict) -> str:
        active_tenant_id = data.get("active_tenant_id")
        tenants = data.get("tenants") or []
        if active_tenant_id is None:
            return "Nenhum tenant ativo para esta conversa. Use: tenant list"
        selected = next(
            (item for item in tenants if int(item.get("tenant_id", -1)) == int(active_tenant_id)),
            None,
        )
        if not selected:
            return f"Tenant ativo: {active_tenant_id}"
        return f"Tenant ativo: {selected.get('tenant_id')} - {selected.get('name')} ({selected.get('status')})"

    @staticmethod
    def _reply_help(message_text: str) -> str:
        prefix = f"Comando nao reconhecido: '{message_text}'.\n" if message_text else ""
        return (
            f"{prefix}Comandos disponiveis:\n"
            "tenant list\n"
            "tenant select <id>\n"
            "tenant active\n"
            "ajuda\n"
            "Ou envie a pergunta em linguagem natural."
        )

    @staticmethod
    def _reply_sql_result(sql_text: str, data: dict) -> str:
        rows = data.get("rows") or []
        columns = data.get("columns") or []
        lines = []
        lines.append(f"SQL executada com sucesso. Linhas: {len(rows)}")
        if columns:
            lines.append(f"Colunas: {', '.join(str(item) for item in columns)}")
        preview = rows[:5]
        if preview:
            lines.append("Preview:")
            for index, row in enumerate(preview, start=1):
                lines.append(f"{index}. {json.dumps(row, ensure_ascii=True)}")
        else:
            lines.append("Consulta sem retorno de linhas.")
        lines.append(f"Comando: sql {sql_text}")
        return "\n".join(lines)

    def _execute_nl_chat_query(self, context: dict, question_text: str) -> dict:
        response_mode = self._resolve_chat_response_mode(context)
        rag = self._build_rag_context(context)
        planned = self._plan_sql_from_question(context, question_text, rag)
        sql_text = planned.get("sql_text")
        if not sql_text:
            return {
                "ok": False,
                "command": "error",
                "reply_text": "Nao consegui interpretar sua pergunta com as tabelas monitoradas.",
                "data": {"rag": rag},
            }
        query_result = self._call_mcp(
            {
                "context": context,
                "tool": "query.execute_safe_sql",
                "input": {"sql_text": sql_text, "explain": False},
            }
        )
        if query_result["status"] != "success":
            return self._channel_error_response(query_result)
        query_data = query_result["data"]
        return {
            "ok": True,
            "reply_text": self._reply_nl_result(question_text, sql_text, query_data, rag, response_mode),
            "data": {
                "question_text": question_text,
                "planned_sql": sql_text,
                "planning_mode": planned.get("mode", "rules"),
                "llm_provider": planned.get("llm_provider"),
                "chat_response_mode": response_mode,
                "rag": rag,
                "result": query_data,
            },
        }

    def _resolve_chat_response_mode(self, context: dict) -> str:
        result = self._call_mcp(
            {
                "context": context,
                "tool": "pref.get_user_tenant",
                "input": {},
            }
        )
        if result.get("status") != "success":
            return "executive"
        pref = (result.get("data") or {}).get("preference") or {}
        mode = str(pref.get("chat_response_mode") or "executive").strip().lower()
        return mode if mode in {"executive", "detailed"} else "executive"

    def _build_rag_context(self, context: dict) -> dict:
        table_result = self._call_mcp(
            {
                "context": context,
                "tool": "inventory.list_tenant_tables",
                "input": {},
            }
        )
        if table_result["status"] != "success":
            return {"tables": [], "columns": {}}
        tables = (table_result.get("data") or {}).get("tables") or []
        tables = tables[:12]
        columns_by_table: dict[str, list[dict]] = {}
        for table in tables:
            table_id = table.get("id")
            if table_id is None:
                continue
            column_result = self._call_mcp(
                {
                    "context": context,
                    "tool": "inventory.list_table_columns",
                    "input": {"monitored_table_id": table_id},
                }
            )
            if column_result["status"] != "success":
                continue
            key = f"{table.get('schema_name')}.{table.get('table_name')}"
            columns_by_table[key] = (column_result.get("data") or {}).get("columns") or []
        relationships = self._infer_table_relationships(tables, columns_by_table)
        return {"tables": tables, "columns": columns_by_table, "relationships": relationships}

    @staticmethod
    def _infer_table_relationships(tables: list[dict], columns_by_table: dict[str, list[dict]]) -> list[dict]:
        def singular(name: str) -> str:
            if name.endswith("ies") and len(name) > 3:
                return f"{name[:-3]}y"
            if name.endswith("s") and len(name) > 1:
                return name[:-1]
            return name

        table_names = {f"{item.get('schema_name')}.{item.get('table_name')}": str(item.get("table_name") or "") for item in tables}
        normalized_cols: dict[str, set[str]] = {}
        for key, cols in columns_by_table.items():
            normalized_cols[key] = {str(col.get("column_name") or "").lower() for col in cols}

        links: list[dict] = []
        keys = list(table_names.keys())
        for source_key in keys:
            source_cols = normalized_cols.get(source_key, set())
            for target_key in keys:
                if source_key == target_key:
                    continue
                target_table_name = singular(table_names[target_key].lower())
                candidate = f"{target_table_name}_id"
                if candidate in source_cols:
                    links.append(
                        {
                            "source_table": source_key,
                            "target_table": target_key,
                            "source_column": candidate,
                            "target_column": "id",
                        }
                    )
        unique: list[dict] = []
        seen = set()
        for item in links:
            key = (
                item["source_table"],
                item["source_column"],
                item["target_table"],
                item["target_column"],
            )
            if key in seen:
                continue
            seen.add(key)
            unique.append(item)
        return unique

    def _plan_sql_from_question(self, context: dict, question_text: str, rag: dict) -> dict:
        llm_plan = self._plan_sql_with_llm(context, question_text, rag)
        if llm_plan.get("sql_text"):
            return llm_plan
        return self._plan_sql_with_rules(question_text, rag)

    @staticmethod
    def _plan_sql_with_rules(question_text: str, rag: dict) -> dict:
        q = question_text.lower()
        if "incidente" in q and ("aberto" in q or "abertos" in q):
            return {
                "mode": "rules",
                "sql_text": "SELECT status, COUNT(*) AS total FROM iaops_gov.incident WHERE status IN ('open','ack') GROUP BY status",
            }
        if "evento" in q and ("critico" in q or "criticos" in q):
            return {
                "mode": "rules",
                "sql_text": "SELECT severity, COUNT(*) AS total FROM iaops_gov.schema_change_event WHERE severity = 'critical' GROUP BY severity",
            }
        if "tabela" in q or "inventario" in q:
            return {
                "mode": "rules",
                "sql_text": "SELECT schema_name, table_name, is_active FROM iaops_gov.monitored_table ORDER BY schema_name, table_name LIMIT 50",
            }
        ranked = IAOpsAPIHandler._rank_tables_for_question(question_text, rag.get("tables") or [])
        if ranked:
            target = ranked[0]
            schema_name = str(target.get("schema_name"))
            table_name = str(target.get("table_name"))
            if any(term in q for term in ["quantos", "qtd", "total", "count"]):
                return {"mode": "rules", "sql_text": f"SELECT COUNT(*) AS total FROM {schema_name}.{table_name}"}
            return {"mode": "rules", "sql_text": f"SELECT * FROM {schema_name}.{table_name} LIMIT 20"}
        return {"mode": "rules", "sql_text": None}

    def _plan_sql_with_llm(self, context: dict, question_text: str, rag: dict) -> dict:
        cfg_result = self._call_mcp(
            {
                "context": context,
                "tool": "tenant_llm.get_config",
                "input": {},
            }
        )
        if cfg_result.get("status") != "success":
            return {"mode": "llm_unavailable", "sql_text": None}
        cfg = (cfg_result.get("data") or {}).get("config") or {}
        provider_name = str(cfg.get("provider_name") or "").strip().lower()
        model_code = str(cfg.get("model_code") or "").strip()
        endpoint_url = str(cfg.get("endpoint_url") or "").strip()
        secret_value = self._resolve_secret_value(cfg.get("secret_ref"))
        if not provider_name or not model_code or not endpoint_url or not secret_value:
            return {"mode": "llm_unavailable", "sql_text": None}

        prompt_payload = {
            "instruction": (
                "Converta a pergunta do usuario para SQL SELECT seguro. "
                "Responda SOMENTE com JSON no formato: {\"sql_text\":\"...\"}. "
                "Nao use DDL/DML. Sem ponto e virgula."
            ),
            "question": question_text,
            "tables": rag.get("tables") or [],
            "columns": rag.get("columns") or {},
            "relationships": rag.get("relationships") or [],
            "allowed_schemas_hint": ["public", "analytics", "iaops_gov"],
        }
        llm_output = self._invoke_llm_json(
            provider_name=provider_name,
            model_code=model_code,
            endpoint_url=endpoint_url,
            api_key=secret_value,
            prompt_payload=prompt_payload,
        )
        sql_text = str((llm_output or {}).get("sql_text") or "").strip()
        if not sql_text:
            return {"mode": "llm_empty", "sql_text": None}
        if not self._is_planned_sql_allowed(sql_text):
            return {"mode": "llm_rejected", "sql_text": None}
        return {"mode": "llm", "sql_text": sql_text, "llm_provider": provider_name}

    @staticmethod
    def _is_planned_sql_allowed(sql_text: str) -> bool:
        lowered = re.sub(r"\s+", " ", sql_text.strip().lower())
        if not lowered.startswith("select"):
            return False
        if len(lowered) > 5000:
            return False
        blocked = [" insert ", " update ", " delete ", " drop ", " alter ", " create ", " truncate ", " grant ", " revoke "]
        decorated = f" {lowered} "
        if any(token in decorated for token in blocked):
            return False
        if ";" in lowered:
            return False
        if "--" in lowered or "/*" in lowered or "*/" in lowered:
            return False
        return True

    def _resolve_secret_value(self, secret_ref: str | None) -> str | None:
        ref = str(secret_ref or "").strip()
        if not ref:
            return None
        if ref.startswith("plain://"):
            return ref.removeprefix("plain://").strip() or None
        if ref.startswith("env://"):
            env_name = ref.removeprefix("env://").strip()
            return os.getenv(env_name)
        if ref in os.environ:
            return os.environ.get(ref)
        normalized = re.sub(r"[^A-Za-z0-9_]+", "_", ref).strip("_").upper()
        if normalized:
            return os.getenv(f"IAOPS_SECRET_{normalized}")
        return None

    def _invoke_llm_json(
        self,
        *,
        provider_name: str,
        model_code: str,
        endpoint_url: str,
        api_key: str,
        prompt_payload: dict,
    ) -> dict | None:
        try:
            content = json.dumps(prompt_payload, ensure_ascii=True)
            messages = [
                {
                    "role": "system",
                    "content": "You are a SQL planner. Return only JSON.",
                },
                {
                    "role": "user",
                    "content": content,
                },
            ]
            base = endpoint_url.rstrip("/")
            if provider_name in {"openai", "azure_openai", "groq", "mistral", "ollama"}:
                url = f"{base}/chat/completions" if base.endswith("/v1") else f"{base}/v1/chat/completions"
                payload = {"model": model_code, "messages": messages, "temperature": 0}
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {api_key}",
                }
                if provider_name == "azure_openai":
                    headers["api-key"] = api_key
                text = self._http_post_json(url, headers, payload)
                data = json.loads(text)
                content_text = (
                    (((data.get("choices") or [{}])[0]).get("message") or {}).get("content") or ""
                ).strip()
                return self._extract_json_object(content_text)
            if provider_name == "anthropic":
                url = f"{base}/messages" if base.endswith("/v1") else f"{base}/v1/messages"
                payload = {
                    "model": model_code,
                    "max_tokens": 300,
                    "temperature": 0,
                    "messages": [{"role": "user", "content": content}],
                }
                headers = {
                    "Content-Type": "application/json",
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                }
                text = self._http_post_json(url, headers, payload)
                data = json.loads(text)
                parts = data.get("content") or []
                joined = " ".join(str(item.get("text") or "") for item in parts if isinstance(item, dict))
                return self._extract_json_object(joined)
            if provider_name == "google_gemini":
                url = f"{base}/models/{model_code}:generateContent?key={api_key}"
                payload = {"contents": [{"parts": [{"text": content}]}]}
                headers = {"Content-Type": "application/json"}
                text = self._http_post_json(url, headers, payload)
                data = json.loads(text)
                candidates = data.get("candidates") or []
                parts = (((candidates[0] if candidates else {}).get("content") or {}).get("parts") or [])
                joined = " ".join(str(item.get("text") or "") for item in parts if isinstance(item, dict))
                return self._extract_json_object(joined)
        except Exception:
            return None
        return None

    @staticmethod
    def _http_post_json(url: str, headers: dict, payload: dict) -> str:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url=url, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read().decode("utf-8")

    @staticmethod
    def _extract_json_object(text: str) -> dict | None:
        if not text:
            return None
        raw = text.strip()
        if raw.startswith("```"):
            raw = raw.strip("`")
            if raw.startswith("json"):
                raw = raw[4:].strip()
        start = raw.find("{")
        end = raw.rfind("}")
        if start < 0 or end < 0 or end <= start:
            return None
        snippet = raw[start : end + 1]
        try:
            parsed = json.loads(snippet)
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None

    @staticmethod
    def _rank_tables_for_question(question_text: str, tables: list[dict]) -> list[dict]:
        tokens = [token for token in re.split(r"[^a-zA-Z0-9_]+", question_text.lower()) if len(token) > 2]
        if not tokens:
            return []
        scored: list[tuple[int, dict]] = []
        for item in tables:
            haystack = f"{item.get('schema_name', '')} {item.get('table_name', '')}".lower()
            score = sum(1 for token in tokens if token in haystack)
            if score > 0:
                scored.append((score, item))
        scored.sort(key=lambda pair: pair[0], reverse=True)
        return [item for _, item in scored]

    @staticmethod
    def _reply_nl_result(question_text: str, sql_text: str, query_data: dict, rag: dict, response_mode: str) -> str:
        rows = query_data.get("rows") or []
        columns = query_data.get("columns") or []
        lines = []
        lines.append(f"Pergunta: {question_text}")
        lines.append(f"Encontrei {len(rows)} registro(s).")
        if rows and "total" in columns and isinstance(rows[0], dict) and "total" in rows[0]:
            lines.append(f"Total: {rows[0]['total']}")
        if rows and response_mode == "detailed":
            lines.append("Preview:")
            for idx, row in enumerate(rows[:5], start=1):
                lines.append(f"{idx}. {json.dumps(row, ensure_ascii=True)}")
        elif rows:
            lines.append("Resumo executivo: dados encontrados e prontos para detalhamento.")
        tables = rag.get("tables") or []
        if tables and response_mode == "detailed":
            lines.append(f"Contexto RAG: {len(tables)} tabela(s) monitorada(s) analisadas.")
        return "\n".join(lines)

    def _handle_security_sql_policy_update(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "security_sql.update_policy",
            "input": {
                "max_rows": body.get("max_rows"),
                "max_calls_per_minute": body.get("max_calls_per_minute"),
                "require_masking": body.get("require_masking"),
                "allowed_schema_patterns": body.get("allowed_schema_patterns", []),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_chat_bi_query(self) -> None:
        body = self._read_json_body()
        question_text = str(body.get("question_text") or body.get("question") or "").strip()
        if not question_text:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "chat-bi.query",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_input", "message": "question_text obrigatorio"},
                },
            )
            return
        response = self._execute_nl_chat_query(self._request_context(), question_text)
        if not response["ok"]:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "chat-bi.query",
                    "correlation_id": None,
                    "data": response.get("data", {}),
                    "error": {"code": "nl_query_failed", "message": response["reply_text"]},
                },
            )
            return
        self._send_json(
            HTTPStatus.OK,
            {
                "status": "success",
                "tool": "chat-bi.query",
                "correlation_id": str(uuid.uuid4()),
                "data": response["data"],
                "error": None,
            },
        )

    def _handle_generic_call(self) -> None:
        body = self._read_json_body()
        context = body.get("context") or self._request_context()
        payload = {
            "context": context,
            "tool": body.get("tool"),
            "input": body.get("input", {}),
        }
        self._dispatch_mcp(payload)

    def _dispatch_mcp(self, payload: dict) -> None:
        result = self._call_mcp(payload)
        if result["status"] == "success":
            self._send_json(HTTPStatus.OK, result)
            return
        if result["status"] == "denied":
            self._send_json(HTTPStatus.FORBIDDEN, result)
            return
        self._send_json(HTTPStatus.BAD_REQUEST, result)

    @staticmethod
    def _call_mcp(payload: dict) -> dict:
        return handle_request(payload)

    def _request_context(self) -> dict:
        return {
            "client_id": int(self.headers.get("X-Client-Id", "1")),
            "tenant_id": int(self.headers.get("X-Tenant-Id", "10")),
            "user_id": int(self.headers.get("X-User-Id", "100")),
            "correlation_id": self.headers.get("X-Correlation-Id", str(uuid.uuid4())),
        }

    def _read_json_body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def _send_json(self, status: HTTPStatus, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Client-Id, X-Tenant-Id, X-User-Id, X-Correlation-Id")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()
        if status != HTTPStatus.NO_CONTENT:
            self.wfile.write(body)


def run(host: str = "127.0.0.1", port: int = 8000) -> None:
    server = ThreadingHTTPServer((host, port), IAOpsAPIHandler)
    print(f"IAOps API running on http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    run()
