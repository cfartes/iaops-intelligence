from __future__ import annotations

import json
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
        if parsed.path == "/api/channel/tenants/list":
            self._handle_channel_list_tenants()
            return
        if parsed.path == "/api/channel/tenant/select":
            self._handle_channel_select_tenant()
            return
        if parsed.path == "/api/channel/tenant/active":
            self._handle_channel_get_active_tenant()
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
        payload = {
            "context": self._request_context(),
            "tool": "query.execute_safe_sql",
            "input": {
                "sql_text": body.get("sql_text"),
                "explain": bool(body.get("explain", False)),
            },
        }
        self._dispatch_mcp(payload)

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
        result = handle_request(payload)
        if result["status"] == "success":
            self._send_json(HTTPStatus.OK, result)
            return
        if result["status"] == "denied":
            self._send_json(HTTPStatus.FORBIDDEN, result)
            return
        self._send_json(HTTPStatus.BAD_REQUEST, result)

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
