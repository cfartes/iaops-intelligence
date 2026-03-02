from __future__ import annotations

import json
import os
import re
import smtplib
import unicodedata
import urllib.error
import urllib.request
import uuid
from email.message import EmailMessage
from hashlib import pbkdf2_hmac
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from secrets import token_hex
from time import time
from urllib.parse import parse_qs, urlparse

from functions import handle_request
from iaops.security.crypto import decrypt_text
from iaops.security.totp import verify_totp
try:
    from psycopg import connect
except Exception:  # pragma: no cover - depende de ambiente
    connect = None


class IAOpsAPIHandler(BaseHTTPRequestHandler):
    server_version = "IAOpsAPI/0.1"
    session_ttl_seconds = 30 * 60
    refresh_ttl_seconds = 7 * 24 * 60 * 60
    login_max_attempts = 5
    login_attempt_window_seconds = 15 * 60
    login_lock_seconds = 15 * 60
    ip_throttle_window_seconds = 30 * 60
    ip_throttle_step_one_attempts = 20
    ip_throttle_step_two_attempts = 40
    ip_throttle_step_one_lock_seconds = 15 * 60
    ip_throttle_step_two_lock_seconds = 60 * 60
    signup_schema = "iaops_gov"
    signup_tables_ready = False
    pending_signups: dict[str, dict] = {}
    confirmed_signups: dict[str, dict] = {}
    mfa_login_challenges: dict[str, dict] = {}
    active_sessions: dict[str, dict] = {}
    refresh_sessions: dict[str, str] = {}
    failed_login_attempts: dict[str, dict] = {}
    failed_login_attempts_ip: dict[str, dict] = {}

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
        if parsed.path == "/api/setup/progress":
            self._handle_setup_progress_get()
            return
        if parsed.path == "/api/auth/sessions":
            self._handle_auth_sessions_list()
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/auth/login":
            self._handle_auth_login()
            return
        if parsed.path == "/api/auth/mfa/verify":
            self._handle_auth_mfa_verify()
            return
        if parsed.path == "/api/auth/session/refresh":
            self._handle_auth_session_refresh()
            return
        if parsed.path == "/api/auth/logout":
            self._handle_auth_logout()
            return
        if parsed.path == "/api/auth/sessions/revoke":
            self._handle_auth_sessions_revoke()
            return
        if parsed.path == "/api/auth/signup":
            self._handle_auth_signup()
            return
        if parsed.path == "/api/auth/confirm":
            self._handle_auth_confirm()
            return
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
        if parsed.path == "/api/setup/progress":
            self._handle_setup_progress_upsert()
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

    def _handle_setup_progress_get(self) -> None:
        payload = {
            "context": self._request_context(),
            "tool": "setup.get_progress",
            "input": {},
        }
        self._dispatch_mcp(payload)

    def _handle_setup_progress_upsert(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "setup.upsert_progress",
            "input": {
                "snapshot": body.get("snapshot") if isinstance(body.get("snapshot"), dict) else {},
            },
        }
        self._dispatch_mcp(payload)

    def _handle_auth_signup(self) -> None:
        body = self._read_json_body()
        required_fields = [
            "trade_name",
            "legal_name",
            "cnpj",
            "address_text",
            "phone_contact",
            "email_contact",
            "email_access",
            "email_notification",
            "password",
            "plan_code",
            "language_code",
        ]
        missing = [field for field in required_fields if not str(body.get(field) or "").strip()]
        if missing:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "auth.signup",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_input", "message": f"Campos obrigatorios ausentes: {', '.join(missing)}"},
                },
            )
            return

        email_access = str(body.get("email_access") or "").strip().lower()
        confirm_token = token_hex(16)
        expires_at = int(time()) + (24 * 60 * 60)
        password = str(body.get("password") or "")
        salt = token_hex(16)
        password_hash = pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 240000).hex()
        encoded_password = f"pbkdf2_sha256$240000${salt}${password_hash}"

        signup_data = {
            "trade_name": str(body.get("trade_name") or "").strip(),
            "legal_name": str(body.get("legal_name") or "").strip(),
            "cnpj": str(body.get("cnpj") or "").strip(),
            "address_text": str(body.get("address_text") or "").strip(),
            "phone_contact": str(body.get("phone_contact") or "").strip(),
            "email_contact": str(body.get("email_contact") or "").strip(),
            "email_access": email_access,
            "email_notification": str(body.get("email_notification") or "").strip(),
            "password_hash": encoded_password,
            "plan_code": str(body.get("plan_code") or "").strip(),
            "language_code": str(body.get("language_code") or "pt-BR").strip(),
            "status": "pending_email_confirmation",
            "created_at_epoch": int(time()),
            "expires_at_epoch": expires_at,
        }

        db_error = self._persist_pending_signup_db(confirm_token=confirm_token, signup_data=signup_data)
        if db_error:
            if db_error == "already_exists":
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {
                        "status": "denied",
                        "tool": "auth.signup",
                        "correlation_id": None,
                        "data": {},
                        "error": {"code": "already_exists", "message": "Cliente ja cadastrado para este CNPJ/e-mail."},
                    },
                )
                return
            if db_error == "pending_exists":
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {
                        "status": "denied",
                        "tool": "auth.signup",
                        "correlation_id": None,
                        "data": {},
                        "error": {"code": "pending_exists", "message": "Ja existe cadastro pendente para este CNPJ/e-mail."},
                    },
                )
                return
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "auth.signup",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "db_unavailable", "message": "Nao foi possivel persistir o cadastro no banco."},
                },
            )
            return

        delivered, detail = self._send_signup_email(
            to_email=email_access,
            trade_name=signup_data["trade_name"],
            confirm_token=confirm_token,
        )

        response_data = {
            "pending_email_access": email_access,
            "expires_at_epoch": expires_at,
            "delivery": detail,
        }
        if not delivered:
            response_data["confirm_token"] = confirm_token
        self._send_json(
            HTTPStatus.OK,
            {
                "status": "success",
                "tool": "auth.signup",
                "correlation_id": str(uuid.uuid4()),
                "data": response_data,
                "error": None,
            },
        )

    def _handle_auth_login(self) -> None:
        self._cleanup_ephemeral_auth_state()
        body = self._read_json_body()
        email = str(body.get("email") or "").strip().lower()
        password = str(body.get("password") or "")
        client_ip = self._request_ip()
        tenant_id_raw = body.get("tenant_id")
        tenant_id = None
        if tenant_id_raw not in (None, ""):
            try:
                tenant_id = int(tenant_id_raw)
            except (TypeError, ValueError):
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {
                        "status": "denied",
                        "tool": "auth.login",
                        "correlation_id": None,
                        "data": {},
                        "error": {"code": "invalid_input", "message": "tenant_id invalido"},
                    },
                )
                return
        if not email or not password:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "auth.login",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_input", "message": "email e password obrigatorios"},
                },
            )
            return

        throttle = self._check_login_throttle(email=email, client_ip=client_ip)
        if throttle:
            self._log_auth_event(
                action_code="auth.login.blocked",
                client_id=None,
                tenant_id=None,
                user_id=None,
                target_id=email,
                payload={
                    "email": email,
                    "ip": client_ip,
                    "blocked_remaining_seconds": throttle["blocked_remaining_seconds"],
                },
            )
            self._send_json(
                HTTPStatus.TOO_MANY_REQUESTS,
                {
                    "status": "denied",
                    "tool": "auth.login",
                    "correlation_id": None,
                    "data": {},
                    "error": {
                        "code": "too_many_attempts",
                        "message": f"Tentativas excedidas. Tente novamente em {throttle['blocked_remaining_seconds']}s.",
                    },
                },
            )
            return
        ip_throttle = self._check_ip_login_throttle(client_ip=client_ip)
        if ip_throttle:
            self._log_auth_event(
                action_code="auth.login.blocked_ip",
                client_id=None,
                tenant_id=None,
                user_id=None,
                target_id=email,
                payload={
                    "email": email,
                    "ip": client_ip,
                    "blocked_remaining_seconds": ip_throttle["blocked_remaining_seconds"],
                },
            )
            self._send_json(
                HTTPStatus.TOO_MANY_REQUESTS,
                {
                    "status": "denied",
                    "tool": "auth.login",
                    "correlation_id": None,
                    "data": {},
                    "error": {
                        "code": "too_many_attempts_ip",
                        "message": f"Muitas tentativas neste IP. Tente novamente em {ip_throttle['blocked_remaining_seconds']}s.",
                    },
                },
            )
            return

        result = self._login_user(email=email, password=password, tenant_id=tenant_id)
        if result.get("error_code"):
            if result.get("error_code") in {"invalid_credentials"}:
                self._register_failed_login(email=email, client_ip=client_ip)
            self._log_auth_event(
                action_code="auth.login.failed",
                client_id=None,
                tenant_id=None,
                user_id=None,
                target_id=email,
                payload={
                    "email": email,
                    "ip": client_ip,
                    "error_code": result.get("error_code"),
                },
            )
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "auth.login",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": result["error_code"], "message": result.get("message", "Falha no login")},
                },
            )
            return
        self._clear_failed_login(email=email, client_ip=client_ip)
        auth_ctx = result.get("auth_context") or {}
        self._log_auth_event(
            action_code="auth.login.success",
            client_id=auth_ctx.get("client_id"),
            tenant_id=auth_ctx.get("tenant_id"),
            user_id=auth_ctx.get("user_id"),
            target_id=email,
            payload={
                "email": email,
                "ip": client_ip,
                "mfa_required": bool(result.get("mfa_required")),
            },
        )
        self._send_json(
            HTTPStatus.OK,
            {
                "status": "success",
                "tool": "auth.login",
                "correlation_id": str(uuid.uuid4()),
                "data": result,
                "error": None,
            },
        )

    def _handle_auth_mfa_verify(self) -> None:
        body = self._read_json_body()
        challenge_token = str(body.get("challenge_token") or "").strip()
        otp_code = str(body.get("otp_code") or "").strip()
        challenge_snapshot = self.mfa_login_challenges.get(challenge_token) if challenge_token else None
        if not challenge_token or not otp_code:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "auth.mfa.verify",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_input", "message": "challenge_token e otp_code obrigatorios"},
                },
            )
            return

        result = self._verify_login_mfa(challenge_token=challenge_token, otp_code=otp_code)
        if result.get("error_code"):
            self._log_auth_event(
                action_code="auth.mfa.failed",
                client_id=challenge_snapshot.get("client_id") if isinstance(challenge_snapshot, dict) else None,
                tenant_id=challenge_snapshot.get("tenant_id") if isinstance(challenge_snapshot, dict) else None,
                user_id=challenge_snapshot.get("user_id") if isinstance(challenge_snapshot, dict) else None,
                target_id=str(challenge_snapshot.get("email")) if isinstance(challenge_snapshot, dict) else None,
                payload={
                    "error_code": result.get("error_code"),
                },
            )
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "auth.mfa.verify",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": result["error_code"], "message": result.get("message", "Falha na validacao MFA")},
                },
            )
            return
        auth_ctx = result.get("auth_context") or {}
        profile = result.get("profile") or {}
        self._log_auth_event(
            action_code="auth.mfa.success",
            client_id=auth_ctx.get("client_id"),
            tenant_id=auth_ctx.get("tenant_id"),
            user_id=auth_ctx.get("user_id"),
            target_id=profile.get("email"),
            payload={"mfa_required": False},
        )
        self._send_json(
            HTTPStatus.OK,
            {
                "status": "success",
                "tool": "auth.mfa.verify",
                "correlation_id": str(uuid.uuid4()),
                "data": result,
                "error": None,
            },
        )

    def _handle_auth_session_refresh(self) -> None:
        body = self._read_json_body()
        refresh_token = str(body.get("refresh_token") or "").strip()
        if not refresh_token:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "auth.session.refresh",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_input", "message": "refresh_token obrigatorio"},
                },
            )
            return
        result = self._refresh_session(refresh_token=refresh_token)
        if result.get("error_code"):
            self._log_auth_event(
                action_code="auth.session.refresh.failed",
                client_id=None,
                tenant_id=None,
                user_id=None,
                target_id=None,
                payload={"error_code": result.get("error_code")},
            )
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "auth.session.refresh",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": result["error_code"], "message": result.get("message", "Falha no refresh")},
                },
            )
            return
        auth_ctx = result.get("auth_context") or {}
        profile = result.get("profile") or {}
        self._log_auth_event(
            action_code="auth.session.refresh.success",
            client_id=auth_ctx.get("client_id"),
            tenant_id=auth_ctx.get("tenant_id"),
            user_id=auth_ctx.get("user_id"),
            target_id=profile.get("email"),
            payload={"refreshed": True},
        )
        self._send_json(
            HTTPStatus.OK,
            {
                "status": "success",
                "tool": "auth.session.refresh",
                "correlation_id": str(uuid.uuid4()),
                "data": result,
                "error": None,
            },
        )

    def _handle_auth_logout(self) -> None:
        body = self._read_json_body()
        refresh_token = str(body.get("refresh_token") or "").strip()
        session_token = str(self.headers.get("X-Session-Token", "") or "").strip()
        if not session_token:
            session_token = str(body.get("session_token") or "").strip()
        logout_ctx = self._resolve_context_from_tokens(session_token=session_token, refresh_token=refresh_token)
        self._invalidate_session(session_token=session_token, refresh_token=refresh_token)
        self._log_auth_event(
            action_code="auth.logout",
            client_id=logout_ctx.get("client_id"),
            tenant_id=logout_ctx.get("tenant_id"),
            user_id=logout_ctx.get("user_id"),
            target_id=logout_ctx.get("email"),
            payload={"logout": True},
        )
        self._send_json(
            HTTPStatus.OK,
            {
                "status": "success",
                "tool": "auth.logout",
                "correlation_id": str(uuid.uuid4()),
                "data": {"logged_out": True},
                "error": None,
            },
        )

    def _handle_auth_sessions_list(self) -> None:
        session_token = str(self.headers.get("X-Session-Token", "") or "").strip()
        if not session_token:
            self._send_json(
                HTTPStatus.UNAUTHORIZED,
                {
                    "status": "denied",
                    "tool": "auth.sessions.list",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_session", "message": "Sessao obrigatoria."},
                },
            )
            return
        result = self._list_active_sessions_for_actor(session_token=session_token)
        if result.get("error_code"):
            self._send_json(
                HTTPStatus.FORBIDDEN,
                {
                    "status": "denied",
                    "tool": "auth.sessions.list",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": result["error_code"], "message": result.get("message", "Acesso negado")},
                },
            )
            return
        self._send_json(
            HTTPStatus.OK,
            {
                "status": "success",
                "tool": "auth.sessions.list",
                "correlation_id": str(uuid.uuid4()),
                "data": result,
                "error": None,
            },
        )

    def _handle_auth_sessions_revoke(self) -> None:
        session_token = str(self.headers.get("X-Session-Token", "") or "").strip()
        body = self._read_json_body()
        target_session_token = str(body.get("session_token") or "").strip()
        if not session_token or not target_session_token:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "auth.sessions.revoke",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_input", "message": "Sessao atual e session_token alvo sao obrigatorios."},
                },
            )
            return
        result = self._revoke_active_session_for_actor(
            actor_session_token=session_token,
            target_session_token=target_session_token,
        )
        if result.get("error_code"):
            self._send_json(
                HTTPStatus.FORBIDDEN,
                {
                    "status": "denied",
                    "tool": "auth.sessions.revoke",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": result["error_code"], "message": result.get("message", "Acesso negado")},
                },
            )
            return
        self._send_json(
            HTTPStatus.OK,
            {
                "status": "success",
                "tool": "auth.sessions.revoke",
                "correlation_id": str(uuid.uuid4()),
                "data": result,
                "error": None,
            },
        )

    def _handle_auth_confirm(self) -> None:
        body = self._read_json_body()
        confirm_token = str(body.get("confirm_token") or "").strip()
        if not confirm_token:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "auth.confirm",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_input", "message": "confirm_token obrigatorio"},
                },
            )
            return

        confirm_result = self._confirm_pending_signup_db(confirm_token=confirm_token)
        if confirm_result is None:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "auth.confirm",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_token", "message": "Token de confirmacao invalido."},
                },
            )
            return
        if confirm_result == "expired_token":
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "auth.confirm",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "expired_token", "message": "Token de confirmacao expirado."},
                },
            )
            return
        if confirm_result == "invalid_token":
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "auth.confirm",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_token", "message": "Token de confirmacao invalido."},
                },
            )
            return
        if confirm_result == "already_exists":
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "auth.confirm",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "already_exists", "message": "Cliente ja confirmado para este CNPJ/e-mail."},
                },
            )
            return
        if confirm_result == "plan_not_found":
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "auth.confirm",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "plan_not_found", "message": "Plano informado nao foi encontrado."},
                },
            )
            return
        if confirm_result == "db_unavailable":
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "auth.confirm",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "db_unavailable", "message": "Nao foi possivel confirmar cadastro no banco."},
                },
            )
            return
        self._send_json(
            HTTPStatus.OK,
            {
                "status": "success",
                "tool": "auth.confirm",
                "correlation_id": str(uuid.uuid4()),
                "data": confirm_result,
                "error": None,
            },
        )

    def _persist_pending_signup_db(self, *, confirm_token: str, signup_data: dict) -> str | None:
        if not self._is_db_enabled():
            if signup_data["email_access"] in self.confirmed_signups:
                return "already_exists"
            self.pending_signups[confirm_token] = signup_data
            return None
        try:
            self._ensure_signup_tables()
            schema = self.signup_schema
            with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT 1
                    FROM {schema}.client c
                    WHERE c.cnpj = %(cnpj)s
                       OR LOWER(c.access_email) = %(email_access)s
                    LIMIT 1
                    """,
                    {
                        "cnpj": signup_data["cnpj"],
                        "email_access": signup_data["email_access"],
                    },
                )
                if cur.fetchone():
                    return "already_exists"
                cur.execute(
                    f"""
                    SELECT 1
                    FROM {schema}.client_signup_pending p
                    WHERE p.status = 'pending'
                      AND p.expires_at > NOW()
                      AND (
                        p.cnpj = %(cnpj)s
                        OR LOWER(p.email_access) = %(email_access)s
                      )
                    LIMIT 1
                    """,
                    {
                        "cnpj": signup_data["cnpj"],
                        "email_access": signup_data["email_access"],
                    },
                )
                if cur.fetchone():
                    return "pending_exists"
                cur.execute(
                    f"""
                    INSERT INTO {schema}.client_signup_pending (
                        confirm_token,
                        trade_name,
                        legal_name,
                        cnpj,
                        address_text,
                        phone_contact,
                        email_contact,
                        email_access,
                        email_notification,
                        password_hash,
                        plan_code,
                        language_code,
                        status,
                        expires_at
                    )
                    VALUES (
                        %(confirm_token)s,
                        %(trade_name)s,
                        %(legal_name)s,
                        %(cnpj)s,
                        %(address_text)s,
                        %(phone_contact)s,
                        %(email_contact)s,
                        %(email_access)s,
                        %(email_notification)s,
                        %(password_hash)s,
                        %(plan_code)s,
                        %(language_code)s,
                        'pending',
                        NOW() + INTERVAL '24 hours'
                    )
                    """,
                    {
                        "confirm_token": confirm_token,
                        "trade_name": signup_data["trade_name"],
                        "legal_name": signup_data["legal_name"],
                        "cnpj": signup_data["cnpj"],
                        "address_text": signup_data["address_text"],
                        "phone_contact": signup_data["phone_contact"],
                        "email_contact": signup_data["email_contact"],
                        "email_access": signup_data["email_access"],
                        "email_notification": signup_data["email_notification"],
                        "password_hash": signup_data["password_hash"],
                        "plan_code": signup_data["plan_code"],
                        "language_code": signup_data["language_code"],
                    },
                )
                conn.commit()
            return None
        except Exception:
            return "db_unavailable"

    def _confirm_pending_signup_db(self, *, confirm_token: str) -> dict | str | None:
        if not self._is_db_enabled():
            record = self.pending_signups.get(confirm_token)
            if not record:
                return None
            if int(record.get("expires_at_epoch") or 0) < int(time()):
                self.pending_signups.pop(confirm_token, None)
                return "expired_token"
            record["status"] = "active"
            record["confirmed_at_epoch"] = int(time())
            self.confirmed_signups[record["email_access"]] = record
            self.pending_signups.pop(confirm_token, None)
            return {
                "email_access": record["email_access"],
                "status": record["status"],
                "confirmed_at_epoch": record["confirmed_at_epoch"],
            }

        try:
            self._ensure_signup_tables()
            schema = self.signup_schema
            with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        id,
                        trade_name,
                        legal_name,
                        cnpj,
                        address_text,
                        phone_contact,
                        email_contact,
                        email_access,
                        email_notification,
                        password_hash,
                        plan_code,
                        language_code,
                        expires_at,
                        status
                    FROM {schema}.client_signup_pending
                    WHERE confirm_token = %(confirm_token)s
                    LIMIT 1
                    """,
                    {"confirm_token": confirm_token},
                )
                pending = cur.fetchone()
                if not pending:
                    return None
                if pending[13] != "pending":
                    return "invalid_token"
                if pending[12] is None or pending[12].timestamp() < time():
                    cur.execute(
                        f"""
                        UPDATE {schema}.client_signup_pending
                           SET status = 'expired'
                         WHERE id = %(id)s
                        """,
                        {"id": pending[0]},
                    )
                    conn.commit()
                    return "expired_token"

                cnpj = str(pending[3]).strip()
                email_access = str(pending[7]).strip().lower()
                cur.execute(
                    f"""
                    SELECT 1
                    FROM {schema}.client c
                    WHERE c.cnpj = %(cnpj)s
                       OR LOWER(c.access_email) = %(email_access)s
                    LIMIT 1
                    """,
                    {"cnpj": cnpj, "email_access": email_access},
                )
                if cur.fetchone():
                    return "already_exists"

                cur.execute(
                    f"""
                    SELECT id
                    FROM {schema}.plan
                    WHERE code = %(plan_code)s
                    LIMIT 1
                    """,
                    {"plan_code": pending[10]},
                )
                plan_row = cur.fetchone()
                if not plan_row:
                    return "plan_not_found"
                plan_id = int(plan_row[0])

                cur.execute(
                    f"""
                    INSERT INTO {schema}.client (
                        fantasy_name,
                        legal_name,
                        cnpj,
                        address_text,
                        contact_phone,
                        contact_email,
                        access_email,
                        notification_email,
                        password_hash,
                        email_confirmed_at,
                        status
                    )
                    VALUES (
                        %(fantasy_name)s,
                        %(legal_name)s,
                        %(cnpj)s,
                        %(address_text)s,
                        %(contact_phone)s,
                        %(contact_email)s,
                        %(access_email)s,
                        %(notification_email)s,
                        %(password_hash)s,
                        NOW(),
                        'active'
                    )
                    RETURNING id
                    """,
                    {
                        "fantasy_name": pending[1],
                        "legal_name": pending[2],
                        "cnpj": cnpj,
                        "address_text": pending[4],
                        "contact_phone": pending[5],
                        "contact_email": pending[6],
                        "access_email": email_access,
                        "notification_email": pending[8],
                        "password_hash": pending[9],
                    },
                )
                client_id = int(cur.fetchone()[0])

                cur.execute(
                    f"""
                    INSERT INTO {schema}.subscription (
                        client_id,
                        plan_id,
                        starts_at,
                        ends_at,
                        status
                    )
                    VALUES (
                        %(client_id)s,
                        %(plan_id)s,
                        NOW(),
                        NULL,
                        'active'
                    )
                    """,
                    {
                        "client_id": client_id,
                        "plan_id": plan_id,
                    },
                )

                tenant_name = str(pending[1]).strip() or "Tenant Principal"
                tenant_slug = self._build_unique_tenant_slug(
                    cur=cur,
                    client_id=client_id,
                    desired_name=tenant_name,
                    schema=schema,
                )
                cur.execute(
                    f"""
                    INSERT INTO {schema}.tenant (
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
                    RETURNING id
                    """,
                    {
                        "client_id": client_id,
                        "name": tenant_name,
                        "slug": tenant_slug,
                    },
                )
                tenant_id = int(cur.fetchone()[0])

                cur.execute(
                    f"""
                    INSERT INTO {schema}.app_user (
                        client_id,
                        email,
                        full_name,
                        password_hash,
                        is_active
                    )
                    VALUES (
                        %(client_id)s,
                        %(email)s,
                        %(full_name)s,
                        %(password_hash)s,
                        TRUE
                    )
                    RETURNING id
                    """,
                    {
                        "client_id": client_id,
                        "email": email_access,
                        "full_name": str(pending[1]).strip() or str(pending[2]).strip(),
                        "password_hash": pending[9],
                    },
                )
                owner_user_id = int(cur.fetchone()[0])

                cur.execute(
                    f"""
                    INSERT INTO {schema}.tenant_user_role (
                        tenant_id,
                        user_id,
                        role
                    )
                    VALUES (
                        %(tenant_id)s,
                        %(user_id)s,
                        'owner'
                    )
                    """,
                    {
                        "tenant_id": tenant_id,
                        "user_id": owner_user_id,
                    },
                )

                cur.execute(
                    f"""
                    INSERT INTO {schema}.user_tenant_preference (
                        tenant_id,
                        user_id,
                        language_code,
                        theme_code
                    )
                    VALUES (
                        %(tenant_id)s,
                        %(user_id)s,
                        %(language_code)s,
                        'light'
                    )
                    """,
                    {
                        "tenant_id": tenant_id,
                        "user_id": owner_user_id,
                        "language_code": str(pending[11] or "pt-BR"),
                    },
                )

                cur.execute(
                    f"""
                    UPDATE {schema}.client_signup_pending
                       SET status = 'confirmed',
                           confirmed_at = NOW()
                     WHERE id = %(id)s
                    """,
                    {"id": pending[0]},
                )
                conn.commit()
                return {
                    "email_access": email_access,
                    "status": "active",
                    "client_id": client_id,
                    "tenant_id": tenant_id,
                    "user_id": owner_user_id,
                    "confirmed_at_epoch": int(time()),
                }
        except Exception:
            return "db_unavailable"

    def _login_user(self, *, email: str, password: str, tenant_id: int | None) -> dict:
        if not self._is_db_enabled():
            return {"error_code": "db_unavailable", "message": "Login requer IAOPS_DB_DSN configurado."}
        try:
            schema = self.signup_schema
            with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        u.id AS user_id,
                        u.client_id,
                        u.email,
                        u.full_name,
                        u.password_hash,
                        u.is_active,
                        c.status AS client_status,
                        c.email_confirmed_at
                    FROM {schema}.app_user u
                    JOIN {schema}.client c ON c.id = u.client_id
                    WHERE LOWER(u.email) = %(email)s
                    LIMIT 1
                    """,
                    {"email": email},
                )
                user = cur.fetchone()
                if not user:
                    return {"error_code": "invalid_credentials", "message": "Credenciais invalidas."}
                if not bool(user[5]):
                    return {"error_code": "user_inactive", "message": "Usuario inativo."}
                if str(user[6] or "") != "active":
                    return {"error_code": "client_inactive", "message": "Cliente inativo."}
                if user[7] is None:
                    return {"error_code": "email_unconfirmed", "message": "E-mail do cliente nao confirmado."}
                if not self._verify_password_hash(password, str(user[4] or "")):
                    return {"error_code": "invalid_credentials", "message": "Credenciais invalidas."}

                role_and_tenant = self._resolve_login_tenant(cur=cur, client_id=int(user[1]), user_id=int(user[0]), tenant_id=tenant_id)
                if not role_and_tenant:
                    return {"error_code": "tenant_not_found", "message": "Nenhum tenant ativo vinculado ao usuario."}
                resolved_tenant_id, tenant_name, role = role_and_tenant

                cur.execute(
                    f"""
                    SELECT is_enabled, totp_secret_ciphertext
                    FROM {schema}.user_mfa_config
                    WHERE user_id = %(user_id)s
                    LIMIT 1
                    """,
                    {"user_id": int(user[0])},
                )
                mfa_row = cur.fetchone()
                mfa_enabled = bool(mfa_row and mfa_row[0])
                if mfa_enabled:
                    challenge_token = token_hex(18)
                    self.mfa_login_challenges[challenge_token] = {
                        "client_id": int(user[1]),
                        "tenant_id": resolved_tenant_id,
                        "user_id": int(user[0]),
                        "email": str(user[2]),
                        "full_name": str(user[3] or ""),
                        "role": role,
                        "tenant_name": tenant_name,
                        "totp_secret_ciphertext": str(mfa_row[1] or ""),
                        "expires_at_epoch": int(time()) + (5 * 60),
                    }
                    return {
                        "mfa_required": True,
                        "challenge_token": challenge_token,
                        "expires_at_epoch": int(time()) + (5 * 60),
                    }

                session_data = self._issue_session(
                    client_id=int(user[1]),
                    tenant_id=resolved_tenant_id,
                    user_id=int(user[0]),
                    email=str(user[2]),
                    full_name=str(user[3] or ""),
                    role=role,
                    tenant_name=tenant_name,
                )
                return {
                    "mfa_required": False,
                    "auth_context": {
                        "client_id": int(user[1]),
                        "tenant_id": resolved_tenant_id,
                        "user_id": int(user[0]),
                    },
                    "profile": {
                        "email": str(user[2]),
                        "full_name": str(user[3] or ""),
                        "role": role,
                        "tenant_name": tenant_name,
                    },
                    "session": session_data,
                }
        except Exception:
            return {"error_code": "login_failed", "message": "Falha ao processar login."}

    def _verify_login_mfa(self, *, challenge_token: str, otp_code: str) -> dict:
        challenge = self.mfa_login_challenges.get(challenge_token)
        if not challenge:
            return {"error_code": "invalid_challenge", "message": "Challenge MFA invalido."}
        if int(challenge.get("expires_at_epoch") or 0) < int(time()):
            self.mfa_login_challenges.pop(challenge_token, None)
            return {"error_code": "expired_challenge", "message": "Challenge MFA expirado."}
        secret_ciphertext = str(challenge.get("totp_secret_ciphertext") or "")
        if not secret_ciphertext:
            return {"error_code": "mfa_unavailable", "message": "Segredo MFA indisponivel."}
        try:
            secret = decrypt_text(secret_ciphertext)
        except Exception:
            return {"error_code": "mfa_unavailable", "message": "Nao foi possivel validar MFA."}
        if not verify_totp(secret, otp_code):
            return {"error_code": "invalid_otp", "message": "Codigo TOTP invalido."}
        self.mfa_login_challenges.pop(challenge_token, None)
        session_data = self._issue_session(
            client_id=int(challenge["client_id"]),
            tenant_id=int(challenge["tenant_id"]),
            user_id=int(challenge["user_id"]),
            email=str(challenge["email"]),
            full_name=str(challenge["full_name"] or ""),
            role=str(challenge["role"] or "viewer"),
            tenant_name=str(challenge["tenant_name"] or ""),
        )
        return {
            "mfa_required": False,
            "auth_context": {
                "client_id": int(challenge["client_id"]),
                "tenant_id": int(challenge["tenant_id"]),
                "user_id": int(challenge["user_id"]),
            },
            "profile": {
                "email": str(challenge["email"]),
                "full_name": str(challenge["full_name"] or ""),
                "role": str(challenge["role"] or "viewer"),
                "tenant_name": str(challenge["tenant_name"] or ""),
            },
            "session": session_data,
        }

    @staticmethod
    def _verify_password_hash(password: str, encoded_hash: str) -> bool:
        value = str(encoded_hash or "").strip()
        if not value:
            return False
        if value.startswith("pbkdf2_sha256$"):
            parts = value.split("$")
            if len(parts) != 4:
                return False
            try:
                iterations = int(parts[1])
            except ValueError:
                return False
            salt = parts[2]
            expected = parts[3]
            calculated = pbkdf2_hmac(
                "sha256",
                str(password).encode("utf-8"),
                salt.encode("utf-8"),
                iterations,
            ).hex()
            return calculated == expected
        return str(password) == value

    @staticmethod
    def _resolve_login_tenant(*, cur, client_id: int, user_id: int, tenant_id: int | None) -> tuple[int, str, str] | None:
        base_sql = """
            SELECT
                t.id AS tenant_id,
                t.name AS tenant_name,
                tur.role
            FROM iaops_gov.tenant_user_role tur
            JOIN iaops_gov.tenant t ON t.id = tur.tenant_id
            WHERE tur.user_id = %(user_id)s
              AND t.client_id = %(client_id)s
              AND t.status = 'active'
        """
        if tenant_id:
            cur.execute(
                base_sql + " AND t.id = %(tenant_id)s LIMIT 1",
                {
                    "user_id": user_id,
                    "client_id": client_id,
                    "tenant_id": int(tenant_id),
                },
            )
            row = cur.fetchone()
            if not row:
                return None
            return int(row[0]), str(row[1]), str(row[2])
        cur.execute(
            base_sql
            + """
              ORDER BY CASE tur.role WHEN 'owner' THEN 3 WHEN 'admin' THEN 2 ELSE 1 END DESC, t.id
              LIMIT 1
            """,
            {
                "user_id": user_id,
                "client_id": client_id,
            },
        )
        row = cur.fetchone()
        if not row:
            return None
        return int(row[0]), str(row[1]), str(row[2])

    def _issue_session(
        self,
        *,
        client_id: int,
        tenant_id: int,
        user_id: int,
        email: str,
        full_name: str,
        role: str,
        tenant_name: str,
    ) -> dict:
        now_epoch = int(time())
        session_token = token_hex(24)
        refresh_token = token_hex(30)
        session_expires_at_epoch = now_epoch + int(self.session_ttl_seconds)
        refresh_expires_at_epoch = now_epoch + int(self.refresh_ttl_seconds)
        session_payload = {
            "client_id": int(client_id),
            "tenant_id": int(tenant_id),
            "user_id": int(user_id),
            "email": str(email),
            "full_name": str(full_name or ""),
            "role": str(role or "viewer"),
            "tenant_name": str(tenant_name or ""),
            "session_token": session_token,
            "refresh_token": refresh_token,
            "issued_at_epoch": now_epoch,
            "last_seen_epoch": now_epoch,
            "session_expires_at_epoch": session_expires_at_epoch,
            "refresh_expires_at_epoch": refresh_expires_at_epoch,
        }
        self.active_sessions[session_token] = dict(session_payload)
        self.refresh_sessions[refresh_token] = session_token
        return {
            "session_token": session_token,
            "refresh_token": refresh_token,
            "session_expires_at_epoch": session_expires_at_epoch,
            "refresh_expires_at_epoch": refresh_expires_at_epoch,
        }

    def _refresh_session(self, *, refresh_token: str) -> dict:
        self._cleanup_expired_sessions()
        session_token = self.refresh_sessions.get(refresh_token)
        if not session_token:
            return {"error_code": "invalid_refresh_token", "message": "Refresh token invalido."}
        current = self.active_sessions.get(session_token)
        if not current:
            self.refresh_sessions.pop(refresh_token, None)
            return {"error_code": "invalid_refresh_token", "message": "Sessao nao encontrada para o refresh."}
        if int(current.get("refresh_expires_at_epoch") or 0) < int(time()):
            self._invalidate_session(session_token=session_token, refresh_token=refresh_token)
            return {"error_code": "expired_refresh_token", "message": "Refresh token expirado."}
        self._invalidate_session(session_token=session_token, refresh_token=refresh_token)
        session_data = self._issue_session(
            client_id=int(current["client_id"]),
            tenant_id=int(current["tenant_id"]),
            user_id=int(current["user_id"]),
            email=str(current["email"]),
            full_name=str(current.get("full_name") or ""),
            role=str(current.get("role") or "viewer"),
            tenant_name=str(current.get("tenant_name") or ""),
        )
        return {
            "auth_context": {
                "client_id": int(current["client_id"]),
                "tenant_id": int(current["tenant_id"]),
                "user_id": int(current["user_id"]),
            },
            "profile": {
                "email": str(current["email"]),
                "full_name": str(current.get("full_name") or ""),
                "role": str(current.get("role") or "viewer"),
                "tenant_name": str(current.get("tenant_name") or ""),
            },
            "session": session_data,
        }

    def _invalidate_session(self, *, session_token: str | None, refresh_token: str | None) -> None:
        if session_token:
            data = self.active_sessions.pop(session_token, None)
            if data:
                token = str(data.get("refresh_token") or "")
                if token:
                    self.refresh_sessions.pop(token, None)
        if refresh_token:
            mapped_session = self.refresh_sessions.pop(refresh_token, None)
            if mapped_session:
                self.active_sessions.pop(mapped_session, None)

    def _cleanup_expired_sessions(self) -> None:
        now_epoch = int(time())
        expired_tokens = []
        for session_token, data in self.active_sessions.items():
            if int(data.get("refresh_expires_at_epoch") or 0) < now_epoch:
                expired_tokens.append(session_token)
                continue
            if int(data.get("session_expires_at_epoch") or 0) < now_epoch:
                refresh_token = str(data.get("refresh_token") or "")
                if refresh_token:
                    self.refresh_sessions.pop(refresh_token, None)
                expired_tokens.append(session_token)
        for session_token in expired_tokens:
            self.active_sessions.pop(session_token, None)

    def _resolve_session_context(self, session_token: str) -> dict | None:
        if not session_token:
            return None
        self._cleanup_expired_sessions()
        data = self.active_sessions.get(session_token)
        if not data:
            return None
        if int(data.get("session_expires_at_epoch") or 0) < int(time()):
            self._invalidate_session(
                session_token=session_token,
                refresh_token=str(data.get("refresh_token") or ""),
            )
            return None
        data["last_seen_epoch"] = int(time())
        self.active_sessions[session_token] = data
        return {
            "client_id": int(data["client_id"]),
            "tenant_id": int(data["tenant_id"]),
            "user_id": int(data["user_id"]),
        }

    def _resolve_context_from_tokens(self, *, session_token: str | None, refresh_token: str | None) -> dict:
        token = str(session_token or "").strip()
        if not token and refresh_token:
            mapped = self.refresh_sessions.get(str(refresh_token))
            token = str(mapped or "")
        if not token:
            return {}
        data = self.active_sessions.get(token) or {}
        if not data:
            return {}
        return {
            "client_id": data.get("client_id"),
            "tenant_id": data.get("tenant_id"),
            "user_id": data.get("user_id"),
            "email": data.get("email"),
        }

    def _list_active_sessions_for_actor(self, *, session_token: str) -> dict:
        self._cleanup_ephemeral_auth_state()
        actor = self.active_sessions.get(session_token)
        if not actor:
            return {"error_code": "invalid_session", "message": "Sessao invalida ou expirada."}
        role = self._resolve_actor_role_from_session(actor)
        allow_all_client = role in {"owner", "admin"}
        actor_user_id = int(actor.get("user_id") or 0)
        actor_client_id = int(actor.get("client_id") or 0)
        items = []
        for token, data in self.active_sessions.items():
            if int(data.get("client_id") or 0) != actor_client_id:
                continue
            if not allow_all_client and int(data.get("user_id") or 0) != actor_user_id:
                continue
            items.append(
                {
                    "session_token": token,
                    "user_id": int(data.get("user_id") or 0),
                    "email": str(data.get("email") or ""),
                    "role": str(data.get("role") or "viewer"),
                    "tenant_id": int(data.get("tenant_id") or 0),
                    "tenant_name": str(data.get("tenant_name") or ""),
                    "issued_at_epoch": int(data.get("issued_at_epoch") or 0),
                    "last_seen_epoch": int(data.get("last_seen_epoch") or 0),
                    "session_expires_at_epoch": int(data.get("session_expires_at_epoch") or 0),
                    "is_current": token == session_token,
                }
            )
        items.sort(key=lambda row: row["issued_at_epoch"], reverse=True)
        return {
            "sessions": items,
            "scope": "client" if allow_all_client else "self",
            "actor_role": role,
        }

    def _revoke_active_session_for_actor(self, *, actor_session_token: str, target_session_token: str) -> dict:
        self._cleanup_ephemeral_auth_state()
        actor = self.active_sessions.get(actor_session_token)
        target = self.active_sessions.get(target_session_token)
        if not actor or not target:
            return {"error_code": "invalid_session", "message": "Sessao nao encontrada."}
        actor_role = self._resolve_actor_role_from_session(actor)
        if int(actor.get("client_id") or 0) != int(target.get("client_id") or 0):
            return {"error_code": "forbidden", "message": "Sessao alvo fora do cliente."}
        is_self = actor_session_token == target_session_token
        if not is_self and actor_role not in {"owner", "admin"}:
            return {"error_code": "forbidden", "message": "Somente owner/admin podem revogar outras sessoes."}
        self._invalidate_session(
            session_token=target_session_token,
            refresh_token=str(target.get("refresh_token") or ""),
        )
        self._log_auth_event(
            action_code="auth.session.revoke",
            client_id=actor.get("client_id"),
            tenant_id=actor.get("tenant_id"),
            user_id=actor.get("user_id"),
            target_id=str(target.get("email") or ""),
            payload={
                "revoked_session_token": target_session_token,
                "revoked_user_id": target.get("user_id"),
                "is_self": is_self,
            },
        )
        return {"revoked": True, "session_token": target_session_token, "is_self": is_self}

    def _resolve_actor_role_from_session(self, session_data: dict) -> str:
        if not isinstance(session_data, dict):
            return "viewer"
        if not self._is_db_enabled():
            return str(session_data.get("role") or "viewer")
        try:
            with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT tur.role
                    FROM iaops_gov.tenant_user_role tur
                    WHERE tur.tenant_id = %(tenant_id)s
                      AND tur.user_id = %(user_id)s
                    LIMIT 1
                    """,
                    {
                        "tenant_id": int(session_data.get("tenant_id") or 0),
                        "user_id": int(session_data.get("user_id") or 0),
                    },
                )
                row = cur.fetchone()
                if row and row[0]:
                    return str(row[0])
        except Exception:
            pass
        return str(session_data.get("role") or "viewer")

    def _check_login_throttle(self, *, email: str, client_ip: str) -> dict | None:
        key = self._login_throttle_key(email=email, client_ip=client_ip)
        now_epoch = int(time())
        state = self.failed_login_attempts.get(key)
        if not state:
            return None
        locked_until = int(state.get("locked_until") or 0)
        if locked_until > now_epoch:
            return {"blocked_remaining_seconds": locked_until - now_epoch}
        attempts = [ts for ts in (state.get("attempts") or []) if int(ts) >= now_epoch - int(self.login_attempt_window_seconds)]
        state["attempts"] = attempts
        state["locked_until"] = 0
        if attempts:
            self.failed_login_attempts[key] = state
        else:
            self.failed_login_attempts.pop(key, None)
        return None

    def _register_failed_login(self, *, email: str, client_ip: str) -> None:
        key = self._login_throttle_key(email=email, client_ip=client_ip)
        now_epoch = int(time())
        state = self.failed_login_attempts.get(key) or {"attempts": [], "locked_until": 0}
        attempts = [ts for ts in (state.get("attempts") or []) if int(ts) >= now_epoch - int(self.login_attempt_window_seconds)]
        attempts.append(now_epoch)
        locked_until = int(state.get("locked_until") or 0)
        if len(attempts) >= int(self.login_max_attempts):
            locked_until = now_epoch + int(self.login_lock_seconds)
            attempts = []
        self.failed_login_attempts[key] = {"attempts": attempts, "locked_until": locked_until}

        ip_state = self.failed_login_attempts_ip.get(client_ip) or {"attempts": [], "locked_until": 0}
        ip_attempts = [ts for ts in (ip_state.get("attempts") or []) if int(ts) >= now_epoch - int(self.ip_throttle_window_seconds)]
        ip_attempts.append(now_epoch)
        ip_locked_until = int(ip_state.get("locked_until") or 0)
        if len(ip_attempts) >= int(self.ip_throttle_step_two_attempts):
            ip_locked_until = now_epoch + int(self.ip_throttle_step_two_lock_seconds)
            ip_attempts = []
        elif len(ip_attempts) >= int(self.ip_throttle_step_one_attempts):
            ip_locked_until = now_epoch + int(self.ip_throttle_step_one_lock_seconds)
            ip_attempts = []
        self.failed_login_attempts_ip[client_ip] = {"attempts": ip_attempts, "locked_until": ip_locked_until}

    def _clear_failed_login(self, *, email: str, client_ip: str) -> None:
        key = self._login_throttle_key(email=email, client_ip=client_ip)
        self.failed_login_attempts.pop(key, None)

    def _check_ip_login_throttle(self, *, client_ip: str) -> dict | None:
        now_epoch = int(time())
        state = self.failed_login_attempts_ip.get(client_ip)
        if not state:
            return None
        locked_until = int(state.get("locked_until") or 0)
        if locked_until > now_epoch:
            return {"blocked_remaining_seconds": locked_until - now_epoch}
        attempts = [ts for ts in (state.get("attempts") or []) if int(ts) >= now_epoch - int(self.ip_throttle_window_seconds)]
        state["attempts"] = attempts
        state["locked_until"] = 0
        if attempts:
            self.failed_login_attempts_ip[client_ip] = state
        else:
            self.failed_login_attempts_ip.pop(client_ip, None)
        return None

    @staticmethod
    def _login_throttle_key(*, email: str, client_ip: str) -> str:
        return f"{str(email or '').strip().lower()}|{str(client_ip or '').strip()}"

    def _request_ip(self) -> str:
        forwarded = str(self.headers.get("X-Forwarded-For", "") or "").strip()
        if forwarded:
            return forwarded.split(",")[0].strip()
        remote = self.client_address[0] if self.client_address else ""
        return str(remote or "").strip()

    def _cleanup_ephemeral_auth_state(self) -> None:
        now_epoch = int(time())
        expired_challenges = [
            token
            for token, data in self.mfa_login_challenges.items()
            if int(data.get("expires_at_epoch") or 0) <= now_epoch
        ]
        for token in expired_challenges:
            self.mfa_login_challenges.pop(token, None)
        self._cleanup_expired_sessions()

        for key, state in list(self.failed_login_attempts.items()):
            locked_until = int(state.get("locked_until") or 0)
            attempts = [ts for ts in (state.get("attempts") or []) if int(ts) >= now_epoch - int(self.login_attempt_window_seconds)]
            if locked_until > now_epoch or attempts:
                self.failed_login_attempts[key] = {"attempts": attempts, "locked_until": locked_until if locked_until > now_epoch else 0}
            else:
                self.failed_login_attempts.pop(key, None)

        for ip, state in list(self.failed_login_attempts_ip.items()):
            locked_until = int(state.get("locked_until") or 0)
            attempts = [ts for ts in (state.get("attempts") or []) if int(ts) >= now_epoch - int(self.ip_throttle_window_seconds)]
            if locked_until > now_epoch or attempts:
                self.failed_login_attempts_ip[ip] = {"attempts": attempts, "locked_until": locked_until if locked_until > now_epoch else 0}
            else:
                self.failed_login_attempts_ip.pop(ip, None)

    def _log_auth_event(
        self,
        *,
        action_code: str,
        client_id: int | None,
        tenant_id: int | None,
        user_id: int | None,
        target_id: str | None,
        payload: dict | None,
    ) -> None:
        if not self._is_db_enabled():
            return
        try:
            schema = self.signup_schema
            with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {schema}.audit_log (
                        client_id,
                        tenant_id,
                        user_id,
                        action_code,
                        target_type,
                        target_id,
                        payload_json
                    )
                    VALUES (
                        %(client_id)s,
                        %(tenant_id)s,
                        %(user_id)s,
                        %(action_code)s,
                        'auth',
                        %(target_id)s,
                        %(payload_json)s::jsonb
                    )
                    """,
                    {
                        "client_id": int(client_id) if client_id else None,
                        "tenant_id": int(tenant_id) if tenant_id else None,
                        "user_id": int(user_id) if user_id else None,
                        "action_code": action_code,
                        "target_id": str(target_id) if target_id else None,
                        "payload_json": json.dumps(payload or {}, ensure_ascii=True),
                    },
                )
                conn.commit()
        except Exception:
            # auditoria nao deve bloquear fluxo de autenticacao
            return

    @staticmethod
    def _slugify_text(value: str) -> str:
        normalized = re.sub(r"[^a-zA-Z0-9]+", "-", str(value or "").strip().lower())
        normalized = re.sub(r"-+", "-", normalized).strip("-")
        return normalized or "tenant-principal"

    def _build_unique_tenant_slug(self, *, cur, client_id: int, desired_name: str, schema: str) -> str:
        base = self._slugify_text(desired_name)
        for index in range(0, 100):
            slug = base if index == 0 else f"{base}-{index + 1}"
            cur.execute(
                f"""
                SELECT 1
                FROM {schema}.tenant
                WHERE client_id = %(client_id)s
                  AND slug = %(slug)s
                LIMIT 1
                """,
                {
                    "client_id": client_id,
                    "slug": slug,
                },
            )
            if not cur.fetchone():
                return slug
        return f"{base}-{token_hex(2)}"

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
        language_code = self._resolve_language_code(context)
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
                return self._channel_error_response(result, language_code)
            data = result["data"]
            return {
                "ok": True,
                "command": "tenant_list",
                "reply_text": self._reply_tenant_list(data, language_code),
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
                return self._channel_error_response(result, language_code)
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
            active_lang = self._resolve_language_code_from_active(context, active_data, language_code)
            return {
                "ok": True,
                "command": "tenant_select",
                "reply_text": self._reply_active_tenant(active_data, active_lang),
                "data": {
                    "selection": result["data"].get("selection"),
                    "active": active_data,
                },
            }

        if kind == "tenant_select_guess":
            listed = self._call_mcp(
                {
                    "context": context,
                    "tool": "channel.list_user_tenants",
                    "input": {
                        "channel_type": channel_type,
                        "external_user_key": external_user_key,
                    },
                }
            )
            if listed["status"] != "success":
                return self._channel_error_response(listed, language_code)
            listed_data = listed["data"]
            resolved_tenant_id = self._resolve_tenant_candidate(command.get("selection_text", ""), listed_data.get("tenants") or [])
            if resolved_tenant_id is None:
                return {
                    "ok": False,
                    "command": "tenant_select",
                    "reply_text": "\n".join(
                        [
                            self._t(language_code, "tenant_selection_not_understood"),
                            self._reply_tenant_list(listed_data, language_code),
                        ]
                    ),
                    "data": {"tenants": listed_data.get("tenants") or []},
                }
            result = self._call_mcp(
                {
                    "context": context,
                    "tool": "channel.set_active_tenant",
                    "input": {
                        "channel_type": channel_type,
                        "conversation_key": conversation_key,
                        "external_user_key": external_user_key,
                        "tenant_id": resolved_tenant_id,
                    },
                }
            )
            if result["status"] != "success":
                return self._channel_error_response(result, language_code)
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
            active_lang = self._resolve_language_code_from_active(context, active_data, language_code)
            return {
                "ok": True,
                "command": "tenant_select",
                "reply_text": self._reply_active_tenant(active_data, active_lang),
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
                return self._channel_error_response(result, language_code)
            active_lang = self._resolve_language_code_from_active(context, result["data"], language_code)
            return {
                "ok": True,
                "command": "tenant_active",
                "reply_text": self._reply_active_tenant(result["data"], active_lang),
                "data": result["data"],
            }

        if kind == "sql_query":
            return {
                "ok": False,
                "command": "error",
                "reply_text": self._t(language_code, "sql_not_allowed"),
                "data": {},
            }

        if kind == "nl_query":
            runtime = self._resolve_channel_runtime_context(
                context=context,
                channel_type=channel_type,
                external_user_key=external_user_key,
                conversation_key=conversation_key,
                message_text=command["question_text"],
                language_code=language_code,
            )
            if runtime.get("error"):
                return runtime["error"]
            runtime_language = self._resolve_language_code(runtime["context"])
            nl_response = self._execute_nl_chat_query(runtime["context"], command["question_text"], runtime_language)
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
            "reply_text": self._reply_help(message_text, language_code),
            "data": {},
        }

    def _resolve_channel_runtime_context(
        self,
        *,
        context: dict,
        channel_type: str,
        external_user_key: str,
        conversation_key: str,
        message_text: str = "",
        language_code: str = "pt-BR",
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
            return {"error": self._channel_error_response(active, language_code)}
        active_data = active["data"]
        active_tenant_id = active_data.get("active_tenant_id")
        user = active_data.get("user") or {}
        active_user_id = user.get("user_id")
        if active_tenant_id is None:
            tenants_data = self._call_mcp(
                {
                    "context": context,
                    "tool": "channel.list_user_tenants",
                    "input": {
                        "channel_type": channel_type,
                        "external_user_key": external_user_key,
                    },
                }
            )
            if tenants_data["status"] != "success":
                return {"error": self._channel_error_response(tenants_data, language_code)}
            listed_data = tenants_data["data"]
            guessed_tenant_id = self._resolve_tenant_candidate(message_text, listed_data.get("tenants") or [])
            if guessed_tenant_id is not None:
                selected = self._call_mcp(
                    {
                        "context": context,
                        "tool": "channel.set_active_tenant",
                        "input": {
                            "channel_type": channel_type,
                            "conversation_key": conversation_key,
                            "external_user_key": external_user_key,
                            "tenant_id": guessed_tenant_id,
                        },
                    }
                )
                if selected["status"] == "success":
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
                    if active["status"] == "success":
                        active_data = active["data"]
                        active_tenant_id = active_data.get("active_tenant_id")
                        user = active_data.get("user") or {}
                        active_user_id = user.get("user_id")
            if active_tenant_id is None:
                no_tenant_reply = "\n".join(
                    [
                        self._t(language_code, "no_active_tenant_conversation"),
                        self._reply_tenant_list(listed_data, language_code),
                    ]
                )
                return {
                    "error": {
                        "ok": False,
                        "command": "error",
                        "reply_text": no_tenant_reply,
                        "data": {"active": active_data, "tenants": listed_data.get("tenants") or []},
                    }
                }
        if active_tenant_id is None:
            return {
                "error": {
                    "ok": False,
                    "command": "error",
                    "reply_text": self._t(language_code, "no_active_tenant_conversation"),
                    "data": {"active": active_data},
                }
            }
        if active_user_id is None:
            return {
                "error": {
                    "ok": False,
                    "command": "error",
                    "reply_text": self._t(language_code, "channel_user_not_identified"),
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
        if re.match(r"^/?(?:select|selecionar|seleccione|escolher|choose|usar|use|trocar|cambiar)\s+", normalized):
            return {"kind": "tenant_select_guess", "selection_text": raw}
        if re.match(r"^/?(?:tenant|tenants?)\s+.+$", normalized):
            return {"kind": "tenant_select_guess", "selection_text": raw}
        if re.match(r"^\d+$", normalized):
            return {"kind": "tenant_select_guess", "selection_text": raw}
        if re.match(r"^/?sql\s+", raw, flags=re.IGNORECASE):
            return {"kind": "sql_query"}
        return {"kind": "nl_query", "question_text": raw}

    def _channel_error_response(self, result: dict, language_code: str = "pt-BR") -> dict:
        message = (
            result.get("error", {}).get("message")
            or self._t(language_code, "channel_command_failed")
        )
        return {
            "ok": False,
            "command": "error",
            "reply_text": self._t(language_code, "error_prefix", message=message),
            "data": {"mcp": result},
        }

    def _reply_tenant_list(self, data: dict, language_code: str) -> str:
        tenants = data.get("tenants") or []
        user = data.get("user") or {}
        lines = []
        lines.append(self._t(language_code, "user_label", value=(user.get("full_name") or user.get("email") or "n/a")))
        lines.append(self._t(language_code, "available_tenants"))
        for item in tenants:
            status_label = self._t(language_code, "tenant_status_label", value=item.get("status"))
            role_label = self._t(language_code, "tenant_role_label", value=item.get("role"))
            lines.append(
                f"- {item.get('tenant_id')}: {item.get('name')} ({status_label}; {role_label})"
            )
        lines.append(self._t(language_code, "hint_tenant_pick_natural"))
        lines.append(self._t(language_code, "hint_tenant_select"))
        lines.append(self._t(language_code, "hint_tenant_active"))
        return "\n".join(lines)

    def _reply_active_tenant(self, data: dict, language_code: str) -> str:
        active_tenant_id = data.get("active_tenant_id")
        tenants = data.get("tenants") or []
        if active_tenant_id is None:
            return self._t(language_code, "no_active_tenant")
        selected = next(
            (item for item in tenants if int(item.get("tenant_id", -1)) == int(active_tenant_id)),
            None,
        )
        if not selected:
            return self._t(language_code, "active_tenant_id_only", tenant_id=active_tenant_id)
        return self._t(
            language_code,
            "active_tenant_full",
            tenant_id=selected.get("tenant_id"),
            tenant_name=selected.get("name"),
            tenant_status=selected.get("status"),
        )

    def _reply_help(self, message_text: str, language_code: str) -> str:
        prefix = self._t(language_code, "unknown_command", value=message_text) if message_text else ""
        return (
            f"{prefix}{self._t(language_code, 'available_commands')}\n"
            "tenant list\n"
            "tenant select <id>\n"
            "tenant active\n"
            f"{self._t(language_code, 'help_keyword')}\n"
            f"{self._t(language_code, 'ask_natural_language')}"
        )

    @staticmethod
    def _normalize_lookup_text(value: str) -> str:
        if not value:
            return ""
        normalized = unicodedata.normalize("NFKD", value)
        no_accent = "".join(ch for ch in normalized if not unicodedata.combining(ch))
        cleaned = re.sub(r"[^a-zA-Z0-9]+", " ", no_accent.lower())
        return " ".join(cleaned.split())

    def _resolve_tenant_candidate(self, selection_text: str, tenants: list[dict]) -> int | None:
        normalized = self._normalize_lookup_text(selection_text)
        if not normalized or not tenants:
            return None

        id_match = re.search(r"(?:^|\\s)(\\d+)(?:$|\\s)", normalized)
        if id_match:
            tenant_id = int(id_match.group(1))
            if any(int(item.get("tenant_id", -1)) == tenant_id for item in tenants):
                return tenant_id

        matches: list[int] = []
        for item in tenants:
            tenant_name = self._normalize_lookup_text(str(item.get("name") or ""))
            if not tenant_name:
                continue
            if normalized == tenant_name or tenant_name in normalized:
                matches.append(int(item.get("tenant_id")))
        unique_matches = sorted(set(matches))
        if len(unique_matches) == 1:
            return unique_matches[0]
        return None

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

    def _execute_nl_chat_query(self, context: dict, question_text: str, language_code: str | None = None) -> dict:
        resolved_language = language_code or self._resolve_language_code(context)
        response_mode = self._resolve_chat_response_mode(context)
        rag = self._build_rag_context(context)
        planned = self._plan_sql_from_question(context, question_text, rag)
        sql_text = planned.get("sql_text")
        if not sql_text:
            return {
                "ok": False,
                "command": "error",
                "reply_text": self._t(resolved_language, "could_not_interpret_question"),
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
            return self._channel_error_response(query_result, resolved_language)
        query_data = query_result["data"]
        return {
            "ok": True,
            "reply_text": self._reply_nl_result(
                question_text, sql_text, query_data, rag, response_mode, resolved_language
            ),
            "data": {
                "question_text": question_text,
                "planned_sql": sql_text,
                "planning_mode": planned.get("mode", "rules"),
                "llm_provider": planned.get("llm_provider"),
                "language_code": resolved_language,
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

    def _resolve_language_code(self, context: dict) -> str:
        result = self._call_mcp(
            {
                "context": context,
                "tool": "pref.get_user_tenant",
                "input": {},
            }
        )
        if result.get("status") != "success":
            return "pt-BR"
        pref = (result.get("data") or {}).get("preference") or {}
        lang = str(pref.get("language_code") or "pt-BR").strip()
        return lang or "pt-BR"

    def _resolve_language_code_from_active(
        self,
        base_context: dict,
        active_data: dict,
        fallback_language: str = "pt-BR",
    ) -> str:
        user = active_data.get("user") or {}
        active_tenant_id = active_data.get("active_tenant_id")
        if active_tenant_id is None or user.get("user_id") is None:
            return fallback_language
        runtime_context = {
            "client_id": int(base_context["client_id"]),
            "tenant_id": int(active_tenant_id),
            "user_id": int(user["user_id"]),
            "correlation_id": str(uuid.uuid4()),
        }
        return self._resolve_language_code(runtime_context)

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

    def _reply_nl_result(
        self,
        question_text: str,
        sql_text: str,
        query_data: dict,
        rag: dict,
        response_mode: str,
        language_code: str,
    ) -> str:
        rows = query_data.get("rows") or []
        columns = query_data.get("columns") or []
        lines = []
        lines.append(self._t(language_code, "question_label", value=question_text))
        lines.append(self._t(language_code, "found_records", total=len(rows)))
        if rows and "total" in columns and isinstance(rows[0], dict) and "total" in rows[0]:
            lines.append(self._t(language_code, "total_label", value=rows[0]["total"]))
        if rows and response_mode == "detailed":
            lines.append(self._t(language_code, "preview_label"))
            for idx, row in enumerate(rows[:5], start=1):
                lines.append(f"{idx}. {json.dumps(row, ensure_ascii=True)}")
        elif rows:
            lines.append(self._t(language_code, "executive_summary"))
        tables = rag.get("tables") or []
        if tables and response_mode == "detailed":
            lines.append(self._t(language_code, "rag_context_tables", total=len(tables)))
        return "\n".join(lines)

    @staticmethod
    def _t(language_code: str, key: str, **kwargs: object) -> str:
        lang = str(language_code or "pt-BR").lower()
        bucket = "pt"
        if lang.startswith("en"):
            bucket = "en"
        elif lang.startswith("es"):
            bucket = "es"
        texts = {
            "pt": {
                "sql_not_allowed": "Comando SQL nao e aceito. Envie a pergunta em linguagem natural.",
                "no_active_tenant_conversation": "Nenhum tenant ativo na conversa. Use: tenant list e tenant select <id>.",
                "channel_user_not_identified": "Usuario do canal nao identificado.",
                "channel_command_failed": "Nao foi possivel processar o comando no canal.",
                "error_prefix": "Erro: {message}",
                "user_label": "Usuario: {value}",
                "available_tenants": "Tenants disponiveis:",
                "available_commands": "Comandos disponiveis:",
                "tenant_selection_not_understood": "Nao consegui identificar o tenant. Escolha pelo numero ou nome.",
                "tenant_status_label": "status={value}",
                "tenant_role_label": "perfil={value}",
                "hint_tenant_pick_natural": "Dica: responda com o numero do tenant ou com o nome (ex.: Comercial).",
                "hint_tenant_select": "Use: tenant select <id>",
                "hint_tenant_active": "Use: tenant active",
                "no_active_tenant": "Nenhum tenant ativo para esta conversa. Use: tenant list",
                "active_tenant_id_only": "Tenant ativo: {tenant_id}",
                "active_tenant_full": "Tenant ativo: {tenant_id} - {tenant_name} ({tenant_status})",
                "unknown_command": "Comando nao reconhecido: '{value}'.\n",
                "help_keyword": "ajuda",
                "ask_natural_language": "Ou envie a pergunta em linguagem natural.",
                "chat_question_required": "question_text obrigatorio",
                "could_not_interpret_question": "Nao consegui interpretar sua pergunta com as tabelas monitoradas.",
                "question_label": "Pergunta: {value}",
                "found_records": "Encontrei {total} registro(s).",
                "total_label": "Total: {value}",
                "preview_label": "Preview:",
                "executive_summary": "Resumo executivo: dados encontrados e prontos para detalhamento.",
                "rag_context_tables": "Contexto RAG: {total} tabela(s) monitorada(s) analisadas.",
            },
            "en": {
                "sql_not_allowed": "SQL command is not accepted. Send your question in natural language.",
                "no_active_tenant_conversation": "No active tenant in this conversation. Use: tenant list and tenant select <id>.",
                "channel_user_not_identified": "Channel user not identified.",
                "channel_command_failed": "Could not process channel command.",
                "error_prefix": "Error: {message}",
                "user_label": "User: {value}",
                "available_tenants": "Available tenants:",
                "available_commands": "Available commands:",
                "tenant_selection_not_understood": "I could not identify the tenant. Choose by number or name.",
                "tenant_status_label": "status={value}",
                "tenant_role_label": "role={value}",
                "hint_tenant_pick_natural": "Tip: reply with tenant number or name (e.g., Sales).",
                "hint_tenant_select": "Use: tenant select <id>",
                "hint_tenant_active": "Use: tenant active",
                "no_active_tenant": "No active tenant for this conversation. Use: tenant list",
                "active_tenant_id_only": "Active tenant: {tenant_id}",
                "active_tenant_full": "Active tenant: {tenant_id} - {tenant_name} ({tenant_status})",
                "unknown_command": "Unknown command: '{value}'.\n",
                "help_keyword": "help",
                "ask_natural_language": "Or send your question in natural language.",
                "chat_question_required": "question_text is required",
                "could_not_interpret_question": "I could not interpret your question using monitored tables.",
                "question_label": "Question: {value}",
                "found_records": "I found {total} record(s).",
                "total_label": "Total: {value}",
                "preview_label": "Preview:",
                "executive_summary": "Executive summary: data found and ready for drill-down.",
                "rag_context_tables": "RAG context: {total} monitored table(s) analyzed.",
            },
            "es": {
                "sql_not_allowed": "No se acepta comando SQL. Envie su pregunta en lenguaje natural.",
                "no_active_tenant_conversation": "No hay tenant activo en esta conversacion. Use: tenant list y tenant select <id>.",
                "channel_user_not_identified": "Usuario del canal no identificado.",
                "channel_command_failed": "No fue posible procesar el comando del canal.",
                "error_prefix": "Error: {message}",
                "user_label": "Usuario: {value}",
                "available_tenants": "Tenants disponibles:",
                "available_commands": "Comandos disponibles:",
                "tenant_selection_not_understood": "No pude identificar el tenant. Elija por numero o nombre.",
                "tenant_status_label": "estado={value}",
                "tenant_role_label": "rol={value}",
                "hint_tenant_pick_natural": "Sugerencia: responda con numero o nombre del tenant (ej.: Comercial).",
                "hint_tenant_select": "Use: tenant select <id>",
                "hint_tenant_active": "Use: tenant active",
                "no_active_tenant": "No hay tenant activo para esta conversacion. Use: tenant list",
                "active_tenant_id_only": "Tenant activo: {tenant_id}",
                "active_tenant_full": "Tenant activo: {tenant_id} - {tenant_name} ({tenant_status})",
                "unknown_command": "Comando no reconocido: '{value}'.\n",
                "help_keyword": "ayuda",
                "ask_natural_language": "O envie su pregunta en lenguaje natural.",
                "chat_question_required": "question_text es obligatorio",
                "could_not_interpret_question": "No pude interpretar su pregunta con las tablas monitoreadas.",
                "question_label": "Pregunta: {value}",
                "found_records": "Encontre {total} registro(s).",
                "total_label": "Total: {value}",
                "preview_label": "Vista previa:",
                "executive_summary": "Resumen ejecutivo: datos encontrados y listos para detalle.",
                "rag_context_tables": "Contexto RAG: {total} tabla(s) monitoreada(s) analizadas.",
            },
        }
        template = texts.get(bucket, texts["pt"]).get(key, texts["pt"].get(key, key))
        return template.format(**kwargs)

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
        request_context = self._request_context()
        language_code = self._resolve_language_code(request_context)
        question_text = str(body.get("question_text") or body.get("question") or "").strip()
        if not question_text:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "chat-bi.query",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_input", "message": self._t(language_code, "chat_question_required")},
                },
            )
            return
        response = self._execute_nl_chat_query(request_context, question_text, language_code)
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
        context = payload.get("context") if isinstance(payload, dict) else None
        if isinstance(context, dict) and context.get("invalid_session"):
            self._send_json(
                HTTPStatus.UNAUTHORIZED,
                {
                    "status": "denied",
                    "tool": payload.get("tool"),
                    "correlation_id": context.get("correlation_id"),
                    "data": {},
                    "error": {"code": "invalid_session", "message": "Sessao invalida ou expirada. Faca login novamente."},
                },
            )
            return
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
        session_token = str(self.headers.get("X-Session-Token", "") or "").strip()
        if session_token:
            session_context = self._resolve_session_context(session_token)
            if not session_context:
                return {
                    "client_id": 0,
                    "tenant_id": 0,
                    "user_id": 0,
                    "correlation_id": self.headers.get("X-Correlation-Id", str(uuid.uuid4())),
                    "invalid_session": True,
                }
            return {
                "client_id": int(session_context["client_id"]),
                "tenant_id": int(session_context["tenant_id"]),
                "user_id": int(session_context["user_id"]),
                "correlation_id": self.headers.get("X-Correlation-Id", str(uuid.uuid4())),
            }
        return {
            "client_id": int(self.headers.get("X-Client-Id", "1")),
            "tenant_id": int(self.headers.get("X-Tenant-Id", "10")),
            "user_id": int(self.headers.get("X-User-Id", "100")),
            "correlation_id": self.headers.get("X-Correlation-Id", str(uuid.uuid4())),
        }

    @staticmethod
    def _get_db_dsn() -> str | None:
        explicit = os.getenv("IAOPS_DB_DSN")
        if explicit:
            return explicit
        host = os.getenv("IAOPS_DB_HOST") or os.getenv("PGHOST")
        user = os.getenv("IAOPS_DB_USER") or os.getenv("PGUSER")
        password = os.getenv("IAOPS_DB_PASSWORD") or os.getenv("PGPASSWORD")
        dbname = os.getenv("IAOPS_DB_NAME") or os.getenv("PGDATABASE")
        port = os.getenv("IAOPS_DB_PORT") or os.getenv("PGPORT") or "5432"
        if host and user and dbname:
            if password:
                return f"host={host} port={port} dbname={dbname} user={user} password={password}"
            return f"host={host} port={port} dbname={dbname} user={user}"
        return None

    @classmethod
    def _is_db_enabled(cls) -> bool:
        return bool(connect is not None and cls._get_db_dsn())

    @classmethod
    def _ensure_signup_tables(cls) -> None:
        if cls.signup_tables_ready:
            return
        dsn = cls._get_db_dsn()
        if not dsn or connect is None:
            return
        schema = cls.signup_schema
        with connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.client_signup_pending (
                    id BIGSERIAL PRIMARY KEY,
                    confirm_token TEXT NOT NULL UNIQUE,
                    trade_name TEXT NOT NULL,
                    legal_name TEXT NOT NULL,
                    cnpj TEXT NOT NULL,
                    address_text TEXT NOT NULL,
                    phone_contact TEXT NOT NULL,
                    email_contact TEXT NOT NULL,
                    email_access TEXT NOT NULL,
                    email_notification TEXT NOT NULL,
                    password_hash TEXT NOT NULL,
                    plan_code TEXT NOT NULL,
                    language_code TEXT NOT NULL DEFAULT 'pt-BR',
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    expires_at TIMESTAMPTZ NOT NULL,
                    confirmed_at TIMESTAMPTZ
                )
                """
            )
            cur.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_client_signup_pending_email
                    ON {schema}.client_signup_pending (LOWER(email_access))
                """
            )
            cur.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_client_signup_pending_cnpj
                    ON {schema}.client_signup_pending (cnpj)
                """
            )
            conn.commit()
        cls.signup_tables_ready = True

    def _read_json_body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    @staticmethod
    def _send_signup_email(*, to_email: str, trade_name: str, confirm_token: str) -> tuple[bool, str]:
        smtp_host = os.getenv("IAOPS_SMTP_HOST") or os.getenv("SMTP_HOST")
        smtp_port = int(os.getenv("IAOPS_SMTP_PORT") or os.getenv("SMTP_PORT") or "587")
        smtp_user = os.getenv("IAOPS_SMTP_USER") or os.getenv("SMTP_USER") or ""
        smtp_pass = os.getenv("IAOPS_SMTP_PASS") or os.getenv("SMTP_PASS") or ""
        smtp_from = os.getenv("IAOPS_SMTP_FROM") or os.getenv("SMTP_FROM") or smtp_user
        smtp_tls = str(os.getenv("IAOPS_SMTP_STARTTLS") or "1").strip() not in {"0", "false", "False"}

        if not smtp_host or not smtp_from:
            return False, "SMTP nao configurado no ambiente de desenvolvimento."

        subject = "IAOps Governance - Confirmacao de cadastro"
        body = (
            f"Ola, {trade_name}.\n\n"
            "Seu cadastro foi recebido. Use o token abaixo para confirmar o acesso no app:\n\n"
            f"{confirm_token}\n\n"
            "Se voce nao solicitou este cadastro, ignore esta mensagem."
        )
        message = EmailMessage()
        message["Subject"] = subject
        message["From"] = smtp_from
        message["To"] = to_email
        message.set_content(body)
        try:
            with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as smtp:
                if smtp_tls:
                    smtp.starttls()
                if smtp_user:
                    smtp.login(smtp_user, smtp_pass)
                smtp.send_message(message)
            return True, f"Token enviado para {to_email}."
        except Exception:
            return False, "Falha no envio por SMTP. Token retornado no payload para uso em desenvolvimento."

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
