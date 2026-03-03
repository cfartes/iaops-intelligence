from __future__ import annotations

import json
import os
import re
import csv
import io
import datetime as dt
import decimal
import socket
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
from urllib.parse import parse_qs, quote_plus, urlparse

from functions import handle_request
from iaops.jobs import get_job_queue
from iaops.jobs.pipeline import search_rag_documents
from iaops.security.crypto import decrypt_text, encrypt_text
from iaops.security.totp import generate_base32_secret, provisioning_uri, verify_totp
try:
    from psycopg import connect
except Exception:  # pragma: no cover - depende de ambiente
    connect = None
try:
    import pyodbc
except Exception:  # pragma: no cover - opcional
    pyodbc = None
try:
    import pymysql
except Exception:  # pragma: no cover - opcional
    pymysql = None
try:
    import oracledb
except Exception:  # pragma: no cover - opcional
    oracledb = None


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
    pending_password_resets: dict[str, dict] = {}
    mfa_login_challenges: dict[str, dict] = {}
    active_sessions: dict[str, dict] = {}
    refresh_sessions: dict[str, str] = {}
    failed_login_attempts: dict[str, dict] = {}
    failed_login_attempts_ip: dict[str, dict] = {}
    route_rate_limits: dict[str, dict] = {}
    chat_rate_limit_window_seconds = 60
    chat_rate_limit_max_calls = 20
    chat_rate_limit_lock_seconds = 60
    jobs_rate_limit_window_seconds = 60
    jobs_rate_limit_max_calls = 15
    jobs_rate_limit_lock_seconds = 60
    local_env_loaded = False
    smtp_runtime_config: dict[str, object] = {}
    lgpd_schema_ready = False
    billing_plan_limits_ready = False
    smtp_table_ready = False

    @staticmethod
    def _build_signup_confirm_link(confirm_token: str) -> str:
        base = (os.getenv("IAOPS_SIGNUP_CONFIRM_URL_BASE") or "http://127.0.0.1:8000/api/auth/confirm-link").strip()
        sep = "&" if "?" in base else "?"
        return f"{base}{sep}confirm_token={quote_plus(str(confirm_token or '').strip())}"

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
        if parsed.path == "/api/security-mcp/policies":
            self._handle_security_mcp_policies_list()
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
        if parsed.path == "/api/admin/llm/models":
            self._handle_admin_llm_models_list(parsed.query)
            return
        if parsed.path == "/api/admin/llm/config":
            self._handle_admin_llm_config_get()
            return
        if parsed.path == "/api/admin/smtp/config":
            self._handle_admin_smtp_config_get()
            return
        if parsed.path == "/api/tenant-llm/providers":
            self._handle_tenant_llm_providers_list()
            return
        if parsed.path == "/api/tenant-llm/models":
            self._handle_tenant_llm_models_list(parsed.query)
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
        if parsed.path == "/api/auth/confirm-link":
            self._handle_auth_confirm_link(parsed.query)
            return
        if parsed.path == "/api/lgpd/policy":
            self._handle_lgpd_policy_get()
            return
        if parsed.path == "/api/lgpd/rules":
            self._handle_lgpd_rules_list()
            return
        if parsed.path == "/api/lgpd/dsr":
            self._handle_lgpd_dsr_list(parsed.query)
            return
        if parsed.path == "/api/billing/plans":
            self._handle_billing_plans_list()
            return
        if parsed.path == "/api/billing/subscription":
            self._handle_billing_subscription_get()
            return
        if parsed.path == "/api/billing/installments":
            self._handle_billing_installments_list(parsed.query)
            return
        if parsed.path == "/api/billing/llm-usage":
            self._handle_billing_llm_usage(parsed.query)
            return
        if parsed.path == "/api/billing/llm-usage.csv":
            self._handle_billing_llm_usage_export_csv(parsed.query)
            return
        if parsed.path == "/api/jobs":
            self._handle_jobs_list(parsed.query)
            return
        if parsed.path == "/api/observability/metrics":
            self._handle_observability_metrics()
            return
        if parsed.path == "/api/mcp/connections":
            self._handle_mcp_connections_list()
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
        if parsed.path == "/api/auth/password/request":
            self._handle_auth_password_request()
            return
        if parsed.path == "/api/auth/password/reset":
            self._handle_auth_password_reset()
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
        if parsed.path == "/api/data-sources/test-connection":
            self._handle_data_source_test_connection()
            return
        if parsed.path == "/api/data-sources/discover-tables":
            self._handle_data_source_discover_tables()
            return
        if parsed.path == "/api/data-sources/discover-columns":
            self._handle_data_source_discover_columns()
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
        if parsed.path == "/api/onboarding/monitored-columns/enrich":
            self._handle_onboarding_monitored_columns_enrich()
            return
        if parsed.path == "/api/onboarding/monitored-columns/confirm-description":
            self._handle_onboarding_monitored_column_confirm_description()
            return
        if parsed.path == "/api/onboarding/monitored-columns/update":
            self._handle_onboarding_monitored_column_update()
            return
        if parsed.path == "/api/onboarding/monitored-columns/delete":
            self._handle_onboarding_monitored_column_delete()
            return
        if parsed.path == "/api/security-sql/policy":
            self._handle_security_sql_policy_update()
            return
        if parsed.path == "/api/security-mcp/policies":
            self._handle_security_mcp_policy_update()
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
        if parsed.path == "/api/admin/smtp/config":
            self._handle_admin_smtp_config_update()
            return
        if parsed.path == "/api/admin/smtp/test":
            self._handle_admin_smtp_test()
            return
        if parsed.path == "/api/admin/smtp/send-test":
            self._handle_admin_smtp_send_test()
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
        if parsed.path == "/api/lgpd/policy":
            self._handle_lgpd_policy_upsert()
            return
        if parsed.path == "/api/lgpd/rules":
            self._handle_lgpd_rule_upsert()
            return
        if parsed.path == "/api/lgpd/dsr":
            self._handle_lgpd_dsr_open()
            return
        if parsed.path == "/api/lgpd/dsr/resolve":
            self._handle_lgpd_dsr_resolve()
            return
        if parsed.path == "/api/billing/subscription":
            self._handle_billing_subscription_upsert()
            return
        if parsed.path == "/api/billing/plans/upsert":
            self._handle_billing_plan_upsert()
            return
        if parsed.path == "/api/billing/plans/delete":
            self._handle_billing_plan_delete()
            return
        if parsed.path == "/api/billing/installments/generate":
            self._handle_billing_installments_generate()
            return
        if parsed.path == "/api/billing/installments/pay":
            self._handle_billing_installment_pay()
            return
        if parsed.path == "/api/jobs/ingest-metadata":
            self._handle_jobs_enqueue("ingest_metadata")
            return
        if parsed.path == "/api/jobs/rag-rebuild":
            self._handle_jobs_enqueue("rag_rebuild")
            return
        if parsed.path == "/api/jobs/monitor-scan":
            self._handle_jobs_enqueue("monitor_scan")
            return
        if parsed.path == "/api/jobs/billing-cycle":
            self._handle_jobs_enqueue("billing_cycle")
            return
        if parsed.path == "/api/jobs/housekeeping":
            self._handle_jobs_enqueue("housekeeping")
            return
        if parsed.path == "/api/jobs/retry":
            self._handle_jobs_retry()
            return
        if parsed.path == "/api/mcp/connections":
            self._handle_mcp_connection_upsert()
            return
        if parsed.path == "/api/mcp/connections/status":
            self._handle_mcp_connection_status_update()
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
                "rag_enabled": bool(body.get("rag_enabled", False)),
                "rag_context_text": body.get("rag_context_text"),
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
                "rag_enabled": body.get("rag_enabled"),
                "rag_context_text": body.get("rag_context_text"),
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

    def _handle_data_source_test_connection(self) -> None:
        body = self._read_json_body()
        source_type = str(body.get("source_type") or "").strip().lower()
        if not source_type:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "source.test_connection",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_input", "message": "source_type obrigatorio"},
                },
            )
            return
        try:
            profile = self._extract_connection_profile(
                conn_secret_ref=str(body.get("conn_secret_ref") or ""),
                secret_payload=body.get("secret_payload"),
            )
            test_result = self._run_connection_test(source_type=source_type, profile=profile)
            self._send_json(
                HTTPStatus.OK if test_result.get("ok") else HTTPStatus.BAD_REQUEST,
                {
                    "status": "success" if test_result.get("ok") else "denied",
                    "tool": "source.test_connection",
                    "correlation_id": str(uuid.uuid4()),
                    "data": test_result,
                    "error": None
                    if test_result.get("ok")
                    else {"code": "connection_failed", "message": str(test_result.get("message") or "Falha na conexao")},
                },
            )
        except ValueError as exc:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "source.test_connection",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_input", "message": str(exc)},
                },
            )

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

    def _handle_onboarding_monitored_column_confirm_description(self) -> None:
        context = self._request_context()
        if context.get("invalid_session"):
            self._send_json(
                HTTPStatus.UNAUTHORIZED,
                {
                    "status": "denied",
                    "tool": "inventory.confirm_column_description",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_session", "message": "Sessao invalida ou expirada. Faca login novamente."},
                },
            )
            return
        if not self._is_db_enabled():
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "inventory.confirm_column_description",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "db_unavailable", "message": "Banco nao configurado para confirmar descricao."},
                },
            )
            return
        body = self._read_json_body()
        monitored_column_id = body.get("monitored_column_id")
        if monitored_column_id is None:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "inventory.confirm_column_description",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_input", "message": "monitored_column_id obrigatorio"},
                },
            )
            return
        try:
            result = self._db_confirm_monitored_column_description(
                tenant_id=int(context["tenant_id"]),
                user_id=int(context["user_id"]),
                monitored_column_id=int(monitored_column_id),
            )
            self._send_json(
                HTTPStatus.OK,
                {
                    "status": "success",
                    "tool": "inventory.confirm_column_description",
                    "correlation_id": str(uuid.uuid4()),
                    "data": result,
                    "error": None,
                },
            )
        except Exception as exc:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "inventory.confirm_column_description",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "confirm_error", "message": str(exc)},
                },
            )

    def _handle_onboarding_monitored_column_update(self) -> None:
        context = self._request_context()
        if context.get("invalid_session"):
            self._send_json(
                HTTPStatus.UNAUTHORIZED,
                {
                    "status": "denied",
                    "tool": "inventory.update_column",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_session", "message": "Sessao invalida ou expirada. Faca login novamente."},
                },
            )
            return
        if not self._is_db_enabled():
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "inventory.update_column",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "db_unavailable", "message": "Banco nao configurado para atualizar coluna monitorada."},
                },
            )
            return
        body = self._read_json_body()
        monitored_column_id = body.get("monitored_column_id")
        if monitored_column_id is None:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "inventory.update_column",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_input", "message": "monitored_column_id obrigatorio"},
                },
            )
            return
        try:
            result = self._db_update_monitored_column(
                tenant_id=int(context["tenant_id"]),
                monitored_column_id=int(monitored_column_id),
                classification=body.get("classification"),
                description_text=body.get("description_text"),
                llm_description_confirmed=body.get("llm_description_confirmed"),
            )
            self._send_json(
                HTTPStatus.OK,
                {
                    "status": "success",
                    "tool": "inventory.update_column",
                    "correlation_id": str(uuid.uuid4()),
                    "data": {"column": result},
                    "error": None,
                },
            )
        except Exception as exc:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "inventory.update_column",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "update_error", "message": str(exc)},
                },
            )

    def _handle_onboarding_monitored_columns_enrich(self) -> None:
        context = self._request_context()
        if context.get("invalid_session"):
            self._send_json(
                HTTPStatus.UNAUTHORIZED,
                {
                    "status": "denied",
                    "tool": "inventory.enrich_columns",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_session", "message": "Sessao invalida ou expirada. Faca login novamente."},
                },
            )
            return
        if not self._is_db_enabled():
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "inventory.enrich_columns",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "db_unavailable", "message": "Banco nao configurado para enriquecimento."},
                },
            )
            return

        body = self._read_json_body()
        monitored_table_id = body.get("monitored_table_id")
        source_type = str(body.get("source_type") or "").strip().lower()
        schema_name = str(body.get("schema_name") or "").strip()
        table_name = str(body.get("table_name") or "").strip()
        conn_secret_ref = str(body.get("conn_secret_ref") or "").strip()
        if monitored_table_id is None:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "inventory.enrich_columns",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_input", "message": "monitored_table_id obrigatorio"},
                },
            )
            return
        if not source_type or not table_name or not conn_secret_ref:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "inventory.enrich_columns",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_input", "message": "source_type, table_name e conn_secret_ref obrigatorios"},
                },
            )
            return

        try:
            profile = self._extract_connection_profile(conn_secret_ref=conn_secret_ref, secret_payload=body.get("secret_payload"))
            discovered = self._discover_source_columns(
                source_type=source_type,
                profile=profile,
                schema_name=schema_name,
                table_name=table_name,
            )
            sample_rows = self._sample_source_table_rows(
                source_type=source_type,
                profile=profile,
                schema_name=schema_name,
                table_name=table_name,
                limit=25,
            )
            enriched = self._db_enrich_monitored_columns(
                tenant_id=int(context["tenant_id"]),
                monitored_table_id=int(monitored_table_id),
                source_type=source_type,
                schema_name=schema_name or "public",
                table_name=table_name,
                discovered_columns=discovered,
                sample_rows=sample_rows,
                context=context,
            )
            self._send_json(
                HTTPStatus.OK,
                {
                    "status": "success",
                    "tool": "inventory.enrich_columns",
                    "correlation_id": str(uuid.uuid4()),
                    "data": enriched,
                    "error": None,
                },
            )
        except Exception as exc:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "inventory.enrich_columns",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "enrich_error", "message": str(exc)},
                },
            )

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

    def _handle_security_mcp_policies_list(self) -> None:
        payload = {
            "context": self._request_context(),
            "tool": "security_mcp.list_policies",
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
        context = self._request_context()
        if self._is_global_superadmin_context(context):
            try:
                mfa = self._db_get_user_mfa_status_global(user_id=int(context.get("user_id") or 0))
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "status": "success",
                        "tool": "security.mfa.get_status",
                        "correlation_id": str(uuid.uuid4()),
                        "data": {"mfa": mfa},
                        "error": None,
                    },
                )
            except Exception as exc:
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {
                        "status": "denied",
                        "tool": "security.mfa.get_status",
                        "correlation_id": None,
                        "data": {},
                        "error": {"code": "mfa_error", "message": str(exc)},
                    },
                )
            return
        payload = {
            "context": context,
            "tool": "security.mfa.get_status",
            "input": {},
        }
        self._dispatch_mcp(payload)

    def _handle_mfa_setup_begin(self) -> None:
        body = self._read_json_body()
        context = self._request_context()
        if self._is_global_superadmin_context(context):
            try:
                setup = self._db_begin_user_mfa_setup_global(
                    user_id=int(context.get("user_id") or 0),
                    issuer=str(body.get("issuer", "IAOps Governance")),
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "status": "success",
                        "tool": "security.mfa.begin_setup",
                        "correlation_id": str(uuid.uuid4()),
                        "data": {"setup": setup},
                        "error": None,
                    },
                )
            except Exception as exc:
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {
                        "status": "denied",
                        "tool": "security.mfa.begin_setup",
                        "correlation_id": None,
                        "data": {},
                        "error": {"code": "mfa_error", "message": str(exc)},
                    },
                )
            return
        payload = {
            "context": context,
            "tool": "security.mfa.begin_setup",
            "input": {
                "issuer": body.get("issuer", "IAOps Governance"),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_mfa_enable(self) -> None:
        body = self._read_json_body()
        context = self._request_context()
        if self._is_global_superadmin_context(context):
            try:
                mfa = self._db_enable_user_mfa_global(
                    user_id=int(context.get("user_id") or 0),
                    otp_code=str(body.get("otp_code") or "").strip(),
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "status": "success",
                        "tool": "security.mfa.enable",
                        "correlation_id": str(uuid.uuid4()),
                        "data": {"mfa": mfa},
                        "error": None,
                    },
                )
            except Exception as exc:
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {
                        "status": "denied",
                        "tool": "security.mfa.enable",
                        "correlation_id": None,
                        "data": {},
                        "error": {"code": "mfa_error", "message": str(exc)},
                    },
                )
            return
        payload = {
            "context": context,
            "tool": "security.mfa.enable",
            "input": {
                "otp_code": body.get("otp_code"),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_mfa_disable(self) -> None:
        body = self._read_json_body()
        context = self._request_context()
        if self._is_global_superadmin_context(context):
            try:
                mfa = self._db_disable_user_mfa_global(
                    user_id=int(context.get("user_id") or 0),
                    otp_code=str(body.get("otp_code") or "").strip(),
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "status": "success",
                        "tool": "security.mfa.disable_self",
                        "correlation_id": str(uuid.uuid4()),
                        "data": {"mfa": mfa},
                        "error": None,
                    },
                )
            except Exception as exc:
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {
                        "status": "denied",
                        "tool": "security.mfa.disable_self",
                        "correlation_id": None,
                        "data": {},
                        "error": {"code": "mfa_error", "message": str(exc)},
                    },
                )
            return
        payload = {
            "context": context,
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

    def _handle_admin_llm_models_list(self, query: str) -> None:
        qs = parse_qs(query)
        provider_name = qs.get("provider_name", [""])[0]
        payload = {
            "context": self._request_context(),
            "tool": "llm_admin.list_models",
            "input": {
                "provider_name": provider_name,
            },
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

    def _handle_admin_smtp_config_get(self) -> None:
        context = self._request_context()
        if context.get("invalid_session"):
            self._send_json(
                HTTPStatus.UNAUTHORIZED,
                {
                    "status": "denied",
                    "tool": "admin.smtp.get_config",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_session", "message": "Sessao invalida ou expirada. Faca login novamente."},
                },
            )
            return
        if not self._is_superadmin_user(user_id=int(context.get("user_id") or 0)):
            self._send_json(
                HTTPStatus.FORBIDDEN,
                {
                    "status": "denied",
                    "tool": "admin.smtp.get_config",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "superadmin_required", "message": "Acesso restrito a superadmin."},
                },
            )
            return
        self._send_json(
            HTTPStatus.OK,
            {
                "status": "success",
                "tool": "admin.smtp.get_config",
                "correlation_id": str(uuid.uuid4()),
                "data": {"config": self._smtp_public_config()},
                "error": None,
            },
        )

    def _handle_admin_smtp_config_update(self) -> None:
        context = self._request_context()
        if context.get("invalid_session"):
            self._send_json(
                HTTPStatus.UNAUTHORIZED,
                {
                    "status": "denied",
                    "tool": "admin.smtp.update_config",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_session", "message": "Sessao invalida ou expirada. Faca login novamente."},
                },
            )
            return
        if not self._is_superadmin_user(user_id=int(context.get("user_id") or 0)):
            self._send_json(
                HTTPStatus.FORBIDDEN,
                {
                    "status": "denied",
                    "tool": "admin.smtp.update_config",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "superadmin_required", "message": "Acesso restrito a superadmin."},
                },
            )
            return
        body = self._read_json_body()
        try:
            updated = self._update_smtp_runtime_config(body if isinstance(body, dict) else {})
            self._send_json(
                HTTPStatus.OK,
                {
                    "status": "success",
                    "tool": "admin.smtp.update_config",
                    "correlation_id": str(uuid.uuid4()),
                    "data": {"config": updated},
                    "error": None,
                },
            )
        except ValueError as exc:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "admin.smtp.update_config",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_input", "message": str(exc)},
                },
            )

    def _handle_data_source_discover_tables(self) -> None:
        body = self._read_json_body()
        source_type = str(body.get("source_type") or "").strip().lower()
        if not source_type:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "source.discover_tables",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_input", "message": "source_type obrigatorio"},
                },
            )
            return
        try:
            profile = self._extract_connection_profile(
                conn_secret_ref=str(body.get("conn_secret_ref") or ""),
                secret_payload=body.get("secret_payload"),
            )
            tables = self._discover_source_tables(source_type=source_type, profile=profile)
            self._send_json(
                HTTPStatus.OK,
                {
                    "status": "success",
                    "tool": "source.discover_tables",
                    "correlation_id": str(uuid.uuid4()),
                    "data": {
                        "source_type": source_type,
                        "tables": tables,
                        "total": len(tables),
                    },
                    "error": None,
                },
            )
        except ValueError as exc:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "source.discover_tables",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_input", "message": str(exc)},
                },
            )
        except Exception as exc:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "error",
                    "tool": "source.discover_tables",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "discover_failed", "message": str(exc)},
                },
            )

    def _handle_data_source_discover_columns(self) -> None:
        body = self._read_json_body()
        source_type = str(body.get("source_type") or "").strip().lower()
        schema_name = str(body.get("schema_name") or "").strip()
        table_name = str(body.get("table_name") or "").strip()
        if not source_type:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "source.discover_columns",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_input", "message": "source_type obrigatorio"},
                },
            )
            return
        if not table_name:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "source.discover_columns",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_input", "message": "table_name obrigatorio"},
                },
            )
            return
        try:
            profile = self._extract_connection_profile(
                conn_secret_ref=str(body.get("conn_secret_ref") or ""),
                secret_payload=body.get("secret_payload"),
            )
            columns = self._discover_source_columns(
                source_type=source_type,
                profile=profile,
                schema_name=schema_name,
                table_name=table_name,
            )
            self._send_json(
                HTTPStatus.OK,
                {
                    "status": "success",
                    "tool": "source.discover_columns",
                    "correlation_id": str(uuid.uuid4()),
                    "data": {
                        "source_type": source_type,
                        "schema_name": schema_name,
                        "table_name": table_name,
                        "columns": columns,
                        "total": len(columns),
                    },
                    "error": None,
                },
            )
        except ValueError as exc:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "source.discover_columns",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_input", "message": str(exc)},
                },
            )
        except Exception as exc:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "error",
                    "tool": "source.discover_columns",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "discover_failed", "message": str(exc)},
                },
            )

    def _handle_admin_smtp_test(self) -> None:
        context = self._request_context()
        if context.get("invalid_session"):
            self._send_json(
                HTTPStatus.UNAUTHORIZED,
                {
                    "status": "denied",
                    "tool": "admin.smtp.test",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_session", "message": "Sessao invalida ou expirada. Faca login novamente."},
                },
            )
            return
        if not self._is_superadmin_user(user_id=int(context.get("user_id") or 0)):
            self._send_json(
                HTTPStatus.FORBIDDEN,
                {
                    "status": "denied",
                    "tool": "admin.smtp.test",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "superadmin_required", "message": "Acesso restrito a superadmin."},
                },
            )
            return
        body = self._read_json_body()
        try:
            result = self._test_smtp_config(body if isinstance(body, dict) else {})
            self._send_json(
                HTTPStatus.OK,
                {
                    "status": "success",
                    "tool": "admin.smtp.test",
                    "correlation_id": str(uuid.uuid4()),
                    "data": result,
                    "error": None,
                },
            )
        except ValueError as exc:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "admin.smtp.test",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "smtp_test_error", "message": str(exc)},
                },
            )

    def _handle_admin_smtp_send_test(self) -> None:
        context = self._request_context()
        if context.get("invalid_session"):
            self._send_json(
                HTTPStatus.UNAUTHORIZED,
                {
                    "status": "denied",
                    "tool": "admin.smtp.send_test",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_session", "message": "Sessao invalida ou expirada. Faca login novamente."},
                },
            )
            return
        if not self._is_superadmin_user(user_id=int(context.get("user_id") or 0)):
            self._send_json(
                HTTPStatus.FORBIDDEN,
                {
                    "status": "denied",
                    "tool": "admin.smtp.send_test",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "superadmin_required", "message": "Acesso restrito a superadmin."},
                },
            )
            return
        body = self._read_json_body()
        try:
            result = self._send_test_smtp_email(body if isinstance(body, dict) else {})
            self._send_json(
                HTTPStatus.OK,
                {
                    "status": "success",
                    "tool": "admin.smtp.send_test",
                    "correlation_id": str(uuid.uuid4()),
                    "data": result,
                    "error": None,
                },
            )
        except ValueError as exc:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "admin.smtp.send_test",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "smtp_send_error", "message": str(exc)},
                },
            )

    def _handle_tenant_llm_providers_list(self) -> None:
        payload = {
            "context": self._request_context(),
            "tool": "tenant_llm.list_providers",
            "input": {},
        }
        self._dispatch_mcp(payload)

    def _handle_tenant_llm_models_list(self, query: str) -> None:
        qs = parse_qs(query)
        provider_name = qs.get("provider_name", [""])[0]
        payload = {
            "context": self._request_context(),
            "tool": "tenant_llm.list_models",
            "input": {
                "provider_name": provider_name,
            },
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
        password_error = self._validate_password_strength(password)
        if password_error:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "auth.signup",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "weak_password", "message": password_error},
                },
            )
            return
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

    def _handle_auth_password_request(self) -> None:
        body = self._read_json_body()
        email_access = str(body.get("email_access") or body.get("email") or "").strip().lower()
        if not email_access:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "auth.password.request",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_input", "message": "email_access obrigatorio"},
                },
            )
            return
        reset_token: str | None = None
        pending_signup: dict | None = None
        user_record = self._find_user_for_password_reset(email_access=email_access)
        if not user_record:
            pending_signup = self._find_pending_signup_for_email(email_access=email_access)
        if user_record:
            reset_token = self._create_password_reset_request(user_record=user_record)
        delivered = False
        detail = "Se o e-mail existir, voce recebera instrucoes para redefinir a senha."
        if user_record and reset_token:
            delivered, detail = self._send_password_reset_email(
                to_email=email_access,
                display_name=str(user_record.get("full_name") or user_record.get("email") or email_access),
                reset_token=reset_token,
            )
        elif pending_signup:
            delivered, detail = self._send_signup_email(
                to_email=email_access,
                trade_name=str(pending_signup.get("trade_name") or email_access),
                confirm_token=str(pending_signup.get("confirm_token") or ""),
            )
            if delivered:
                detail = f"Cadastro pendente localizado. {detail}"
        data = {"delivery": detail}
        if user_record and reset_token and not delivered:
            data["reset_token"] = reset_token
        if pending_signup and not delivered:
            data["confirm_token"] = str(pending_signup.get("confirm_token") or "")
        if pending_signup:
            data["signup_pending"] = True
        self._send_json(
            HTTPStatus.OK,
            {
                "status": "success",
                "tool": "auth.password.request",
                "correlation_id": str(uuid.uuid4()),
                "data": data,
                "error": None,
            },
        )

    def _handle_auth_confirm_link(self, query: str) -> None:
        qs = parse_qs(query)
        confirm_token = str(qs.get("confirm_token", [""])[0] or "").strip()
        if not confirm_token:
            self._send_html(
                HTTPStatus.BAD_REQUEST,
                "<h2>Token de confirmacao ausente</h2><p>Use o link completo enviado por e-mail.</p>",
            )
            return
        result = self._confirm_pending_signup_db(confirm_token=confirm_token)
        if isinstance(result, dict):
            self._send_html(
                HTTPStatus.OK,
                "<h2>Cadastro confirmado</h2><p>Seu acesso foi ativado com sucesso. Volte ao app e faca login.</p>",
            )
            return
        messages = {
            "expired_token": "O token expirou. Solicite novo cadastro ou use 'Esqueci a senha' para reenviar validacao.",
            "invalid_token": "Token invalido.",
            "already_exists": "Cadastro ja confirmado para este cliente/e-mail.",
            "plan_not_found": "Plano nao encontrado para este cadastro.",
            "db_unavailable": "Falha ao confirmar cadastro no banco.",
        }
        self._send_html(
            HTTPStatus.BAD_REQUEST,
            f"<h2>Nao foi possivel confirmar</h2><p>{messages.get(str(result), 'Token invalido ou inexistente.')}</p>",
        )

    def _handle_auth_password_reset(self) -> None:
        body = self._read_json_body()
        reset_token = str(body.get("reset_token") or "").strip()
        new_password = str(body.get("new_password") or "").strip()
        if not reset_token or not new_password:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "auth.password.reset",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_input", "message": "reset_token e new_password obrigatorios"},
                },
            )
            return
        password_error = self._validate_password_strength(new_password)
        if password_error:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "auth.password.reset",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "weak_password", "message": password_error},
                },
            )
            return
        consumed = self._consume_password_reset_token(reset_token=reset_token, new_password=new_password)
        if not consumed:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "auth.password.reset",
                    "correlation_id": None,
                    "data": {},
                    "error": {"code": "invalid_token", "message": "Token de redefinicao invalido ou expirado."},
                },
            )
            return
        self._revoke_sessions_for_user(user_id=int(consumed["user_id"]))
        self._send_json(
            HTTPStatus.OK,
            {
                "status": "success",
                "tool": "auth.password.reset",
                "correlation_id": str(uuid.uuid4()),
                "data": {"email_access": consumed["email_access"], "password_updated": True},
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

    def _find_user_for_password_reset(self, *, email_access: str) -> dict | None:
        email = str(email_access or "").strip().lower()
        if not email:
            return None
        if not self._is_db_enabled():
            for value in self.confirmed_signups.values():
                if str(value.get("email_access") or "").strip().lower() == email:
                    return {
                        "user_id": 0,
                        "client_id": 1,
                        "email": email,
                        "full_name": str(value.get("trade_name") or email),
                    }
            return None
        try:
            schema = self.signup_schema
            with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT u.id, u.client_id, u.email, COALESCE(u.full_name, u.email) AS full_name
                    FROM {schema}.app_user u
                    JOIN {schema}.client c ON c.id = u.client_id
                    WHERE LOWER(u.email) = %(email)s
                      AND COALESCE(u.is_active, FALSE) = TRUE
                      AND COALESCE(c.status, '') = 'active'
                      AND c.email_confirmed_at IS NOT NULL
                    LIMIT 1
                    """,
                    {"email": email},
                )
                row = cur.fetchone()
            if not row:
                return None
            return {
                "user_id": int(row[0]),
                "client_id": int(row[1]),
                "email": str(row[2] or email),
                "full_name": str(row[3] or row[2] or email),
            }
        except Exception:
            return None

    def _find_pending_signup_for_email(self, *, email_access: str) -> dict | None:
        email = str(email_access or "").strip().lower()
        if not email:
            return None
        if not self._is_db_enabled():
            for token, value in self.pending_signups.items():
                if str(value.get("email_access") or "").strip().lower() != email:
                    continue
                if int(value.get("expires_at_epoch") or 0) < int(time()):
                    continue
                status = str(value.get("status") or "pending")
                if status not in {"pending", "pending_email_confirmation"}:
                    continue
                return {
                    "confirm_token": token,
                    "trade_name": str(value.get("trade_name") or ""),
                }
            return None
        try:
            self._ensure_signup_tables()
            schema = self.signup_schema
            with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT confirm_token, trade_name
                    FROM {schema}.client_signup_pending
                    WHERE LOWER(email_access) = %(email)s
                      AND status = 'pending'
                      AND expires_at > NOW()
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    {"email": email},
                )
                row = cur.fetchone()
            if not row:
                return None
            return {
                "confirm_token": str(row[0] or ""),
                "trade_name": str(row[1] or ""),
            }
        except Exception:
            return None

    def _create_password_reset_request(self, *, user_record: dict) -> str | None:
        user_id = int(user_record.get("user_id") or 0)
        email = str(user_record.get("email") or "").strip().lower()
        if not email:
            return None
        reset_token = token_hex(16)
        expires_at_epoch = int(time()) + (30 * 60)
        if not self._is_db_enabled() or user_id <= 0:
            self.pending_password_resets[reset_token] = {
                "email_access": email,
                "user_id": user_id,
                "expires_at_epoch": expires_at_epoch,
            }
            return reset_token
        try:
            self._ensure_signup_tables()
            schema = self.signup_schema
            with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {schema}.user_password_reset
                       SET status = 'expired'
                     WHERE user_id = %(user_id)s
                       AND status = 'pending'
                    """,
                    {"user_id": user_id},
                )
                cur.execute(
                    f"""
                    INSERT INTO {schema}.user_password_reset (
                        reset_token,
                        user_id,
                        email_access,
                        status,
                        expires_at
                    )
                    VALUES (
                        %(reset_token)s,
                        %(user_id)s,
                        %(email_access)s,
                        'pending',
                        NOW() + INTERVAL '30 minutes'
                    )
                    """,
                    {
                        "reset_token": reset_token,
                        "user_id": user_id,
                        "email_access": email,
                    },
                )
                conn.commit()
            return reset_token
        except Exception:
            self.pending_password_resets[reset_token] = {
                "email_access": email,
                "user_id": user_id,
                "expires_at_epoch": expires_at_epoch,
            }
            return reset_token

    def _consume_password_reset_token(self, *, reset_token: str, new_password: str) -> dict | None:
        token = str(reset_token or "").strip()
        if not token:
            return None
        encoded_password = self._encode_password(new_password)
        if not self._is_db_enabled():
            payload = self.pending_password_resets.get(token)
            if not payload:
                return None
            if int(payload.get("expires_at_epoch") or 0) < int(time()):
                self.pending_password_resets.pop(token, None)
                return None
            self.pending_password_resets.pop(token, None)
            return {
                "user_id": int(payload.get("user_id") or 0),
                "email_access": str(payload.get("email_access") or ""),
            }
        try:
            self._ensure_signup_tables()
            schema = self.signup_schema
            with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id, user_id, email_access
                    FROM {schema}.user_password_reset
                    WHERE reset_token = %(reset_token)s
                      AND status = 'pending'
                      AND expires_at > NOW()
                    LIMIT 1
                    """,
                    {"reset_token": token},
                )
                row = cur.fetchone()
                if not row:
                    conn.rollback()
                    return None
                reset_id = int(row[0])
                user_id = int(row[1])
                email_access = str(row[2] or "")
                cur.execute(
                    f"""
                    UPDATE {schema}.app_user
                       SET password_hash = %(password_hash)s
                     WHERE id = %(user_id)s
                    """,
                    {"password_hash": encoded_password, "user_id": user_id},
                )
                cur.execute(
                    f"""
                    UPDATE {schema}.user_password_reset
                       SET status = 'used',
                           consumed_at = NOW()
                     WHERE id = %(id)s
                    """,
                    {"id": reset_id},
                )
                conn.commit()
            return {"user_id": user_id, "email_access": email_access}
        except Exception:
            return None

    @staticmethod
    def _validate_password_strength(password: str) -> str | None:
        value = str(password or "")
        if len(value) < 8:
            return "Senha deve ter no minimo 8 caracteres."
        if not re.search(r"[A-Z]", value):
            return "Senha deve conter ao menos uma letra maiuscula."
        if not re.search(r"[a-z]", value):
            return "Senha deve conter ao menos uma letra minuscula."
        if not re.search(r"[0-9]", value):
            return "Senha deve conter ao menos um numero."
        if not re.search(r"[^a-zA-Z0-9]", value):
            return "Senha deve conter ao menos um caractere especial."
        return None

    @staticmethod
    def _encode_password(password: str) -> str:
        salt = token_hex(16)
        password_hash = pbkdf2_hmac("sha256", str(password).encode("utf-8"), salt.encode("utf-8"), 240000).hex()
        return f"pbkdf2_sha256$240000${salt}${password_hash}"

    def _revoke_sessions_for_user(self, *, user_id: int) -> None:
        tokens = [token for token, session in self.active_sessions.items() if int(session.get("user_id") or 0) == int(user_id)]
        for token in tokens:
            session = self.active_sessions.pop(token, None) or {}
            refresh_token = str(session.get("refresh_token") or "")
            if refresh_token:
                self.refresh_sessions.pop(refresh_token, None)

    @classmethod
    def _smtp_effective_config(cls) -> dict[str, object]:
        runtime = cls.smtp_runtime_config or {}
        db_cfg = cls._db_load_smtp_config()
        host = str(runtime.get("host") or db_cfg.get("host") or os.getenv("IAOPS_SMTP_HOST") or os.getenv("SMTP_HOST") or "").strip()
        user = str(runtime.get("user") or db_cfg.get("user") or os.getenv("IAOPS_SMTP_USER") or os.getenv("SMTP_USER") or "").strip()
        from_email = str(runtime.get("from_email") or db_cfg.get("from_email") or os.getenv("IAOPS_SMTP_FROM") or os.getenv("SMTP_FROM") or user).strip()

        raw_port = runtime.get("port")
        if raw_port in (None, ""):
            raw_port = db_cfg.get("port")
        if raw_port in (None, ""):
            raw_port = os.getenv("IAOPS_SMTP_PORT") or os.getenv("SMTP_PORT") or "587"
        try:
            port = int(raw_port)
        except Exception:
            port = 587

        runtime_has_password = "password" in runtime
        if runtime_has_password:
            password = str(runtime.get("password") or "")
        else:
            password = str(db_cfg.get("password") or os.getenv("IAOPS_SMTP_PASS") or os.getenv("SMTP_PASS") or "")

        raw_tls = runtime.get("starttls")
        if raw_tls is None:
            raw_tls = db_cfg.get("starttls")
        if raw_tls is None:
            raw_tls = os.getenv("IAOPS_SMTP_STARTTLS") or "1"
        starttls = str(raw_tls).strip().lower() not in {"0", "false", "no", "off"}

        return {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "from_email": from_email,
            "starttls": bool(starttls),
            "password_set": bool(password),
        }

    @classmethod
    def _smtp_public_config(cls) -> dict[str, object]:
        cfg = cls._smtp_effective_config()
        return {
            "host": cfg["host"],
            "port": cfg["port"],
            "user": cfg["user"],
            "from_email": cfg["from_email"],
            "starttls": cfg["starttls"],
            "password_set": cfg["password_set"],
        }

    @classmethod
    def _update_smtp_runtime_config(cls, payload: dict[str, object]) -> dict[str, object]:
        host = str(payload.get("host") or "").strip()
        user = str(payload.get("user") or "").strip()
        from_email = str(payload.get("from_email") or "").strip()
        raw_port = payload.get("port")
        raw_tls = payload.get("starttls")
        password = payload.get("password")
        clear_password = bool(payload.get("clear_password"))

        if host:
            cls.smtp_runtime_config["host"] = host
        if user or "user" in payload:
            cls.smtp_runtime_config["user"] = user
        if from_email or "from_email" in payload:
            cls.smtp_runtime_config["from_email"] = from_email
        if raw_port not in (None, ""):
            try:
                port = int(raw_port)
            except Exception as exc:
                raise ValueError("port invalida") from exc
            if port <= 0 or port > 65535:
                raise ValueError("port deve estar entre 1 e 65535")
            cls.smtp_runtime_config["port"] = port
        if raw_tls is not None:
            cls.smtp_runtime_config["starttls"] = bool(raw_tls)
        if clear_password:
            cls.smtp_runtime_config["password"] = ""
        elif password is not None and str(password).strip():
            cls.smtp_runtime_config["password"] = str(password)
        cls._db_save_smtp_config(payload)
        return cls._smtp_public_config()

    @classmethod
    def _test_smtp_config(cls, overrides: dict[str, object]) -> dict[str, object]:
        cfg = cls._smtp_effective_config()
        host = str(overrides.get("host") or cfg.get("host") or "").strip()
        user = str(overrides.get("user") or cfg.get("user") or "").strip()
        from_email = str(overrides.get("from_email") or cfg.get("from_email") or "").strip()
        raw_port = overrides.get("port")
        if raw_port in (None, ""):
            raw_port = cfg.get("port") or 587
        try:
            port = int(raw_port)
        except Exception as exc:
            raise ValueError("port invalida para teste SMTP") from exc
        starttls = bool(overrides.get("starttls")) if "starttls" in overrides else bool(cfg.get("starttls"))
        password = str(overrides.get("password") or "") if "password" in overrides else str(cfg.get("password") or "")
        if not host:
            raise ValueError("host SMTP obrigatorio para teste")
        if not from_email:
            raise ValueError("from_email obrigatorio para teste")
        try:
            with smtplib.SMTP(host, port, timeout=15) as smtp:
                smtp.ehlo()
                if starttls:
                    smtp.starttls()
                    smtp.ehlo()
                if user:
                    smtp.login(user, password)
            return {
                "ok": True,
                "message": "Conexao SMTP validada com sucesso.",
                "config": {
                    "host": host,
                    "port": port,
                    "user": user,
                    "from_email": from_email,
                    "starttls": starttls,
                },
            }
        except Exception as exc:
            raise ValueError(f"Falha ao validar SMTP: {exc}") from exc

    @classmethod
    def _send_test_smtp_email(cls, payload: dict[str, object]) -> dict[str, object]:
        to_email = str(payload.get("to_email") or "").strip()
        if not to_email:
            raise ValueError("to_email obrigatorio para envio de teste")
        cfg = cls._smtp_effective_config()
        host = str(payload.get("host") or cfg.get("host") or "").strip()
        user = str(payload.get("user") or cfg.get("user") or "").strip()
        from_email = str(payload.get("from_email") or cfg.get("from_email") or "").strip()
        raw_port = payload.get("port")
        if raw_port in (None, ""):
            raw_port = cfg.get("port") or 587
        try:
            port = int(raw_port)
        except Exception as exc:
            raise ValueError("port invalida para teste SMTP") from exc
        starttls = bool(payload.get("starttls")) if "starttls" in payload else bool(cfg.get("starttls"))
        password = str(payload.get("password") or "") if "password" in payload else str(cfg.get("password") or "")
        if not host:
            raise ValueError("host SMTP obrigatorio para envio de teste")
        if not from_email:
            raise ValueError("from_email obrigatorio para envio de teste")

        subject = "IAOps Governance - Teste de SMTP"
        body = (
            "Este e-mail confirma que o envio SMTP do IAOps foi executado com sucesso.\n\n"
            f"Host: {host}\n"
            f"Porta: {port}\n"
            f"STARTTLS: {'sim' if starttls else 'nao'}\n"
            f"Remetente: {from_email}\n"
        )
        message = EmailMessage()
        message["Subject"] = subject
        message["From"] = from_email
        message["To"] = to_email
        message.set_content(body)

        try:
            with smtplib.SMTP(host, port, timeout=20) as smtp:
                smtp.ehlo()
                if starttls:
                    smtp.starttls()
                    smtp.ehlo()
                if user:
                    smtp.login(user, password)
                smtp.send_message(message)
            return {"ok": True, "message": f"E-mail de teste enviado para {to_email}."}
        except Exception as exc:
            raise ValueError(f"Falha ao enviar e-mail de teste: {exc}") from exc

    @staticmethod
    def _send_password_reset_email(*, to_email: str, display_name: str, reset_token: str) -> tuple[bool, str]:
        smtp = IAOpsAPIHandler._smtp_effective_config()
        smtp_host = str(smtp.get("host") or "")
        smtp_port = int(smtp.get("port") or 587)
        smtp_user = str(smtp.get("user") or "")
        smtp_pass = str(smtp.get("password") or "")
        smtp_from = str(smtp.get("from_email") or "")
        smtp_tls = bool(smtp.get("starttls"))
        if not smtp_host or not smtp_from:
            return False, "SMTP nao configurado no ambiente."
        subject = "IAOps Governance - Redefinicao de senha"
        body = (
            f"Ola, {display_name}.\n\n"
            "Recebemos uma solicitacao para redefinir sua senha.\n"
            "Use o token abaixo no app para concluir a redefinicao (validade: 30 minutos):\n\n"
            f"{reset_token}\n\n"
            "Se voce nao solicitou, ignore este e-mail."
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
            return True, f"Instrucoes de redefinicao enviadas para {to_email}."
        except Exception as exc:
            return False, f"Falha no envio por SMTP: {exc}. Token retornado no payload para uso em desenvolvimento."

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
                        c.email_confirmed_at,
                        COALESCE(u.is_superadmin, FALSE) AS is_superadmin
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
                is_superadmin = bool(user[8])

                resolved_tenant_id = 0
                tenant_name = "Global"
                role = "owner"
                if not is_superadmin:
                    role_and_tenant = self._resolve_login_tenant(
                        cur=cur,
                        client_id=int(user[1]),
                        user_id=int(user[0]),
                        tenant_id=tenant_id,
                    )
                    if not role_and_tenant:
                        return {"error_code": "tenant_not_found", "message": "Nenhum tenant ativo vinculado ao usuario."}
                    resolved_tenant_id, tenant_name, role = role_and_tenant

                mfa_row = None
                try:
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
                except Exception:
                    # Ambiente pode estar sem as tabelas de MFA (migração não aplicada).
                    # Neste caso, permite login sem MFA em vez de falhar genericamente.
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    mfa_row = None
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
                        "is_superadmin": is_superadmin,
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
                    is_superadmin=is_superadmin,
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
                        "is_superadmin": is_superadmin,
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
            is_superadmin=bool(challenge.get("is_superadmin")),
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
                "is_superadmin": bool(challenge.get("is_superadmin")),
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
        is_superadmin: bool,
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
            "is_superadmin": bool(is_superadmin),
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
            is_superadmin=bool(current.get("is_superadmin")),
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
                "is_superadmin": bool(current.get("is_superadmin")),
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

    def _check_route_rate_limit(
        self,
        *,
        route_key: str,
        actor_key: str,
        max_calls: int,
        window_seconds: int,
        lock_seconds: int,
    ) -> dict | None:
        now_epoch = int(time())
        key = f"{route_key}|{actor_key}"
        state = self.route_rate_limits.get(key) or {"attempts": [], "locked_until": 0}
        locked_until = int(state.get("locked_until") or 0)
        if locked_until > now_epoch:
            return {"blocked_remaining_seconds": locked_until - now_epoch}
        attempts = [
            ts for ts in (state.get("attempts") or []) if int(ts) >= now_epoch - int(max(1, window_seconds))
        ]
        attempts.append(now_epoch)
        if len(attempts) > int(max(1, max_calls)):
            self.route_rate_limits[key] = {"attempts": [], "locked_until": now_epoch + int(max(1, lock_seconds))}
            return {"blocked_remaining_seconds": int(max(1, lock_seconds))}
        self.route_rate_limits[key] = {"attempts": attempts, "locked_until": 0}
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

        for route_key, state in list(self.route_rate_limits.items()):
            locked_until = int(state.get("locked_until") or 0)
            attempts = [ts for ts in (state.get("attempts") or []) if int(ts) >= now_epoch - 3600]
            if locked_until > now_epoch or attempts:
                self.route_rate_limits[route_key] = {
                    "attempts": attempts,
                    "locked_until": locked_until if locked_until > now_epoch else 0,
                }
            else:
                self.route_rate_limits.pop(route_key, None)

        expired_resets = [
            token
            for token, data in self.pending_password_resets.items()
            if int(data.get("expires_at_epoch") or 0) <= now_epoch
        ]
        for token in expired_resets:
            self.pending_password_resets.pop(token, None)

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
            channel_reply = str(nl_response["reply_text"] or "")
            chart_hint = self._render_channel_chart_hint(
                (nl_response.get("data") or {}).get("visualization"),
                runtime_language,
            )
            if chart_hint:
                channel_reply = f"{channel_reply}\n\n{chart_hint}".strip()
            return {
                "ok": True,
                "command": "nl_query",
                "reply_text": channel_reply,
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

    @staticmethod
    def _render_channel_chart_hint(visualization: dict | None, language_code: str) -> str:
        if not isinstance(visualization, dict):
            return ""
        labels = visualization.get("labels") or []
        series = visualization.get("series") or []
        values = series[0].get("values") if series and isinstance(series[0], dict) else []
        chart_url = str(visualization.get("chart_url") or "").strip()
        if not isinstance(labels, list) or not isinstance(values, list) or len(labels) < 2:
            return ""
        top_lines = []
        for idx, label in enumerate(labels[:5]):
            if idx >= len(values):
                break
            top_lines.append(f"- {label}: {values[idx]}")
        if not top_lines:
            return ""
        bucket = IAOpsAPIHandler._language_bucket(language_code)
        if bucket == "en":
            header = "Chart summary:"
        elif bucket == "es":
            header = "Resumen del grafico:"
        else:
            header = "Resumo do grafico:"
        lines = [header] + top_lines
        if chart_url:
            if bucket == "en":
                lines.append(f"Chart: {chart_url}")
            elif bucket == "es":
                lines.append(f"Grafico: {chart_url}")
            else:
                lines.append(f"Grafico: {chart_url}")
        return "\n".join(lines)

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

    def _execute_nl_chat_query(
        self,
        context: dict,
        question_text: str,
        language_code: str | None = None,
        data_source_id: int | None = None,
    ) -> dict:
        resolved_language = language_code or self._resolve_language_code(context)
        response_mode = self._resolve_chat_response_mode(context)
        intent = self._route_nl_intent(question_text)
        rag = self._build_rag_context(context, question_text, data_source_id=data_source_id)
        planned = self._plan_sql_from_question(context, question_text, rag, intent)
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
            fallback = self._try_query_from_monitored_source(
                context=context,
                sql_text=sql_text,
                rag=rag,
            )
            if not fallback.get("ok"):
                return {
                    "ok": False,
                    "command": "error",
                    "reply_text": self._t(resolved_language, "chat_query_failed_user"),
                    "data": {
                        "question_text": question_text,
                        "planned_sql": sql_text,
                        "planning_mode": planned.get("mode", "rules"),
                        "llm_provider": planned.get("llm_provider"),
                        "natural_response_template": planned.get("natural_response_template"),
                        "chart_suggestion": planned.get("chart_suggestion"),
                        "language_code": resolved_language,
                        "chat_response_mode": response_mode,
                        "rag": rag,
                        "mcp": query_result,
                    },
                }
            query_data = fallback["data"]
        else:
            query_data = query_result["data"]
        visualization = self._build_visualization_payload(
            question_text=question_text,
            query_data=query_data,
            language_code=resolved_language,
            intent=intent,
            chart_suggestion=planned.get("chart_suggestion"),
        )
        final_reply = self._reply_nl_result(
            question_text,
            sql_text,
            query_data,
            rag,
            response_mode,
            resolved_language,
            intent,
            planned.get("natural_response_template"),
        )
        return {
            "ok": True,
            "reply_text": final_reply,
            "data": {
                "question_text": question_text,
                "reply_text": final_reply,
                "intent": intent,
                "planned_sql": sql_text,
                "planning_mode": planned.get("mode", "rules"),
                "llm_provider": planned.get("llm_provider"),
                "natural_response_template": planned.get("natural_response_template"),
                "chart_suggestion": planned.get("chart_suggestion"),
                "language_code": resolved_language,
                "chat_response_mode": response_mode,
                "rag": rag,
                "result": query_data,
                "visualization": visualization,
            },
        }

    def _try_query_from_monitored_source(self, *, context: dict, sql_text: str, rag: dict) -> dict:
        spec = self._parse_supported_source_query(sql_text)
        if not spec:
            return {"ok": False}
        matched_table = self._match_monitored_table(
            rag=rag,
            schema_name=str(spec.get("schema_name") or ""),
            table_name=str(spec.get("table_name") or ""),
        )
        if not matched_table:
            return {"ok": False}
        data_source_id = matched_table.get("data_source_id")
        if data_source_id is None:
            return {"ok": False}
        source_result = self._call_mcp(
            {
                "context": context,
                "tool": "source.list_tenant",
                "input": {},
            }
        )
        if source_result.get("status") != "success":
            return {"ok": False}
        source = next(
            (
                item
                for item in ((source_result.get("data") or {}).get("sources") or [])
                if int(item.get("id") or 0) == int(data_source_id)
            ),
            None,
        )
        if not source:
            return {"ok": False}
        source_type = str(source.get("source_type") or "").strip().lower()
        conn_secret_ref = str(source.get("conn_secret_ref") or "").strip()
        if not source_type or not conn_secret_ref:
            return {"ok": False}
        try:
            profile = self._extract_connection_profile(conn_secret_ref=conn_secret_ref, secret_payload=None)
            lower_sql = str(sql_text or "").strip().lower()
            if " join " in lower_sql and self._is_planned_sql_allowed(sql_text):
                try:
                    query_data = self._execute_source_raw_select(
                        source_type=source_type,
                        profile=profile,
                        sql_text=sql_text,
                    )
                    return {"ok": True, "data": query_data}
                except Exception:
                    pass
            query_data = self._execute_source_fallback_query(source_type=source_type, profile=profile, spec=spec)
        except Exception:
            return {"ok": False}
        return {"ok": True, "data": query_data}

    def _execute_source_raw_select(self, *, source_type: str, profile: dict, sql_text: str) -> dict:
        kind = str(source_type or "").strip().lower()
        if kind in {"postgres", "postgresql"}:
            return self._execute_source_raw_postgres(profile=profile, sql_text=sql_text)
        if kind == "mysql":
            return self._execute_source_raw_mysql(profile=profile, sql_text=sql_text)
        if kind in {"sqlserver", "sql_server", "mssql"}:
            return self._execute_source_raw_sqlserver(profile=profile, sql_text=sql_text)
        if kind == "oracle":
            return self._execute_source_raw_oracle(profile=profile, sql_text=sql_text)
        raise ValueError(f"Execucao SQL direta nao suportada para source_type={kind}")

    def _execute_source_raw_postgres(self, *, profile: dict, sql_text: str) -> dict:
        if connect is None:
            raise ValueError("Driver PostgreSQL indisponivel (psycopg).")
        dsn = str(profile.get("dsn") or "").strip()
        if not dsn:
            host = str(profile.get("host") or "").strip()
            user = str(profile.get("user") or "").strip()
            password = str(profile.get("password") or "").strip()
            dbname = str(profile.get("dbname") or profile.get("database") or "").strip()
            port = str(profile.get("port") or "5432").strip()
            if not host or not user or not dbname:
                raise ValueError("Informe dsn ou host/user/password/dbname no secret_payload.")
            dsn = f"host={host} port={port} dbname={dbname} user={user}"
            if password:
                dsn += f" password={password}"
        with connect(dsn, connect_timeout=8) as conn, conn.cursor() as cur:
            cur.execute(sql_text)
            rows = cur.fetchall()
            columns = [str(item[0]) for item in (cur.description or [])]
        return {"columns": columns, "rows": [dict(zip(columns, row)) for row in rows]}

    def _execute_source_raw_mysql(self, *, profile: dict, sql_text: str) -> dict:
        if pymysql is None:
            raise ValueError("Driver pymysql nao instalado para consulta MySQL.")
        host = str(profile.get("host") or profile.get("server") or "").strip()
        port = int(profile.get("port") or 3306)
        database = str(profile.get("database") or profile.get("dbname") or "").strip()
        user = str(profile.get("user") or profile.get("username") or "").strip()
        password = str(profile.get("password") or "").strip()
        timeout = int(profile.get("timeout_seconds") or 8)
        if not host or not user:
            raise ValueError("Informe host, user e password no secret_payload.")
        with pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database or None,
            connect_timeout=timeout,
            read_timeout=timeout,
            write_timeout=timeout,
            charset="utf8mb4",
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_text)
                rows = cur.fetchall()
                columns = [str(item[0]) for item in (cur.description or [])]
        return {"columns": columns, "rows": [dict(zip(columns, row)) for row in rows]}

    def _execute_source_raw_sqlserver(self, *, profile: dict, sql_text: str) -> dict:
        if pyodbc is None:
            raise ValueError("Driver pyodbc nao instalado para consulta SQL Server.")
        host = str(profile.get("host") or profile.get("server") or "").strip()
        port = int(profile.get("port") or 1433)
        database = str(profile.get("database") or profile.get("dbname") or "master").strip()
        user = str(profile.get("user") or profile.get("username") or "").strip()
        password = str(profile.get("password") or "").strip()
        timeout = int(profile.get("timeout_seconds") or 8)
        if not host or not user:
            raise ValueError("Informe host, user e password no secret_payload.")
        dsn = str(profile.get("dsn") or "").strip()
        conn_str = dsn
        if not conn_str:
            configured_driver = str(profile.get("driver") or "").strip()
            if configured_driver:
                driver = configured_driver
            else:
                available = list(pyodbc.drivers())
                preferred = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server"]
                driver = next((item for item in preferred if item in available), (available[0] if available else "SQL Server"))
            conn_str = (
                f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={database};UID={user};PWD={password};"
                f"Encrypt=yes;TrustServerCertificate=yes;Connection Timeout={timeout};"
            )
        with pyodbc.connect(conn_str, timeout=timeout) as conn:
            cur = conn.cursor()
            cur.execute(sql_text)
            rows = cur.fetchall()
            columns = [str(item[0]) for item in (cur.description or [])]
        return {"columns": columns, "rows": [dict(zip(columns, row)) for row in rows]}

    def _execute_source_raw_oracle(self, *, profile: dict, sql_text: str) -> dict:
        if oracledb is None:
            raise ValueError("Driver oracledb nao instalado para consulta Oracle.")
        host = str(profile.get("host") or profile.get("server") or "").strip()
        port = int(profile.get("port") or 1521)
        user = str(profile.get("user") or profile.get("username") or "").strip()
        password = str(profile.get("password") or "").strip()
        service_name = str(profile.get("service_name") or "").strip()
        sid = str(profile.get("sid") or "").strip()
        if not host or not user:
            raise ValueError("Informe host, user e password no secret_payload.")
        dsn = str(profile.get("dsn") or "").strip()
        if not dsn:
            if service_name:
                dsn = f"{host}:{port}/{service_name}"
            elif sid:
                dsn = f"{host}:{port}/{sid}"
            else:
                dsn = f"{host}:{port}/XEPDB1"
        with oracledb.connect(user=user, password=password, dsn=dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_text)
                rows = cur.fetchall()
                columns = [str(item[0]) for item in (cur.description or [])]
        return {"columns": columns, "rows": [dict(zip(columns, row)) for row in rows]}

    @staticmethod
    def _parse_supported_source_query(sql_text: str) -> dict | None:
        sql = str(sql_text or "").strip()
        if not sql:
            return None
        match = re.search(r"\bfrom\s+([^\s;]+)", sql, flags=re.IGNORECASE)
        if not match:
            return None
        raw_target = str(match.group(1) or "").strip()
        if not raw_target:
            return None
        cleaned = raw_target.replace("`", "").replace('"', "").replace("[", "").replace("]", "")
        parts = [part for part in cleaned.split(".") if part]
        if len(parts) >= 2:
            schema_name, table_name = parts[-2], parts[-1]
        elif len(parts) == 1:
            schema_name, table_name = "public", parts[0]
        else:
            return None
        limit_match = re.search(r"\blimit\s+(\d+)", sql, flags=re.IGNORECASE)
        limit_value = int(limit_match.group(1)) if limit_match else 20
        limit_value = int(max(1, min(limit_value, 500)))
        select_match = re.search(r"^\s*select\s+(.*?)\s+from\s", sql, flags=re.IGNORECASE | re.DOTALL)
        if not select_match:
            return None
        select_expr = str(select_match.group(1) or "").strip()
        if select_expr == "*":
            return {
                "kind": "list",
                "schema_name": schema_name,
                "table_name": table_name,
                "limit": limit_value,
            }
        chunks = [item.strip() for item in select_expr.split(",") if item.strip()]
        group_match = re.search(r"\bgroup\s+by\s+([A-Za-z_][A-Za-z0-9_]*)", sql, flags=re.IGNORECASE)
        group_col = str(group_match.group(1) or "").strip().lower() if group_match else None
        order_match = re.search(r"\border\s+by\s+([A-Za-z_][A-Za-z0-9_]*)(?:\s+(asc|desc))?", sql, flags=re.IGNORECASE)
        order_col = str(order_match.group(1) or "").strip().lower() if order_match else None
        order_dir = str(order_match.group(2) or "asc").strip().lower() if order_match else "asc"
        if order_dir not in {"asc", "desc"}:
            order_dir = "asc"
        plain_columns: list[str] = []
        metrics: list[dict] = []
        for chunk in chunks:
            plain_match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)$", chunk)
            if plain_match:
                plain_columns.append(str(plain_match.group(1)).lower())
                continue
            metric_match = re.match(
                r"^(count|sum|avg|min|max)\s*\(\s*(distinct\s+)?(\*|[A-Za-z_][A-Za-z0-9_]*)\s*\)\s*(?:as\s+([A-Za-z_][A-Za-z0-9_]*))?$",
                chunk,
                flags=re.IGNORECASE,
            )
            if not metric_match:
                return None
            func = str(metric_match.group(1) or "").lower()
            distinct = bool(metric_match.group(2))
            column = str(metric_match.group(3) or "").lower()
            alias = str(metric_match.group(4) or f"{func}_{column if column != '*' else 'all'}").lower()
            metrics.append(
                {
                    "func": func,
                    "distinct": distinct,
                    "column": column,
                    "alias": alias,
                }
            )
        if not metrics:
            return None
        if group_col:
            if len(plain_columns) != 1 or plain_columns[0] != group_col:
                return None
        elif plain_columns:
            return None
        return {
            "kind": "aggregate",
            "schema_name": schema_name,
            "table_name": table_name,
            "group_col": group_col,
            "metrics": metrics,
            "order_col": order_col,
            "order_dir": order_dir,
            "limit": limit_value,
        }

    @staticmethod
    def _match_monitored_table(*, rag: dict, schema_name: str, table_name: str) -> dict | None:
        tables = rag.get("tables") or []
        schema_v = str(schema_name or "").strip().lower()
        table_v = str(table_name or "").strip().lower()
        exact = next(
            (
                item
                for item in tables
                if str(item.get("schema_name") or "").strip().lower() == schema_v
                and str(item.get("table_name") or "").strip().lower() == table_v
            ),
            None,
        )
        if exact:
            return exact
        by_name = [
            item
            for item in tables
            if str(item.get("table_name") or "").strip().lower() == table_v
        ]
        if len(by_name) == 1:
            return by_name[0]
        return None

    def _execute_source_fallback_query(self, *, source_type: str, profile: dict, spec: dict) -> dict:
        kind = str(source_type or "").strip().lower()
        if kind in {"postgres", "postgresql"}:
            return self._execute_source_fallback_postgres(profile=profile, spec=spec)
        if kind == "mysql":
            return self._execute_source_fallback_mysql(profile=profile, spec=spec)
        if kind in {"sqlserver", "sql_server", "mssql"}:
            return self._execute_source_fallback_sqlserver(profile=profile, spec=spec)
        if kind == "oracle":
            return self._execute_source_fallback_oracle(profile=profile, spec=spec)
        raise ValueError(f"Fallback de consulta nao suportado para source_type={kind}")

    def _execute_source_fallback_postgres(self, *, profile: dict, spec: dict) -> dict:
        if connect is None:
            raise ValueError("Driver PostgreSQL indisponivel (psycopg).")
        dsn = str(profile.get("dsn") or "").strip()
        if not dsn:
            host = str(profile.get("host") or "").strip()
            user = str(profile.get("user") or "").strip()
            password = str(profile.get("password") or "").strip()
            dbname = str(profile.get("dbname") or profile.get("database") or "").strip()
            port = str(profile.get("port") or "5432").strip()
            if not host or not user or not dbname:
                raise ValueError("Informe dsn ou host/user/password/dbname no secret_payload.")
            dsn = f"host={host} port={port} dbname={dbname} user={user}"
            if password:
                dsn += f" password={password}"
        sql, columns = self._build_fallback_sql(
            dialect="postgres",
            spec=spec,
            default_schema=str(profile.get("schema") or "public"),
        )
        with connect(dsn, connect_timeout=8) as conn, conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            result_columns = columns or [str(item[0]) for item in (cur.description or [])]
        return {
            "columns": result_columns,
            "rows": [dict(zip(result_columns, row)) for row in rows],
        }

    def _execute_source_fallback_mysql(self, *, profile: dict, spec: dict) -> dict:
        if pymysql is None:
            raise ValueError("Driver pymysql nao instalado para consulta MySQL.")
        host = str(profile.get("host") or profile.get("server") or "").strip()
        port = int(profile.get("port") or 3306)
        database = str(profile.get("database") or profile.get("dbname") or "").strip()
        user = str(profile.get("user") or profile.get("username") or "").strip()
        password = str(profile.get("password") or "").strip()
        timeout = int(profile.get("timeout_seconds") or 8)
        if not host or not user:
            raise ValueError("Informe host, user e password no secret_payload.")
        sql, columns = self._build_fallback_sql(
            dialect="mysql",
            spec=spec,
            default_schema=database,
        )
        with pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database or None,
            connect_timeout=timeout,
            read_timeout=timeout,
            write_timeout=timeout,
            charset="utf8mb4",
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                result_columns = columns or [str(item[0]) for item in (cur.description or [])]
        return {
            "columns": result_columns,
            "rows": [dict(zip(result_columns, row)) for row in rows],
        }

    def _execute_source_fallback_sqlserver(self, *, profile: dict, spec: dict) -> dict:
        if pyodbc is None:
            raise ValueError("Driver pyodbc nao instalado para consulta SQL Server.")
        host = str(profile.get("host") or profile.get("server") or "").strip()
        port = int(profile.get("port") or 1433)
        database = str(profile.get("database") or profile.get("dbname") or "master").strip()
        user = str(profile.get("user") or profile.get("username") or "").strip()
        password = str(profile.get("password") or "").strip()
        timeout = int(profile.get("timeout_seconds") or 8)
        if not host or not user:
            raise ValueError("Informe host, user e password no secret_payload.")
        dsn = str(profile.get("dsn") or "").strip()
        conn_str = dsn
        if not conn_str:
            configured_driver = str(profile.get("driver") or "").strip()
            if configured_driver:
                driver = configured_driver
            else:
                available = list(pyodbc.drivers())
                preferred = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server"]
                driver = next((item for item in preferred if item in available), (available[0] if available else "SQL Server"))
            conn_str = (
                f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={database};UID={user};PWD={password};"
                f"Encrypt=yes;TrustServerCertificate=yes;Connection Timeout={timeout};"
            )
        sql, columns = self._build_fallback_sql(
            dialect="sqlserver",
            spec=spec,
            default_schema="dbo",
        )
        with pyodbc.connect(conn_str, timeout=timeout) as conn:
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            result_columns = columns or [str(item[0]) for item in (cur.description or [])]
        return {
            "columns": result_columns,
            "rows": [dict(zip(result_columns, row)) for row in rows],
        }

    def _execute_source_fallback_oracle(self, *, profile: dict, spec: dict) -> dict:
        if oracledb is None:
            raise ValueError("Driver oracledb nao instalado para consulta Oracle.")
        host = str(profile.get("host") or profile.get("server") or "").strip()
        port = int(profile.get("port") or 1521)
        user = str(profile.get("user") or profile.get("username") or "").strip()
        password = str(profile.get("password") or "").strip()
        service_name = str(profile.get("service_name") or "").strip()
        sid = str(profile.get("sid") or "").strip()
        if not host or not user:
            raise ValueError("Informe host, user e password no secret_payload.")
        dsn = str(profile.get("dsn") or "").strip()
        if not dsn:
            if service_name:
                dsn = f"{host}:{port}/{service_name}"
            elif sid:
                dsn = f"{host}:{port}/{sid}"
            else:
                dsn = f"{host}:{port}/XEPDB1"
        sql, columns = self._build_fallback_sql(
            dialect="oracle",
            spec=spec,
            default_schema=str(profile.get("owner") or profile.get("schema") or user),
        )
        with oracledb.connect(user=user, password=password, dsn=dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                result_columns = columns or [str(item[0]) for item in (cur.description or [])]
        return {
            "columns": result_columns,
            "rows": [dict(zip(result_columns, row)) for row in rows],
        }

    def _build_fallback_sql(self, *, dialect: str, spec: dict, default_schema: str) -> tuple[str, list[str]]:
        schema = self._safe_identifier(str(spec.get("schema_name") or default_schema or "public"))
        table = self._safe_identifier(str(spec.get("table_name") or ""))
        kind = str(spec.get("kind") or "")
        limit_value = int(max(1, min(int(spec.get("limit") or 20), 500)))
        q_table = self._q_table(dialect=dialect, schema=schema, table=table)
        if kind == "list":
            if dialect == "sqlserver":
                return f"SELECT TOP {limit_value} * FROM {q_table}", []
            if dialect == "oracle":
                return f"SELECT * FROM {q_table} FETCH FIRST {limit_value} ROWS ONLY", []
            return f"SELECT * FROM {q_table} LIMIT {limit_value}", []
        metrics = list(spec.get("metrics") or [])
        if not metrics:
            raise ValueError("Consulta sem metricas suportadas.")
        group_col = str(spec.get("group_col") or "").strip()
        select_parts: list[str] = []
        output_cols: list[str] = []
        if group_col:
            safe_group = self._safe_identifier(group_col)
            select_parts.append(self._q_ident(dialect, safe_group))
            output_cols.append(safe_group)
        for metric in metrics:
            func = str(metric.get("func") or "").lower()
            distinct = bool(metric.get("distinct"))
            column = str(metric.get("column") or "").lower()
            alias = self._safe_identifier(str(metric.get("alias") or f"{func}_{column or 'value'}").lower())
            if column == "*":
                expr = "COUNT(*)"
            else:
                safe_col = self._safe_identifier(column)
                q_col = self._q_ident(dialect, safe_col)
                expr = f"{func.upper()}(DISTINCT {q_col})" if distinct else f"{func.upper()}({q_col})"
            select_parts.append(f"{expr} AS {self._q_ident(dialect, alias)}")
            output_cols.append(alias)
        sql = f"SELECT {', '.join(select_parts)} FROM {q_table}"
        if group_col:
            sql += f" GROUP BY {self._q_ident(dialect, self._safe_identifier(group_col))}"
            order_col = str(spec.get("order_col") or "").strip().lower()
            order_dir = str(spec.get("order_dir") or "asc").strip().lower()
            if order_col and order_col in set(output_cols):
                sql += f" ORDER BY {self._q_ident(dialect, self._safe_identifier(order_col))} {'DESC' if order_dir == 'desc' else 'ASC'}"
            if dialect == "sqlserver":
                sql += f" OFFSET 0 ROWS FETCH NEXT {limit_value} ROWS ONLY"
            elif dialect == "oracle":
                sql += f" FETCH FIRST {limit_value} ROWS ONLY"
            else:
                sql += f" LIMIT {limit_value}"
        return sql, output_cols

    @staticmethod
    def _q_ident(dialect: str, ident: str) -> str:
        if dialect == "mysql":
            return f"`{ident}`"
        if dialect == "sqlserver":
            return f"[{ident}]"
        return f'"{ident}"' if dialect in {"postgres", "oracle"} else ident

    def _q_table(self, *, dialect: str, schema: str, table: str) -> str:
        if dialect == "mysql":
            return f"`{schema}`.`{table}`"
        if dialect == "sqlserver":
            return f"[{schema}].[{table}]"
        if dialect == "oracle":
            return f"{schema.upper()}.{table.upper()}"
        return f'"{schema}"."{table}"'

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

    def _build_rag_context(self, context: dict, question_text: str = "", data_source_id: int | None = None) -> dict:
        semantic_docs: list[dict] = []
        data_source_rag: list[dict] = []
        try:
            semantic_docs = search_rag_documents(
                tenant_id=int(context.get("tenant_id") or 0),
                query_text=str(question_text or ""),
                limit=8,
            )
        except Exception:
            semantic_docs = []
        source_result = self._call_mcp(
            {
                "context": context,
                "tool": "source.list_tenant",
                "input": {},
            }
        )
        if source_result.get("status") == "success":
            for src in ((source_result.get("data") or {}).get("sources") or []):
                if data_source_id is not None and int(src.get("id") or 0) != int(data_source_id):
                    continue
                if not bool(src.get("rag_enabled")):
                    continue
                rag_text = str(src.get("rag_context_text") or "").strip()
                if not rag_text:
                    continue
                rag_prompt_text = self._rag_context_to_prompt_text(rag_text)
                data_source_rag.append(
                    {
                        "data_source_id": src.get("id"),
                        "source_name": src.get("source_name") or src.get("source_type"),
                        "source_type": src.get("source_type"),
                        "context_text": rag_prompt_text,
                    }
                )
        data_source_rag = data_source_rag[:6]
        table_result = self._call_mcp(
            {
                "context": context,
                "tool": "inventory.list_tenant_tables",
                "input": {"data_source_id": int(data_source_id)} if data_source_id is not None else {},
            }
        )
        if table_result["status"] != "success":
            return {"tables": [], "columns": {}, "relationships": [], "semantic_docs": semantic_docs, "data_source_rag": data_source_rag}
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
        return {
            "tables": tables,
            "columns": columns_by_table,
            "relationships": relationships,
            "semantic_docs": semantic_docs,
            "data_source_rag": data_source_rag,
        }

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

    def _plan_sql_from_question(self, context: dict, question_text: str, rag: dict, intent: dict | None = None) -> dict:
        llm_plan = self._plan_sql_with_llm(context, question_text, rag)
        if llm_plan.get("sql_text"):
            return llm_plan
        return self._plan_sql_with_rules(question_text, rag, intent=intent)

    @staticmethod
    def _plan_sql_with_rules(question_text: str, rag: dict, intent: dict | None = None) -> dict:
        q = question_text.lower()
        tokens = IAOpsAPIHandler._normalize_query_tokens(question_text)
        routed = intent or IAOpsAPIHandler._route_nl_intent(question_text)
        wants_dimension = bool(routed.get("needs_dimension"))
        if ("incidente" in q or "incident" in q) and ("aberto" in q or "abertos" in q or "open" in q):
            severity = IAOpsAPIHandler._resolve_severity_token(tokens)
            if severity:
                return {
                    "mode": "rules",
                    "sql_text": (
                        "SELECT COUNT(*) AS total "
                        "FROM iaops_gov.incident "
                        f"WHERE status IN ('open','ack') AND severity = '{severity}'"
                    ),
                }
            return {
                "mode": "rules",
                "sql_text": (
                    "SELECT severity, COUNT(*) AS total "
                    "FROM iaops_gov.incident "
                    "WHERE status IN ('open','ack') "
                    "GROUP BY severity "
                    "ORDER BY total DESC"
                ),
            }
        if ("evento" in q or "event" in q) and ("critico" in q or "criticos" in q or "critical" in q):
            return {
                "mode": "rules",
                "sql_text": "SELECT severity, COUNT(*) AS total FROM iaops_gov.schema_change_event WHERE severity = 'critical' GROUP BY severity",
            }
        top_relation = IAOpsAPIHandler._plan_top_relation_sql(
            tokens=tokens,
            rag=rag,
            include_dimension=wants_dimension,
            top_n=int(routed.get("top_n") or 20),
        )
        if top_relation:
            return {"mode": "rules", "sql_text": top_relation}
        grouped_metric = IAOpsAPIHandler._plan_grouped_metric_sql(tokens=tokens, rag=rag)
        if grouped_metric:
            return {"mode": "rules", "sql_text": grouped_metric}
        asks_inventory = ("inventario" in q or "catalogo" in q or "schema" in q or "schemas" in q)
        asks_table_list = (
            ("tabela" in q or "tabelas" in q or "table" in q or "tables" in q)
            and any(term in q for term in ["monitorad", "cadastrad", "catalog", "schema", "estrutur"])
        )
        if asks_inventory or asks_table_list:
            return {
                "mode": "rules",
                "sql_text": "SELECT schema_name, table_name, is_active FROM iaops_gov.monitored_table ORDER BY schema_name, table_name LIMIT 50",
            }
        grouped = IAOpsAPIHandler._plan_grouped_count_sql(tokens=tokens, rag=rag)
        if grouped:
            return {"mode": "rules", "sql_text": grouped}
        ranked = IAOpsAPIHandler._rank_tables_for_question(question_text, rag.get("tables") or [])
        if ranked:
            target = ranked[0]
            schema_name = str(target.get("schema_name"))
            table_name = str(target.get("table_name"))
            if any(term in tokens for term in ["quantos", "qtd", "total", "count", "how", "many", "cuantos"]):
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
        use_app_default_llm = bool(cfg.get("use_app_default_llm"))
        provider_name = str(cfg.get("provider_name") or "").strip().lower()
        model_code = str(cfg.get("model_code") or "").strip()
        endpoint_url = str(cfg.get("endpoint_url") or "").strip()
        secret_value = self._resolve_secret_value(cfg.get("secret_ref"))
        if not provider_name or not model_code or not endpoint_url or not secret_value:
            return {"mode": "llm_unavailable", "sql_text": None}

        prompt_payload = {
            "instruction": (
                "PERFIL E MISSAO: Voce e o Planejador Semantico do IAOps Governance. "
                "Traduza perguntas de usuarios de negocio (leigos) em SQL seguro e em resposta humanizada.\n"
                "DIRETRIZES DE LINGUAGEM NATURAL: "
                "1) Atue como analista de BI atencioso (ex.: 'Atualmente, temos...', 'Identifiquei que...'). "
                "2) Nunca exponha nomes tecnicos de coluna ao usuario final. "
                "3) Contextualize resultados com significado de negocio.\n"
                "REGRAS DE OURO: "
                "a) respeitar tenant ativo (multi-tenant), "
                "b) apenas SELECT, sem DDL/DML, sem ponto e virgula, "
                "c) usar joins por FK/chaves de negocio quando necessario.\n"
                "ROTEAMENTO DE INTENCAO: "
                "'quantos' => COUNT/SUM; "
                "'quais/listar' => SELECT DISTINCT de nomes/descricoes; "
                "'melhor/pior/top' => ORDER BY com LIMIT 20 (ou TOP N solicitado).\n"
                "FORMATO OBRIGATORIO: responder SOMENTE JSON com as chaves: "
                "{\"sql_text\":\"...\","
                "\"natural_response_template\":\"...\","
                "\"chart_suggestion\":\"bar|line|pie|none\"}.\n"
                "Seguranca: SQL deve ser exclusivamente SELECT seguro."
            ),
            "question": question_text,
            "tables": rag.get("tables") or [],
            "columns": rag.get("columns") or {},
            "relationships": rag.get("relationships") or [],
            "semantic_docs": [
                {
                    "doc_key": item.get("doc_key"),
                    "content_text": item.get("content_text"),
                    "score": item.get("score"),
                }
                for item in (rag.get("semantic_docs") or [])[:8]
            ],
            "data_source_rag": [
                {
                    "data_source_id": item.get("data_source_id"),
                    "source_name": item.get("source_name"),
                    "source_type": item.get("source_type"),
                    "context_text": item.get("context_text"),
                }
                for item in (rag.get("data_source_rag") or [])[:6]
            ],
            "allowed_schemas_hint": ["public", "analytics", "iaops_gov"],
        }
        llm_output = self._invoke_llm_json(
            provider_name=provider_name,
            model_code=model_code,
            endpoint_url=endpoint_url,
            api_key=secret_value,
            prompt_payload=prompt_payload,
        )
        if use_app_default_llm:
            try:
                self._record_app_llm_usage(
                    tenant_id=int(context.get("tenant_id") or 0),
                    feature_code="chat_bi_planner",
                    provider_name=provider_name,
                    model_code=model_code,
                    prompt_payload=prompt_payload,
                    llm_output=llm_output,
                )
            except Exception:
                pass
        sql_text = str((llm_output or {}).get("sql_text") or "").strip()
        if not sql_text:
            return {"mode": "llm_empty", "sql_text": None}
        if not self._is_planned_sql_allowed(sql_text):
            return {"mode": "llm_rejected", "sql_text": None}
        natural_template = str((llm_output or {}).get("natural_response_template") or "").strip() or None
        chart_suggestion = str((llm_output or {}).get("chart_suggestion") or "").strip().lower()
        if chart_suggestion not in {"bar", "line", "pie", "none"}:
            chart_suggestion = None
        return {
            "mode": "llm",
            "sql_text": sql_text,
            "llm_provider": provider_name,
            "natural_response_template": natural_template,
            "chart_suggestion": chart_suggestion,
        }

    def _record_app_llm_usage(
        self,
        *,
        tenant_id: int,
        feature_code: str,
        provider_name: str,
        model_code: str,
        prompt_payload: dict,
        llm_output: dict | None,
    ) -> None:
        if tenant_id <= 0 or not self._is_db_enabled():
            return
        input_tokens = self._estimate_tokens(json.dumps(prompt_payload or {}, ensure_ascii=True))
        output_tokens = self._estimate_tokens(json.dumps(llm_output or {}, ensure_ascii=True))
        total_tokens = int(input_tokens + output_tokens)
        price_per_1k = int(os.getenv("IAOPS_APP_LLM_PRICE_PER_1K_CENTS") or "50")
        amount_cents = int((total_tokens * price_per_1k + 999) // 1000)
        schema = self.signup_schema
        with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {schema}.llm_usage_meter (
                    tenant_id, feature_code, model_code, provider_name,
                    input_tokens, output_tokens, total_tokens, amount_cents
                )
                VALUES (
                    %(tenant_id)s, %(feature_code)s, %(model_code)s, %(provider_name)s,
                    %(input_tokens)s, %(output_tokens)s, %(total_tokens)s, %(amount_cents)s
                )
                """,
                {
                    "tenant_id": tenant_id,
                    "feature_code": feature_code,
                    "model_code": model_code or None,
                    "provider_name": provider_name or None,
                    "input_tokens": int(max(1, input_tokens)),
                    "output_tokens": int(max(1, output_tokens)),
                    "total_tokens": int(max(1, total_tokens)),
                    "amount_cents": int(max(0, amount_cents)),
                },
            )
            conn.commit()

    @staticmethod
    def _estimate_tokens(text: str) -> int:
        size = len(str(text or ""))
        return max(1, size // 4)

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
            env_value = os.getenv(f"IAOPS_SECRET_{normalized}")
            if env_value:
                return env_value
        # Compatibilidade: se a chave foi salva diretamente no secret_ref, usa valor bruto.
        if "://" not in ref and len(ref) >= 12:
            return ref
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
                    "content": (
                        "You are the IAOps semantic SQL planner for business users. "
                        "Return only valid JSON with keys sql_text, natural_response_template, chart_suggestion. "
                        "SQL must be SELECT-only and tenant-safe."
                    ),
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
        tokens = IAOpsAPIHandler._normalize_query_tokens(question_text)
        if not tokens:
            return []
        scored: list[tuple[int, dict]] = []
        for item in tables:
            schema_name = str(item.get("schema_name", "") or "").lower()
            table_name = str(item.get("table_name", "") or "").lower()
            singular = table_name[:-1] if table_name.endswith("s") else table_name
            haystack = f"{schema_name} {table_name} {singular}"
            score = sum(1 for token in tokens if token in haystack)
            if score > 0:
                scored.append((score, item))
        scored.sort(key=lambda pair: pair[0], reverse=True)
        return [item for _, item in scored]

    @staticmethod
    def _normalize_query_tokens(question_text: str) -> list[str]:
        raw = unicodedata.normalize("NFKD", str(question_text or "").lower())
        raw = "".join(ch for ch in raw if not unicodedata.combining(ch))
        tokens = [token for token in re.split(r"[^a-zA-Z0-9_]+", raw) if len(token) > 1]
        synonyms = {
            "filme": "film",
            "filmes": "film",
            "ator": "actor",
            "atores": "actor",
            "quais": "which",
            "qual": "which",
            "quem": "who",
            "cliente": "customer",
            "clientes": "customer",
            "aluguel": "rental",
            "locacao": "rental",
            "devolucao": "return",
            "pagamento": "payment",
            "pagamentos": "payment",
            "loja": "store",
            "lojas": "store",
            "inventario": "inventory",
            "inventarios": "inventory",
            "por": "by",
            "cuantos": "count",
        }
        return [synonyms.get(token, token) for token in tokens]

    @staticmethod
    def _is_dimension_question(tokens: list[str]) -> bool:
        token_set = set(tokens or [])
        return bool(token_set & {"which", "who", "list", "mostrar", "mostre", "show", "top", "qual", "quais"})

    @staticmethod
    def _route_nl_intent(question_text: str) -> dict:
        tokens = IAOpsAPIHandler._normalize_query_tokens(question_text)
        token_set = set(tokens)
        mode = "summary"
        if token_set & {"list", "listar", "lista"}:
            mode = "list"
        elif token_set & {"which", "who", "show", "mostrar", "mostre"}:
            mode = "which"
        elif token_set & {"quantos", "qtd", "count", "how", "many", "cuantos", "total"}:
            mode = "count"
        if token_set & {"top", "mais", "maiores", "ranking", "highest", "most"}:
            mode = "top"
        top_n = 20
        try:
            match = re.search(r"\btop\s*(\d{1,3})\b", str(question_text or "").lower())
            if match:
                top_n = max(1, min(int(match.group(1)), 100))
        except Exception:
            top_n = 20
        needs_dimension = mode in {"which", "list", "top"} or ("by" in token_set) or ("por" in token_set)
        intent_type = mode
        return {
            "mode": mode,
            "intent_type": intent_type,
            "top_n": top_n,
            "needs_dimension": bool(needs_dimension),
            "tokens": tokens,
        }

    @staticmethod
    def _resolve_severity_token(tokens: list[str]) -> str | None:
        token_set = set(tokens or [])
        if token_set & {"low", "baixa", "baixo"}:
            return "low"
        if token_set & {"medium", "media", "mediana"}:
            return "medium"
        if token_set & {"high", "alta", "alto"}:
            return "high"
        if token_set & {"critical", "critica", "critico"}:
            return "critical"
        return None

    @staticmethod
    def _plan_top_relation_sql(tokens: list[str], rag: dict, include_dimension: bool, top_n: int = 20) -> str | None:
        if not tokens:
            return None
        token_set = set(tokens)
        if not (token_set & {"mais", "top", "ranking", "maiores", "highest", "most"}):
            return None
        if not ({"actor", "film"} <= token_set):
            return None
        limit_n = int(max(1, min(int(top_n or 20), 100)))
        tables = rag.get("tables") or []
        columns_by_table = rag.get("columns") or {}
        for table in tables:
            schema_name = str(table.get("schema_name") or "").strip()
            table_name = str(table.get("table_name") or "").strip()
            key = f"{schema_name}.{table_name}"
            cols = {str(col.get("column_name") or "").strip().lower() for col in (columns_by_table.get(key) or [])}
            if {"actor_id", "film_id"} <= cols:
                if not include_dimension:
                    return f"SELECT COUNT(DISTINCT actor_id) AS total FROM {schema_name}.{table_name}"
                actor_table = next(
                    (
                        t
                        for t in tables
                        if str(t.get("schema_name") or "").strip().lower() == schema_name.lower()
                        and str(t.get("table_name") or "").strip().lower() == "actor"
                    ),
                    None,
                )
                actor_cols = set()
                if actor_table:
                    actor_key = f"{actor_table.get('schema_name')}.{actor_table.get('table_name')}"
                    actor_cols = {str(col.get("column_name") or "").strip().lower() for col in (columns_by_table.get(actor_key) or [])}
                if {"actor_id", "first_name", "last_name"} <= actor_cols:
                    return (
                        "SELECT fa.actor_id, a.first_name, a.last_name, COUNT(DISTINCT fa.film_id) AS total "
                        f"FROM {schema_name}.{table_name} fa "
                        f"JOIN {schema_name}.actor a ON a.actor_id = fa.actor_id "
                        "GROUP BY fa.actor_id, a.first_name, a.last_name "
                        "ORDER BY total DESC "
                        f"LIMIT {limit_n}"
                    )
                if {"actor_id", "name"} <= actor_cols:
                    return (
                        "SELECT fa.actor_id, a.name, COUNT(DISTINCT fa.film_id) AS total "
                        f"FROM {schema_name}.{table_name} fa "
                        f"JOIN {schema_name}.actor a ON a.actor_id = fa.actor_id "
                        "GROUP BY fa.actor_id, a.name "
                        "ORDER BY total DESC "
                        f"LIMIT {limit_n}"
                    )
                return (
                    f"SELECT actor_id, COUNT(DISTINCT film_id) AS total "
                    f"FROM {schema_name}.{table_name} "
                    f"GROUP BY actor_id "
                    f"ORDER BY total DESC "
                    f"LIMIT {limit_n}"
                )
        return None

    @staticmethod
    def _plan_grouped_count_sql(tokens: list[str], rag: dict) -> str | None:
        if not tokens:
            return None
        if not IAOpsAPIHandler._is_dimension_question(tokens):
            return None
        if "by" not in tokens and "por" not in tokens:
            return None
        if not any(term in tokens for term in ["quantos", "qtd", "total", "count", "how", "many"]):
            return None
        by_idx = tokens.index("by") if "by" in tokens else tokens.index("por")
        if by_idx <= 0 or by_idx >= len(tokens) - 1:
            return None
        measure_entity = tokens[by_idx - 1]
        group_entity = tokens[by_idx + 1]
        if not re.fullmatch(r"[a-z_][a-z0-9_]*", measure_entity):
            return None
        if not re.fullmatch(r"[a-z_][a-z0-9_]*", group_entity):
            return None

        tables = rag.get("tables") or []
        columns_by_table = rag.get("columns") or {}
        for table in tables:
            schema_name = str(table.get("schema_name") or "").strip()
            table_name = str(table.get("table_name") or "").strip()
            if not schema_name or not table_name:
                continue
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema_name):
                continue
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name):
                continue
            key = f"{schema_name}.{table_name}"
            cols = columns_by_table.get(key) or []
            col_names = {str(col.get("column_name") or "").strip().lower() for col in cols}
            group_col = f"{group_entity}_id"
            measure_col = f"{measure_entity}_id"
            if group_col in col_names and measure_col in col_names:
                return (
                    f"SELECT {group_col}, COUNT(DISTINCT {measure_col}) AS total "
                    f"FROM {schema_name}.{table_name} "
                    f"GROUP BY {group_col} "
                    f"ORDER BY total DESC "
                    f"LIMIT 50"
                )
        return None

    @staticmethod
    def _plan_grouped_metric_sql(tokens: list[str], rag: dict) -> str | None:
        if not tokens:
            return None
        token_set = set(tokens)
        if "by" not in token_set and "por" not in token_set:
            return None
        if not any(term in token_set for term in {"valor", "total", "faturamento", "receita", "soma", "amount", "revenue", "sales"}):
            return None
        by_idx = tokens.index("by") if "by" in tokens else tokens.index("por")
        if by_idx >= len(tokens) - 1:
            return None
        group_entity = tokens[by_idx + 1]
        if not re.fullmatch(r"[a-z_][a-z0-9_]*", group_entity):
            return None
        if group_entity.endswith("s") and len(group_entity) > 3:
            group_entity = group_entity[:-1]

        tables = rag.get("tables") or []
        columns_by_table = rag.get("columns") or {}
        for table in tables:
            schema_name = str(table.get("schema_name") or "").strip()
            table_name = str(table.get("table_name") or "").strip()
            if not schema_name or not table_name:
                continue
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema_name):
                continue
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name):
                continue
            key = f"{schema_name}.{table_name}"
            cols = columns_by_table.get(key) or []
            if not cols:
                continue

            group_col = None
            metric_col = None
            for col in cols:
                col_name = str(col.get("column_name") or "").strip().lower()
                if not col_name:
                    continue
                if group_col is None:
                    if (
                        col_name == group_entity
                        or col_name == f"{group_entity}_id"
                        or col_name.startswith(f"{group_entity}_")
                        or group_entity in col_name
                    ):
                        group_col = col_name

                if metric_col is None:
                    class_hint = str(col.get("classification") or col.get("llm_classification_suggested") or "").strip().lower()
                    if class_hint == "financial":
                        metric_col = col_name
                    elif any(term in col_name for term in ["valor", "amount", "price", "preco", "total", "revenue", "fatur", "receita", "sale"]):
                        metric_col = col_name

            if group_col and metric_col:
                return (
                    f"SELECT {group_col}, SUM({metric_col}) AS total "
                    f"FROM {schema_name}.{table_name} "
                    f"GROUP BY {group_col} "
                    f"ORDER BY total DESC "
                    f"LIMIT 100"
                )
        return None

    def _reply_nl_result(
        self,
        question_text: str,
        sql_text: str,
        query_data: dict,
        rag: dict,
        response_mode: str,
        language_code: str,
        intent: dict | None = None,
        natural_response_template: str | None = None,
    ) -> str:
        rows = query_data.get("rows") or []
        columns = query_data.get("columns") or []
        bucket = self._language_bucket(language_code)
        question = str(question_text or "").strip()
        lowered = question.lower()
        routed = intent or {}
        mode = str(routed.get("mode") or "summary")

        def _row_label(row: dict) -> str:
            if not isinstance(row, dict):
                return ""
            first_name = str(row.get("first_name") or "").strip()
            last_name = str(row.get("last_name") or "").strip()
            if first_name or last_name:
                return " ".join(part for part in [first_name, last_name] if part).strip()
            preferred = ["name", "nome", "title", "descricao", "description", "label", "ator", "actor"]
            for key in preferred:
                value = str(row.get(key) or "").strip()
                if value:
                    return value
            for col in columns:
                if str(col).lower() == "total":
                    continue
                value = str(row.get(col) or "").strip()
                if value:
                    return value
            return ""

        templated = self._apply_natural_response_template(
            natural_response_template=natural_response_template,
            rows=rows,
            columns=columns,
        )
        if templated:
            return templated

        if not rows:
            if bucket == "en":
                return "I did not find records for this question."
            if bucket == "es":
                return "No encontre registros para esta pregunta."
            return "Nao encontrei registros para esta pergunta."

        def _fmt_total(total_value: int, groups: int) -> str:
            if bucket == "en":
                if groups <= 1:
                    return f"Total found: {total_value}."
                return f"Total found: {total_value} (across {groups} groups)."
            if bucket == "es":
                if groups <= 1:
                    return f"Total encontrado: {total_value}."
                return f"Total encontrado: {total_value} (distribuido en {groups} grupos)."
            if groups <= 1:
                return f"Total encontrado: {total_value}."
            return f"Total encontrado: {total_value} (distribuido em {groups} grupos)."

        def _fmt_main_items(items: list[str], *, top_mode: bool = False) -> str:
            joined = ", ".join(items)
            if bucket == "en":
                return f"{'Top results' if top_mode else 'Main items'}: {joined}."
            if bucket == "es":
                return f"{'Top resultados' if top_mode else 'Principales elementos'}: {joined}."
            return f"{'Top resultados' if top_mode else 'Principais itens'}: {joined}."

        def _fmt_found_rows(total_rows: int) -> str:
            if bucket == "en":
                return f"I found {total_rows} result(s)."
            if bucket == "es":
                return f"Encontre {total_rows} resultado(s)."
            return f"Encontrei {total_rows} resultado(s)."

        totals = []
        if "total" in columns:
            for row in rows:
                if isinstance(row, dict):
                    value = row.get("total")
                    if isinstance(value, (int, float)):
                        totals.append(value)
                    else:
                        try:
                            totals.append(float(value))
                        except Exception:
                            pass
        if totals:
            total_sum = int(sum(totals))
            if "incidente" in lowered and "aberto" in lowered:
                if "severity" in columns:
                    parts = []
                    for row in rows:
                        if not isinstance(row, dict):
                            continue
                        sev = str(row.get("severity") or "-").strip().lower()
                        sev_pt = {"low": "baixa", "medium": "media", "high": "alta", "critical": "critica"}.get(sev, sev)
                        sev_en = {"low": "low", "medium": "medium", "high": "high", "critical": "critical"}.get(sev, sev)
                        sev_es = {"low": "baja", "medium": "media", "high": "alta", "critical": "critica"}.get(sev, sev)
                        qtd = row.get("total")
                        if bucket == "en":
                            parts.append(f"{qtd} with {sev_en} severity")
                        elif bucket == "es":
                            parts.append(f"{qtd} de gravedad {sev_es}")
                        else:
                            parts.append(f"{qtd} de gravidade {sev_pt}")
                    if parts:
                        if bucket == "en":
                            return f"{total_sum} open incidents: {', '.join(parts)}."
                        if bucket == "es":
                            return f"{total_sum} incidentes abiertos: {', '.join(parts)}."
                        return f"{total_sum} incidentes abertos: {', '.join(parts)}."
                if bucket == "en":
                    return f"{total_sum} open incidents."
                if bucket == "es":
                    return f"{total_sum} incidentes abiertos."
                return f"{total_sum} incidentes abertos."
            if ("ator" in lowered or "atores" in lowered or "actor" in lowered) and ("filme" in lowered or "film" in lowered):
                top = []
                for row in rows[:5]:
                    if not isinstance(row, dict):
                        continue
                    qtd = row.get("total")
                    if qtd is None:
                        continue
                    first_name = str(row.get("first_name") or "").strip()
                    last_name = str(row.get("last_name") or "").strip()
                    actor_name = str(row.get("name") or "").strip()
                    actor_id = row.get("actor_id")
                    label = actor_name or " ".join(part for part in [first_name, last_name] if part).strip()
                    if not label and actor_id is not None:
                        label = f"ator {actor_id}"
                    if label:
                        if bucket == "en":
                            top.append(f"{label} ({qtd} films)")
                        elif bucket == "es":
                            top.append(f"{label} ({qtd} peliculas)")
                        else:
                            top.append(f"{label} ({qtd} filmes)")
                if top:
                    if bucket == "en":
                        return f"Top actors by number of films: {', '.join(top)}."
                    if bucket == "es":
                        return f"Actores principales por cantidad de peliculas: {', '.join(top)}."
                    return f"Top atores por quantidade de filmes: {', '.join(top)}."
            if mode in {"which", "list", "top"}:
                details = []
                for row in rows[:5]:
                    if not isinstance(row, dict):
                        continue
                    label = _row_label(row)
                    qtd = row.get("total")
                    if label and qtd is not None:
                        details.append(f"{label} ({qtd})")
                    elif label:
                        details.append(label)
                if details:
                    return _fmt_main_items(details, top_mode=(mode == "top"))
                return _fmt_found_rows(len(rows))
            return _fmt_total(total_sum, len(rows))

        if ("ator" in lowered or "atores" in lowered or "actor" in lowered) and ("filme" in lowered or "film" in lowered):
            top_lines = []
            for row in rows[:5]:
                if isinstance(row, dict):
                    top_lines.append(", ".join(f"{k}: {row.get(k)}" for k in columns[:3]))
            if top_lines:
                if bucket == "en":
                    return "Top results found: " + " | ".join(top_lines)
                if bucket == "es":
                    return "Resultados principales encontrados: " + " | ".join(top_lines)
                return "Top resultados encontrados: " + " | ".join(top_lines)

        if response_mode == "detailed":
            preview = []
            for idx, row in enumerate(rows[:5], start=1):
                preview.append(f"{idx}. {json.dumps(row, ensure_ascii=True)}")
            return f"{self._t(language_code, 'found_records', total=len(rows))}\n{self._t(language_code, 'preview_label')}\n" + "\n".join(preview)
        return f"{self._t(language_code, 'found_records', total=len(rows))} {self._t(language_code, 'executive_summary')}"

    @staticmethod
    def _apply_natural_response_template(
        *,
        natural_response_template: str | None,
        rows: list[dict],
        columns: list[str],
    ) -> str | None:
        template = str(natural_response_template or "").strip()
        if not template:
            return None
        safe_rows = [row for row in (rows or []) if isinstance(row, dict)]
        if not safe_rows:
            return None
        first_row = safe_rows[0]
        total_value = None
        if "total" in columns:
            try:
                total_value = float(first_row.get("total"))
                if float(total_value).is_integer():
                    total_value = int(total_value)
            except Exception:
                total_value = None
        top_items = []
        for row in safe_rows[:5]:
            label = ""
            if str(row.get("first_name") or "").strip() or str(row.get("last_name") or "").strip():
                label = " ".join(
                    part for part in [str(row.get("first_name") or "").strip(), str(row.get("last_name") or "").strip()] if part
                ).strip()
            if not label:
                for key in ("name", "nome", "title", "descricao", "description", "label"):
                    value = str(row.get(key) or "").strip()
                    if value:
                        label = value
                        break
            if not label:
                for col in columns:
                    if str(col).lower() == "total":
                        continue
                    value = str(row.get(col) or "").strip()
                    if value:
                        label = value
                        break
            qtd = row.get("total")
            if label and qtd is not None:
                top_items.append(f"{label} ({qtd})")
            elif label:
                top_items.append(label)
        rendered = template
        rendered = rendered.replace("{{row_count}}", str(len(safe_rows)))
        rendered = rendered.replace("{{top_items}}", ", ".join(top_items))
        if total_value is not None:
            rendered = rendered.replace("{{total}}", str(total_value))
        # Substitui placeholders diretos por colunas da primeira linha: {{coluna}}
        for match in re.findall(r"\{\{([A-Za-z0-9_]+)\}\}", rendered):
            key = str(match or "").strip()
            if not key:
                continue
            replacement = first_row.get(key)
            if replacement is None and key.lower() != key:
                replacement = first_row.get(key.lower())
            rendered = rendered.replace(f"{{{{{key}}}}}", "" if replacement is None else str(replacement))
        rendered = re.sub(r"\s+", " ", rendered).strip()
        return rendered or None

    @staticmethod
    def _build_visualization_payload(
        *,
        question_text: str,
        query_data: dict,
        language_code: str,
        intent: dict | None = None,
        chart_suggestion: str | None = None,
    ) -> dict | None:
        rows = list(query_data.get("rows") or [])
        columns = list(query_data.get("columns") or [])
        if len(rows) < 2 or not columns:
            return None
        routed = intent or {}
        mode = str(routed.get("mode") or "")
        if mode == "count" and not routed.get("needs_dimension"):
            return None
        metric_col = "total" if "total" in columns else None
        if metric_col is None:
            numeric_candidates = []
            for col in columns:
                if any(isinstance((row or {}).get(col), (int, float)) for row in rows if isinstance(row, dict)):
                    numeric_candidates.append(col)
            if not numeric_candidates:
                return None
            metric_col = numeric_candidates[0]
        dimension_col = next((col for col in columns if col != metric_col), None)
        if not dimension_col:
            return None
        labels = []
        values = []
        for row in rows[:20]:
            if not isinstance(row, dict):
                continue
            label = str(row.get(dimension_col) or "").strip()
            if not label:
                continue
            value = row.get(metric_col)
            try:
                numeric = float(value)
            except Exception:
                continue
            labels.append(label)
            values.append(numeric)
        if len(labels) < 2:
            return None
        bucket = IAOpsAPIHandler._language_bucket(language_code)
        if bucket == "en":
            title = f"{dimension_col} by {metric_col}"
        elif bucket == "es":
            title = f"{dimension_col} por {metric_col}"
        else:
            title = f"{dimension_col} por {metric_col}"
        chart_type = "bar"
        suggested = str(chart_suggestion or "").strip().lower()
        if suggested in {"bar", "line", "pie"}:
            chart_type = suggested
        elif "evolucao" in str(question_text or "").lower() or "trend" in str(question_text or "").lower():
            chart_type = "line"
        chart_url = IAOpsAPIHandler._build_quickchart_url(
            chart_type=chart_type,
            title=title,
            labels=labels,
            values=values,
            series_name=str(metric_col),
        )
        return {
            "chart_type": chart_type,
            "title": title,
            "x_field": dimension_col,
            "y_field": metric_col,
            "labels": labels,
            "series": [{"name": metric_col, "values": values}],
            "chart_url": chart_url,
        }

    @staticmethod
    def _build_quickchart_url(
        *,
        chart_type: str,
        title: str,
        labels: list[str],
        values: list[float],
        series_name: str,
    ) -> str:
        payload = {
            "type": chart_type if chart_type in {"bar", "line", "pie"} else "bar",
            "data": {
                "labels": labels[:20],
                "datasets": [
                    {
                        "label": series_name,
                        "data": values[:20],
                        "backgroundColor": "#0f5c84",
                        "borderColor": "#0f5c84",
                        "fill": False,
                    }
                ],
            },
            "options": {
                "plugins": {"title": {"display": True, "text": title}},
                "legend": {"display": False},
            },
        }
        encoded = quote_plus(json.dumps(payload, ensure_ascii=True))
        return f"https://quickchart.io/chart?c={encoded}"

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
                "chat_query_failed_user": "Nao consegui executar esta consulta na base monitorada. Tente reformular a pergunta.",
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
                "chat_query_failed_user": "I could not run this query on the monitored data source. Please rephrase your question.",
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
                "chat_query_failed_user": "No pude ejecutar esta consulta en la fuente monitoreada. Intente reformular su pregunta.",
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

    def _handle_security_mcp_policy_update(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "security_mcp.update_policy",
            "input": {
                "tool_name": body.get("tool_name"),
                "is_enabled": body.get("is_enabled", True),
                "max_rows": body.get("max_rows"),
                "max_calls_per_minute": body.get("max_calls_per_minute"),
                "require_masking": body.get("require_masking", True),
                "allowed_schema_patterns": body.get("allowed_schema_patterns", []),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_mcp_connections_list(self) -> None:
        payload = {
            "context": self._request_context(),
            "tool": "mcp_client.list_connections",
            "input": {},
        }
        self._dispatch_mcp(payload)

    def _handle_mcp_connection_upsert(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "mcp_client.upsert_connection",
            "input": {
                "connection_name": body.get("connection_name"),
                "transport_type": body.get("transport_type"),
                "endpoint_url": body.get("endpoint_url"),
                "auth_secret_ref": body.get("auth_secret_ref"),
                "is_active": body.get("is_active", True),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_mcp_connection_status_update(self) -> None:
        body = self._read_json_body()
        payload = {
            "context": self._request_context(),
            "tool": "mcp_client.update_status",
            "input": {
                "connection_id": body.get("connection_id"),
                "is_active": body.get("is_active", True),
            },
        }
        self._dispatch_mcp(payload)

    def _handle_chat_bi_query(self) -> None:
        body = self._read_json_body()
        request_context = self._request_context()
        throttle = self._check_route_rate_limit(
            route_key="chat_bi_query",
            actor_key=f"{request_context.get('tenant_id')}:{request_context.get('user_id')}:{self._request_ip()}",
            max_calls=int(self.chat_rate_limit_max_calls),
            window_seconds=int(self.chat_rate_limit_window_seconds),
            lock_seconds=int(self.chat_rate_limit_lock_seconds),
        )
        if throttle:
            self._send_json(
                HTTPStatus.TOO_MANY_REQUESTS,
                {
                    "status": "denied",
                    "tool": "chat-bi.query",
                    "correlation_id": None,
                    "data": {},
                    "error": {
                        "code": "rate_limited",
                        "message": f"Muitas requisicoes ao Chat BI. Tente novamente em {throttle['blocked_remaining_seconds']}s.",
                    },
                },
            )
            return
        if self._is_db_enabled():
            allowed = self._is_tenant_operational_db(
                client_id=int(request_context.get("client_id") or 0),
                tenant_id=int(request_context.get("tenant_id") or 0),
            )
            if not allowed:
                self._send_json(
                    HTTPStatus.FORBIDDEN,
                    {
                        "status": "denied",
                        "tool": "chat-bi.query",
                        "correlation_id": None,
                        "data": {},
                        "error": {
                            "code": "tenant_blocked",
                            "message": "Tenant bloqueado por inadimplencia ou inatividade. Regularize o faturamento para continuar.",
                        },
                    },
                )
                return
        language_code = self._resolve_language_code(request_context)
        question_text = str(body.get("question_text") or body.get("question") or "").strip()
        raw_source_id = body.get("data_source_id")
        data_source_id: int | None
        try:
            data_source_id = int(raw_source_id) if raw_source_id not in (None, "", 0, "0") else None
        except Exception:
            data_source_id = None
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
        response = self._execute_nl_chat_query(
            request_context,
            question_text,
            language_code,
            data_source_id=data_source_id,
        )
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

    def _handle_lgpd_policy_get(self) -> None:
        context = self._request_context()
        try:
            policy = self._db_get_lgpd_policy(tenant_id=int(context["tenant_id"]))
            self._send_json(
                HTTPStatus.OK,
                {
                    "status": "success",
                    "tool": "lgpd.policy.get",
                    "correlation_id": str(uuid.uuid4()),
                    "data": {"policy": policy},
                    "error": None,
                },
            )
        except Exception as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"status": "denied", "tool": "lgpd.policy.get", "data": {}, "error": {"code": "lgpd_error", "message": str(exc)}})

    def _handle_lgpd_policy_upsert(self) -> None:
        context = self._request_context()
        body = self._read_json_body()
        try:
            policy = self._db_upsert_lgpd_policy(
                tenant_id=int(context["tenant_id"]),
                user_id=int(context["user_id"]),
                dpo_name=body.get("dpo_name"),
                dpo_email=body.get("dpo_email"),
                retention_days=body.get("retention_days"),
                legal_notes=body.get("legal_notes"),
            )
            self._send_json(HTTPStatus.OK, {"status": "success", "tool": "lgpd.policy.upsert", "correlation_id": str(uuid.uuid4()), "data": {"policy": policy}, "error": None})
        except Exception as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"status": "denied", "tool": "lgpd.policy.upsert", "data": {}, "error": {"code": "lgpd_error", "message": str(exc)}})

    def _handle_lgpd_rules_list(self) -> None:
        context = self._request_context()
        try:
            rows = self._db_list_lgpd_rules(tenant_id=int(context["tenant_id"]))
            self._send_json(HTTPStatus.OK, {"status": "success", "tool": "lgpd.rules.list", "correlation_id": str(uuid.uuid4()), "data": {"rules": rows}, "error": None})
        except Exception as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"status": "denied", "tool": "lgpd.rules.list", "data": {}, "error": {"code": "lgpd_error", "message": str(exc)}})

    def _handle_lgpd_rule_upsert(self) -> None:
        context = self._request_context()
        body = self._read_json_body()
        try:
            rule = self._db_upsert_lgpd_rule(
                tenant_id=int(context["tenant_id"]),
                user_id=int(context["user_id"]),
                rule_id=body.get("id"),
                schema_name=body.get("schema_name"),
                table_name=body.get("table_name"),
                column_name=body.get("column_name"),
                rule_type=body.get("rule_type"),
                rule_config=body.get("rule_config") if isinstance(body.get("rule_config"), dict) else {},
                is_active=bool(body.get("is_active", True)),
            )
            self._send_json(HTTPStatus.OK, {"status": "success", "tool": "lgpd.rules.upsert", "correlation_id": str(uuid.uuid4()), "data": {"rule": rule}, "error": None})
        except Exception as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"status": "denied", "tool": "lgpd.rules.upsert", "data": {}, "error": {"code": "lgpd_error", "message": str(exc)}})

    def _handle_lgpd_dsr_list(self, query: str) -> None:
        context = self._request_context()
        qs = parse_qs(query)
        status = qs.get("status", [None])[0]
        try:
            rows = self._db_list_lgpd_dsr(tenant_id=int(context["tenant_id"]), status=status)
            self._send_json(HTTPStatus.OK, {"status": "success", "tool": "lgpd.dsr.list", "correlation_id": str(uuid.uuid4()), "data": {"requests": rows}, "error": None})
        except Exception as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"status": "denied", "tool": "lgpd.dsr.list", "data": {}, "error": {"code": "lgpd_error", "message": str(exc)}})

    def _handle_lgpd_dsr_open(self) -> None:
        context = self._request_context()
        body = self._read_json_body()
        try:
            req = self._db_open_lgpd_dsr(
                tenant_id=int(context["tenant_id"]),
                user_id=int(context["user_id"]),
                requester_name=body.get("requester_name"),
                requester_email=body.get("requester_email"),
                request_type=body.get("request_type"),
                subject_key=body.get("subject_key"),
                notes=body.get("notes"),
            )
            self._send_json(HTTPStatus.OK, {"status": "success", "tool": "lgpd.dsr.open", "correlation_id": str(uuid.uuid4()), "data": {"request": req}, "error": None})
        except Exception as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"status": "denied", "tool": "lgpd.dsr.open", "data": {}, "error": {"code": "lgpd_error", "message": str(exc)}})

    def _handle_lgpd_dsr_resolve(self) -> None:
        context = self._request_context()
        body = self._read_json_body()
        try:
            req = self._db_resolve_lgpd_dsr(
                tenant_id=int(context["tenant_id"]),
                user_id=int(context["user_id"]),
                request_id=body.get("request_id"),
                notes=body.get("notes"),
            )
            self._send_json(HTTPStatus.OK, {"status": "success", "tool": "lgpd.dsr.resolve", "correlation_id": str(uuid.uuid4()), "data": {"request": req}, "error": None})
        except Exception as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"status": "denied", "tool": "lgpd.dsr.resolve", "data": {}, "error": {"code": "lgpd_error", "message": str(exc)}})

    def _handle_billing_plans_list(self) -> None:
        try:
            plans = self._db_list_billing_plans()
            self._send_json(HTTPStatus.OK, {"status": "success", "tool": "billing.plans.list", "correlation_id": str(uuid.uuid4()), "data": {"plans": plans}, "error": None})
        except Exception as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"status": "denied", "tool": "billing.plans.list", "data": {}, "error": {"code": "billing_error", "message": str(exc)}})

    def _handle_billing_subscription_get(self) -> None:
        context = self._request_context()
        try:
            sub = self._db_get_billing_subscription(client_id=int(context["client_id"]))
            self._send_json(HTTPStatus.OK, {"status": "success", "tool": "billing.subscription.get", "correlation_id": str(uuid.uuid4()), "data": {"subscription": sub}, "error": None})
        except Exception as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"status": "denied", "tool": "billing.subscription.get", "data": {}, "error": {"code": "billing_error", "message": str(exc)}})

    def _handle_billing_subscription_upsert(self) -> None:
        context = self._request_context()
        body = self._read_json_body()
        try:
            sub = self._db_upsert_billing_subscription(
                client_id=int(context["client_id"]),
                plan_code=body.get("plan_code"),
                tolerance_days=body.get("tolerance_days"),
            )
            self._send_json(HTTPStatus.OK, {"status": "success", "tool": "billing.subscription.upsert", "correlation_id": str(uuid.uuid4()), "data": {"subscription": sub}, "error": None})
        except Exception as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"status": "denied", "tool": "billing.subscription.upsert", "data": {}, "error": {"code": "billing_error", "message": str(exc)}})

    def _handle_billing_plan_upsert(self) -> None:
        context = self._request_context()
        if not self._is_superadmin_user(user_id=int(context.get("user_id") or 0)):
            self._send_json(
                HTTPStatus.FORBIDDEN,
                {
                    "status": "denied",
                    "tool": "billing.plan.upsert",
                    "data": {},
                    "error": {"code": "superadmin_required", "message": "Acesso restrito a superadmin."},
                },
            )
            return
        body = self._read_json_body()
        try:
            plan = self._db_upsert_billing_plan(
                plan_id=body.get("id"),
                code=body.get("code"),
                name=body.get("name"),
                max_tenants=body.get("max_tenants"),
                max_users=body.get("max_users"),
                max_data_sources_per_client=body.get("max_data_sources_per_client"),
                max_data_sources_per_tenant=body.get("max_data_sources_per_tenant"),
                monthly_price_cents=body.get("monthly_price_cents"),
                is_active=body.get("is_active"),
            )
            self._send_json(
                HTTPStatus.OK,
                {
                    "status": "success",
                    "tool": "billing.plan.upsert",
                    "correlation_id": str(uuid.uuid4()),
                    "data": {"plan": plan},
                    "error": None,
                },
            )
        except Exception as exc:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "billing.plan.upsert",
                    "data": {},
                    "error": {"code": "billing_error", "message": str(exc)},
                },
            )

    def _handle_billing_plan_delete(self) -> None:
        context = self._request_context()
        if not self._is_superadmin_user(user_id=int(context.get("user_id") or 0)):
            self._send_json(
                HTTPStatus.FORBIDDEN,
                {
                    "status": "denied",
                    "tool": "billing.plan.delete",
                    "data": {},
                    "error": {"code": "superadmin_required", "message": "Acesso restrito a superadmin."},
                },
            )
            return
        body = self._read_json_body()
        try:
            result = self._db_delete_billing_plan(plan_id=body.get("id"))
            self._send_json(
                HTTPStatus.OK,
                {
                    "status": "success",
                    "tool": "billing.plan.delete",
                    "correlation_id": str(uuid.uuid4()),
                    "data": result,
                    "error": None,
                },
            )
        except Exception as exc:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "billing.plan.delete",
                    "data": {},
                    "error": {"code": "billing_error", "message": str(exc)},
                },
            )

    def _handle_billing_installments_list(self, query: str) -> None:
        context = self._request_context()
        qs = parse_qs(query)
        status = qs.get("status", [None])[0]
        try:
            rows = self._db_list_billing_installments(client_id=int(context["client_id"]), status=status)
            self._send_json(HTTPStatus.OK, {"status": "success", "tool": "billing.installments.list", "correlation_id": str(uuid.uuid4()), "data": {"installments": rows}, "error": None})
        except Exception as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"status": "denied", "tool": "billing.installments.list", "data": {}, "error": {"code": "billing_error", "message": str(exc)}})

    def _handle_billing_llm_usage(self, query: str) -> None:
        context = self._request_context()
        qs = parse_qs(query)
        days = int(qs.get("days", ["30"])[0])
        tenant_qs = qs.get("tenant_id", [None])[0]
        tenant_id = int(tenant_qs) if tenant_qs not in (None, "") else int(context["tenant_id"])
        if tenant_id <= 0:
            self._send_json(
                HTTPStatus.OK,
                {
                    "status": "success",
                    "tool": "billing.llm_usage",
                    "correlation_id": str(uuid.uuid4()),
                    "data": {
                        "summary": {
                            "days": max(1, min(days, 365)),
                            "calls": 0,
                            "input_tokens": 0,
                            "output_tokens": 0,
                            "total_tokens": 0,
                            "amount_cents": 0,
                        },
                        "by_feature": [],
                        "recent": [],
                    },
                    "error": None,
                },
            )
            return
        if not self._can_access_tenant_billing_usage(
            user_id=int(context["user_id"]),
            client_id=int(context["client_id"]),
            tenant_id=tenant_id,
        ):
            self._send_json(
                HTTPStatus.FORBIDDEN,
                {
                    "status": "denied",
                    "tool": "billing.llm_usage",
                    "data": {},
                    "error": {"code": "insufficient_role", "message": "Sem permissao para consultar consumo deste tenant."},
                },
            )
            return
        try:
            usage = self._db_get_llm_usage_report(tenant_id=tenant_id, days=max(1, min(days, 365)))
            self._send_json(
                HTTPStatus.OK,
                {
                    "status": "success",
                    "tool": "billing.llm_usage",
                    "correlation_id": str(uuid.uuid4()),
                    "data": usage,
                    "error": None,
                },
            )
        except Exception as exc:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "denied",
                    "tool": "billing.llm_usage",
                    "data": {},
                    "error": {"code": "billing_error", "message": str(exc)},
                },
            )

    def _handle_billing_llm_usage_export_csv(self, query: str) -> None:
        context = self._request_context()
        qs = parse_qs(query)
        days = int(qs.get("days", ["30"])[0])
        tenant_qs = qs.get("tenant_id", [None])[0]
        tenant_id = int(tenant_qs) if tenant_qs not in (None, "") else int(context["tenant_id"])
        if tenant_id <= 0:
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(["section", "feature_code", "provider_name", "model_code", "calls", "input_tokens", "output_tokens", "total_tokens", "amount_cents", "created_at"])
            payload = output.getvalue().encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", f'attachment; filename="llm-usage-tenant-global-{days}d.csv"')
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
            self.send_header(
                "Access-Control-Allow-Headers",
                "Content-Type, X-Client-Id, X-Tenant-Id, X-User-Id, X-Correlation-Id, X-Session-Token",
            )
            self.end_headers()
            self.wfile.write(payload)
            return
        if not self._can_access_tenant_billing_usage(
            user_id=int(context["user_id"]),
            client_id=int(context["client_id"]),
            tenant_id=tenant_id,
        ):
            self._send_json(
                HTTPStatus.FORBIDDEN,
                {
                    "status": "denied",
                    "tool": "billing.llm_usage.csv",
                    "data": {},
                    "error": {"code": "insufficient_role", "message": "Sem permissao para exportar consumo deste tenant."},
                },
            )
            return
        usage = self._db_get_llm_usage_report(tenant_id=tenant_id, days=max(1, min(days, 365)))
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["section", "feature_code", "provider_name", "model_code", "calls", "input_tokens", "output_tokens", "total_tokens", "amount_cents", "created_at"])
        summary = usage.get("summary") or {}
        writer.writerow(
            [
                "summary",
                "",
                "",
                "",
                summary.get("calls", 0),
                summary.get("input_tokens", 0),
                summary.get("output_tokens", 0),
                summary.get("total_tokens", 0),
                summary.get("amount_cents", 0),
                "",
            ]
        )
        for row in usage.get("by_feature") or []:
            writer.writerow(
                [
                    "by_feature",
                    row.get("feature_code", ""),
                    "",
                    "",
                    row.get("calls", 0),
                    "",
                    "",
                    row.get("total_tokens", 0),
                    row.get("amount_cents", 0),
                    "",
                ]
            )
        for row in usage.get("recent") or []:
            writer.writerow(
                [
                    "recent",
                    row.get("feature_code", ""),
                    row.get("provider_name", ""),
                    row.get("model_code", ""),
                    "",
                    row.get("input_tokens", 0),
                    row.get("output_tokens", 0),
                    row.get("total_tokens", 0),
                    row.get("amount_cents", 0),
                    row.get("created_at", ""),
                ]
            )
        payload = output.getvalue().encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header(
            "Content-Disposition",
            f'attachment; filename="llm-usage-tenant-{tenant_id}-{days}d.csv"',
        )
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header(
            "Access-Control-Allow-Headers",
            "Content-Type, X-Client-Id, X-Tenant-Id, X-User-Id, X-Correlation-Id, X-Session-Token",
        )
        self.end_headers()
        self.wfile.write(payload)

    def _handle_billing_installments_generate(self) -> None:
        context = self._request_context()
        body = self._read_json_body()
        due_date = body.get("due_date")
        if not due_date:
            self._send_json(HTTPStatus.BAD_REQUEST, {"status": "denied", "tool": "billing.installments.generate", "data": {}, "error": {"code": "invalid_input", "message": "due_date obrigatorio"}})
            return
        try:
            result = self._db_generate_billing_installment(client_id=int(context["client_id"]), due_date=str(due_date))
            self._send_json(HTTPStatus.OK, {"status": "success", "tool": "billing.installments.generate", "correlation_id": str(uuid.uuid4()), "data": result, "error": None})
        except Exception as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"status": "denied", "tool": "billing.installments.generate", "data": {}, "error": {"code": "billing_error", "message": str(exc)}})

    def _handle_billing_installment_pay(self) -> None:
        body = self._read_json_body()
        installment_id = body.get("installment_id")
        if installment_id is None:
            self._send_json(HTTPStatus.BAD_REQUEST, {"status": "denied", "tool": "billing.installments.pay", "data": {}, "error": {"code": "invalid_input", "message": "installment_id obrigatorio"}})
            return
        try:
            row = self._db_pay_billing_installment(installment_id=int(installment_id))
            self._send_json(HTTPStatus.OK, {"status": "success", "tool": "billing.installments.pay", "correlation_id": str(uuid.uuid4()), "data": {"installment": row}, "error": None})
        except Exception as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"status": "denied", "tool": "billing.installments.pay", "data": {}, "error": {"code": "billing_error", "message": str(exc)}})

    def _handle_jobs_enqueue(self, job_kind: str) -> None:
        body = self._read_json_body()
        context = self._request_context()
        throttle = self._check_route_rate_limit(
            route_key=f"jobs_enqueue:{job_kind}",
            actor_key=f"{context.get('tenant_id')}:{context.get('user_id')}:{self._request_ip()}",
            max_calls=int(self.jobs_rate_limit_max_calls),
            window_seconds=int(self.jobs_rate_limit_window_seconds),
            lock_seconds=int(self.jobs_rate_limit_lock_seconds),
        )
        if throttle:
            self._send_json(
                HTTPStatus.TOO_MANY_REQUESTS,
                {
                    "status": "denied",
                    "tool": f"jobs.{job_kind}",
                    "data": {},
                    "error": {
                        "code": "rate_limited",
                        "message": f"Muitas requisicoes de jobs. Tente novamente em {throttle['blocked_remaining_seconds']}s.",
                    },
                },
            )
            return
        if self._is_db_enabled():
            allowed = self._is_tenant_operational_db(
                client_id=int(context.get("client_id") or 0),
                tenant_id=int(context.get("tenant_id") or 0),
            )
            if not allowed:
                self._send_json(
                    HTTPStatus.FORBIDDEN,
                    {
                        "status": "denied",
                        "tool": f"jobs.{job_kind}",
                        "data": {},
                        "error": {
                            "code": "tenant_blocked",
                            "message": "Tenant bloqueado por inadimplencia ou inatividade. Regularize o faturamento para enfileirar jobs.",
                        },
                    },
                )
                return
        queue = get_job_queue(self._get_db_dsn(), self.signup_schema)
        payload = dict(body) if isinstance(body, dict) else {}
        payload.setdefault("tenant_id", int(context.get("tenant_id") or 0))
        result = queue.enqueue(tenant_id=int(context.get("tenant_id") or 0), job_kind=job_kind, payload=payload)
        self._send_json(HTTPStatus.OK, {"status": "success", "tool": f"jobs.{job_kind}", "correlation_id": str(uuid.uuid4()), "data": result, "error": None})

    def _handle_jobs_list(self, query: str) -> None:
        context = self._request_context()
        qs = parse_qs(query)
        limit = int(qs.get("limit", ["50"])[0])
        offset = int(qs.get("offset", ["0"])[0])
        queue = get_job_queue(self._get_db_dsn(), self.signup_schema)
        rows = queue.list_jobs(tenant_id=int(context.get("tenant_id") or 0), limit=limit, offset=offset)
        self._send_json(
            HTTPStatus.OK,
            {
                "status": "success",
                "tool": "jobs.list",
                "correlation_id": str(uuid.uuid4()),
                "data": {"jobs": rows, "limit": limit, "offset": offset},
                "error": None,
            },
        )

    def _handle_jobs_retry(self) -> None:
        body = self._read_json_body()
        job_id = body.get("job_id")
        if job_id in (None, ""):
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {"status": "denied", "tool": "jobs.retry", "data": {}, "error": {"code": "invalid_input", "message": "job_id obrigatorio"}},
            )
            return
        context = self._request_context()
        queue = get_job_queue(self._get_db_dsn(), self.signup_schema)
        try:
            result = queue.retry_job(tenant_id=int(context.get("tenant_id") or 0), job_id=int(job_id))
        except Exception as exc:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {"status": "denied", "tool": "jobs.retry", "data": {}, "error": {"code": "jobs_retry_error", "message": str(exc)}},
            )
            return
        self._send_json(
            HTTPStatus.OK,
            {"status": "success", "tool": "jobs.retry", "correlation_id": str(uuid.uuid4()), "data": result, "error": None},
        )

    def _handle_observability_metrics(self) -> None:
        context = self._request_context()
        tenant_id = int(context.get("tenant_id") or 0)
        metrics = {
            "active_sessions": len(self.active_sessions),
            "failed_login_buckets": len(self.failed_login_attempts),
            "failed_ip_buckets": len(self.failed_login_attempts_ip),
            "pending_mfa_challenges": len(self.mfa_login_challenges),
            "pending_password_resets": len(self.pending_password_resets),
            "tenant_id": tenant_id,
        }
        if self._is_db_enabled():
            try:
                metrics.update(self._db_collect_observability_metrics(tenant_id=tenant_id))
            except Exception:
                pass
        self._send_json(HTTPStatus.OK, {"status": "success", "tool": "observability.metrics", "correlation_id": str(uuid.uuid4()), "data": metrics, "error": None})

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
    def _load_local_env_file() -> None:
        if IAOpsAPIHandler.local_env_loaded:
            return
        IAOpsAPIHandler.local_env_loaded = True
        env_path = os.path.join(os.getcwd(), ".env")
        if not os.path.exists(env_path):
            return
        try:
            with open(env_path, "r", encoding="utf-8") as handle:
                for raw_line in handle:
                    line = raw_line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, value = line.split("=", 1)
                    key = key.strip()
                    if not key or key in os.environ:
                        continue
                    os.environ[key] = value.strip().strip("\"").strip("'")
        except Exception:
            return

    @staticmethod
    def _get_db_dsn() -> str | None:
        IAOpsAPIHandler._load_local_env_file()
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
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.user_password_reset (
                    id BIGSERIAL PRIMARY KEY,
                    reset_token TEXT NOT NULL UNIQUE,
                    user_id BIGINT,
                    email_access TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    expires_at TIMESTAMPTZ NOT NULL,
                    consumed_at TIMESTAMPTZ
                )
                """
            )
            cur.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_user_password_reset_email
                    ON {schema}.user_password_reset (LOWER(email_access), status)
                """
            )
            conn.commit()
        cls.signup_tables_ready = True

    @classmethod
    def _ensure_billing_plan_limits_columns(cls) -> None:
        if cls.billing_plan_limits_ready:
            return
        dsn = cls._get_db_dsn()
        if not dsn or connect is None:
            return
        schema = cls.signup_schema
        with connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                ALTER TABLE {schema}.billing_plan
                ADD COLUMN IF NOT EXISTS max_data_sources_per_client INTEGER NOT NULL DEFAULT 10
                """
            )
            cur.execute(
                f"""
                ALTER TABLE {schema}.billing_plan
                ADD COLUMN IF NOT EXISTS max_data_sources_per_tenant INTEGER NOT NULL DEFAULT 5
                """
            )
            conn.commit()
        cls.billing_plan_limits_ready = True

    @classmethod
    def _ensure_smtp_table(cls) -> None:
        if cls.smtp_table_ready:
            return
        dsn = cls._get_db_dsn()
        if not dsn or connect is None:
            return
        schema = cls.signup_schema
        with connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.app_smtp_config (
                    id SMALLINT PRIMARY KEY DEFAULT 1,
                    host TEXT,
                    port INTEGER NOT NULL DEFAULT 587,
                    smtp_user TEXT,
                    from_email TEXT,
                    starttls BOOLEAN NOT NULL DEFAULT TRUE,
                    password_enc TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            conn.commit()
        cls.smtp_table_ready = True

    @classmethod
    def _db_load_smtp_config(cls) -> dict[str, object]:
        dsn = cls._get_db_dsn()
        if not dsn or connect is None:
            return {}
        cls._ensure_smtp_table()
        schema = cls.signup_schema
        with connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT host, port, smtp_user, from_email, starttls, password_enc
                FROM {schema}.app_smtp_config
                WHERE id = 1
                LIMIT 1
                """
            )
            row = cur.fetchone()
        if not row:
            return {}
        host = str(row[0] or "").strip()
        try:
            port = int(row[1] or 587)
        except Exception:
            port = 587
        user = str(row[2] or "").strip()
        from_email = str(row[3] or "").strip()
        starttls = bool(row[4]) if row[4] is not None else True
        password = ""
        password_enc = str(row[5] or "").strip()
        if password_enc:
            try:
                password = decrypt_text(password_enc)
            except Exception:
                password = ""
        return {
            "host": host,
            "port": port,
            "user": user,
            "from_email": from_email,
            "starttls": starttls,
            "password": password,
        }

    @classmethod
    def _db_save_smtp_config(cls, payload: dict[str, object]) -> None:
        dsn = cls._get_db_dsn()
        if not dsn or connect is None:
            return
        cls._ensure_smtp_table()
        schema = cls.signup_schema
        host = str(payload.get("host") or "").strip() or None
        user = str(payload.get("user") or "").strip() or None
        from_email = str(payload.get("from_email") or "").strip() or None
        starttls = bool(payload.get("starttls", True))
        try:
            port = int(payload.get("port") or 587)
        except Exception:
            port = 587
        password_raw = payload.get("password")
        clear_password = bool(payload.get("clear_password"))
        password_enc = None
        include_password = False
        if clear_password:
            include_password = True
            password_enc = None
        elif password_raw is not None and str(password_raw).strip():
            include_password = True
            password_enc = encrypt_text(str(password_raw))

        with connect(dsn) as conn, conn.cursor() as cur:
            if include_password:
                cur.execute(
                    f"""
                    INSERT INTO {schema}.app_smtp_config (
                        id, host, port, smtp_user, from_email, starttls, password_enc, updated_at
                    )
                    VALUES (
                        1, %(host)s, %(port)s, %(smtp_user)s, %(from_email)s, %(starttls)s, %(password_enc)s, NOW()
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        host = EXCLUDED.host,
                        port = EXCLUDED.port,
                        smtp_user = EXCLUDED.smtp_user,
                        from_email = EXCLUDED.from_email,
                        starttls = EXCLUDED.starttls,
                        password_enc = EXCLUDED.password_enc,
                        updated_at = NOW()
                    """,
                    {
                        "host": host,
                        "port": port,
                        "smtp_user": user,
                        "from_email": from_email,
                        "starttls": starttls,
                        "password_enc": password_enc,
                    },
                )
            else:
                cur.execute(
                    f"""
                    INSERT INTO {schema}.app_smtp_config (
                        id, host, port, smtp_user, from_email, starttls, updated_at
                    )
                    VALUES (
                        1, %(host)s, %(port)s, %(smtp_user)s, %(from_email)s, %(starttls)s, NOW()
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        host = EXCLUDED.host,
                        port = EXCLUDED.port,
                        smtp_user = EXCLUDED.smtp_user,
                        from_email = EXCLUDED.from_email,
                        starttls = EXCLUDED.starttls,
                        updated_at = NOW()
                    """,
                    {
                        "host": host,
                        "port": port,
                        "smtp_user": user,
                        "from_email": from_email,
                        "starttls": starttls,
                    },
                )
            conn.commit()

    @staticmethod
    def _extract_connection_profile(*, conn_secret_ref: str, secret_payload: object) -> dict:
        if isinstance(secret_payload, dict):
            return secret_payload
        raw = str(conn_secret_ref or "").strip()
        if not raw:
            raise ValueError("conn_secret_ref ou secret_payload obrigatorio")
        if raw.startswith("json:"):
            try:
                parsed = json.loads(raw[5:])
            except Exception as exc:
                raise ValueError(f"conn_secret_ref json invalido: {exc}") from exc
            if not isinstance(parsed, dict):
                raise ValueError("conn_secret_ref json deve ser objeto")
            return parsed
        if raw.startswith("enc:"):
            try:
                decrypted = decrypt_text(raw[4:])
                parsed = json.loads(decrypted)
            except Exception as exc:
                raise ValueError(f"conn_secret_ref criptografado invalido: {exc}") from exc
            if not isinstance(parsed, dict):
                raise ValueError("segredo descriptografado deve ser objeto json")
            return parsed
        if raw.startswith("{") and raw.endswith("}"):
            try:
                parsed = json.loads(raw)
            except Exception as exc:
                raise ValueError(f"conn_secret_ref json invalido: {exc}") from exc
            if isinstance(parsed, dict):
                return parsed
        raise ValueError("Use secret_payload (objeto JSON) ou conn_secret_ref com prefixo json:/enc:")

    def _run_connection_test(self, *, source_type: str, profile: dict) -> dict:
        kind = str(source_type or "").strip().lower()
        if kind in {"postgres", "postgresql"}:
            return self._test_postgres_connection(profile)
        if kind in {"sqlserver", "sql_server", "mssql"}:
            return self._test_sqlserver_connection(profile)
        if kind in {"mysql"}:
            return self._test_mysql_connection(profile)
        if kind in {"oracle"}:
            return self._test_oracle_connection(profile)
        if kind in {"power_bi", "powerbi"}:
            return self._test_bearer_http_connection(
                profile=profile,
                default_url="https://api.powerbi.com/v1.0/myorg/groups?$top=1",
                label="Power BI",
            )
        if kind in {"fabric", "microsoft_fabric"}:
            return self._test_bearer_http_connection(
                profile=profile,
                default_url="https://api.fabric.microsoft.com/v1/workspaces?top=1",
                label="Microsoft Fabric",
            )
        return {
            "ok": False,
            "source_type": kind,
            "message": f"Teste de conexao ainda nao implementado para {kind}.",
        }

    def _discover_source_tables(self, *, source_type: str, profile: dict) -> list[dict[str, str]]:
        kind = str(source_type or "").strip().lower()
        if kind in {"postgres", "postgresql"}:
            return self._discover_postgres_tables(profile)
        if kind in {"mysql"}:
            return self._discover_mysql_tables(profile)
        if kind in {"sqlserver", "sql_server", "mssql"}:
            return self._discover_sqlserver_tables(profile)
        if kind in {"oracle"}:
            return self._discover_oracle_tables(profile)
        raise ValueError(f"Descoberta de tabelas ainda nao implementada para {kind}.")

    def _discover_source_columns(
        self,
        *,
        source_type: str,
        profile: dict,
        schema_name: str,
        table_name: str,
    ) -> list[dict[str, str]]:
        kind = str(source_type or "").strip().lower()
        if kind in {"postgres", "postgresql"}:
            return self._discover_postgres_columns(profile, schema_name=schema_name, table_name=table_name)
        if kind in {"mysql"}:
            return self._discover_mysql_columns(profile, schema_name=schema_name, table_name=table_name)
        if kind in {"sqlserver", "sql_server", "mssql"}:
            return self._discover_sqlserver_columns(profile, schema_name=schema_name, table_name=table_name)
        if kind in {"oracle"}:
            return self._discover_oracle_columns(profile, schema_name=schema_name, table_name=table_name)
        raise ValueError(f"Descoberta de colunas ainda nao implementada para {kind}.")

    def _sample_source_table_rows(
        self,
        *,
        source_type: str,
        profile: dict,
        schema_name: str,
        table_name: str,
        limit: int = 25,
    ) -> list[dict[str, object]]:
        kind = str(source_type or "").strip().lower()
        if kind in {"postgres", "postgresql"}:
            return self._sample_postgres_rows(profile, schema_name=schema_name, table_name=table_name, limit=limit)
        if kind in {"mysql"}:
            return self._sample_mysql_rows(profile, schema_name=schema_name, table_name=table_name, limit=limit)
        if kind in {"sqlserver", "sql_server", "mssql"}:
            return self._sample_sqlserver_rows(profile, schema_name=schema_name, table_name=table_name, limit=limit)
        if kind in {"oracle"}:
            return self._sample_oracle_rows(profile, schema_name=schema_name, table_name=table_name, limit=limit)
        return []

    @staticmethod
    def _safe_identifier(value: str) -> str:
        ident = str(value or "").strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", ident):
            raise ValueError(f"identificador invalido: {ident}")
        return ident

    @staticmethod
    def _sample_value_as_text(value: object) -> str:
        if value is None:
            return ""
        text = str(value)
        return text[:120]

    def _sample_postgres_rows(self, profile: dict, *, schema_name: str, table_name: str, limit: int) -> list[dict[str, object]]:
        if connect is None:
            return []
        dsn = str(profile.get("dsn") or "").strip()
        if not dsn:
            host = str(profile.get("host") or "").strip()
            user = str(profile.get("user") or "").strip()
            password = str(profile.get("password") or "").strip()
            dbname = str(profile.get("dbname") or profile.get("database") or "").strip()
            port = str(profile.get("port") or "5432").strip()
            if not host or not user or not dbname:
                return []
            dsn = f"host={host} port={port} dbname={dbname} user={user}"
            if password:
                dsn += f" password={password}"
        schema = self._safe_identifier(schema_name or str(profile.get("schema") or "public"))
        table = self._safe_identifier(table_name)
        sql = f'SELECT * FROM "{schema}"."{table}" LIMIT %(limit)s'
        with connect(dsn, connect_timeout=8) as conn, conn.cursor() as cur:
            cur.execute(sql, {"limit": int(max(1, min(limit, 100)))})
            cols = [str(item[0]) for item in (cur.description or [])]
            rows = cur.fetchall()
        return [dict(zip(cols, row)) for row in rows]

    def _sample_mysql_rows(self, profile: dict, *, schema_name: str, table_name: str, limit: int) -> list[dict[str, object]]:
        if pymysql is None:
            return []
        host = str(profile.get("host") or profile.get("server") or "").strip()
        port = int(profile.get("port") or 3306)
        database = str(profile.get("database") or profile.get("dbname") or "").strip()
        user = str(profile.get("user") or profile.get("username") or "").strip()
        password = str(profile.get("password") or "").strip()
        timeout = int(profile.get("timeout_seconds") or 8)
        if not host or not user:
            return []
        schema = self._safe_identifier(schema_name or database)
        table = self._safe_identifier(table_name)
        sql = f"SELECT * FROM `{schema}`.`{table}` LIMIT %s"
        with pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database or schema,
            connect_timeout=timeout,
            read_timeout=timeout,
            write_timeout=timeout,
            charset="utf8mb4",
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(max(1, min(limit, 100))),))
                cols = [str(item[0]) for item in (cur.description or [])]
                rows = cur.fetchall()
        return [dict(zip(cols, row)) for row in rows]

    def _sample_sqlserver_rows(self, profile: dict, *, schema_name: str, table_name: str, limit: int) -> list[dict[str, object]]:
        if pyodbc is None:
            return []
        host = str(profile.get("host") or profile.get("server") or "").strip()
        port = int(profile.get("port") or 1433)
        database = str(profile.get("database") or profile.get("dbname") or "master").strip()
        user = str(profile.get("user") or profile.get("username") or "").strip()
        password = str(profile.get("password") or "").strip()
        timeout = int(profile.get("timeout_seconds") or 8)
        if not host or not user:
            return []
        dsn = str(profile.get("dsn") or "").strip()
        conn_str = dsn
        if not conn_str:
            configured_driver = str(profile.get("driver") or "").strip()
            if configured_driver:
                driver = configured_driver
            else:
                available = list(pyodbc.drivers())
                preferred = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server"]
                driver = next((item for item in preferred if item in available), (available[0] if available else "SQL Server"))
            conn_str = (
                f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={database};UID={user};PWD={password};"
                "Encrypt=yes;TrustServerCertificate=yes;"
            )
        schema = self._safe_identifier(schema_name or "dbo")
        table = self._safe_identifier(table_name)
        sql = f"SELECT TOP {int(max(1, min(limit, 100)))} * FROM [{schema}].[{table}]"
        with pyodbc.connect(conn_str, timeout=timeout) as conn:
            cur = conn.cursor()
            cur.execute(sql)
            cols = [str(item[0]) for item in (cur.description or [])]
            rows = cur.fetchall()
        return [dict(zip(cols, row)) for row in rows]

    def _sample_oracle_rows(self, profile: dict, *, schema_name: str, table_name: str, limit: int) -> list[dict[str, object]]:
        if oracledb is None:
            return []
        host = str(profile.get("host") or profile.get("server") or "").strip()
        port = int(profile.get("port") or 1521)
        user = str(profile.get("user") or profile.get("username") or "").strip()
        password = str(profile.get("password") or "").strip()
        service_name = str(profile.get("service_name") or "").strip()
        sid = str(profile.get("sid") or "").strip()
        if not host or not user:
            return []
        dsn = str(profile.get("dsn") or "").strip()
        if not dsn:
            if service_name:
                dsn = f"{host}:{port}/{service_name}"
            elif sid:
                dsn = f"{host}:{port}/{sid}"
            else:
                dsn = f"{host}:{port}/XEPDB1"
        owner = self._safe_identifier((schema_name or profile.get("owner") or user).upper())
        table = self._safe_identifier(table_name.upper())
        sql = f'SELECT * FROM "{owner}"."{table}" FETCH FIRST {int(max(1, min(limit, 100)))} ROWS ONLY'
        with oracledb.connect(user=user, password=password, dsn=dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                cols = [str(item[0]) for item in (cur.description or [])]
                rows = cur.fetchall()
        return [dict(zip(cols, row)) for row in rows]

    def _db_ensure_monitored_column_metadata_fields(self) -> None:
        dsn = self._get_db_dsn()
        if not dsn or connect is None:
            return
        schema = self.signup_schema
        ddls = [
            f"ALTER TABLE {schema}.monitored_column ADD COLUMN IF NOT EXISTS source_description_text TEXT",
            f"ALTER TABLE {schema}.monitored_column ADD COLUMN IF NOT EXISTS llm_description_suggested TEXT",
            f"ALTER TABLE {schema}.monitored_column ADD COLUMN IF NOT EXISTS llm_classification_suggested TEXT",
            f"ALTER TABLE {schema}.monitored_column ADD COLUMN IF NOT EXISTS llm_confidence_score NUMERIC(5,2)",
            f"ALTER TABLE {schema}.monitored_column ADD COLUMN IF NOT EXISTS llm_description_confirmed BOOLEAN NOT NULL DEFAULT FALSE",
            f"ALTER TABLE {schema}.monitored_column ADD COLUMN IF NOT EXISTS llm_confirmed_at TIMESTAMPTZ",
            f"ALTER TABLE {schema}.monitored_column ADD COLUMN IF NOT EXISTS llm_confirmed_by_user_id BIGINT",
        ]
        with connect(dsn) as conn, conn.cursor() as cur:
            for ddl in ddls:
                cur.execute(ddl)
            conn.commit()

    def _db_enrich_monitored_columns(
        self,
        *,
        tenant_id: int,
        monitored_table_id: int,
        source_type: str,
        schema_name: str,
        table_name: str,
        discovered_columns: list[dict[str, object]],
        sample_rows: list[dict[str, object]],
        context: dict,
    ) -> dict[str, object]:
        dsn = self._get_db_dsn()
        if not dsn or connect is None:
            raise ValueError("Banco indisponivel para enriquecimento.")
        schema = self.signup_schema
        self._db_ensure_monitored_column_metadata_fields()
        upsert_sql = f"""
            INSERT INTO {schema}.monitored_column (
                tenant_id,
                monitored_table_id,
                column_name,
                data_type,
                source_description_text
            )
            VALUES (
                %(tenant_id)s,
                %(monitored_table_id)s,
                %(column_name)s,
                %(data_type)s,
                %(source_description_text)s
            )
            ON CONFLICT (monitored_table_id, column_name)
            DO UPDATE SET
                data_type = COALESCE(EXCLUDED.data_type, {schema}.monitored_column.data_type),
                source_description_text = COALESCE(
                    NULLIF(TRIM(EXCLUDED.source_description_text), ''),
                    {schema}.monitored_column.source_description_text
                )
        """
        fetch_sql = f"""
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
            FROM {schema}.monitored_column mc
            JOIN {schema}.monitored_table mt ON mt.id = mc.monitored_table_id
            WHERE mt.tenant_id = %(tenant_id)s
              AND mt.id = %(monitored_table_id)s
            ORDER BY mc.column_name
        """
        update_sql = f"""
            UPDATE {schema}.monitored_column mc
               SET llm_classification_suggested = %(classification)s,
                   llm_description_suggested = %(description_text)s,
                   llm_confidence_score = %(confidence_score)s,
                   llm_description_confirmed = CASE
                       WHEN COALESCE(NULLIF(TRIM(mc.description_text), ''), '') <> ''
                            AND COALESCE(NULLIF(TRIM(mc.description_text), ''), '') = COALESCE(NULLIF(TRIM(%(description_text)s), ''), '')
                       THEN TRUE
                       ELSE FALSE
                   END
            FROM {schema}.monitored_table mt
            WHERE mc.monitored_table_id = mt.id
              AND mt.tenant_id = %(tenant_id)s
              AND mt.id = %(monitored_table_id)s
              AND mc.column_name = %(column_name)s
        """
        with connect(dsn) as conn, conn.cursor() as cur:
            for col in discovered_columns:
                col_name = str(col.get("column_name") or "").strip()
                if not col_name:
                    continue
                cur.execute(
                    upsert_sql,
                    {
                        "tenant_id": tenant_id,
                        "monitored_table_id": monitored_table_id,
                        "column_name": col_name,
                        "data_type": str(col.get("data_type") or "").strip() or None,
                        "source_description_text": str(col.get("description_text") or "").strip() or None,
                    },
                )
            cur.execute(fetch_sql, {"tenant_id": tenant_id, "monitored_table_id": monitored_table_id})
            rows = cur.fetchall()
            existing = [
                {
                    "column_name": str(item[0]),
                    "data_type": str(item[1] or ""),
                    "classification": str(item[2] or "").strip() or None,
                    "description_text": str(item[3] or "").strip() or None,
                    "source_description_text": str(item[4] or "").strip() or None,
                    "llm_description_suggested": str(item[5] or "").strip() or None,
                    "llm_classification_suggested": str(item[6] or "").strip() or None,
                    "llm_confidence_score": (float(item[7]) if item[7] is not None else None),
                    "llm_description_confirmed": bool(item[8]),
                }
                for item in rows
            ]
            suggestions = self._suggest_column_enrichment(
                source_type=source_type,
                schema_name=schema_name,
                table_name=table_name,
                columns=existing,
                sample_rows=sample_rows,
                context=context,
                language_code=self._resolve_language_code(context),
            )
            updated = 0
            for item in existing:
                col_name = item["column_name"]
                current_class = item["classification"]
                current_desc = item["description_text"]
                proposed = suggestions.get(col_name.lower()) or {}
                classification = str(proposed.get("classification") or "").strip() or None
                description_text = str(proposed.get("description_text") or "").strip() or None
                confidence_score = proposed.get("confidence_score")
                if confidence_score is None:
                    confidence_score = self._estimate_suggestion_confidence(
                        existing_column=item,
                        proposed_classification=classification,
                        proposed_description=description_text,
                    )
                try:
                    confidence_score = float(confidence_score)
                except Exception:
                    confidence_score = 0.0
                confidence_score = max(0.0, min(1.0, confidence_score))
                if not classification and not description_text:
                    continue
                if (
                    (item["llm_classification_suggested"] or "") == (classification or "")
                    and (item["llm_description_suggested"] or "") == (description_text or "")
                    and (item.get("llm_confidence_score") is not None)
                    and abs(float(item.get("llm_confidence_score") or 0.0) - confidence_score) < 0.001
                ):
                    continue
                cur.execute(
                    update_sql,
                    {
                        "tenant_id": tenant_id,
                        "monitored_table_id": monitored_table_id,
                        "column_name": col_name,
                        "classification": classification,
                        "description_text": description_text,
                        "confidence_score": confidence_score,
                    },
                )
                if cur.rowcount:
                    updated += 1
                    if not current_class and classification:
                        current_class = classification
                    if not current_desc and item["source_description_text"]:
                        current_desc = item["source_description_text"]
            conn.commit()
        return {
            "tenant_id": tenant_id,
            "monitored_table_id": monitored_table_id,
            "discovered_columns": len(discovered_columns),
            "sample_rows": len(sample_rows),
            "updated_columns": updated,
        }

    def _db_confirm_monitored_column_description(self, *, tenant_id: int, user_id: int, monitored_column_id: int) -> dict[str, object]:
        dsn = self._get_db_dsn()
        if not dsn or connect is None:
            raise ValueError("Banco indisponivel para confirmar descricao.")
        schema = self.signup_schema
        self._db_ensure_monitored_column_metadata_fields()
        select_sql = f"""
            SELECT
                mc.id,
                mc.monitored_table_id,
                mc.column_name,
                mc.description_text,
                mc.llm_description_suggested,
                mc.llm_classification_suggested
            FROM {schema}.monitored_column mc
            JOIN {schema}.monitored_table mt ON mt.id = mc.monitored_table_id
            WHERE mt.tenant_id = %(tenant_id)s
              AND mc.id = %(monitored_column_id)s
            LIMIT 1
        """
        update_sql = f"""
            UPDATE {schema}.monitored_column
               SET description_text = %(description_text)s,
                   classification = CASE
                       WHEN COALESCE(NULLIF(TRIM(%(classification)s), ''), '') = '' THEN classification
                       ELSE %(classification)s
                   END,
                   llm_description_confirmed = TRUE,
                   llm_confirmed_at = NOW(),
                   llm_confirmed_by_user_id = %(user_id)s
             WHERE id = %(monitored_column_id)s
        """
        with connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(select_sql, {"tenant_id": tenant_id, "monitored_column_id": monitored_column_id})
            row = cur.fetchone()
            if not row:
                raise ValueError("coluna monitorada nao encontrada para o tenant")
            llm_description = str(row[4] or "").strip()
            llm_classification = str(row[5] or "").strip()
            if not llm_description:
                raise ValueError("nao ha descricao sugerida pela LLM para confirmar")
            cur.execute(
                update_sql,
                {
                    "monitored_column_id": monitored_column_id,
                    "description_text": llm_description,
                    "classification": llm_classification or None,
                    "user_id": user_id,
                },
            )
            conn.commit()
        return {
            "monitored_column_id": monitored_column_id,
            "description_text": llm_description,
            "classification": llm_classification or None,
            "confirmed": True,
        }

    def _db_update_monitored_column(
        self,
        *,
        tenant_id: int,
        monitored_column_id: int,
        classification: object | None,
        description_text: object | None,
        llm_description_confirmed: object | None,
    ) -> dict[str, object]:
        dsn = self._get_db_dsn()
        if not dsn or connect is None:
            raise ValueError("Banco indisponivel para atualizar coluna.")
        schema = self.signup_schema
        self._db_ensure_monitored_column_metadata_fields()
        select_sql = f"""
            SELECT
                mc.classification,
                mc.description_text,
                mc.llm_description_suggested,
                mc.llm_confidence_score,
                mc.llm_description_confirmed
            FROM {schema}.monitored_column mc
            JOIN {schema}.monitored_table mt ON mt.id = mc.monitored_table_id
            WHERE mt.tenant_id = %(tenant_id)s
              AND mc.id = %(monitored_column_id)s
            LIMIT 1
        """
        update_sql = f"""
            UPDATE {schema}.monitored_column mc
               SET classification = %(classification)s,
                   description_text = %(description_text)s,
                   llm_description_confirmed = %(llm_description_confirmed)s
            FROM {schema}.monitored_table mt
            WHERE mc.monitored_table_id = mt.id
              AND mt.tenant_id = %(tenant_id)s
              AND mc.id = %(monitored_column_id)s
            RETURNING
                mc.id,
                mc.column_name,
                mc.data_type,
                mc.classification,
                mc.description_text,
                mc.source_description_text,
                mc.llm_description_suggested,
                mc.llm_classification_suggested,
                mc.llm_confidence_score,
                mc.llm_description_confirmed
        """
        with connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(select_sql, {"tenant_id": tenant_id, "monitored_column_id": monitored_column_id})
            row = cur.fetchone()
            if not row:
                raise ValueError("coluna monitorada nao encontrada para o tenant")

            next_classification = str(row[0] or "").strip() or None
            next_description = str(row[1] or "").strip() or None
            llm_description = str(row[2] or "").strip() or None
            next_confirmed = bool(row[4])

            if classification is not None:
                next_classification = str(classification or "").strip() or None
            if description_text is not None:
                next_description = str(description_text or "").strip() or None

            if llm_description_confirmed is None:
                if description_text is not None:
                    next_confirmed = bool(llm_description and next_description and next_description == llm_description)
            else:
                next_confirmed = bool(llm_description_confirmed)

            cur.execute(
                update_sql,
                {
                    "tenant_id": tenant_id,
                    "monitored_column_id": monitored_column_id,
                    "classification": next_classification,
                    "description_text": next_description,
                    "llm_description_confirmed": next_confirmed,
                },
            )
            updated = cur.fetchone()
            if not updated:
                raise ValueError("falha ao atualizar coluna monitorada")
            conn.commit()

        return {
            "id": int(updated[0]),
            "column_name": str(updated[1] or ""),
            "data_type": str(updated[2] or "").strip() or None,
            "classification": str(updated[3] or "").strip() or None,
            "description_text": str(updated[4] or "").strip() or None,
            "source_description_text": str(updated[5] or "").strip() or None,
            "llm_description_suggested": str(updated[6] or "").strip() or None,
            "llm_classification_suggested": str(updated[7] or "").strip() or None,
            "llm_confidence_score": (float(updated[8]) if updated[8] is not None else None),
            "llm_description_confirmed": bool(updated[9]),
        }

    def _suggest_column_enrichment(
        self,
        *,
        source_type: str,
        schema_name: str,
        table_name: str,
        columns: list[dict[str, object]],
        sample_rows: list[dict[str, object]],
        context: dict,
        language_code: str = "pt-BR",
    ) -> dict[str, dict[str, object]]:
        columns_index = {
            str(col.get("column_name") or "").strip().lower(): col
            for col in columns
            if str(col.get("column_name") or "").strip()
        }
        glossary = self._load_tenant_glossary_overrides(
            context=context,
            schema_name=schema_name,
            table_name=table_name,
        )
        llm_map = self._suggest_column_enrichment_with_llm(
            source_type=source_type,
            schema_name=schema_name,
            table_name=table_name,
            columns=columns,
            sample_rows=sample_rows,
            context=context,
            language_code=language_code,
        )
        if llm_map:
            for key, item in llm_map.items():
                col_meta = columns_index.get(str(key or "").strip().lower()) or {}
                col_name = str(col_meta.get("column_name") or key or "").strip()
                classification = str(item.get("classification") or "").strip().lower() or "attribute"
                desc = str(item.get("description_text") or "").strip()
                override_desc = glossary.get(f"{table_name.lower()}.{col_name.lower()}") or glossary.get(col_name.lower())
                if override_desc:
                    desc = override_desc
                source_desc = str(col_meta.get("source_description_text") or "").strip()
                if not desc and source_desc:
                    desc = source_desc
                if not desc:
                    remembered = self._load_confirmed_description_memory(
                        context=context,
                        table_name=table_name,
                        column_name=col_name,
                    )
                    if remembered:
                        desc = remembered
                if (not desc) or self._is_generic_column_description(desc, column_name=col_name, schema_name=schema_name, table_name=table_name):
                    desc = self._build_contextual_column_description(
                        column_name=col_name,
                        schema_name=schema_name,
                        table_name=table_name,
                        classification=classification,
                        data_type=str(col_meta.get("data_type") or ""),
                        language_code=language_code,
                    )
                desc = self._normalize_business_description(
                    description_text=desc,
                    column_name=col_name,
                    schema_name=schema_name,
                    table_name=table_name,
                    language_code=language_code,
                )
                item["classification"] = classification
                item["description_text"] = desc
            return self._deduplicate_column_descriptions(
                suggestions=llm_map,
                table_name=table_name,
                schema_name=schema_name,
                columns_index=columns_index,
                language_code=language_code,
            )
        result: dict[str, dict[str, str]] = {}
        for col in columns:
            name = str(col.get("column_name") or "").strip()
            if not name:
                continue
            data_type = str(col.get("data_type") or "").strip().lower()
            lower = name.lower()
            classification = "attribute"
            if any(token in lower for token in ["id", "codigo", "code", "uuid"]):
                classification = "identifier"
            elif any(token in lower for token in ["email", "mail", "telefone", "phone", "celular"]):
                classification = "contact"
            elif any(token in lower for token in ["cpf", "cnpj", "document", "doc", "rg"]):
                classification = "sensitive"
            elif any(token in lower for token in ["valor", "price", "amount", "total", "saldo"]):
                classification = "financial"
            elif "date" in data_type or "time" in data_type:
                classification = "temporal"
            elif "int" in data_type or "decimal" in data_type or "numeric" in data_type:
                classification = "measure"
            source_desc = str(col.get("source_description_text") or "").strip()
            override_desc = glossary.get(f"{table_name.lower()}.{name.lower()}") or glossary.get(name.lower())
            remembered = self._load_confirmed_description_memory(
                context=context,
                table_name=table_name,
                column_name=name,
            )
            description_text = override_desc or source_desc or remembered or self._build_contextual_column_description(
                column_name=name,
                schema_name=schema_name,
                table_name=table_name,
                classification=classification,
                data_type=data_type,
                language_code=language_code,
            )
            description_text = self._normalize_business_description(
                description_text=description_text,
                column_name=name,
                schema_name=schema_name,
                table_name=table_name,
                language_code=language_code,
            )
            result[lower] = {
                "classification": classification,
                "description_text": description_text,
            }
        return self._deduplicate_column_descriptions(
            suggestions=result,
            table_name=table_name,
            schema_name=schema_name,
            columns_index=columns_index,
            language_code=language_code,
        )

    @staticmethod
    def _is_generic_column_description(description_text: str, *, column_name: str, schema_name: str, table_name: str) -> bool:
        desc = str(description_text or "").strip().lower()
        col = str(column_name or "").strip().lower()
        schema = str(schema_name or "").strip().lower()
        table = str(table_name or "").strip().lower()
        if not desc:
            return True
        if desc.startswith("campo ") or desc.startswith("coluna "):
            if " da tabela " in desc:
                return True
        if desc.startswith("informacao de ") or desc.startswith("information of ") or desc.startswith("informacion de "):
            return True
        if desc in {"data de referencia", "reference date", "fecha de referencia", "valor financeiro", "financial amount", "valor financiero"}:
            return True
        if desc.startswith("field ") and " table " in desc:
            return True
        if col and desc in {
            f"campo {col}.",
            f"campo {col} da tabela {table}.",
            f"campo {col} da tabela {schema}.{table}.",
            f"coluna {col}.",
            f"coluna {col} da tabela {table}.",
            f"coluna {col} da tabela {schema}.{table}.",
            f"field {col}.",
            f"field {col} from table {table}.",
            f"field {col} from table {schema}.{table}.",
        }:
            return True
        return False

    @staticmethod
    def _build_contextual_column_description(
        *,
        column_name: str,
        schema_name: str,
        table_name: str,
        classification: str,
        data_type: str,
        language_code: str = "pt-BR",
    ) -> str:
        col = str(column_name or "").strip().lower()
        schema = str(schema_name or "").strip().lower()
        table = str(table_name or "").strip().lower()
        data_t = str(data_type or "").strip().lower()
        lang = IAOpsAPIHandler._language_bucket(language_code)

        temporal_named: dict[str, tuple[str, str, str]] = {
            "data_pagamento": ("Data do pagamento", "Payment date", "Fecha de pago"),
            "data_vencimento": ("Data de vencimento", "Due date", "Fecha de vencimiento"),
            "data_venda": ("Data da venda", "Sale date", "Fecha de venta"),
            "data_emissao": ("Data de emissao", "Issue date", "Fecha de emision"),
            "data_cadastro": ("Data de cadastro", "Registration date", "Fecha de registro"),
            "created_at": ("Data e hora de criacao", "Creation date and time", "Fecha y hora de creacion"),
            "updated_at": ("Data e hora de atualizacao", "Update date and time", "Fecha y hora de actualizacion"),
        }
        if col in temporal_named:
            values = temporal_named[col]
            return values[0] if lang == "pt" else (values[1] if lang == "en" else values[2])

        if col == "rental_date":
            if lang == "en":
                return "Rental date"
            if lang == "es":
                return "Fecha de alquiler"
            return "Data do aluguel"
        if col == "return_date":
            if lang == "en":
                return "Return date"
            if lang == "es":
                return "Fecha de devolucion"
            return "Data da devolucao"
        if any(token in col for token in ["amount", "valor"]) and "payment" in table:
            if lang == "en":
                return "Amount received"
            if lang == "es":
                return "Valor recibido"
            return "Valor recebido"
        if col in {"valor_venda", "sale_value"}:
            if lang == "en":
                return "Sale amount"
            if lang == "es":
                return "Valor de la venta"
            return "Valor da venda"
        if col in {"valor_total", "total_value"}:
            if lang == "en":
                return "Total amount"
            if lang == "es":
                return "Valor total"
            return "Valor total"
        if col in {"valor_pago", "paid_amount"}:
            if lang == "en":
                return "Paid amount"
            if lang == "es":
                return "Valor pagado"
            return "Valor pago"
        if col == "payment_date":
            if lang == "en":
                return "Payment date"
            if lang == "es":
                return "Fecha del pago"
            return "Data do pagamento"
        if col in {"last_update", "updated_at", "update_date"}:
            if "time" in data_t or "timestamp" in data_t:
                if lang == "en":
                    return "Last update date and time"
                if lang == "es":
                    return "Fecha y hora de la ultima actualizacion"
                return "Data e hora da ultima atualizacao"
            if lang == "en":
                return "Last update date"
            if lang == "es":
                return "Fecha de la ultima actualizacion"
            return "Data da ultima atualizacao"
        if col in {"customer_id", "id_customer"}:
            if lang == "en":
                return "Customer code"
            if lang == "es":
                return "Codigo del cliente"
            return "Codigo do cliente"
        if col.endswith("_id"):
            entity = col[: -len("_id")].replace("_", " ").strip()
            if entity:
                label, prep = IAOpsAPIHandler._entity_label(entity, language_code=language_code)
                if IAOpsAPIHandler._is_likely_foreign_key(table_name=table, column_name=col):
                    if lang == "en":
                        return f"{label} code (FK to table {schema}.{entity})"
                    if lang == "es":
                        return f"Codigo {prep} {label} (FK de la tabla {schema}.{entity})"
                    return f"Codigo {prep} {label} (FK da tabela {schema}.{entity})"
                if lang == "en":
                    return f"{label} code"
                return f"Codigo {prep} {label}"
            if lang == "en":
                return "Identifier code"
            if lang == "es":
                return "Codigo identificador"
            return "Codigo identificador"
        if any(token in col for token in ["email", "mail"]):
            if lang == "en":
                return "Contact e-mail"
            if lang == "es":
                return "Correo de contacto"
            return "E-mail de contato"
        if any(token in col for token in ["phone", "telefone", "celular"]):
            if lang == "en":
                return "Contact phone"
            if lang == "es":
                return "Telefono de contacto"
            return "Telefone de contato"
        if "date" in col:
            if lang == "en":
                return "Event date"
            if lang == "es":
                return "Fecha del evento"
            return "Data do evento"
        if "time" in col:
            if lang == "en":
                return "Event time"
            if lang == "es":
                return "Hora del evento"
            return "Horario do evento"
        if classification == "financial":
            if lang == "en":
                return "Financial amount"
            if lang == "es":
                return "Valor financiero"
            return "Valor financeiro"
        if classification == "identifier":
            if lang == "en":
                return "Identifier code"
            if lang == "es":
                return "Codigo identificador"
            return "Codigo identificador"
        if classification == "temporal":
            if lang == "en":
                return "Reference date"
            if lang == "es":
                return "Fecha de referencia"
            return "Data de referencia"
        readable = col.replace("_", " ").strip()
        if readable:
            if lang == "en":
                return f"{readable} information"
            if lang == "es":
                return f"Informacion de {readable}"
            return f"Informacao de {readable}"
        if lang == "en":
            return "Field information"
        if lang == "es":
            return "Informacion del campo"
        return "Informacao do campo"

    @staticmethod
    def _is_likely_foreign_key(*, table_name: str, column_name: str) -> bool:
        table = str(table_name or "").strip().lower()
        col = str(column_name or "").strip().lower()
        if not col.endswith("_id") or col == "id":
            return False
        base_table = table[:-1] if table.endswith("s") else table
        return col != f"{base_table}_id"

    @staticmethod
    def _entity_label(entity_raw: str, *, language_code: str = "pt-BR") -> tuple[str, str]:
        entity = str(entity_raw or "").strip().lower().replace("-", "_").replace(" ", "_")
        lang = IAOpsAPIHandler._language_bucket(language_code)
        mapping_pt = {
            "film": ("filme", "do"),
            "store": ("loja", "da"),
            "inventory": ("inventario", "do"),
            "customer": ("cliente", "do"),
            "staff": ("funcionario", "do"),
            "rental": ("locacao", "da"),
            "payment": ("pagamento", "do"),
            "actor": ("ator", "do"),
            "category": ("categoria", "da"),
            "city": ("cidade", "da"),
            "country": ("pais", "do"),
            "language": ("idioma", "do"),
            "address": ("endereco", "do"),
        }
        mapping_es = {
            "film": ("pelicula", "del"),
            "store": ("tienda", "de la"),
            "inventory": ("inventario", "del"),
            "customer": ("cliente", "del"),
            "staff": ("empleado", "del"),
            "rental": ("alquiler", "del"),
            "payment": ("pago", "del"),
            "actor": ("actor", "del"),
            "category": ("categoria", "de la"),
            "city": ("ciudad", "de la"),
            "country": ("pais", "del"),
            "language": ("idioma", "del"),
            "address": ("direccion", "de la"),
        }
        mapping_en = {
            "film": ("film", "of"),
            "store": ("store", "of"),
            "inventory": ("inventory", "of"),
            "customer": ("customer", "of"),
            "staff": ("staff", "of"),
            "rental": ("rental", "of"),
            "payment": ("payment", "of"),
            "actor": ("actor", "of"),
            "category": ("category", "of"),
            "city": ("city", "of"),
            "country": ("country", "of"),
            "language": ("language", "of"),
            "address": ("address", "of"),
        }
        mapping = mapping_pt if lang == "pt" else (mapping_es if lang == "es" else mapping_en)
        if entity in mapping:
            return mapping[entity]
        label = entity.replace("_", " ")
        if lang == "en":
            prep = "of"
        elif lang == "es":
            prep = "de la" if label.endswith("a") else "del"
        else:
            prep = "da" if label.endswith("a") else "do"
        return label, prep

    @staticmethod
    def _normalize_business_description(
        *,
        description_text: str,
        column_name: str,
        schema_name: str,
        table_name: str,
        language_code: str = "pt-BR",
    ) -> str:
        desc = str(description_text or "").strip()
        col = str(column_name or "").strip().lower()
        lang = IAOpsAPIHandler._language_bucket(language_code)
        if not desc:
            return desc
        if col in {"data_pagamento", "payment_date"}:
            return "Payment date" if lang == "en" else ("Fecha de pago" if lang == "es" else "Data do pagamento")
        if col == "data_vencimento":
            return "Due date" if lang == "en" else ("Fecha de vencimiento" if lang == "es" else "Data de vencimento")
        if col in {"data_venda", "sale_date"}:
            return "Sale date" if lang == "en" else ("Fecha de venta" if lang == "es" else "Data da venda")
        if col in {"valor_venda", "sale_value"}:
            return "Sale amount" if lang == "en" else ("Valor de la venta" if lang == "es" else "Valor da venda")
        if col in {"valor_total", "total_value"}:
            return "Total amount" if lang == "en" else ("Valor total" if lang == "es" else "Valor total")
        match = re.match(r"^(codigo de|code of|code for) ([a-zA-Z0-9_ ]+)\.?$", desc.strip(), re.IGNORECASE)
        if match:
            entity = str(match.group(2) or "").strip().lower().replace(" ", "_")
            label, prep = IAOpsAPIHandler._entity_label(entity, language_code=language_code)
            if lang == "en":
                desc = f"{label} code"
            else:
                desc = f"Codigo {prep} {label}"
        if col.endswith("_id"):
            entity = col[: -len("_id")]
            label, prep = IAOpsAPIHandler._entity_label(entity, language_code=language_code)
            if IAOpsAPIHandler._is_likely_foreign_key(table_name=table_name, column_name=col):
                fk_ref = f"{schema_name}.{entity}"
                if lang == "en":
                    if "fk to table" not in desc.lower():
                        desc = f"{label} code (FK to table {fk_ref})"
                elif lang == "es":
                    if "fk de la tabla" not in desc.lower():
                        desc = f"Codigo {prep} {label} (FK de la tabla {fk_ref})"
                else:
                    if "fk da tabela" not in desc.lower():
                        desc = f"Codigo {prep} {label} (FK da tabela {fk_ref})"
            else:
                if lang == "en":
                    desc = f"{label} code"
                else:
                    desc = f"Codigo {prep} {label}"
        return desc

    @staticmethod
    def _estimate_suggestion_confidence(
        *,
        existing_column: dict[str, object],
        proposed_classification: str | None,
        proposed_description: str | None,
    ) -> float:
        score = 0.25
        if str(proposed_classification or "").strip():
            score += 0.2
        if str(proposed_description or "").strip():
            score += 0.2
        if str(existing_column.get("source_description_text") or "").strip():
            score += 0.15
        if str(existing_column.get("data_type") or "").strip():
            score += 0.1
        lowered = str(proposed_description or "").strip().lower()
        if lowered and not (
            lowered.startswith("informacao de ")
            or lowered.startswith("campo ")
            or lowered == "data de referencia"
        ):
            score += 0.1
        return max(0.0, min(1.0, score))

    @staticmethod
    def _language_bucket(language_code: str | None) -> str:
        code = str(language_code or "pt-BR").strip().lower()
        if code.startswith("en"):
            return "en"
        if code.startswith("es"):
            return "es"
        return "pt"

    def _load_tenant_glossary_overrides(self, *, context: dict, schema_name: str, table_name: str) -> dict[str, str]:
        result: dict[str, str] = {}
        try:
            src_result = self._call_mcp(
                {
                    "context": context,
                    "tool": "source.list_tenant",
                    "input": {},
                }
            )
            if src_result.get("status") != "success":
                return result
            for src in ((src_result.get("data") or {}).get("sources") or []):
                if not bool(src.get("rag_enabled")):
                    continue
                text = str(src.get("rag_context_text") or "").strip()
                if not text:
                    continue
                parsed = self._parse_glossary_text(text=text, schema_name=schema_name, table_name=table_name)
                result.update(parsed)
        except Exception:
            return result
        return result

    @staticmethod
    def _parse_rag_context_model(text: str) -> dict | None:
        raw = str(text or "").strip()
        if not raw:
            return None
        candidates = [raw]
        if raw.startswith("json:"):
            candidates.insert(0, raw[5:].strip())
        for candidate in candidates:
            try:
                parsed = json.loads(candidate)
            except Exception:
                continue
            if isinstance(parsed, dict) and isinstance(parsed.get("entities"), list):
                return parsed
        return None

    @staticmethod
    def _rag_context_to_prompt_text(text: str) -> str:
        raw = str(text or "").strip()
        if not raw:
            return ""
        model = IAOpsAPIHandler._parse_rag_context_model(raw)
        if not model:
            return raw
        lines: list[str] = []
        tenant_id = str(model.get("tenant_id") or "").strip()
        if tenant_id:
            lines.append(f"tenant_id: {tenant_id}")
            lines.append("")
        entities = model.get("entities") or []
        for entity in entities[:50]:
            if not isinstance(entity, dict):
                continue
            table_name = str(entity.get("table_name") or "").strip()
            friendly_name = str(entity.get("friendly_name") or "").strip()
            description = str(entity.get("description") or "").strip()
            if not table_name:
                continue
            lines.append(f"## tabela: {table_name}")
            if friendly_name:
                lines.append(f"nome_amigavel: {friendly_name}")
            if description:
                lines.append(f"descricao: {description}")
            columns = entity.get("columns") or []
            for col in columns[:200]:
                if not isinstance(col, dict):
                    continue
                col_name = str(col.get("name") or "").strip()
                if not col_name:
                    continue
                lines.append(f"- coluna: {col_name}")
                col_friendly = str(col.get("friendly_name") or "").strip()
                col_type = str(col.get("type") or "").strip()
                synonyms = col.get("synonyms") or []
                if col_friendly:
                    lines.append(f"  nome_amigavel: {col_friendly}")
                if col_type:
                    lines.append(f"  tipo: {col_type}")
                if isinstance(synonyms, list) and synonyms:
                    syn_text = ", ".join(str(item).strip() for item in synonyms if str(item).strip())
                    if syn_text:
                        lines.append(f"  sinonimos: {syn_text}")
            relationships = entity.get("relationships") or []
            for rel in relationships[:100]:
                if not isinstance(rel, dict):
                    continue
                to_table = str(rel.get("to_table") or "").strip()
                join_condition = str(rel.get("join_condition") or "").strip()
                rel_desc = str(rel.get("description") or "").strip()
                if not to_table and not join_condition and not rel_desc:
                    continue
                lines.append("- relacionamento:")
                if to_table:
                    lines.append(f"  to_table: {to_table}")
                if join_condition:
                    lines.append(f"  join_condition: {join_condition}")
                if rel_desc:
                    lines.append(f"  descricao: {rel_desc}")
            lines.append("")
        return "\n".join(lines).strip() or raw

    @staticmethod
    def _parse_glossary_text(*, text: str, schema_name: str, table_name: str) -> dict[str, str]:
        mapping: dict[str, str] = {}
        model = IAOpsAPIHandler._parse_rag_context_model(str(text or ""))
        if model:
            target_schema = str(schema_name or "").strip().lower()
            target_table = str(table_name or "").strip().lower()
            for entity in (model.get("entities") or []):
                if not isinstance(entity, dict):
                    continue
                raw_table = str(entity.get("table_name") or "").strip()
                if not raw_table:
                    continue
                parts = raw_table.split(".")
                if len(parts) == 2:
                    ent_schema = parts[0].strip().lower()
                    ent_table = parts[1].strip().lower()
                else:
                    ent_schema = target_schema
                    ent_table = raw_table.strip().lower()
                if ent_table != target_table:
                    continue
                if target_schema and ent_schema and ent_schema != target_schema:
                    continue
                for col in (entity.get("columns") or []):
                    if not isinstance(col, dict):
                        continue
                    col_name = str(col.get("name") or "").strip().lower()
                    if not col_name:
                        continue
                    friendly = str(col.get("friendly_name") or "").strip()
                    desc = str(col.get("description") or "").strip()
                    chosen = friendly or desc
                    if chosen:
                        mapping[f"{ent_table}.{col_name}"] = chosen
                        mapping[col_name] = chosen
            if mapping:
                return mapping

        schema = str(schema_name or "").strip().lower()
        table = str(table_name or "").strip().lower()
        lines = str(text or "").splitlines()
        for raw in lines:
            line = str(raw or "").strip()
            if not line or line.startswith("#"):
                continue
            separator = "=" if "=" in line else (":" if ":" in line else None)
            if not separator:
                continue
            left, right = line.split(separator, 1)
            key = str(left or "").strip().lower()
            val = str(right or "").strip()
            if not key or not val:
                continue
            if "." in key:
                parts = [p for p in key.split(".") if p]
                if len(parts) == 3 and parts[0] == schema and parts[1] == table:
                    mapping[f"{table}.{parts[2]}"] = val
                elif len(parts) == 2 and parts[0] == table:
                    mapping[key] = val
            else:
                mapping[key] = val
        return mapping

    def _load_confirmed_description_memory(self, *, context: dict, table_name: str, column_name: str) -> str | None:
        if not self._is_db_enabled():
            return None
        tenant_id = int(context.get("tenant_id") or 0)
        if tenant_id <= 0:
            return None
        schema = self.signup_schema
        sql = f"""
            SELECT mc.description_text
            FROM {schema}.monitored_column mc
            JOIN {schema}.monitored_table mt ON mt.id = mc.monitored_table_id
            WHERE mt.tenant_id = %(tenant_id)s
              AND LOWER(mc.column_name) = LOWER(%(column_name)s)
              AND COALESCE(NULLIF(TRIM(mc.description_text), ''), '') <> ''
            ORDER BY
              CASE WHEN LOWER(mt.table_name) = LOWER(%(table_name)s) THEN 0 ELSE 1 END,
              COALESCE(mc.llm_confirmed_at, mt.created_at) DESC
            LIMIT 1
        """
        try:
            with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
                cur.execute(
                    sql,
                    {
                        "tenant_id": tenant_id,
                        "table_name": str(table_name or "").strip(),
                        "column_name": str(column_name or "").strip(),
                    },
                )
                row = cur.fetchone()
            if not row:
                return None
            return str(row[0] or "").strip() or None
        except Exception:
            return None

    def _deduplicate_column_descriptions(
        self,
        *,
        suggestions: dict[str, dict[str, str]],
        table_name: str,
        schema_name: str,
        columns_index: dict[str, dict[str, object]],
        language_code: str,
    ) -> dict[str, dict[str, str]]:
        groups: dict[str, list[str]] = {}
        for key, item in suggestions.items():
            desc = str(item.get("description_text") or "").strip().lower()
            if not desc:
                continue
            groups.setdefault(desc, []).append(key)
        for _, keys in groups.items():
            if len(keys) <= 1:
                continue
            for idx, key in enumerate(keys):
                col_name = str((columns_index.get(key) or {}).get("column_name") or key)
                if idx == 0 and not self._is_generic_column_description(
                    str((suggestions.get(key) or {}).get("description_text") or ""),
                    column_name=col_name,
                    schema_name=schema_name,
                    table_name=table_name,
                ):
                    continue
                item = suggestions.get(key) or {}
                rebuilt = self._build_contextual_column_description(
                    column_name=col_name,
                    schema_name=schema_name,
                    table_name=table_name,
                    classification=str(item.get("classification") or "attribute"),
                    data_type=str((columns_index.get(key) or {}).get("data_type") or ""),
                    language_code=language_code,
                )
                item["description_text"] = self._normalize_business_description(
                    description_text=rebuilt,
                    column_name=col_name,
                    schema_name=schema_name,
                    table_name=table_name,
                    language_code=language_code,
                )
                suggestions[key] = item
        return suggestions

    def _suggest_column_enrichment_with_llm(
        self,
        *,
        source_type: str,
        schema_name: str,
        table_name: str,
        columns: list[dict[str, object]],
        sample_rows: list[dict[str, object]],
        context: dict,
        language_code: str = "pt-BR",
    ) -> dict[str, dict[str, str]]:
        try:
            cfg_result = self._call_mcp(
                {
                    "context": context,
                    "tool": "tenant_llm.get_config",
                    "input": {},
                }
            )
            if cfg_result.get("status") != "success":
                return {}
            cfg = (cfg_result.get("data") or {}).get("config") or {}
            provider_name = str(cfg.get("provider_name") or "").strip().lower()
            model_code = str(cfg.get("model_code") or "").strip()
            endpoint_url = str(cfg.get("endpoint_url") or "").strip()
            secret_ref = str(cfg.get("secret_ref") or "").strip()
            if not provider_name or not model_code:
                return {}
            if not endpoint_url:
                endpoint_url = "https://api.openai.com/v1" if provider_name != "google_gemini" else "https://generativelanguage.googleapis.com/v1beta"
            api_key = self._resolve_secret_value(secret_ref)
            if not api_key:
                return {}
            sample_by_col: dict[str, list[str]] = {}
            for col in columns:
                col_name = str(col.get("column_name") or "").strip()
                if not col_name:
                    continue
                vals: list[str] = []
                for row in sample_rows:
                    val = self._sample_value_as_text((row or {}).get(col_name))
                    if val:
                        vals.append(val)
                    if len(vals) >= 5:
                        break
                sample_by_col[col_name] = vals
            payload = {
                "task": "metadata_enrichment",
                "instructions": (
                    "Classifique e descreva colunas de dados em linguagem de negocio clara, evitando frases genericas. "
                    f"Responda sempre no idioma {language_code}. "
                    "Use descricoes curtas e objetivas, como: "
                    "'amount' na tabela 'payment' -> 'Valor recebido'; "
                    "'customer_id' -> 'Codigo do cliente'; "
                    "'payment_date' -> 'Data do pagamento'; "
                    "'inventory_id' em tabela diferente de 'inventory' -> 'Codigo do inventario (FK da tabela sakila.inventory)'. "
                    "Responda APENAS JSON no formato {'columns':[{'column_name':'','classification':'','description_text':'','confidence_score':0.0}]}. "
                    "confidence_score deve ir de 0.0 a 1.0."
                ),
                "output_language": language_code,
                "source_type": source_type,
                "table": {"schema_name": schema_name, "table_name": table_name},
                "columns": [
                    {
                        "column_name": str(col.get("column_name") or ""),
                        "data_type": str(col.get("data_type") or ""),
                        "samples": sample_by_col.get(str(col.get("column_name") or ""), []),
                    }
                    for col in columns
                ],
                "classification_allowed": [
                    "identifier",
                    "sensitive",
                    "contact",
                    "financial",
                    "temporal",
                    "measure",
                    "attribute",
                ],
            }
            llm_output = self._invoke_llm_json(
                provider_name=provider_name,
                model_code=model_code,
                endpoint_url=endpoint_url,
                api_key=api_key,
                prompt_payload=payload,
            )
            if not isinstance(llm_output, dict):
                return {}
            if bool(cfg.get("use_app_default_llm")):
                try:
                    self._record_app_llm_usage(
                        tenant_id=int(context.get("tenant_id") or 0),
                        feature_code="metadata_enrichment",
                        prompt_payload=payload,
                        llm_output=llm_output,
                    )
                except Exception:
                    pass
            rows = llm_output.get("columns")
            if not isinstance(rows, list):
                return {}
            result: dict[str, dict[str, object]] = {}
            for item in rows:
                if not isinstance(item, dict):
                    continue
                col_name = str(item.get("column_name") or "").strip()
                if not col_name:
                    continue
                classification = str(item.get("classification") or "").strip()
                description_text = str(item.get("description_text") or "").strip()
                confidence_score = item.get("confidence_score")
                if not classification and not description_text:
                    continue
                if confidence_score is not None:
                    try:
                        confidence_score = max(0.0, min(1.0, float(confidence_score)))
                    except Exception:
                        confidence_score = None
                result[col_name.lower()] = {
                    "classification": classification,
                    "description_text": description_text,
                    "confidence_score": confidence_score,
                }
            return result
        except Exception:
            return {}

    @staticmethod
    def _test_postgres_connection(profile: dict) -> dict:
        if connect is None:
            return {"ok": False, "source_type": "postgres", "message": "Driver PostgreSQL indisponivel (psycopg)."}
        dsn = str(profile.get("dsn") or "").strip()
        if not dsn:
            host = str(profile.get("host") or "").strip()
            user = str(profile.get("user") or "").strip()
            password = str(profile.get("password") or "").strip()
            dbname = str(profile.get("dbname") or profile.get("database") or "").strip()
            port = str(profile.get("port") or "5432").strip()
            if not host or not user or not dbname:
                return {
                    "ok": False,
                    "source_type": "postgres",
                    "message": "Informe dsn ou host/user/password/dbname no secret_payload.",
                }
            dsn = f"host={host} port={port} dbname={dbname} user={user}"
            if password:
                dsn += f" password={password}"
        try:
            with connect(dsn, connect_timeout=8) as conn, conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            return {"ok": True, "source_type": "postgres", "message": "Conexao PostgreSQL validada com sucesso."}
        except Exception as exc:
            return {"ok": False, "source_type": "postgres", "message": f"Falha na conexao PostgreSQL: {exc}"}

    @staticmethod
    def _discover_postgres_tables(profile: dict) -> list[dict[str, str]]:
        if connect is None:
            raise ValueError("Driver PostgreSQL indisponivel (psycopg).")
        dsn = str(profile.get("dsn") or "").strip()
        if not dsn:
            host = str(profile.get("host") or "").strip()
            user = str(profile.get("user") or "").strip()
            password = str(profile.get("password") or "").strip()
            dbname = str(profile.get("dbname") or profile.get("database") or "").strip()
            port = str(profile.get("port") or "5432").strip()
            if not host or not user or not dbname:
                raise ValueError("Informe dsn ou host/user/password/dbname no secret_payload.")
            dsn = f"host={host} port={port} dbname={dbname} user={user}"
            if password:
                dsn += f" password={password}"
        schema_name = str(profile.get("schema") or "").strip()
        sql = """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('information_schema', 'pg_catalog')
        """
        params: list[object] = []
        if schema_name:
            sql += " AND table_schema = %s"
            params.append(schema_name)
        sql += " ORDER BY table_schema, table_name"
        with connect(dsn, connect_timeout=8) as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return [{"schema_name": str(row[0]), "table_name": str(row[1])} for row in rows]

    @staticmethod
    def _discover_postgres_columns(profile: dict, *, schema_name: str, table_name: str) -> list[dict[str, str]]:
        if connect is None:
            raise ValueError("Driver PostgreSQL indisponivel (psycopg).")
        dsn = str(profile.get("dsn") or "").strip()
        if not dsn:
            host = str(profile.get("host") or "").strip()
            user = str(profile.get("user") or "").strip()
            password = str(profile.get("password") or "").strip()
            dbname = str(profile.get("dbname") or profile.get("database") or "").strip()
            port = str(profile.get("port") or "5432").strip()
            if not host or not user or not dbname:
                raise ValueError("Informe dsn ou host/user/password/dbname no secret_payload.")
            dsn = f"host={host} port={port} dbname={dbname} user={user}"
            if password:
                dsn += f" password={password}"
        schema = schema_name or str(profile.get("schema") or "public").strip()
        sql = """
            SELECT
                c.column_name,
                c.data_type,
                c.ordinal_position,
                pgd.description
            FROM information_schema.columns c
            LEFT JOIN pg_catalog.pg_namespace n
              ON n.nspname = c.table_schema
            LEFT JOIN pg_catalog.pg_class cls
              ON cls.relname = c.table_name
             AND cls.relnamespace = n.oid
            LEFT JOIN pg_catalog.pg_attribute a
              ON a.attrelid = cls.oid
             AND a.attname = c.column_name
            LEFT JOIN pg_catalog.pg_description pgd
              ON pgd.objoid = cls.oid
             AND pgd.objsubid = a.attnum
            WHERE c.table_schema = %s
              AND c.table_name = %s
            ORDER BY c.ordinal_position
        """
        with connect(dsn, connect_timeout=8) as conn, conn.cursor() as cur:
            cur.execute(sql, [schema, table_name])
            rows = cur.fetchall()
        return [
            {
                "column_name": str(row[0]),
                "data_type": str(row[1]),
                "ordinal_position": int(row[2]),
                "description_text": str(row[3] or "").strip() or None,
            }
            for row in rows
        ]

    @staticmethod
    def _test_sqlserver_connection(profile: dict) -> dict:
        host = str(profile.get("host") or profile.get("server") or "").strip()
        port = int(profile.get("port") or 1433)
        database = str(profile.get("database") or profile.get("dbname") or "master").strip()
        user = str(profile.get("user") or profile.get("username") or "").strip()
        password = str(profile.get("password") or "").strip()
        timeout = int(profile.get("timeout_seconds") or 8)
        if pyodbc is None:
            tcp = IAOpsAPIHandler._test_tcp_connection(
                profile={"host": host, "port": port, "timeout_seconds": timeout},
                default_port=1433,
                label="SQL Server",
                source_code="sqlserver",
            )
            tcp["message"] = f"Driver pyodbc nao instalado. Fallback TCP: {tcp.get('message')}"
            return tcp
        if not host or not user:
            return {
                "ok": False,
                "source_type": "sqlserver",
                "message": "Informe host, user e password no secret_payload.",
            }
        dsn = str(profile.get("dsn") or "").strip()
        conn_str = dsn
        if not conn_str:
            configured_driver = str(profile.get("driver") or "").strip()
            if configured_driver:
                driver = configured_driver
            else:
                available = list(pyodbc.drivers())
                preferred = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server"]
                driver = next((item for item in preferred if item in available), (available[0] if available else "SQL Server"))
            encrypt = str(profile.get("encrypt") or "yes").strip()
            trust = str(profile.get("trust_server_certificate") or "yes").strip()
            conn_str = (
                f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={database};UID={user};PWD={password};"
                f"Encrypt={encrypt};TrustServerCertificate={trust};Connection Timeout={timeout};"
            )
        try:
            with pyodbc.connect(conn_str, timeout=timeout) as conn:
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.fetchone()
            return {"ok": True, "source_type": "sqlserver", "message": "Conexao SQL Server validada com sucesso."}
        except Exception as exc:
            return {"ok": False, "source_type": "sqlserver", "message": f"Falha na conexao SQL Server: {exc}"}

    @staticmethod
    def _discover_sqlserver_tables(profile: dict) -> list[dict[str, str]]:
        if pyodbc is None:
            raise ValueError("Driver pyodbc nao instalado para descoberta SQL Server.")
        host = str(profile.get("host") or profile.get("server") or "").strip()
        port = int(profile.get("port") or 1433)
        database = str(profile.get("database") or profile.get("dbname") or "master").strip()
        user = str(profile.get("user") or profile.get("username") or "").strip()
        password = str(profile.get("password") or "").strip()
        timeout = int(profile.get("timeout_seconds") or 8)
        if not host or not user:
            raise ValueError("Informe host, user e password no secret_payload.")
        dsn = str(profile.get("dsn") or "").strip()
        conn_str = dsn
        if not conn_str:
            configured_driver = str(profile.get("driver") or "").strip()
            if configured_driver:
                driver = configured_driver
            else:
                available = list(pyodbc.drivers())
                preferred = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server"]
                driver = next((item for item in preferred if item in available), (available[0] if available else "SQL Server"))
            conn_str = (
                f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={database};UID={user};PWD={password};"
                f"Encrypt=yes;TrustServerCertificate=yes;Connection Timeout={timeout};"
            )
        sql = """
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        with pyodbc.connect(conn_str, timeout=timeout) as conn:
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
        return [{"schema_name": str(row[0]), "table_name": str(row[1])} for row in rows]

    @staticmethod
    def _discover_sqlserver_columns(profile: dict, *, schema_name: str, table_name: str) -> list[dict[str, str]]:
        if pyodbc is None:
            raise ValueError("Driver pyodbc nao instalado para descoberta SQL Server.")
        host = str(profile.get("host") or profile.get("server") or "").strip()
        port = int(profile.get("port") or 1433)
        database = str(profile.get("database") or profile.get("dbname") or "master").strip()
        user = str(profile.get("user") or profile.get("username") or "").strip()
        password = str(profile.get("password") or "").strip()
        timeout = int(profile.get("timeout_seconds") or 8)
        if not host or not user:
            raise ValueError("Informe host, user e password no secret_payload.")
        dsn = str(profile.get("dsn") or "").strip()
        conn_str = dsn
        if not conn_str:
            configured_driver = str(profile.get("driver") or "").strip()
            if configured_driver:
                driver = configured_driver
            else:
                available = list(pyodbc.drivers())
                preferred = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server"]
                driver = next((item for item in preferred if item in available), (available[0] if available else "SQL Server"))
            conn_str = (
                f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={database};UID={user};PWD={password};"
                f"Encrypt=yes;TrustServerCertificate=yes;Connection Timeout={timeout};"
            )
        schema = schema_name or "dbo"
        sql = """
            SELECT
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.ORDINAL_POSITION,
                CAST(ep.value AS NVARCHAR(4000)) AS description_text
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN sys.extended_properties ep
              ON ep.major_id = OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME))
             AND ep.minor_id = c.ORDINAL_POSITION
             AND ep.name = 'MS_Description'
            WHERE c.TABLE_SCHEMA = ?
              AND c.TABLE_NAME = ?
            ORDER BY c.ORDINAL_POSITION
        """
        with pyodbc.connect(conn_str, timeout=timeout) as conn:
            cur = conn.cursor()
            cur.execute(sql, (schema, table_name))
            rows = cur.fetchall()
        return [
            {
                "column_name": str(row[0]),
                "data_type": str(row[1]),
                "ordinal_position": int(row[2]),
                "description_text": str(row[3] or "").strip() or None,
            }
            for row in rows
        ]

    @staticmethod
    def _test_mysql_connection(profile: dict) -> dict:
        host = str(profile.get("host") or profile.get("server") or "").strip()
        port = int(profile.get("port") or 3306)
        database = str(profile.get("database") or profile.get("dbname") or "").strip()
        user = str(profile.get("user") or profile.get("username") or "").strip()
        password = str(profile.get("password") or "").strip()
        timeout = int(profile.get("timeout_seconds") or 8)
        if pymysql is None:
            tcp = IAOpsAPIHandler._test_tcp_connection(
                profile={"host": host, "port": port, "timeout_seconds": timeout},
                default_port=3306,
                label="MySQL",
                source_code="mysql",
            )
            tcp["message"] = f"Driver pymysql nao instalado. Fallback TCP: {tcp.get('message')}"
            return tcp
        if not host or not user:
            return {"ok": False, "source_type": "mysql", "message": "Informe host, user e password no secret_payload."}
        try:
            with pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database or None,
                connect_timeout=timeout,
                read_timeout=timeout,
                write_timeout=timeout,
                charset="utf8mb4",
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            return {"ok": True, "source_type": "mysql", "message": "Conexao MySQL validada com sucesso."}
        except Exception as exc:
            return {"ok": False, "source_type": "mysql", "message": f"Falha na conexao MySQL: {exc}"}

    @staticmethod
    def _discover_mysql_tables(profile: dict) -> list[dict[str, str]]:
        if pymysql is None:
            raise ValueError("Driver pymysql nao instalado para descoberta MySQL.")
        host = str(profile.get("host") or profile.get("server") or "").strip()
        port = int(profile.get("port") or 3306)
        database = str(profile.get("database") or profile.get("dbname") or "").strip()
        user = str(profile.get("user") or profile.get("username") or "").strip()
        password = str(profile.get("password") or "").strip()
        timeout = int(profile.get("timeout_seconds") or 8)
        if not host or not user:
            raise ValueError("Informe host, user e password no secret_payload.")
        with pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database or None,
            connect_timeout=timeout,
            read_timeout=timeout,
            write_timeout=timeout,
            charset="utf8mb4",
        ) as conn:
            with conn.cursor() as cur:
                if database:
                    cur.execute(
                        """
                        SELECT table_schema, table_name
                        FROM information_schema.tables
                        WHERE table_type = 'BASE TABLE'
                          AND table_schema = %s
                        ORDER BY table_schema, table_name
                        """,
                        (database,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT table_schema, table_name
                        FROM information_schema.tables
                        WHERE table_type = 'BASE TABLE'
                          AND table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
                        ORDER BY table_schema, table_name
                        """
                    )
                rows = cur.fetchall()
        return [{"schema_name": str(row[0]), "table_name": str(row[1])} for row in rows]

    @staticmethod
    def _discover_mysql_columns(profile: dict, *, schema_name: str, table_name: str) -> list[dict[str, str]]:
        if pymysql is None:
            raise ValueError("Driver pymysql nao instalado para descoberta MySQL.")
        host = str(profile.get("host") or profile.get("server") or "").strip()
        port = int(profile.get("port") or 3306)
        database = str(profile.get("database") or profile.get("dbname") or "").strip()
        user = str(profile.get("user") or profile.get("username") or "").strip()
        password = str(profile.get("password") or "").strip()
        timeout = int(profile.get("timeout_seconds") or 8)
        if not host or not user:
            raise ValueError("Informe host, user e password no secret_payload.")
        schema = schema_name or database
        if not schema:
            raise ValueError("schema_name ou database obrigatorio para MySQL.")
        with pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database or schema,
            connect_timeout=timeout,
            read_timeout=timeout,
            write_timeout=timeout,
            charset="utf8mb4",
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT column_name, data_type, ordinal_position, column_comment
                    FROM information_schema.columns
                    WHERE table_schema = %s
                      AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (schema, table_name),
                )
                rows = cur.fetchall()
        return [
            {
                "column_name": str(row[0]),
                "data_type": str(row[1]),
                "ordinal_position": int(row[2]),
                "description_text": str(row[3] or "").strip() or None,
            }
            for row in rows
        ]

    @staticmethod
    def _test_oracle_connection(profile: dict) -> dict:
        host = str(profile.get("host") or profile.get("server") or "").strip()
        port = int(profile.get("port") or 1521)
        user = str(profile.get("user") or profile.get("username") or "").strip()
        password = str(profile.get("password") or "").strip()
        service_name = str(profile.get("service_name") or "").strip()
        sid = str(profile.get("sid") or "").strip()
        timeout = int(profile.get("timeout_seconds") or 8)
        if oracledb is None:
            tcp = IAOpsAPIHandler._test_tcp_connection(
                profile={"host": host, "port": port, "timeout_seconds": timeout},
                default_port=1521,
                label="Oracle",
                source_code="oracle",
            )
            tcp["message"] = f"Driver oracledb nao instalado. Fallback TCP: {tcp.get('message')}"
            return tcp
        if not host or not user:
            return {
                "ok": False,
                "source_type": "oracle",
                "message": "Informe host, user e password no secret_payload.",
            }
        dsn = str(profile.get("dsn") or "").strip()
        if not dsn:
            if service_name:
                dsn = f"{host}:{port}/{service_name}"
            elif sid:
                dsn = f"{host}:{port}/{sid}"
            else:
                dsn = f"{host}:{port}/XEPDB1"
        try:
            with oracledb.connect(user=user, password=password, dsn=dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM dual")
                    cur.fetchone()
            return {"ok": True, "source_type": "oracle", "message": "Conexao Oracle validada com sucesso."}
        except Exception as exc:
            return {"ok": False, "source_type": "oracle", "message": f"Falha na conexao Oracle: {exc}"}

    @staticmethod
    def _discover_oracle_tables(profile: dict) -> list[dict[str, str]]:
        if oracledb is None:
            raise ValueError("Driver oracledb nao instalado para descoberta Oracle.")
        host = str(profile.get("host") or profile.get("server") or "").strip()
        port = int(profile.get("port") or 1521)
        user = str(profile.get("user") or profile.get("username") or "").strip()
        password = str(profile.get("password") or "").strip()
        service_name = str(profile.get("service_name") or "").strip()
        sid = str(profile.get("sid") or "").strip()
        if not host or not user:
            raise ValueError("Informe host, user e password no secret_payload.")
        dsn = str(profile.get("dsn") or "").strip()
        if not dsn:
            if service_name:
                dsn = f"{host}:{port}/{service_name}"
            elif sid:
                dsn = f"{host}:{port}/{sid}"
            else:
                dsn = f"{host}:{port}/XEPDB1"
        owner = str(profile.get("owner") or profile.get("schema") or user).strip().upper()
        with oracledb.connect(user=user, password=password, dsn=dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT owner, table_name
                    FROM all_tables
                    WHERE owner = :owner
                    ORDER BY table_name
                    """,
                    {"owner": owner},
                )
                rows = cur.fetchall()
        return [{"schema_name": str(row[0]), "table_name": str(row[1])} for row in rows]

    @staticmethod
    def _discover_oracle_columns(profile: dict, *, schema_name: str, table_name: str) -> list[dict[str, str]]:
        if oracledb is None:
            raise ValueError("Driver oracledb nao instalado para descoberta Oracle.")
        host = str(profile.get("host") or profile.get("server") or "").strip()
        port = int(profile.get("port") or 1521)
        user = str(profile.get("user") or profile.get("username") or "").strip()
        password = str(profile.get("password") or "").strip()
        service_name = str(profile.get("service_name") or "").strip()
        sid = str(profile.get("sid") or "").strip()
        if not host or not user:
            raise ValueError("Informe host, user e password no secret_payload.")
        dsn = str(profile.get("dsn") or "").strip()
        if not dsn:
            if service_name:
                dsn = f"{host}:{port}/{service_name}"
            elif sid:
                dsn = f"{host}:{port}/{sid}"
            else:
                dsn = f"{host}:{port}/XEPDB1"
        owner = str(schema_name or profile.get("owner") or profile.get("schema") or user).strip().upper()
        table = str(table_name or "").strip().upper()
        with oracledb.connect(user=user, password=password, dsn=dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        c.column_name,
                        c.data_type,
                        c.column_id,
                        cc.comments AS description_text
                    FROM all_tab_columns c
                    LEFT JOIN all_col_comments cc
                      ON cc.owner = c.owner
                     AND cc.table_name = c.table_name
                     AND cc.column_name = c.column_name
                    WHERE c.owner = :owner
                      AND c.table_name = :table_name
                    ORDER BY c.column_id
                    """,
                    {"owner": owner, "table_name": table},
                )
                rows = cur.fetchall()
        return [
            {
                "column_name": str(row[0]),
                "data_type": str(row[1]),
                "ordinal_position": int(row[2]),
                "description_text": str(row[3] or "").strip() or None,
            }
            for row in rows
        ]

    @staticmethod
    def _test_bearer_http_connection(*, profile: dict, default_url: str, label: str) -> dict:
        api_url = str(profile.get("api_url") or default_url).strip()
        access_token = str(profile.get("access_token") or "").strip()
        if not access_token:
            return {
                "ok": False,
                "source_type": label.lower(),
                "message": f"access_token obrigatorio para validar {label}.",
            }
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "User-Agent": "IAOps-Governance/1.0",
        }
        req = urllib.request.Request(url=api_url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=12) as resp:
                status = int(resp.getcode() or 0)
                if 200 <= status < 300:
                    return {
                        "ok": True,
                        "source_type": label.lower(),
                        "message": f"Conexao {label} validada com sucesso (HTTP {status}).",
                        "http_status": status,
                    }
                return {
                    "ok": False,
                    "source_type": label.lower(),
                    "message": f"Endpoint {label} retornou HTTP {status}.",
                    "http_status": status,
                }
        except urllib.error.HTTPError as exc:
            return {
                "ok": False,
                "source_type": label.lower(),
                "message": f"Falha HTTP no {label}: {exc.code}",
                "http_status": int(exc.code or 0),
            }
        except Exception as exc:
            return {
                "ok": False,
                "source_type": label.lower(),
                "message": f"Falha na conexao {label}: {exc}",
            }

    @staticmethod
    def _test_tcp_connection(*, profile: dict, default_port: int, label: str, source_code: str) -> dict:
        host = str(profile.get("host") or profile.get("server") or "").strip()
        port = int(profile.get("port") or default_port)
        timeout = float(profile.get("timeout_seconds") or 8)
        if not host:
            return {
                "ok": False,
                "source_type": source_code,
                "message": f"Informe host para validar conexao {label}.",
            }
        try:
            with socket.create_connection((host, port), timeout=timeout):
                pass
            return {
                "ok": True,
                "source_type": source_code,
                "message": f"Porta {host}:{port} acessivel para {label}.",
                "network": {"host": host, "port": port},
            }
        except Exception as exc:
            return {
                "ok": False,
                "source_type": source_code,
                "message": f"Falha de conectividade {label} em {host}:{port} - {exc}",
                "network": {"host": host, "port": port},
            }

    def _db_get_lgpd_policy(self, *, tenant_id: int) -> dict:
        if not self._is_db_enabled():
            return {}
        self._ensure_lgpd_schema_meta()
        schema = self.signup_schema
        sql = f"""
            SELECT dpo_name, dpo_email, retention_days, legal_notes, updated_by_user_id, updated_at
            FROM {schema}.lgpd_policy
            WHERE tenant_id = %(tenant_id)s
            LIMIT 1
        """
        with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
            cur.execute(sql, {"tenant_id": tenant_id})
            row = cur.fetchone()
        if not row:
            return {}
        return {
            "dpo_name": row[0],
            "dpo_email": row[1],
            "retention_days": row[2],
            "legal_notes": row[3],
            "updated_by_user_id": row[4],
            "updated_at": row[5].isoformat() if row[5] else None,
        }

    def _db_upsert_lgpd_policy(
        self,
        *,
        tenant_id: int,
        user_id: int,
        dpo_name: object,
        dpo_email: object,
        retention_days: object,
        legal_notes: object,
    ) -> dict:
        if not self._is_db_enabled():
            raise ValueError("Banco nao configurado")
        self._ensure_lgpd_schema_meta()
        schema = self.signup_schema
        sql = f"""
            INSERT INTO {schema}.lgpd_policy (
                tenant_id, dpo_name, dpo_email, retention_days, legal_notes, updated_by_user_id, updated_at
            )
            VALUES (
                %(tenant_id)s, %(dpo_name)s, %(dpo_email)s, %(retention_days)s, %(legal_notes)s, %(user_id)s, NOW()
            )
            ON CONFLICT (tenant_id)
            DO UPDATE SET
                dpo_name = EXCLUDED.dpo_name,
                dpo_email = EXCLUDED.dpo_email,
                retention_days = EXCLUDED.retention_days,
                legal_notes = EXCLUDED.legal_notes,
                updated_by_user_id = EXCLUDED.updated_by_user_id,
                updated_at = NOW()
        """
        with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "tenant_id": tenant_id,
                    "dpo_name": str(dpo_name or "").strip() or None,
                    "dpo_email": str(dpo_email or "").strip() or None,
                    "retention_days": int(retention_days) if retention_days not in (None, "") else None,
                    "legal_notes": str(legal_notes or "").strip() or None,
                    "user_id": user_id,
                },
            )
            conn.commit()
        return self._db_get_lgpd_policy(tenant_id=tenant_id)

    def _db_list_lgpd_rules(self, *, tenant_id: int) -> list[dict]:
        if not self._is_db_enabled():
            return []
        self._ensure_lgpd_schema_meta()
        schema = self.signup_schema
        sql = f"""
            SELECT id, schema_name, table_name, column_name, rule_type, rule_config, is_active, updated_at
            FROM {schema}.lgpd_rule
            WHERE tenant_id = %(tenant_id)s
            ORDER BY id DESC
        """
        with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
            cur.execute(sql, {"tenant_id": tenant_id})
            rows = cur.fetchall()
        return [
            {
                "id": int(row[0]),
                "schema_name": row[1],
                "table_name": row[2],
                "column_name": row[3],
                "rule_type": row[4],
                "rule_config": row[5] or {},
                "is_active": bool(row[6]),
                "updated_at": row[7].isoformat() if row[7] else None,
            }
            for row in rows
        ]

    def _db_upsert_lgpd_rule(
        self,
        *,
        tenant_id: int,
        user_id: int,
        rule_id: object,
        schema_name: object,
        table_name: object,
        column_name: object,
        rule_type: object,
        rule_config: dict,
        is_active: bool,
    ) -> dict:
        if not self._is_db_enabled():
            raise ValueError("Banco nao configurado")
        self._ensure_lgpd_schema_meta()
        schema_name_v = str(schema_name or "").strip()
        table_name_v = str(table_name or "").strip()
        column_name_v = str(column_name or "").strip()
        rule_type_v = str(rule_type or "").strip()
        rule_name_v = f"{schema_name_v}.{table_name_v}.{column_name_v}:{rule_type_v}"[:190]
        rule_expression_v = f"{rule_type_v}({schema_name_v}.{table_name_v}.{column_name_v})"[:240]
        if not schema_name_v or not table_name_v or not column_name_v or not rule_type_v:
            raise ValueError("schema_name, table_name, column_name e rule_type obrigatorios")
        schema = self.signup_schema
        with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
            if rule_id not in (None, ""):
                cur.execute(
                    f"""
                    UPDATE {schema}.lgpd_rule
                       SET rule_name = %(rule_name)s,
                           rule_expression = %(rule_expression)s,
                           schema_name = %(schema_name)s,
                           table_name = %(table_name)s,
                           column_name = %(column_name)s,
                           rule_type = %(rule_type)s,
                           rule_config = %(rule_config)s::jsonb,
                           is_active = %(is_active)s,
                           updated_at = NOW()
                     WHERE id = %(id)s
                       AND tenant_id = %(tenant_id)s
                    RETURNING id
                    """,
                    {
                        "id": int(rule_id),
                        "tenant_id": tenant_id,
                        "rule_name": rule_name_v,
                        "rule_expression": rule_expression_v,
                        "schema_name": schema_name_v,
                        "table_name": table_name_v,
                        "column_name": column_name_v,
                        "rule_type": rule_type_v,
                        "rule_config": json.dumps(rule_config or {}, ensure_ascii=True),
                        "is_active": bool(is_active),
                    },
                )
                row = cur.fetchone()
                if not row:
                    raise ValueError("regra nao encontrada")
                new_id = int(row[0])
            else:
                cur.execute(
                    f"""
                    INSERT INTO {schema}.lgpd_rule (
                        tenant_id, rule_name, rule_expression, schema_name, table_name, column_name, rule_type, rule_config, is_active, created_by_user_id
                    )
                    VALUES (
                        %(tenant_id)s, %(rule_name)s, %(rule_expression)s, %(schema_name)s, %(table_name)s, %(column_name)s, %(rule_type)s, %(rule_config)s::jsonb, %(is_active)s, %(user_id)s
                    )
                    RETURNING id
                    """,
                    {
                        "tenant_id": tenant_id,
                        "rule_name": rule_name_v,
                        "rule_expression": rule_expression_v,
                        "schema_name": schema_name_v,
                        "table_name": table_name_v,
                        "column_name": column_name_v,
                        "rule_type": rule_type_v,
                        "rule_config": json.dumps(rule_config or {}, ensure_ascii=True),
                        "is_active": bool(is_active),
                        "user_id": user_id,
                    },
                )
                new_id = int(cur.fetchone()[0])
            conn.commit()
        rows = self._db_list_lgpd_rules(tenant_id=tenant_id)
        return next((item for item in rows if int(item["id"]) == int(new_id)), {})

    def _ensure_lgpd_schema_meta(self) -> None:
        if self.lgpd_schema_ready or not self._is_db_enabled():
            return
        schema = self.signup_schema
        ddls = [
            f"ALTER TABLE {schema}.lgpd_policy ADD COLUMN IF NOT EXISTS updated_by_user_id BIGINT",
            f"ALTER TABLE {schema}.lgpd_policy ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ",
            f"ALTER TABLE {schema}.lgpd_rule ADD COLUMN IF NOT EXISTS rule_name TEXT",
            f"ALTER TABLE {schema}.lgpd_rule ADD COLUMN IF NOT EXISTS rule_expression TEXT",
            f"ALTER TABLE {schema}.lgpd_rule ADD COLUMN IF NOT EXISTS rule_config JSONB NOT NULL DEFAULT '{{}}'::jsonb",
            f"ALTER TABLE {schema}.lgpd_rule ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE",
            f"ALTER TABLE {schema}.lgpd_rule ADD COLUMN IF NOT EXISTS created_by_user_id BIGINT",
            f"ALTER TABLE {schema}.lgpd_rule ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ",
            f"ALTER TABLE {schema}.lgpd_dsr_request ADD COLUMN IF NOT EXISTS notes TEXT",
            f"ALTER TABLE {schema}.lgpd_dsr_request ADD COLUMN IF NOT EXISTS resolved_by_user_id BIGINT",
            f"ALTER TABLE {schema}.lgpd_dsr_request ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ",
        ]
        try:
            with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
                for ddl in ddls:
                    try:
                        cur.execute(ddl)
                    except Exception:
                        # compatibilidade com esquemas antigos/parciais
                        pass
                try:
                    cur.execute(
                        f"""
                        CREATE OR REPLACE FUNCTION {schema}.fn_fill_lgpd_rule_fields()
                        RETURNS trigger
                        LANGUAGE plpgsql
                        AS $$
                        BEGIN
                            IF NEW.rule_name IS NULL OR BTRIM(NEW.rule_name) = '' THEN
                                NEW.rule_name := CONCAT(
                                    COALESCE(NULLIF(BTRIM(NEW.schema_name), ''), 'schema'),
                                    '.',
                                    COALESCE(NULLIF(BTRIM(NEW.table_name), ''), 'table'),
                                    '.',
                                    COALESCE(NULLIF(BTRIM(NEW.column_name), ''), 'column'),
                                    ':',
                                    COALESCE(NULLIF(BTRIM(NEW.rule_type), ''), 'rule')
                                );
                            END IF;
                            IF NEW.rule_expression IS NULL OR BTRIM(NEW.rule_expression) = '' THEN
                                NEW.rule_expression := CONCAT(
                                    COALESCE(NULLIF(BTRIM(NEW.rule_type), ''), 'rule'),
                                    '(',
                                    COALESCE(NULLIF(BTRIM(NEW.schema_name), ''), 'schema'),
                                    '.',
                                    COALESCE(NULLIF(BTRIM(NEW.table_name), ''), 'table'),
                                    '.',
                                    COALESCE(NULLIF(BTRIM(NEW.column_name), ''), 'column'),
                                    ')'
                                );
                            END IF;
                            RETURN NEW;
                        END;
                        $$;
                        """
                    )
                    cur.execute(f"DROP TRIGGER IF EXISTS trg_fill_lgpd_rule_name ON {schema}.lgpd_rule")
                    cur.execute(f"DROP TRIGGER IF EXISTS trg_fill_lgpd_rule_fields ON {schema}.lgpd_rule")
                    cur.execute(
                        f"""
                        CREATE TRIGGER trg_fill_lgpd_rule_fields
                        BEFORE INSERT OR UPDATE ON {schema}.lgpd_rule
                        FOR EACH ROW
                        EXECUTE FUNCTION {schema}.fn_fill_lgpd_rule_fields()
                        """
                    )
                except Exception:
                    pass
                try:
                    cur.execute(
                        f"""
                        UPDATE {schema}.lgpd_rule
                           SET rule_name = CONCAT(
                               COALESCE(NULLIF(TRIM(schema_name), ''), 'schema'),
                               '.',
                               COALESCE(NULLIF(TRIM(table_name), ''), 'table'),
                               '.',
                               COALESCE(NULLIF(TRIM(column_name), ''), 'column'),
                               ':',
                               COALESCE(NULLIF(TRIM(rule_type), ''), 'rule')
                           )
                         WHERE rule_name IS NULL OR TRIM(rule_name) = ''
                        """
                    )
                except Exception:
                    pass
                try:
                    cur.execute(
                        f"""
                        UPDATE {schema}.lgpd_rule
                           SET rule_expression = CONCAT(
                               COALESCE(NULLIF(TRIM(rule_type), ''), 'rule'),
                               '(',
                               COALESCE(NULLIF(TRIM(schema_name), ''), 'schema'),
                               '.',
                               COALESCE(NULLIF(TRIM(table_name), ''), 'table'),
                               '.',
                               COALESCE(NULLIF(TRIM(column_name), ''), 'column'),
                               ')'
                           )
                         WHERE rule_expression IS NULL OR TRIM(rule_expression) = ''
                        """
                    )
                except Exception:
                    pass
                conn.commit()
            self.lgpd_schema_ready = True
        except Exception:
            # nao bloquear fluxo caso migracao automatica falhe
            return

    def _db_list_lgpd_dsr(self, *, tenant_id: int, status: str | None) -> list[dict]:
        if not self._is_db_enabled():
            return []
        schema = self.signup_schema
        sql = f"""
            SELECT id, requester_name, requester_email, request_type, subject_key, status, notes, created_at, resolved_at
            FROM {schema}.lgpd_dsr_request
            WHERE tenant_id = %(tenant_id)s
              AND (%(status)s::text IS NULL OR status = %(status)s::text)
            ORDER BY id DESC
        """
        with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
            cur.execute(sql, {"tenant_id": tenant_id, "status": status})
            rows = cur.fetchall()
        return [
            {
                "id": int(row[0]),
                "requester_name": row[1],
                "requester_email": row[2],
                "request_type": row[3],
                "subject_key": row[4],
                "status": row[5],
                "notes": row[6],
                "created_at": row[7].isoformat() if row[7] else None,
                "resolved_at": row[8].isoformat() if row[8] else None,
            }
            for row in rows
        ]

    def _db_open_lgpd_dsr(
        self,
        *,
        tenant_id: int,
        user_id: int,
        requester_name: object,
        requester_email: object,
        request_type: object,
        subject_key: object,
        notes: object,
    ) -> dict:
        if not self._is_db_enabled():
            raise ValueError("Banco nao configurado")
        requester_name_v = str(requester_name or "").strip()
        requester_email_v = str(requester_email or "").strip()
        request_type_v = str(request_type or "").strip()
        if not requester_name_v or not requester_email_v or not request_type_v:
            raise ValueError("requester_name, requester_email e request_type obrigatorios")
        schema = self.signup_schema
        with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {schema}.lgpd_dsr_request (
                    tenant_id, requester_name, requester_email, request_type, subject_key, status, notes, opened_by_user_id
                )
                VALUES (
                    %(tenant_id)s, %(requester_name)s, %(requester_email)s, %(request_type)s, %(subject_key)s, 'open', %(notes)s, %(user_id)s
                )
                RETURNING id
                """,
                {
                    "tenant_id": tenant_id,
                    "requester_name": requester_name_v,
                    "requester_email": requester_email_v,
                    "request_type": request_type_v,
                    "subject_key": str(subject_key or "").strip() or None,
                    "notes": str(notes or "").strip() or None,
                    "user_id": user_id,
                },
            )
            req_id = int(cur.fetchone()[0])
            conn.commit()
        rows = self._db_list_lgpd_dsr(tenant_id=tenant_id, status=None)
        return next((item for item in rows if int(item["id"]) == req_id), {})

    def _db_resolve_lgpd_dsr(self, *, tenant_id: int, user_id: int, request_id: object, notes: object) -> dict:
        if request_id in (None, ""):
            raise ValueError("request_id obrigatorio")
        if not self._is_db_enabled():
            raise ValueError("Banco nao configurado")
        schema = self.signup_schema
        with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {schema}.lgpd_dsr_request
                   SET status = 'resolved',
                       notes = COALESCE(%(notes)s, notes),
                       resolved_by_user_id = %(user_id)s,
                       resolved_at = NOW()
                 WHERE tenant_id = %(tenant_id)s
                   AND id = %(id)s
                RETURNING id
                """,
                {
                    "tenant_id": tenant_id,
                    "id": int(request_id),
                    "notes": str(notes or "").strip() or None,
                    "user_id": user_id,
                },
            )
            row = cur.fetchone()
            if not row:
                raise ValueError("solicitacao nao encontrada")
            conn.commit()
        rows = self._db_list_lgpd_dsr(tenant_id=tenant_id, status=None)
        return next((item for item in rows if int(item["id"]) == int(request_id)), {})

    def _db_list_billing_plans(self) -> list[dict]:
        if not self._is_db_enabled():
            return []
        schema = self.signup_schema
        self._ensure_billing_plan_limits_columns()
        with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    id,
                    code,
                    name,
                    max_tenants,
                    max_users,
                    COALESCE(max_data_sources_per_client, 10),
                    COALESCE(max_data_sources_per_tenant, 5),
                    monthly_price_cents,
                    is_active
                FROM {schema}.billing_plan
                ORDER BY id
                """
            )
            rows = cur.fetchall()
        return [
            {
                "id": int(row[0]),
                "code": row[1],
                "name": row[2],
                "max_tenants": int(row[3]),
                "max_users": int(row[4]),
                "max_data_sources_per_client": int(row[5]),
                "max_data_sources_per_tenant": int(row[6]),
                "monthly_price_cents": int(row[7]),
                "is_active": bool(row[8]),
            }
            for row in rows
        ]

    def _db_get_billing_subscription(self, *, client_id: int) -> dict:
        if not self._is_db_enabled():
            return {}
        schema = self.signup_schema
        self._ensure_billing_plan_limits_columns()
        with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    s.id,
                    s.client_id,
                    s.plan_id,
                    p.code,
                    p.name,
                    p.max_tenants,
                    p.max_users,
                    COALESCE(p.max_data_sources_per_client, 10),
                    COALESCE(p.max_data_sources_per_tenant, 5),
                    s.status,
                    s.tolerance_days,
                    s.starts_at,
                    s.ends_at
                FROM {schema}.billing_subscription s
                JOIN {schema}.billing_plan p ON p.id = s.plan_id
                WHERE s.client_id = %(client_id)s
                ORDER BY s.id DESC
                LIMIT 1
                """,
                {"client_id": client_id},
            )
            row = cur.fetchone()
        if not row:
            return {}
        return {
            "id": int(row[0]),
            "client_id": int(row[1]),
            "plan_id": int(row[2]),
            "plan_code": row[3],
            "plan_name": row[4],
            "max_tenants": int(row[5]),
            "max_users": int(row[6]),
            "max_data_sources_per_client": int(row[7]),
            "max_data_sources_per_tenant": int(row[8]),
            "status": row[9],
            "tolerance_days": int(row[10]),
            "starts_at": row[11].isoformat() if row[11] else None,
            "ends_at": row[12].isoformat() if row[12] else None,
        }

    def _db_upsert_billing_subscription(self, *, client_id: int, plan_code: object, tolerance_days: object) -> dict:
        if not self._is_db_enabled():
            raise ValueError("Banco nao configurado")
        plan_code_v = str(plan_code or "").strip().lower()
        if not plan_code_v:
            raise ValueError("plan_code obrigatorio")
        schema = self.signup_schema
        tol = int(tolerance_days) if tolerance_days not in (None, "") else 5
        with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
            cur.execute(f"SELECT id FROM {schema}.billing_plan WHERE code = %(code)s LIMIT 1", {"code": plan_code_v})
            row = cur.fetchone()
            if not row:
                raise ValueError("plan_code nao encontrado")
            plan_id = int(row[0])
            cur.execute(
                f"""
                INSERT INTO {schema}.billing_subscription (
                    client_id, plan_id, status, starts_at, ends_at, tolerance_days, updated_at
                )
                VALUES (
                    %(client_id)s, %(plan_id)s, 'active', NOW(), NULL, %(tolerance_days)s, NOW()
                )
                ON CONFLICT DO NOTHING
                """,
                {"client_id": client_id, "plan_id": plan_id, "tolerance_days": tol},
            )
            cur.execute(
                f"""
                UPDATE {schema}.billing_subscription
                   SET plan_id = %(plan_id)s,
                       tolerance_days = %(tolerance_days)s,
                       status = 'active',
                       updated_at = NOW()
                 WHERE id = (
                    SELECT id FROM {schema}.billing_subscription
                    WHERE client_id = %(client_id)s
                    ORDER BY id DESC
                    LIMIT 1
                 )
                """,
                {"client_id": client_id, "plan_id": plan_id, "tolerance_days": tol},
            )
            conn.commit()
        return self._db_get_billing_subscription(client_id=client_id)

    def _db_upsert_billing_plan(
        self,
        *,
        plan_id: object,
        code: object,
        name: object,
        max_tenants: object,
        max_users: object,
        max_data_sources_per_client: object,
        max_data_sources_per_tenant: object,
        monthly_price_cents: object,
        is_active: object,
    ) -> dict:
        if not self._is_db_enabled():
            raise ValueError("Banco nao configurado")
        self._ensure_billing_plan_limits_columns()
        schema = self.signup_schema
        code_v = str(code or "").strip().lower()
        name_v = str(name or "").strip()
        if not code_v:
            raise ValueError("code obrigatorio")
        if not name_v:
            raise ValueError("name obrigatorio")
        max_tenants_v = max(1, int(max_tenants or 1))
        max_users_v = max(1, int(max_users or 1))
        max_sources_client_v = max(1, int(max_data_sources_per_client or 10))
        max_sources_tenant_v = max(1, int(max_data_sources_per_tenant or 5))
        monthly_price_cents_v = max(0, int(monthly_price_cents or 0))
        is_active_v = bool(True if is_active is None else is_active)
        with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
            if plan_id in (None, ""):
                cur.execute(
                    f"""
                    INSERT INTO {schema}.billing_plan (
                        code, name, max_tenants, max_users, max_data_sources_per_client, max_data_sources_per_tenant, monthly_price_cents, is_active
                    )
                    VALUES (
                        %(code)s, %(name)s, %(max_tenants)s, %(max_users)s, %(max_data_sources_per_client)s, %(max_data_sources_per_tenant)s, %(monthly_price_cents)s, %(is_active)s
                    )
                    RETURNING id
                    """,
                    {
                        "code": code_v,
                        "name": name_v,
                        "max_tenants": max_tenants_v,
                        "max_users": max_users_v,
                        "max_data_sources_per_client": max_sources_client_v,
                        "max_data_sources_per_tenant": max_sources_tenant_v,
                        "monthly_price_cents": monthly_price_cents_v,
                        "is_active": is_active_v,
                    },
                )
                row = cur.fetchone()
                target_id = int(row[0])
            else:
                target_id = int(plan_id)
                cur.execute(
                    f"""
                    UPDATE {schema}.billing_plan
                       SET code = %(code)s,
                           name = %(name)s,
                           max_tenants = %(max_tenants)s,
                           max_users = %(max_users)s,
                           max_data_sources_per_client = %(max_data_sources_per_client)s,
                           max_data_sources_per_tenant = %(max_data_sources_per_tenant)s,
                           monthly_price_cents = %(monthly_price_cents)s,
                           is_active = %(is_active)s
                     WHERE id = %(id)s
                    """,
                    {
                        "id": target_id,
                        "code": code_v,
                        "name": name_v,
                        "max_tenants": max_tenants_v,
                        "max_users": max_users_v,
                        "max_data_sources_per_client": max_sources_client_v,
                        "max_data_sources_per_tenant": max_sources_tenant_v,
                        "monthly_price_cents": monthly_price_cents_v,
                        "is_active": is_active_v,
                    },
                )
                if cur.rowcount <= 0:
                    raise ValueError("plano nao encontrado")
            conn.commit()
        plans = self._db_list_billing_plans()
        for item in plans:
            if int(item["id"]) == int(target_id):
                return item
        raise ValueError("plano nao encontrado")

    def _db_delete_billing_plan(self, *, plan_id: object) -> dict:
        if not self._is_db_enabled():
            raise ValueError("Banco nao configurado")
        if plan_id in (None, ""):
            raise ValueError("id obrigatorio")
        schema = self.signup_schema
        target_id = int(plan_id)
        with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {schema}.billing_subscription WHERE plan_id = %(id)s",
                {"id": target_id},
            )
            ref_count = int((cur.fetchone() or [0])[0])
            if ref_count > 0:
                cur.execute(
                    f"UPDATE {schema}.billing_plan SET is_active = FALSE WHERE id = %(id)s",
                    {"id": target_id},
                )
                if cur.rowcount <= 0:
                    raise ValueError("plano nao encontrado")
                conn.commit()
                return {"deleted": False, "inactivated": True, "id": target_id}
            cur.execute(f"DELETE FROM {schema}.billing_plan WHERE id = %(id)s", {"id": target_id})
            if cur.rowcount <= 0:
                raise ValueError("plano nao encontrado")
            conn.commit()
        return {"deleted": True, "inactivated": False, "id": target_id}

    def _db_list_billing_installments(self, *, client_id: int, status: str | None) -> list[dict]:
        if not self._is_db_enabled():
            return []
        schema = self.signup_schema
        sql = f"""
            SELECT i.id, i.subscription_id, i.due_date, i.amount_cents, i.status, i.paid_at, i.created_at
            FROM {schema}.billing_installment i
            JOIN {schema}.billing_subscription s ON s.id = i.subscription_id
            WHERE s.client_id = %(client_id)s
              AND (%(status)s::text IS NULL OR i.status = %(status)s::text)
            ORDER BY i.due_date DESC, i.id DESC
        """
        with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
            cur.execute(sql, {"client_id": client_id, "status": status})
            rows = cur.fetchall()
        return [
            {
                "id": int(row[0]),
                "subscription_id": int(row[1]),
                "due_date": row[2].isoformat() if row[2] else None,
                "amount_cents": int(row[3]),
                "status": row[4],
                "paid_at": row[5].isoformat() if row[5] else None,
                "created_at": row[6].isoformat() if row[6] else None,
            }
            for row in rows
        ]

    def _db_generate_billing_installment(self, *, client_id: int, due_date: str) -> dict:
        if not self._is_db_enabled():
            raise ValueError("Banco nao configurado")
        schema = self.signup_schema
        with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT s.id, p.monthly_price_cents
                FROM {schema}.billing_subscription s
                JOIN {schema}.billing_plan p ON p.id = s.plan_id
                WHERE s.client_id = %(client_id)s
                ORDER BY s.id DESC
                LIMIT 1
                """,
                {"client_id": client_id},
            )
            row = cur.fetchone()
            if not row:
                raise ValueError("assinatura nao encontrada para o cliente")
            subscription_id = int(row[0])
            amount_cents = int(row[1])
            cur.execute(
                f"""
                INSERT INTO {schema}.billing_installment (
                    subscription_id, due_date, amount_cents, status
                )
                VALUES (
                    %(subscription_id)s, %(due_date)s::date, %(amount_cents)s, 'open'
                )
                RETURNING id
                """,
                {"subscription_id": subscription_id, "due_date": due_date, "amount_cents": amount_cents},
            )
            installment_id = int(cur.fetchone()[0])
            conn.commit()
        rows = self._db_list_billing_installments(client_id=client_id, status=None)
        created = next((item for item in rows if int(item["id"]) == installment_id), None)
        return {"installment": created}

    def _db_pay_billing_installment(self, *, installment_id: int) -> dict:
        if not self._is_db_enabled():
            raise ValueError("Banco nao configurado")
        schema = self.signup_schema
        with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {schema}.billing_installment
                   SET status = 'paid',
                       paid_at = NOW()
                 WHERE id = %(id)s
                RETURNING id, subscription_id, due_date, amount_cents, status, paid_at, created_at
                """,
                {"id": installment_id},
            )
            row = cur.fetchone()
            if not row:
                raise ValueError("parcela nao encontrada")
            conn.commit()
        return {
            "id": int(row[0]),
            "subscription_id": int(row[1]),
            "due_date": row[2].isoformat() if row[2] else None,
            "amount_cents": int(row[3]),
            "status": row[4],
            "paid_at": row[5].isoformat() if row[5] else None,
            "created_at": row[6].isoformat() if row[6] else None,
        }

    def _db_get_llm_usage_report(self, *, tenant_id: int, days: int) -> dict:
        if not self._is_db_enabled():
            return {"summary": {}, "by_feature": [], "recent": []}
        schema = self.signup_schema
        with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    COUNT(*) AS calls,
                    COALESCE(SUM(input_tokens), 0) AS input_tokens,
                    COALESCE(SUM(output_tokens), 0) AS output_tokens,
                    COALESCE(SUM(total_tokens), 0) AS total_tokens,
                    COALESCE(SUM(amount_cents), 0) AS amount_cents
                FROM {schema}.llm_usage_meter
                WHERE tenant_id = %(tenant_id)s
                  AND created_at >= NOW() - (%(days)s::text || ' days')::interval
                """,
                {"tenant_id": tenant_id, "days": days},
            )
            summary_row = cur.fetchone() or [0, 0, 0, 0, 0]
            cur.execute(
                f"""
                SELECT
                    feature_code,
                    COALESCE(SUM(total_tokens), 0) AS total_tokens,
                    COALESCE(SUM(amount_cents), 0) AS amount_cents,
                    COUNT(*) AS calls
                FROM {schema}.llm_usage_meter
                WHERE tenant_id = %(tenant_id)s
                  AND created_at >= NOW() - (%(days)s::text || ' days')::interval
                GROUP BY feature_code
                ORDER BY total_tokens DESC, amount_cents DESC
                """,
                {"tenant_id": tenant_id, "days": days},
            )
            by_feature_rows = cur.fetchall()
            cur.execute(
                f"""
                SELECT
                    feature_code,
                    provider_name,
                    model_code,
                    input_tokens,
                    output_tokens,
                    total_tokens,
                    amount_cents,
                    created_at
                FROM {schema}.llm_usage_meter
                WHERE tenant_id = %(tenant_id)s
                  AND created_at >= NOW() - (%(days)s::text || ' days')::interval
                ORDER BY created_at DESC
                LIMIT 20
                """,
                {"tenant_id": tenant_id, "days": days},
            )
            recent_rows = cur.fetchall()
        return {
            "summary": {
                "days": int(days),
                "calls": int(summary_row[0] or 0),
                "input_tokens": int(summary_row[1] or 0),
                "output_tokens": int(summary_row[2] or 0),
                "total_tokens": int(summary_row[3] or 0),
                "amount_cents": int(summary_row[4] or 0),
            },
            "by_feature": [
                {
                    "feature_code": str(row[0] or ""),
                    "total_tokens": int(row[1] or 0),
                    "amount_cents": int(row[2] or 0),
                    "calls": int(row[3] or 0),
                }
                for row in by_feature_rows
            ],
            "recent": [
                {
                    "feature_code": str(row[0] or ""),
                    "provider_name": str(row[1] or ""),
                    "model_code": str(row[2] or ""),
                    "input_tokens": int(row[3] or 0),
                    "output_tokens": int(row[4] or 0),
                    "total_tokens": int(row[5] or 0),
                    "amount_cents": int(row[6] or 0),
                    "created_at": row[7].isoformat() if row[7] else None,
                }
                for row in recent_rows
            ],
        }

    def _is_global_superadmin_context(self, context: dict) -> bool:
        if not isinstance(context, dict):
            return False
        user_id = int(context.get("user_id") or 0)
        tenant_id = int(context.get("tenant_id") or 0)
        return tenant_id <= 0 and self._is_superadmin_user(user_id=user_id)

    def _db_get_user_mfa_status_global(self, *, user_id: int) -> dict:
        if not self._is_db_enabled():
            return {
                "enabled": False,
                "enabled_at": None,
                "has_pending_setup": False,
                "pending_expires_at": None,
            }
        schema = self.signup_schema
        with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    COALESCE(cfg.is_enabled, FALSE) AS enabled,
                    cfg.enabled_at,
                    pnd.expires_at AS pending_expires_at
                FROM {schema}.app_user u
                LEFT JOIN {schema}.user_mfa_config cfg ON cfg.user_id = u.id
                LEFT JOIN {schema}.user_mfa_pending_setup pnd ON pnd.user_id = u.id
                WHERE u.id = %(user_id)s
                LIMIT 1
                """,
                {"user_id": user_id},
            )
            row = cur.fetchone()
        if not row:
            raise ValueError("usuario nao encontrado")
        return {
            "enabled": bool(row[0]),
            "enabled_at": row[1].isoformat() if row[1] else None,
            "has_pending_setup": bool(row[2]),
            "pending_expires_at": row[2].isoformat() if row[2] else None,
        }

    def _db_begin_user_mfa_setup_global(self, *, user_id: int, issuer: str) -> dict:
        if not self._is_db_enabled():
            raise ValueError("Banco nao configurado")
        schema = self.signup_schema
        secret = generate_base32_secret()
        with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
            cur.execute(f"SELECT email FROM {schema}.app_user WHERE id = %(user_id)s LIMIT 1", {"user_id": user_id})
            user_row = cur.fetchone()
            if not user_row:
                raise ValueError("usuario nao encontrado")
            cur.execute(
                f"""
                INSERT INTO {schema}.user_mfa_pending_setup (
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
                """,
                {
                    "user_id": user_id,
                    "pending_secret_ciphertext": encrypt_text(secret),
                },
            )
            expires_row = cur.fetchone()
            conn.commit()
        email = str(user_row[0] or "")
        return {
            "secret": secret,
            "provisioning_uri": provisioning_uri(issuer=issuer or "IAOps Governance", account_name=email, secret=secret),
            "expires_at": expires_row[0].isoformat() if expires_row and expires_row[0] else None,
        }

    def _db_enable_user_mfa_global(self, *, user_id: int, otp_code: str) -> dict:
        if not self._is_db_enabled():
            raise ValueError("Banco nao configurado")
        code = str(otp_code or "").strip()
        if not code:
            raise ValueError("otp_code obrigatorio")
        schema = self.signup_schema
        with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT pending_secret_ciphertext, expires_at
                FROM {schema}.user_mfa_pending_setup
                WHERE user_id = %(user_id)s
                LIMIT 1
                """,
                {"user_id": user_id},
            )
            pending = cur.fetchone()
            if not pending:
                raise ValueError("setup MFA nao iniciado")
            secret_ciphertext = str(pending[0] or "")
            expires_at = pending[1]
            if not secret_ciphertext:
                raise ValueError("segredo MFA pendente invalido")
            if expires_at is None:
                raise ValueError("setup MFA expirado")
            if expires_at.timestamp() < time():
                raise ValueError("setup MFA expirado")
            try:
                secret = decrypt_text(secret_ciphertext)
            except Exception as exc:
                raise ValueError("nao foi possivel validar setup MFA") from exc
            if not verify_totp(secret, code):
                raise ValueError("codigo TOTP invalido")
            cur.execute(
                f"""
                INSERT INTO {schema}.user_mfa_config (
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
                """,
                {"user_id": user_id, "totp_secret_ciphertext": encrypt_text(secret)},
            )
            cur.execute(f"DELETE FROM {schema}.user_mfa_pending_setup WHERE user_id = %(user_id)s", {"user_id": user_id})
            conn.commit()
        return self._db_get_user_mfa_status_global(user_id=user_id)

    def _db_disable_user_mfa_global(self, *, user_id: int, otp_code: str) -> dict:
        if not self._is_db_enabled():
            raise ValueError("Banco nao configurado")
        code = str(otp_code or "").strip()
        if not code:
            raise ValueError("otp_code obrigatorio")
        schema = self.signup_schema
        with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT totp_secret_ciphertext, is_enabled
                FROM {schema}.user_mfa_config
                WHERE user_id = %(user_id)s
                LIMIT 1
                """,
                {"user_id": user_id},
            )
            row = cur.fetchone()
            if not row or not bool(row[1]):
                raise ValueError("MFA nao habilitado para o usuario")
            secret_ciphertext = str(row[0] or "")
            try:
                secret = decrypt_text(secret_ciphertext)
            except Exception as exc:
                raise ValueError("nao foi possivel validar MFA") from exc
            if not verify_totp(secret, code):
                raise ValueError("codigo TOTP invalido")
            cur.execute(
                f"""
                UPDATE {schema}.user_mfa_config
                   SET is_enabled = FALSE,
                       disabled_at = NOW(),
                       updated_at = NOW()
                 WHERE user_id = %(user_id)s
                """,
                {"user_id": user_id},
            )
            cur.execute(f"DELETE FROM {schema}.user_mfa_pending_setup WHERE user_id = %(user_id)s", {"user_id": user_id})
            conn.commit()
        return self._db_get_user_mfa_status_global(user_id=user_id)

    def _can_access_tenant_billing_usage(self, *, user_id: int, client_id: int, tenant_id: int) -> bool:
        if tenant_id <= 0:
            return False
        if not self._is_db_enabled():
            return tenant_id == 10
        if self._is_superadmin_user(user_id=user_id):
            return True
        schema = self.signup_schema
        try:
            with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT 1
                    FROM {schema}.tenant t
                    JOIN {schema}.tenant_user_role tur ON tur.tenant_id = t.id
                    WHERE t.id = %(tenant_id)s
                      AND t.client_id = %(client_id)s
                      AND tur.user_id = %(user_id)s
                      AND tur.role IN ('owner', 'admin')
                    LIMIT 1
                    """,
                    {"tenant_id": tenant_id, "client_id": client_id, "user_id": user_id},
                )
                row = cur.fetchone()
            return bool(row)
        except Exception:
            return False

    def _is_superadmin_user(self, *, user_id: int) -> bool:
        if user_id <= 0:
            return False
        if not self._is_db_enabled():
            return int(user_id) == 100
        schema = self.signup_schema
        try:
            with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
                cur.execute(
                    f"SELECT COALESCE(is_superadmin, FALSE) FROM {schema}.app_user WHERE id = %(user_id)s LIMIT 1",
                    {"user_id": user_id},
                )
                row = cur.fetchone()
            return bool(row and row[0])
        except Exception:
            return False

    def _is_tenant_operational_db(self, *, client_id: int, tenant_id: int) -> bool:
        if client_id <= 0 or tenant_id <= 0:
            return False
        if not self._is_db_enabled():
            return True
        schema = self.signup_schema
        sql = f"""
            SELECT EXISTS (
                SELECT 1
                FROM {schema}.tenant t
                JOIN {schema}.client c ON c.id = t.client_id
                LEFT JOIN {schema}.v_client_billing_delinquency d ON d.client_id = t.client_id
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
                FROM {schema}.tenant t
                JOIN {schema}.client c ON c.id = t.client_id
                WHERE t.id = %(tenant_id)s
                  AND t.client_id = %(client_id)s
                  AND t.status = 'active'
                  AND c.status = 'active'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {schema}.installment inst
                      JOIN {schema}.invoice inv ON inv.id = inst.invoice_id
                      LEFT JOIN {schema}.subscription sub
                        ON sub.client_id = t.client_id
                       AND sub.status = 'active'
                       AND (sub.ends_at IS NULL OR sub.ends_at >= NOW())
                      LEFT JOIN {schema}.plan p ON p.id = sub.plan_id
                      WHERE inv.client_id = t.client_id
                        AND inst.status IN ('open', 'overdue')
                        AND inst.due_date < CURRENT_DATE - COALESCE(p.late_tolerance_days, 0)
                  )
            ) AS ok
        """
        with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
            try:
                cur.execute(sql, {"client_id": client_id, "tenant_id": tenant_id})
            except Exception:
                cur.execute(fallback_sql, {"client_id": client_id, "tenant_id": tenant_id})
            row = cur.fetchone()
        return bool(row and row[0])

    def _db_collect_observability_metrics(self, *, tenant_id: int) -> dict:
        schema = self.signup_schema
        with connect(self._get_db_dsn()) as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM {schema}.incident
                WHERE tenant_id = %(tenant_id)s
                  AND status = 'open'
                """,
                {"tenant_id": tenant_id},
            )
            open_incidents = int((cur.fetchone() or [0])[0] or 0)
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM {schema}.schema_change_event
                WHERE tenant_id = %(tenant_id)s
                  AND severity = 'critical'
                """,
                {"tenant_id": tenant_id},
            )
            critical_events = int((cur.fetchone() or [0])[0] or 0)
            cur.execute(
                f"""
                SELECT
                    COUNT(*) FILTER (WHERE status IN ('queued','running')) AS jobs_inflight,
                    COUNT(*) FILTER (WHERE status = 'failed') AS jobs_failed,
                    COUNT(*) FILTER (WHERE status = 'retrying') AS jobs_retrying,
                    COUNT(*) FILTER (WHERE status = 'dead_letter') AS jobs_dead_letter
                FROM {schema}.async_job_run
                WHERE tenant_id = %(tenant_id)s
                """,
                {"tenant_id": tenant_id},
            )
            jobs = cur.fetchone()
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM {schema}.mcp_call_log
                WHERE tenant_id = %(tenant_id)s
                  AND error_code = 'lgpd_blocked'
                  AND requested_at >= NOW() - INTERVAL '24 hours'
                """,
                {"tenant_id": tenant_id},
            )
            lgpd_blocked_24h = int((cur.fetchone() or [0])[0] or 0)
            cur.execute(
                f"""
                SELECT
                    COALESCE(SUM(total_tokens), 0) AS total_tokens_24h,
                    COALESCE(SUM(amount_cents), 0) AS amount_cents_24h
                FROM {schema}.llm_usage_meter
                WHERE tenant_id = %(tenant_id)s
                  AND created_at >= NOW() - INTERVAL '24 hours'
                """,
                {"tenant_id": tenant_id},
            )
            llm_row = cur.fetchone()
        return {
            "open_incidents": open_incidents,
            "critical_events": critical_events,
            "jobs_inflight": int((jobs[0] or 0) if jobs else 0),
            "jobs_failed": int((jobs[1] or 0) if jobs else 0),
            "jobs_retrying": int((jobs[2] or 0) if jobs else 0),
            "jobs_dead_letter": int((jobs[3] or 0) if jobs else 0),
            "lgpd_blocked_24h": lgpd_blocked_24h,
            "llm_tokens_24h": int((llm_row[0] or 0) if llm_row else 0),
            "llm_amount_cents_24h": int((llm_row[1] or 0) if llm_row else 0),
        }

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
        smtp = IAOpsAPIHandler._smtp_effective_config()
        smtp_host = str(smtp.get("host") or "")
        smtp_port = int(smtp.get("port") or 587)
        smtp_user = str(smtp.get("user") or "")
        smtp_pass = str(smtp.get("password") or "")
        smtp_from = str(smtp.get("from_email") or "")
        smtp_tls = bool(smtp.get("starttls"))

        if not smtp_host or not smtp_from:
            return False, "SMTP nao configurado no ambiente de desenvolvimento."

        confirm_link = IAOpsAPIHandler._build_signup_confirm_link(confirm_token)
        subject = "IAOps Governance - Confirmacao de cadastro"
        body = (
            f"Ola, {trade_name}.\n\n"
            "Seu cadastro foi recebido. Clique no link abaixo para confirmar seu acesso:\n\n"
            f"{confirm_link}\n\n"
            "Caso prefira confirmar manualmente no app, use o token abaixo:\n\n"
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
        except Exception as exc:
            return False, f"Falha no envio por SMTP: {exc}. Token retornado no payload para uso em desenvolvimento."

    def _send_json(self, status: HTTPStatus, payload: dict) -> None:
        body = json.dumps(payload, default=self._json_default).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Client-Id, X-Tenant-Id, X-User-Id, X-Correlation-Id, X-Session-Token")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()
        if status != HTTPStatus.NO_CONTENT:
            self.wfile.write(body)

    def _send_html(self, status: HTTPStatus, html_body: str) -> None:
        body = (
            "<!doctype html><html><head><meta charset='utf-8'>"
            "<meta name='viewport' content='width=device-width,initial-scale=1'>"
            "<title>IAOps</title></head><body style='font-family:Segoe UI,Arial,sans-serif;padding:24px;'>"
            f"{html_body}</body></html>"
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    @staticmethod
    def _json_default(value):
        if isinstance(value, (dt.datetime, dt.date, dt.time)):
            return value.isoformat()
        if isinstance(value, decimal.Decimal):
            return float(value)
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        if isinstance(value, set):
            return list(value)
        raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def run(host: str = "127.0.0.1", port: int = 8000) -> None:
    server = ThreadingHTTPServer((host, port), IAOpsAPIHandler)
    print(f"IAOps API running on http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    run()
