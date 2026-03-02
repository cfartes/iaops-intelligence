const AUTH_STORAGE_KEY = "iaops_auth_context_v1";
const DEFAULT_HEADERS = {
  "Content-Type": "application/json",
};

function readStoredAuthContext() {
  try {
    const raw = window.localStorage.getItem(AUTH_STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return null;
    if (!parsed.client_id || !parsed.tenant_id || !parsed.user_id) return null;
    return {
      client_id: Number(parsed.client_id),
      tenant_id: Number(parsed.tenant_id),
      user_id: Number(parsed.user_id),
      email: parsed.email || "",
      full_name: parsed.full_name || "",
      role: parsed.role || "",
      tenant_name: parsed.tenant_name || "",
      session_token: parsed.session_token || "",
      refresh_token: parsed.refresh_token || "",
      session_expires_at_epoch: parsed.session_expires_at_epoch || null,
      refresh_expires_at_epoch: parsed.refresh_expires_at_epoch || null,
    };
  } catch (_) {
    return null;
  }
}

function writeStoredAuthContext(value) {
  if (!value) {
    window.localStorage.removeItem(AUTH_STORAGE_KEY);
    return;
  }
  window.localStorage.setItem(AUTH_STORAGE_KEY, JSON.stringify(value));
}

export function getAuthContext() {
  return readStoredAuthContext();
}

export function setAuthContext(value) {
  writeStoredAuthContext(value);
}

export function clearAuthContext() {
  writeStoredAuthContext(null);
}

function buildHeaders(overrides = {}) {
  const auth = readStoredAuthContext();
  const authHeaders = auth
    ? {
        "X-Client-Id": String(auth.client_id),
        "X-Tenant-Id": String(auth.tenant_id),
        "X-User-Id": String(auth.user_id),
        ...(auth.session_token ? { "X-Session-Token": String(auth.session_token) } : {}),
      }
    : {
        "X-Client-Id": "1",
        "X-Tenant-Id": "10",
        "X-User-Id": "100",
      };
  return { ...DEFAULT_HEADERS, ...authHeaders, ...overrides };
}

async function parseResponse(response) {
  const data = await response.json();
  if (!response.ok || data.status !== "success") {
    const message = data?.error?.message || "Falha na chamada MCP";
    const error = new Error(message);
    error.code = data?.error?.code || "mcp_error";
    error.details = data?.data || {};
    throw error;
  }
  return data.data;
}

async function parseWebhookResponse(response) {
  const data = await response.json();
  if (!response.ok || data.ok !== true) {
    const message = data?.error || data?.reply_text || "Falha no webhook de canal";
    throw new Error(message);
  }
  return data;
}

export async function listTables(schemaName) {
  const query = schemaName ? `?schema_name=${encodeURIComponent(schemaName)}` : "";
  const response = await fetch(`/api/inventory/tables${query}`, {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function listColumns(schemaName, tableName) {
  const query = `?schema_name=${encodeURIComponent(schemaName)}&table_name=${encodeURIComponent(tableName)}`;
  const response = await fetch(`/api/inventory/columns${query}`, {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function listOnboardingMonitoredTables(dataSourceId) {
  const query = dataSourceId ? `?data_source_id=${encodeURIComponent(dataSourceId)}` : "";
  const response = await fetch(`/api/onboarding/monitored-tables${query}`, {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function registerMonitoredTable(payload) {
  const response = await fetch("/api/onboarding/monitored-tables", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function deleteMonitoredTable(payload) {
  const response = await fetch("/api/onboarding/monitored-tables/delete", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function listOnboardingMonitoredColumns(monitoredTableId) {
  const query = monitoredTableId ? `?monitored_table_id=${encodeURIComponent(monitoredTableId)}` : "";
  const response = await fetch(`/api/onboarding/monitored-columns${query}`, {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function registerMonitoredColumn(payload) {
  const response = await fetch("/api/onboarding/monitored-columns", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function deleteMonitoredColumn(payload) {
  const response = await fetch("/api/onboarding/monitored-columns/delete", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function listSourceCatalog() {
  const response = await fetch("/api/data-sources/catalog", {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function listTenantDataSources() {
  const response = await fetch("/api/data-sources", {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function registerDataSource(payload) {
  const response = await fetch("/api/data-sources", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function updateDataSourceStatus(payload) {
  const response = await fetch("/api/data-sources/status", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function updateDataSource(payload) {
  const response = await fetch("/api/data-sources/update", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function deleteDataSource(payload) {
  const response = await fetch("/api/data-sources/delete", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function testDataSourceConnection(payload) {
  const response = await fetch("/api/data-sources/test-connection", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function createIncident(payload) {
  const response = await fetch("/api/incidents", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function listIncidents(filters = {}) {
  const query = new URLSearchParams();
  if (filters.status) query.set("status", filters.status);
  if (filters.severity) query.set("severity", filters.severity);
  if (filters.limit) query.set("limit", String(filters.limit));
  const response = await fetch(`/api/incidents?${query.toString()}`, {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function updateIncidentStatus(payload) {
  const response = await fetch("/api/incidents/status", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function listEvents(filters = {}) {
  const query = new URLSearchParams();
  if (filters.severity) query.set("severity", filters.severity);
  if (filters.limit) query.set("limit", String(filters.limit));
  const response = await fetch(`/api/events?${query.toString()}`, {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function getOperationHealth(windowMinutes = 60) {
  const response = await fetch(`/api/operation/health?window_minutes=${encodeURIComponent(windowMinutes)}`, {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function listAuditCalls(filters = {}) {
  const query = new URLSearchParams();
  if (filters.tool_name) query.set("tool_name", filters.tool_name);
  if (filters.status) query.set("status", filters.status);
  if (filters.correlation_id) query.set("correlation_id", filters.correlation_id);
  if (filters.limit) query.set("limit", String(filters.limit));
  const response = await fetch(`/api/audit/calls?${query.toString()}`, {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function getSqlSecurityPolicy() {
  const response = await fetch("/api/security-sql/policy", {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function updateSqlSecurityPolicy(payload) {
  const response = await fetch("/api/security-sql/policy", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function runChatBiQuery(payload) {
  const response = await fetch("/api/chat-bi/query", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function listAccessUsers() {
  const response = await fetch("/api/access/users", {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function getMfaStatus() {
  const response = await fetch("/api/security/mfa/status", {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function beginMfaSetup(payload = {}) {
  const response = await fetch("/api/security/mfa/setup", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function enableMfa(payload) {
  const response = await fetch("/api/security/mfa/enable", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function disableMfa(payload) {
  const response = await fetch("/api/security/mfa/disable", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function adminResetMfa(payload) {
  const response = await fetch("/api/security/mfa/admin-reset", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function listClientTenants() {
  const response = await fetch("/api/tenants", {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function getTenantLimits() {
  const response = await fetch("/api/tenants/limits", {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function createTenant(payload) {
  const response = await fetch("/api/tenants", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function updateTenantStatus(payload) {
  const response = await fetch("/api/tenants/status", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function listAdminLlmProviders() {
  const response = await fetch("/api/admin/llm/providers", {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function getAdminLlmConfig() {
  const response = await fetch("/api/admin/llm/config", {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function updateAdminLlmConfig(payload) {
  const response = await fetch("/api/admin/llm/config", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function listTenantLlmProviders() {
  const response = await fetch("/api/tenant-llm/providers", {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function getTenantLlmConfig() {
  const response = await fetch("/api/tenant-llm/config", {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function updateTenantLlmConfig(payload) {
  const response = await fetch("/api/tenant-llm/config", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function getUserTenantPreference() {
  const response = await fetch("/api/preferences/user-tenant", {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function updateUserTenantPreference(payload) {
  const response = await fetch("/api/preferences/user-tenant", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function getSetupProgress(tenantId) {
  const headers = tenantId ? buildHeaders({ "X-Tenant-Id": String(tenantId) }) : buildHeaders();
  const response = await fetch("/api/setup/progress", {
    method: "GET",
    headers,
  });
  return parseResponse(response);
}

export async function upsertSetupProgress(payload, tenantId) {
  const headers = tenantId ? buildHeaders({ "X-Tenant-Id": String(tenantId) }) : buildHeaders();
  const response = await fetch("/api/setup/progress", {
    method: "POST",
    headers,
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function signupClient(payload) {
  const response = await fetch("/api/auth/signup", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function confirmClientSignup(payload) {
  const response = await fetch("/api/auth/confirm", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function requestPasswordReset(payload) {
  const response = await fetch("/api/auth/password/request", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function confirmPasswordReset(payload) {
  const response = await fetch("/api/auth/password/reset", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function loginClient(payload) {
  const response = await fetch("/api/auth/login", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function verifyLoginMfa(payload) {
  const response = await fetch("/api/auth/mfa/verify", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function refreshAuthSession(payload) {
  const response = await fetch("/api/auth/session/refresh", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function logoutSession(payload = {}) {
  const response = await fetch("/api/auth/logout", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function listAuthSessions() {
  const response = await fetch("/api/auth/sessions", {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function revokeAuthSession(payload) {
  const response = await fetch("/api/auth/sessions/revoke", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function channelListUserTenants(payload) {
  const response = await fetch("/api/channel/tenants/list", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function channelSelectTenant(payload) {
  const response = await fetch("/api/channel/tenant/select", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function channelGetActiveTenant(payload) {
  const response = await fetch("/api/channel/tenant/active", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function channelWebhookTelegram(payload) {
  const response = await fetch("/api/channel/webhook/telegram", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseWebhookResponse(response);
}

export async function channelWebhookWhatsapp(payload) {
  const response = await fetch("/api/channel/webhook/whatsapp", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseWebhookResponse(response);
}

export async function getLgpdPolicy() {
  const response = await fetch("/api/lgpd/policy", {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function upsertLgpdPolicy(payload) {
  const response = await fetch("/api/lgpd/policy", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function listLgpdRules() {
  const response = await fetch("/api/lgpd/rules", {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function upsertLgpdRule(payload) {
  const response = await fetch("/api/lgpd/rules", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function listLgpdDsr(status) {
  const query = status ? `?status=${encodeURIComponent(status)}` : "";
  const response = await fetch(`/api/lgpd/dsr${query}`, {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function openLgpdDsr(payload) {
  const response = await fetch("/api/lgpd/dsr", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function resolveLgpdDsr(payload) {
  const response = await fetch("/api/lgpd/dsr/resolve", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function listBillingPlans() {
  const response = await fetch("/api/billing/plans", {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function getBillingSubscription() {
  const response = await fetch("/api/billing/subscription", {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function upsertBillingSubscription(payload) {
  const response = await fetch("/api/billing/subscription", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function listBillingInstallments(status) {
  const query = status ? `?status=${encodeURIComponent(status)}` : "";
  const response = await fetch(`/api/billing/installments${query}`, {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function getBillingLlmUsage(days = 30, tenantId) {
  const query = new URLSearchParams();
  query.set("days", String(days));
  if (tenantId != null && tenantId !== "") query.set("tenant_id", String(tenantId));
  const response = await fetch(`/api/billing/llm-usage?${query.toString()}`, {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}

export async function generateBillingInstallment(payload) {
  const response = await fetch("/api/billing/installments/generate", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function payBillingInstallment(payload) {
  const response = await fetch("/api/billing/installments/pay", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function enqueueIngestionJob(payload = {}) {
  const response = await fetch("/api/jobs/ingest-metadata", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function enqueueRagRebuildJob(payload = {}) {
  const response = await fetch("/api/jobs/rag-rebuild", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function listAsyncJobs(limit = 50, offset = 0) {
  const response = await fetch(
    `/api/jobs?limit=${encodeURIComponent(limit)}&offset=${encodeURIComponent(offset)}`,
    {
    method: "GET",
    headers: buildHeaders(),
    },
  );
  return parseResponse(response);
}

export async function retryAsyncJob(payload) {
  const response = await fetch("/api/jobs/retry", {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function getObservabilityMetrics() {
  const response = await fetch("/api/observability/metrics", {
    method: "GET",
    headers: buildHeaders(),
  });
  return parseResponse(response);
}
