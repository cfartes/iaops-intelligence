const DEFAULT_HEADERS = {
  "Content-Type": "application/json",
  "X-Client-Id": "1",
  "X-Tenant-Id": "10",
  "X-User-Id": "100",
};

async function parseResponse(response) {
  const data = await response.json();
  if (!response.ok || data.status !== "success") {
    const message = data?.error?.message || "Falha na chamada MCP";
    throw new Error(message);
  }
  return data.data;
}

export async function listTables(schemaName) {
  const query = schemaName ? `?schema_name=${encodeURIComponent(schemaName)}` : "";
  const response = await fetch(`/api/inventory/tables${query}`, {
    method: "GET",
    headers: DEFAULT_HEADERS,
  });
  return parseResponse(response);
}

export async function listColumns(schemaName, tableName) {
  const query = `?schema_name=${encodeURIComponent(schemaName)}&table_name=${encodeURIComponent(tableName)}`;
  const response = await fetch(`/api/inventory/columns${query}`, {
    method: "GET",
    headers: DEFAULT_HEADERS,
  });
  return parseResponse(response);
}

export async function listOnboardingMonitoredTables(dataSourceId) {
  const query = dataSourceId ? `?data_source_id=${encodeURIComponent(dataSourceId)}` : "";
  const response = await fetch(`/api/onboarding/monitored-tables${query}`, {
    method: "GET",
    headers: DEFAULT_HEADERS,
  });
  return parseResponse(response);
}

export async function registerMonitoredTable(payload) {
  const response = await fetch("/api/onboarding/monitored-tables", {
    method: "POST",
    headers: DEFAULT_HEADERS,
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function deleteMonitoredTable(payload) {
  const response = await fetch("/api/onboarding/monitored-tables/delete", {
    method: "POST",
    headers: DEFAULT_HEADERS,
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function listOnboardingMonitoredColumns(monitoredTableId) {
  const query = monitoredTableId ? `?monitored_table_id=${encodeURIComponent(monitoredTableId)}` : "";
  const response = await fetch(`/api/onboarding/monitored-columns${query}`, {
    method: "GET",
    headers: DEFAULT_HEADERS,
  });
  return parseResponse(response);
}

export async function registerMonitoredColumn(payload) {
  const response = await fetch("/api/onboarding/monitored-columns", {
    method: "POST",
    headers: DEFAULT_HEADERS,
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function deleteMonitoredColumn(payload) {
  const response = await fetch("/api/onboarding/monitored-columns/delete", {
    method: "POST",
    headers: DEFAULT_HEADERS,
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function listSourceCatalog() {
  const response = await fetch("/api/data-sources/catalog", {
    method: "GET",
    headers: DEFAULT_HEADERS,
  });
  return parseResponse(response);
}

export async function listTenantDataSources() {
  const response = await fetch("/api/data-sources", {
    method: "GET",
    headers: DEFAULT_HEADERS,
  });
  return parseResponse(response);
}

export async function registerDataSource(payload) {
  const response = await fetch("/api/data-sources", {
    method: "POST",
    headers: DEFAULT_HEADERS,
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function updateDataSourceStatus(payload) {
  const response = await fetch("/api/data-sources/status", {
    method: "POST",
    headers: DEFAULT_HEADERS,
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function updateDataSource(payload) {
  const response = await fetch("/api/data-sources/update", {
    method: "POST",
    headers: DEFAULT_HEADERS,
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function deleteDataSource(payload) {
  const response = await fetch("/api/data-sources/delete", {
    method: "POST",
    headers: DEFAULT_HEADERS,
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function createIncident(payload) {
  const response = await fetch("/api/incidents", {
    method: "POST",
    headers: DEFAULT_HEADERS,
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
    headers: DEFAULT_HEADERS,
  });
  return parseResponse(response);
}

export async function updateIncidentStatus(payload) {
  const response = await fetch("/api/incidents/status", {
    method: "POST",
    headers: DEFAULT_HEADERS,
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
    headers: DEFAULT_HEADERS,
  });
  return parseResponse(response);
}

export async function getOperationHealth(windowMinutes = 60) {
  const response = await fetch(`/api/operation/health?window_minutes=${encodeURIComponent(windowMinutes)}`, {
    method: "GET",
    headers: DEFAULT_HEADERS,
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
    headers: DEFAULT_HEADERS,
  });
  return parseResponse(response);
}

export async function getSqlSecurityPolicy() {
  const response = await fetch("/api/security-sql/policy", {
    method: "GET",
    headers: DEFAULT_HEADERS,
  });
  return parseResponse(response);
}

export async function updateSqlSecurityPolicy(payload) {
  const response = await fetch("/api/security-sql/policy", {
    method: "POST",
    headers: DEFAULT_HEADERS,
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function runChatBiQuery(payload) {
  const response = await fetch("/api/chat-bi/query", {
    method: "POST",
    headers: DEFAULT_HEADERS,
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function listAccessUsers() {
  const response = await fetch("/api/access/users", {
    method: "GET",
    headers: DEFAULT_HEADERS,
  });
  return parseResponse(response);
}

export async function getMfaStatus() {
  const response = await fetch("/api/security/mfa/status", {
    method: "GET",
    headers: DEFAULT_HEADERS,
  });
  return parseResponse(response);
}

export async function beginMfaSetup(payload = {}) {
  const response = await fetch("/api/security/mfa/setup", {
    method: "POST",
    headers: DEFAULT_HEADERS,
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function enableMfa(payload) {
  const response = await fetch("/api/security/mfa/enable", {
    method: "POST",
    headers: DEFAULT_HEADERS,
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function disableMfa(payload) {
  const response = await fetch("/api/security/mfa/disable", {
    method: "POST",
    headers: DEFAULT_HEADERS,
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function adminResetMfa(payload) {
  const response = await fetch("/api/security/mfa/admin-reset", {
    method: "POST",
    headers: DEFAULT_HEADERS,
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function listClientTenants() {
  const response = await fetch("/api/tenants", {
    method: "GET",
    headers: DEFAULT_HEADERS,
  });
  return parseResponse(response);
}

export async function getTenantLimits() {
  const response = await fetch("/api/tenants/limits", {
    method: "GET",
    headers: DEFAULT_HEADERS,
  });
  return parseResponse(response);
}

export async function createTenant(payload) {
  const response = await fetch("/api/tenants", {
    method: "POST",
    headers: DEFAULT_HEADERS,
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function updateTenantStatus(payload) {
  const response = await fetch("/api/tenants/status", {
    method: "POST",
    headers: DEFAULT_HEADERS,
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function listAdminLlmProviders() {
  const response = await fetch("/api/admin/llm/providers", {
    method: "GET",
    headers: DEFAULT_HEADERS,
  });
  return parseResponse(response);
}

export async function getAdminLlmConfig() {
  const response = await fetch("/api/admin/llm/config", {
    method: "GET",
    headers: DEFAULT_HEADERS,
  });
  return parseResponse(response);
}

export async function updateAdminLlmConfig(payload) {
  const response = await fetch("/api/admin/llm/config", {
    method: "POST",
    headers: DEFAULT_HEADERS,
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function listTenantLlmProviders() {
  const response = await fetch("/api/tenant-llm/providers", {
    method: "GET",
    headers: DEFAULT_HEADERS,
  });
  return parseResponse(response);
}

export async function getTenantLlmConfig() {
  const response = await fetch("/api/tenant-llm/config", {
    method: "GET",
    headers: DEFAULT_HEADERS,
  });
  return parseResponse(response);
}

export async function updateTenantLlmConfig(payload) {
  const response = await fetch("/api/tenant-llm/config", {
    method: "POST",
    headers: DEFAULT_HEADERS,
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function channelListUserTenants(payload) {
  const response = await fetch("/api/channel/tenants/list", {
    method: "POST",
    headers: DEFAULT_HEADERS,
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function channelSelectTenant(payload) {
  const response = await fetch("/api/channel/tenant/select", {
    method: "POST",
    headers: DEFAULT_HEADERS,
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}

export async function channelGetActiveTenant(payload) {
  const response = await fetch("/api/channel/tenant/active", {
    method: "POST",
    headers: DEFAULT_HEADERS,
    body: JSON.stringify(payload),
  });
  return parseResponse(response);
}
