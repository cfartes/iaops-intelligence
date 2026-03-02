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
