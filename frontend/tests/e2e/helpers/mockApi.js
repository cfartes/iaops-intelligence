export async function mockDefaultApi(page) {
  await page.route("**/api/**", async (route) => {
    const req = route.request();
    const url = new URL(req.url());
    const path = url.pathname;
    const method = req.method().toUpperCase();

    const json = (status, body) =>
      route.fulfill({
        status,
        contentType: "application/json",
        body: JSON.stringify(body),
      });

    if (path === "/api/auth/login" && method === "POST") {
      return json(200, {
        status: "success",
        tool: "auth.login",
        correlation_id: "test-corr",
        data: {
          mfa_required: false,
          auth_context: { client_id: 1, tenant_id: 10, user_id: 100 },
          profile: { email: "owner@test.local", full_name: "Owner", role: "owner", tenant_name: "Tenant Demo" },
          session: {
            session_token: "sess-1",
            refresh_token: "ref-1",
            session_expires_at_epoch: 9999999999,
            refresh_expires_at_epoch: 9999999999,
          },
        },
        error: null,
      });
    }

    if (path === "/api/auth/refresh" && method === "POST") {
      return json(200, {
        status: "success",
        tool: "auth.refresh",
        correlation_id: "test-corr",
        data: {
          session: {
            session_token: "sess-1",
            refresh_token: "ref-1",
            session_expires_at_epoch: 9999999999,
            refresh_expires_at_epoch: 9999999999,
          },
          auth_context: { client_id: 1, tenant_id: 10, user_id: 100 },
          profile: { email: "owner@test.local", full_name: "Owner", role: "owner", tenant_name: "Tenant Demo" },
        },
        error: null,
      });
    }

    if (path === "/api/setup/progress" && method === "GET") {
      return json(200, { status: "success", data: { progress: null }, error: null });
    }

    if (path === "/api/onboarding/tenant-data-sources" && method === "GET") {
      return json(200, { status: "success", data: { sources: [] }, error: null });
    }

    if (path === "/api/onboarding/monitored-tables" && method === "GET") {
      return json(200, { status: "success", data: { tables: [] }, error: null });
    }

    if (path === "/api/access/users" && method === "GET") {
      return json(200, { status: "success", data: { users: [] }, error: null });
    }

    if (path === "/api/tenant-llm/config" && method === "GET") {
      return json(200, {
        status: "success",
        data: {
          config: {
            use_app_default_llm: true,
            provider_name: "openai",
            model_code: "gpt-4.1-mini",
            endpoint_url: "https://api.openai.com/v1",
          },
        },
        error: null,
      });
    }

    if (path === "/api/security/mfa/status" && method === "GET") {
      return json(200, { status: "success", data: { mfa: { enabled: false, pending_setup: false } }, error: null });
    }

    if (path === "/api/operation/health" && method === "GET") {
      return json(200, {
        status: "success",
        data: { open_incidents: 0, critical_events: 0, channels_health: {}, last_scan_at: null },
        error: null,
      });
    }

    if (path === "/api/jobs" && method === "GET") {
      return json(200, { status: "success", data: { jobs: [], limit: 20, offset: 0 }, error: null });
    }

    if (path === "/api/observability/metrics" && method === "GET") {
      return json(200, {
        status: "success",
        data: {
          active_sessions: 1,
          jobs_inflight: 0,
          jobs_failed: 0,
          jobs_retrying: 0,
          jobs_dead_letter: 0,
          lgpd_blocked_24h: 0,
          llm_tokens_24h: 0,
          llm_amount_cents_24h: 0,
          open_incidents: 0,
          critical_events: 0,
        },
        error: null,
      });
    }

    if (path === "/api/billing/plans" && method === "GET") {
      return json(200, {
        status: "success",
        data: { plans: [{ id: 1, code: "starter", name: "Starter", max_tenants: 1, max_users: 5, monthly_price_cents: 29900 }] },
        error: null,
      });
    }

    if (path === "/api/billing/subscription" && method === "GET") {
      return json(200, { status: "success", data: { subscription: { plan_code: "starter", plan_name: "Starter", tolerance_days: 5, status: "active" } }, error: null });
    }

    if (path === "/api/tenants" && method === "GET") {
      return json(200, { status: "success", data: { tenants: [{ id: 10, name: "Tenant Demo", status: "active" }] }, error: null });
    }

    if (path === "/api/billing/llm-usage" && method === "GET") {
      return json(200, {
        status: "success",
        data: {
          summary: { days: 30, calls: 2, input_tokens: 1200, output_tokens: 500, total_tokens: 1700, amount_cents: 85 },
          by_feature: [{ feature_code: "chat_bi_planner", calls: 2, total_tokens: 1700, amount_cents: 85 }],
          recent: [],
        },
        error: null,
      });
    }

    if (path === "/api/chat-bi/query" && method === "POST") {
      const body = JSON.parse(req.postData() || "{}");
      if (String(body.question_text || "").toLowerCase().includes("cpf")) {
        return json(403, {
          status: "denied",
          tool: "chat-bi.query",
          data: {},
          error: { code: "tenant_blocked", message: "Tenant bloqueado por inadimplencia ou inatividade." },
        });
      }
      return json(200, {
        status: "success",
        tool: "chat-bi.query",
        data: {
          chat_response_mode: "executive",
          result: { columns: ["total"], rows: [{ total: 7 }] },
        },
        error: null,
      });
    }

    return json(200, { status: "success", data: {}, error: null });
  });
}

