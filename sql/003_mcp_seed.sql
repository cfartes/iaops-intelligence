-- Seed MCP baseline (idempotente)
-- Requer execucao previa de:
-- 001_initial_governance_schema.sql
-- 002_mcp_foundation.sql

DO $$
DECLARE
    v_server_id BIGINT;
BEGIN
    SELECT id
    INTO v_server_id
    FROM iaops_gov.mcp_server
    WHERE name = 'iaops-internal'
      AND is_internal = TRUE
    ORDER BY id
    LIMIT 1;

    IF v_server_id IS NULL THEN
        INSERT INTO iaops_gov.mcp_server (
            client_id,
            name,
            transport_type,
            endpoint_url,
            is_internal,
            is_active
        )
        VALUES (
            NULL,
            'iaops-internal',
            'http',
            'internal://mcp-gateway',
            TRUE,
            TRUE
        )
        RETURNING id INTO v_server_id;
    END IF;

    INSERT INTO iaops_gov.mcp_tool (
        mcp_server_id,
        tool_name,
        tool_version,
        min_role,
        risk_level,
        timeout_ms,
        input_schema_json,
        output_schema_json,
        is_active
    )
    SELECT
        v_server_id,
        t.tool_name,
        'v1',
        t.min_role,
        t.risk_level,
        t.timeout_ms,
        t.input_schema_json,
        t.output_schema_json,
        TRUE
    FROM (
        VALUES
            (
                'inventory.list_tables',
                'viewer',
                'low',
                5000,
                '{"type":"object","properties":{"schema_name":{"type":["string","null"]}}}',
                '{"type":"object","properties":{"tables":{"type":"array"}}}'
            ),
            (
                'inventory.list_columns',
                'viewer',
                'low',
                5000,
                '{"type":"object","required":["schema_name","table_name"],"properties":{"schema_name":{"type":"string"},"table_name":{"type":"string"}}}',
                '{"type":"object","properties":{"columns":{"type":"array"}}}'
            ),
            (
                'inventory.list_tenant_tables',
                'viewer',
                'low',
                5000,
                '{"type":"object","properties":{"data_source_id":{"type":["integer","null"]}}}',
                '{"type":"object","properties":{"tables":{"type":"array"}}}'
            ),
            (
                'inventory.register_table',
                'admin',
                'medium',
                5000,
                '{"type":"object","required":["data_source_id","schema_name","table_name"],"properties":{"data_source_id":{"type":"integer"},"schema_name":{"type":"string"},"table_name":{"type":"string"},"is_active":{"type":"boolean"}}}',
                '{"type":"object","properties":{"table":{"type":"object"}}}'
            ),
            (
                'inventory.delete_table',
                'admin',
                'high',
                5000,
                '{"type":"object","required":["monitored_table_id"],"properties":{"monitored_table_id":{"type":"integer"}}}',
                '{"type":"object","properties":{"result":{"type":"object"}}}'
            ),
            (
                'source.list_catalog',
                'viewer',
                'low',
                5000,
                '{"type":"object","properties":{}}',
                '{"type":"object","properties":{"sources":{"type":"array"}}}'
            ),
            (
                'source.list_tenant',
                'viewer',
                'low',
                5000,
                '{"type":"object","properties":{}}',
                '{"type":"object","properties":{"sources":{"type":"array"}}}'
            ),
            (
                'source.register',
                'admin',
                'medium',
                5000,
                '{"type":"object","required":["source_type","conn_secret_ref"],"properties":{"source_type":{"type":"string"},"conn_secret_ref":{"type":"string"},"is_active":{"type":"boolean"}}}',
                '{"type":"object","properties":{"source":{"type":"object"}}}'
            ),
            (
                'source.update_status',
                'admin',
                'medium',
                5000,
                '{"type":"object","required":["data_source_id","is_active"],"properties":{"data_source_id":{"type":"integer"},"is_active":{"type":"boolean"}}}',
                '{"type":"object","properties":{"source":{"type":"object"}}}'
            ),
            (
                'source.update',
                'admin',
                'medium',
                5000,
                '{"type":"object","required":["data_source_id","source_type","conn_secret_ref"],"properties":{"data_source_id":{"type":"integer"},"source_type":{"type":"string"},"conn_secret_ref":{"type":"string"}}}',
                '{"type":"object","properties":{"source":{"type":"object"}}}'
            ),
            (
                'source.delete',
                'admin',
                'high',
                5000,
                '{"type":"object","required":["data_source_id"],"properties":{"data_source_id":{"type":"integer"}}}',
                '{"type":"object","properties":{"result":{"type":"object"}}}'
            ),
            (
                'query.execute_safe_sql',
                'admin',
                'high',
                15000,
                '{"type":"object","required":["sql_text"],"properties":{"sql_text":{"type":"string"},"explain":{"type":"boolean"}}}',
                '{"type":"object","properties":{"rows":{"type":"array"},"columns":{"type":"array"},"execution_ms":{},"applied_masks":{"type":"array"}}}'
            ),
            (
                'incident.create',
                'admin',
                'medium',
                5000,
                '{"type":"object","required":["title","severity"],"properties":{"title":{"type":"string"},"severity":{"type":"string"},"source_event_id":{"type":["integer","null"]}}}',
                '{"type":"object","properties":{"incident_id":{"type":"integer"},"status":{"type":"string"},"sla_due_at":{"type":"string"}}}'
            ),
            (
                'incident.list',
                'viewer',
                'low',
                5000,
                '{"type":"object","properties":{"status":{"type":["string","null"]},"severity":{"type":["string","null"]},"limit":{"type":"integer"}}}',
                '{"type":"object","properties":{"incidents":{"type":"array"}}}'
            ),
            (
                'incident.update_status',
                'admin',
                'medium',
                5000,
                '{"type":"object","required":["incident_id","new_status"],"properties":{"incident_id":{"type":"integer"},"new_status":{"type":"string"}}}',
                '{"type":"object","properties":{"incident":{"type":"object"}}}'
            ),
            (
                'events.list',
                'viewer',
                'low',
                5000,
                '{"type":"object","properties":{"severity":{"type":["string","null"]},"limit":{"type":"integer"}}}',
                '{"type":"object","properties":{"events":{"type":"array"}}}'
            ),
            (
                'audit.list_calls',
                'admin',
                'medium',
                5000,
                '{"type":"object","properties":{"tool_name":{"type":["string","null"]},"status":{"type":["string","null"]},"correlation_id":{"type":["string","null"]},"limit":{"type":"integer"}}}',
                '{"type":"object","properties":{"calls":{"type":"array"}}}'
            ),
            (
                'security_sql.get_policy',
                'viewer',
                'low',
                5000,
                '{"type":"object","properties":{}}',
                '{"type":"object","properties":{"policy":{"type":"object"}}}'
            ),
            (
                'security_sql.update_policy',
                'admin',
                'medium',
                5000,
                '{"type":"object","properties":{"max_rows":{"type":"integer"},"max_calls_per_minute":{"type":"integer"},"require_masking":{"type":"boolean"},"allowed_schema_patterns":{"type":"array"}}}',
                '{"type":"object","properties":{"policy":{"type":"object"}}}'
            ),
            (
                'ops.get_health_summary',
                'viewer',
                'low',
                5000,
                '{"type":"object","properties":{"window_minutes":{"type":"integer"}}}',
                '{"type":"object","properties":{"open_incidents":{"type":"integer"},"critical_events":{"type":"integer"},"channels_health":{"type":"object"},"last_scan_at":{}}}'
            )
    ) AS t(tool_name, min_role, risk_level, timeout_ms, input_schema_json, output_schema_json)
    WHERE NOT EXISTS (
        SELECT 1
        FROM iaops_gov.mcp_tool mt
        WHERE mt.mcp_server_id = v_server_id
          AND mt.tool_name = t.tool_name
          AND mt.tool_version = 'v1'
    );

    INSERT INTO iaops_gov.tenant_mcp_tool_policy (
        tenant_id,
        mcp_tool_id,
        is_enabled,
        max_rows,
        max_calls_per_minute,
        require_masking,
        allowed_schema_patterns,
        created_at,
        updated_at
    )
    SELECT
        ten.id AS tenant_id,
        mt.id AS mcp_tool_id,
        TRUE AS is_enabled,
        CASE WHEN mt.tool_name = 'query.execute_safe_sql' THEN 200 ELSE 1000 END AS max_rows,
        CASE WHEN mt.tool_name = 'query.execute_safe_sql' THEN 30 ELSE 120 END AS max_calls_per_minute,
        TRUE AS require_masking,
        '["public", "analytics"]'::jsonb AS allowed_schema_patterns,
        NOW(),
        NOW()
    FROM iaops_gov.tenant ten
    JOIN iaops_gov.mcp_tool mt ON mt.mcp_server_id = v_server_id AND mt.is_active = TRUE
    LEFT JOIN iaops_gov.tenant_mcp_tool_policy tmp
      ON tmp.tenant_id = ten.id
     AND tmp.mcp_tool_id = mt.id
    WHERE ten.status = 'active'
      AND tmp.id IS NULL;
END $$;
