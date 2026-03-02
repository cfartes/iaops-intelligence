-- Consolidacao de consistencia de billing entre modelo legado e billing_* novo
CREATE SCHEMA IF NOT EXISTS iaops_gov;

CREATE OR REPLACE VIEW iaops_gov.v_client_billing_delinquency AS
SELECT
    x.client_id,
    MIN(x.overdue_since) AS overdue_since
FROM (
    -- Modelo legado
    SELECT
        inv.client_id,
        inst.due_date::date AS overdue_since
    FROM iaops_gov.installment inst
    JOIN iaops_gov.invoice inv ON inv.id = inst.invoice_id
    LEFT JOIN iaops_gov.subscription sub
      ON sub.client_id = inv.client_id
     AND sub.status = 'active'
     AND (sub.ends_at IS NULL OR sub.ends_at >= NOW())
    LEFT JOIN iaops_gov.plan p ON p.id = sub.plan_id
    WHERE inst.status IN ('open', 'overdue')
      AND inst.due_date < CURRENT_DATE - COALESCE(p.late_tolerance_days, 0)

    UNION ALL

    -- Modelo novo billing_*
    SELECT
        bs.client_id,
        bi.due_date::date AS overdue_since
    FROM iaops_gov.billing_installment bi
    JOIN iaops_gov.billing_subscription bs ON bs.id = bi.subscription_id
    WHERE bs.status = 'active'
      AND bi.status IN ('open', 'overdue')
      AND bi.due_date < CURRENT_DATE - COALESCE(bs.tolerance_days, 5)
) x
GROUP BY x.client_id;

