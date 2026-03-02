import unittest

from iaops.mcp.gateway import MCPGateway
from iaops.mcp.repository import InMemoryMCPRepository


class _LgpdRepo(InMemoryMCPRepository):
    def __init__(self, rule_type: str) -> None:
        super().__init__()
        self.rule_type = rule_type

    def execute_safe_sql(self, tenant_id: int, sql_text: str, max_rows: int | None) -> dict:
        _ = tenant_id, sql_text, max_rows
        return {
            "rows": [{"customer_cpf": "12345678901", "total_amount": 100.0}],
            "columns": ["customer_cpf", "total_amount"],
            "execution_ms": 5,
            "applied_masks": [],
        }

    def list_active_lgpd_rules(self, tenant_id: int) -> list[dict]:
        _ = tenant_id
        return [
            {
                "schema_name": "public",
                "table_name": "orders",
                "column_name": "customer_cpf",
                "rule_type": self.rule_type,
                "rule_config": {},
            }
        ]


class MCPGatewayLgpdTests(unittest.TestCase):
    def _payload(self) -> dict:
        return {
            "context": {
                "client_id": 1,
                "tenant_id": 10,
                "user_id": 101,
                "correlation_id": "corr-test",
            },
            "tool": "query.execute_safe_sql",
            "input": {"sql_text": "SELECT customer_cpf, total_amount FROM public.orders"},
        }

    def test_lgpd_block_rule_denies_query(self) -> None:
        gateway = MCPGateway(_LgpdRepo(rule_type="block"))
        result = gateway.handle(self._payload())
        self.assertEqual(result["status"], "denied")
        self.assertEqual((result.get("error") or {}).get("code"), "lgpd_blocked")

    def test_lgpd_mask_rule_masks_field(self) -> None:
        gateway = MCPGateway(_LgpdRepo(rule_type="mask"))
        result = gateway.handle(self._payload())
        self.assertEqual(result["status"], "success")
        rows = ((result.get("data") or {}).get("rows")) or []
        self.assertTrue(rows)
        self.assertEqual(rows[0]["customer_cpf"], "***********")


if __name__ == "__main__":
    unittest.main()

