import unittest

from iaops.api.server import IAOpsAPIHandler


class SecurityRulesTests(unittest.TestCase):
    def test_password_strength_rejects_weak_password(self) -> None:
        error = IAOpsAPIHandler._validate_password_strength("abc")
        self.assertIsNotNone(error)

    def test_password_strength_accepts_strong_password(self) -> None:
        error = IAOpsAPIHandler._validate_password_strength("Strong@1234")
        self.assertIsNone(error)

    def test_planned_sql_allows_safe_select(self) -> None:
        ok = IAOpsAPIHandler._is_planned_sql_allowed("SELECT id FROM public.orders LIMIT 10")
        self.assertTrue(ok)

    def test_planned_sql_blocks_non_select(self) -> None:
        ok = IAOpsAPIHandler._is_planned_sql_allowed("UPDATE public.orders SET total_amount = 1")
        self.assertFalse(ok)

    def test_planned_sql_blocks_multi_statement(self) -> None:
        ok = IAOpsAPIHandler._is_planned_sql_allowed("SELECT * FROM public.orders; SELECT 1")
        self.assertFalse(ok)


if __name__ == "__main__":
    unittest.main()

