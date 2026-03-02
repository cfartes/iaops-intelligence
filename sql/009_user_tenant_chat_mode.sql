ALTER TABLE iaops_gov.user_tenant_preference
ADD COLUMN IF NOT EXISTS chat_response_mode TEXT NOT NULL DEFAULT 'executive';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_user_tenant_pref_chat_response_mode'
    ) THEN
        ALTER TABLE iaops_gov.user_tenant_preference
        ADD CONSTRAINT chk_user_tenant_pref_chat_response_mode
        CHECK (chat_response_mode IN ('executive', 'detailed'));
    END IF;
END $$;
