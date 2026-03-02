ALTER TABLE iaops_gov.app_user
ADD COLUMN IF NOT EXISTS is_superadmin BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_app_user_superadmin
ON iaops_gov.app_user(is_superadmin);
