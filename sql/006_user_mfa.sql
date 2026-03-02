CREATE TABLE IF NOT EXISTS iaops_gov.user_mfa_config (
    user_id BIGINT PRIMARY KEY REFERENCES iaops_gov.app_user(id) ON DELETE CASCADE,
    method TEXT NOT NULL DEFAULT 'totp' CHECK (method IN ('totp')),
    totp_secret_ciphertext TEXT,
    is_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    enabled_at TIMESTAMPTZ,
    disabled_at TIMESTAMPTZ,
    last_reset_by_user_id BIGINT REFERENCES iaops_gov.app_user(id),
    last_reset_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS iaops_gov.user_mfa_pending_setup (
    user_id BIGINT PRIMARY KEY REFERENCES iaops_gov.app_user(id) ON DELETE CASCADE,
    pending_secret_ciphertext TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_user_mfa_config_enabled ON iaops_gov.user_mfa_config(is_enabled);
CREATE INDEX IF NOT EXISTS idx_user_mfa_pending_expires ON iaops_gov.user_mfa_pending_setup(expires_at);
