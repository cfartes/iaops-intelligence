CREATE TABLE IF NOT EXISTS iaops_gov.client_signup_pending (
    id BIGSERIAL PRIMARY KEY,
    confirm_token TEXT NOT NULL UNIQUE,
    trade_name TEXT NOT NULL,
    legal_name TEXT NOT NULL,
    cnpj TEXT NOT NULL,
    address_text TEXT NOT NULL,
    phone_contact TEXT NOT NULL,
    email_contact TEXT NOT NULL,
    email_access TEXT NOT NULL,
    email_notification TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    plan_code TEXT NOT NULL,
    language_code TEXT NOT NULL DEFAULT 'pt-BR',
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    confirmed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_client_signup_pending_email
    ON iaops_gov.client_signup_pending (LOWER(email_access));

CREATE INDEX IF NOT EXISTS idx_client_signup_pending_cnpj
    ON iaops_gov.client_signup_pending (cnpj);
