#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

DOMAIN_DEFAULT="iaops.nexusdataanalytics.tech"
SUPERADMIN_EMAIL="superadmin@iaops.local"
SUPERADMIN_PASSWORD="AndradeFartes@2026!"

echo "[prod] Inicio do deploy em $(date -u +"%Y-%m-%dT%H:%M:%SZ")"

if ! command -v docker >/dev/null 2>&1; then
  echo "[prod] ERRO: docker nao encontrado."
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "[prod] ERRO: docker compose plugin nao encontrado."
  exit 1
fi

if [ ! -f ".env.prod" ]; then
  cp .env.prod.example .env.prod
  echo "[prod] .env.prod criado a partir de .env.prod.example."
  echo "[prod] Ajuste credenciais antes de repetir este script."
fi

if ! grep -q '^IAOPS_DOMAIN=' .env.prod; then
  echo "IAOPS_DOMAIN=${DOMAIN_DEFAULT}" >> .env.prod
fi

DOMAIN_EFFECTIVE="$(grep -E '^IAOPS_DOMAIN=' .env.prod | tail -n1 | cut -d'=' -f2- | tr -d '\r' || true)"
if [ -z "${DOMAIN_EFFECTIVE}" ]; then
  DOMAIN_EFFECTIVE="${DOMAIN_DEFAULT}"
fi
FRONTEND_BIND_PORT="$(grep -E '^IAOPS_FRONTEND_BIND_PORT=' .env.prod | tail -n1 | cut -d'=' -f2- | tr -d '\r' || true)"
if [ -z "${FRONTEND_BIND_PORT}" ]; then
  FRONTEND_BIND_PORT="18080"
fi
USE_EMBEDDED_EDGE="$(grep -E '^IAOPS_USE_EMBEDDED_EDGE=' .env.prod | tail -n1 | cut -d'=' -f2- | tr -d '\r' || true)"
if [ -z "${USE_EMBEDDED_EDGE}" ]; then
  USE_EMBEDDED_EDGE="0"
fi

COMPOSE_CMD=(docker compose --env-file .env.prod -f docker-compose.yml -f docker-compose.prod.yml)
if [ "${USE_EMBEDDED_EDGE}" = "1" ]; then
  COMPOSE_CMD+=(--profile edge)
fi

echo "[prod] Subindo servicos..."
"${COMPOSE_CMD[@]}" up -d --build

if [ "${USE_EMBEDDED_EDGE}" = "1" ]; then
  HEALTH_URL="http://127.0.0.1/health"
  echo "[prod] Modo edge embutido (Caddy). Health: ${HEALTH_URL}"
else
  HEALTH_URL="http://127.0.0.1:${FRONTEND_BIND_PORT}/health"
  echo "[prod] Modo proxy externo. Health: ${HEALTH_URL}"
fi
echo "[prod] Aguardando health..."
TRIES=40
for ((i=1; i<=TRIES; i++)); do
  if curl -fsS "${HEALTH_URL}" >/dev/null 2>&1; then
    echo "[prod] Health OK."
    break
  fi
  if [ "$i" -eq "$TRIES" ]; then
    echo "[prod] ERRO: /health indisponivel apos ${TRIES} tentativas."
    "${COMPOSE_CMD[@]}" ps
    exit 1
  fi
  sleep 3
done

echo "[prod] Aplicando politica de acesso (somente superadmin ativo)..."
"${COMPOSE_CMD[@]}" exec -T api python - <<PY
import os
import secrets
from hashlib import pbkdf2_hmac

import psycopg

schema = os.getenv("IAOPS_DB_SCHEMA", "iaops_gov")
dsn = os.getenv("IAOPS_DB_DSN")
if not dsn:
    raise SystemExit("IAOPS_DB_DSN nao configurado no container api.")

email = "${SUPERADMIN_EMAIL}"
password = "${SUPERADMIN_PASSWORD}"

def encode_password(raw: str) -> str:
    salt = secrets.token_hex(8)
    digest = pbkdf2_hmac("sha256", raw.encode("utf-8"), salt.encode("utf-8"), 240000).hex()
    return f"pbkdf2_sha256$240000${salt}${digest}"

with psycopg.connect(dsn) as conn, conn.cursor() as cur:
    pwd_hash = encode_password(password)
    cur.execute(
        f"SELECT id, client_id FROM {schema}.app_user WHERE LOWER(email)=LOWER(%s) LIMIT 1",
        (email,),
    )
    row = cur.fetchone()

    if row:
        user_id = int(row[0])
        client_id = int(row[1])
        cur.execute(
            f"""
            UPDATE {schema}.app_user
               SET full_name = 'SUPERADMIN',
                   password_hash = %s,
                   is_active = TRUE,
                   is_superadmin = TRUE
             WHERE id = %s
            """,
            (pwd_hash, user_id),
        )
    else:
        cur.execute(f"SELECT id FROM {schema}.client ORDER BY id LIMIT 1")
        client_row = cur.fetchone()
        if client_row:
            client_id = int(client_row[0])
        else:
            cur.execute(
                f"""
                INSERT INTO {schema}.client (
                    fantasy_name, legal_name, cnpj, address_text, contact_phone,
                    contact_email, access_email, notification_email, password_hash,
                    email_confirmed_at, status
                ) VALUES (
                    'IAOps Platform', 'IAOps Platform', '00000000000000', '-', '-',
                    %s, %s, %s, %s, NOW(), 'active'
                )
                RETURNING id
                """,
                (email, email, email, pwd_hash),
            )
            client_id = int(cur.fetchone()[0])

        cur.execute(
            f"""
            INSERT INTO {schema}.app_user (client_id, email, full_name, password_hash, is_active, is_superadmin)
            VALUES (%s, %s, 'SUPERADMIN', %s, TRUE, TRUE)
            """,
            (client_id, email, pwd_hash),
        )

    cur.execute(
        f"""
        UPDATE {schema}.app_user
           SET is_active = FALSE,
               is_superadmin = FALSE
         WHERE LOWER(email) <> LOWER(%s)
        """,
        (email,),
    )
    conn.commit()

print("superadmin policy aplicada com sucesso")
PY

echo "[prod] Estado dos servicos:"
"${COMPOSE_CMD[@]}" ps

echo "[prod] Deploy concluido."
echo "[prod] URL: https://${DOMAIN_EFFECTIVE}"
echo "[prod] Usuario superadmin: ${SUPERADMIN_EMAIL}"
if [ "${USE_EMBEDDED_EDGE}" != "1" ]; then
  echo "[prod] Configure seu proxy central para encaminhar ${DOMAIN_EFFECTIVE} -> http://127.0.0.1:${FRONTEND_BIND_PORT}"
fi
