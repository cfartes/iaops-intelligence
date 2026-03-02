#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

echo "[depois] Iniciando deploy local em $(date -u +"%Y-%m-%dT%H:%M:%SZ")"

if ! command -v docker >/dev/null 2>&1; then
  echo "[depois] ERRO: docker nao encontrado no PATH."
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "[depois] ERRO: docker compose plugin nao encontrado."
  exit 1
fi

if [ ! -f ".env" ]; then
  if [ -f ".env.example" ]; then
    cp .env.example .env
    echo "[depois] .env nao existia. Criado a partir de .env.example."
    echo "[depois] Ajuste credenciais SMTP/cripto no .env antes de ambiente produtivo."
  else
    echo "[depois] ERRO: .env e .env.example ausentes."
    exit 1
  fi
fi

echo "[depois] Atualizando imagens e containers..."
docker compose up -d --build

echo "[depois] Aguardando API responder em /health..."
TRIES=30
for ((i=1; i<=TRIES; i++)); do
  if curl -fsS "http://127.0.0.1:${API_PORT:-8000}/health" >/dev/null 2>&1; then
    echo "[depois] API saudavel."
    break
  fi
  if [ "$i" -eq "$TRIES" ]; then
    echo "[depois] ERRO: API nao respondeu /health apos $TRIES tentativas."
    docker compose ps
    exit 1
  fi
  sleep 2
done

echo "[depois] Estado atual dos servicos:"
docker compose ps

echo "[depois] Limpando imagens dangling..."
docker image prune -f >/dev/null 2>&1 || true

echo "[depois] Deploy concluido com sucesso."

