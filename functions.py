"""IAOps backend entrypoint.

Expose `handle_request` for MCP tool execution with governance checks.
"""

from __future__ import annotations

import os
from typing import Any

from iaops.mcp.gateway import MCPGateway
from iaops.mcp.repository import InMemoryMCPRepository
from iaops.mcp.repository import MCPRepository

_DEFAULT_REPOSITORY: MCPRepository | None = None


def _build_repository() -> MCPRepository:
    dsn = os.getenv("IAOPS_DB_DSN")
    if not dsn:
        return InMemoryMCPRepository()

    schema = os.getenv("IAOPS_DB_SCHEMA", "iaops_gov")
    try:
        from iaops.mcp.postgres_repository import PostgresMCPRepository
    except ImportError as exc:  # pragma: no cover - depende de ambiente
        raise RuntimeError(
            "IAOPS_DB_DSN definido, mas dependencia de PostgreSQL ausente. Instale requirements.txt."
        ) from exc
    return PostgresMCPRepository(dsn=dsn, schema=schema)


def _get_default_repository() -> MCPRepository:
    global _DEFAULT_REPOSITORY
    if _DEFAULT_REPOSITORY is None:
        _DEFAULT_REPOSITORY = _build_repository()
    return _DEFAULT_REPOSITORY


def handle_request(payload: dict[str, Any], repository: MCPRepository | None = None) -> dict[str, Any]:
    """Processa uma chamada MCP com validacoes de acesso e politicas."""
    repo = repository or _get_default_repository()
    gateway = MCPGateway(repo)
    return gateway.handle(payload)


if __name__ == "__main__":
    sample_payload = {
        "context": {
            "client_id": 1,
            "tenant_id": 10,
            "user_id": 100,
            "correlation_id": "demo-001",
        },
        "tool": "inventory.list_tables",
        "input": {"schema_name": "public"},
    }
    print(handle_request(sample_payload))
