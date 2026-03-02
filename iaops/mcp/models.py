from __future__ import annotations

from dataclasses import dataclass
from typing import Any


ROLE_ORDER = {
    "viewer": 1,
    "admin": 2,
    "owner": 3,
}


@dataclass(frozen=True)
class RequestContext:
    client_id: int
    tenant_id: int
    user_id: int
    correlation_id: str


@dataclass(frozen=True)
class ToolPolicy:
    tool_name: str
    min_role: str
    is_enabled: bool = True
    max_rows: int | None = None
    max_calls_per_minute: int | None = None
    require_masking: bool = True
    allowed_schema_patterns: list[str] | None = None


@dataclass(frozen=True)
class ToolExecutionResult:
    status: str
    data: dict[str, Any]
    error_code: str | None = None
    error_message: str | None = None
