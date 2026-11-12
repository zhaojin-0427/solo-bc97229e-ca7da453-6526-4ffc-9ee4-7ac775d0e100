"""Runtime configuration, overridable through environment variables."""

from __future__ import annotations

import os
from pathlib import Path


def db_path() -> str:
    """SQLite database file location (created on first use)."""
    return os.environ.get("SCAN_DB_PATH", "/data/scanwarehouse.db")


def lease_seconds() -> int:
    """Subscriber claim lease duration in seconds (spec default: 30)."""
    raw = os.environ.get("SCAN_LEASE_SECONDS", "30")
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"SCAN_LEASE_SECONDS must be an integer, got {raw!r}") from exc
    if value <= 0:
        raise RuntimeError(f"SCAN_LEASE_SECONDS must be positive, got {value}")
    return value


def ensure_data_dir() -> None:
    Path(db_path()).parent.mkdir(parents=True, exist_ok=True)
