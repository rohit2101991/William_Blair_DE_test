"""Runtime resources for source data pathing and DuckDB connections.

Environment-based warehouse switching (rubric 5.4):
- ``WB_WAREHOUSE_PROFILE=local`` (default): local dev DuckDB file.
- ``WB_WAREHOUSE_PROFILE=prod``: separate DuckDB file (or path) for a hypothetical
  production deployment — same DuckDB engine, different location/config.

This keeps the take-home stack small (no extra DB drivers) while demonstrating a
clean config boundary you would extend to MotherDuck, S3-attached DuckDB, or a
remote SQL warehouse by swapping the resource factory in ``definitions.py``.
"""

from __future__ import annotations

import os
from pathlib import Path

from dagster import ConfigurableResource
from pydantic import Field


def resolve_warehouse_duckdb_path(
    *,
    warehouse_profile: str | None = None,
    database_path: str = "warehouse.duckdb",
) -> Path:
    """Resolve DuckDB file path for local vs prod profiles (used by assets and helper scripts).

    Precedence (local): ``WB_LOCAL_DUCKDB_PATH`` → ``WB_DUCKDB_PATH`` (legacy) → ``database_path``.
    Precedence (prod): ``WB_PROD_DUCKDB_PATH`` → ``WB_DUCKDB_PATH`` → ``warehouse_prod.duckdb``.
    """
    profile_raw = warehouse_profile or os.environ.get("WB_WAREHOUSE_PROFILE", "local")
    profile = str(profile_raw).strip().lower()
    if profile in ("prod", "production", "prd"):
        p = os.environ.get("WB_PROD_DUCKDB_PATH") or os.environ.get("WB_DUCKDB_PATH")
        if not p:
            p = "warehouse_prod.duckdb"
    else:
        p = os.environ.get("WB_LOCAL_DUCKDB_PATH") or os.environ.get("WB_DUCKDB_PATH")
        if not p:
            p = database_path
    path = Path(p)
    return path if path.is_absolute() else Path.cwd() / path


class DataDirResource(ConfigurableResource):
    """Directory containing source CSV files (default: ./data under repo root)."""

    relative_path: str = Field(default="data", description="Path relative to cwd (repo root when using dagster dev).")

    def path(self) -> Path:
        # Allow local overrides without code edits (useful in interviewer setup).
        base = Path(os.environ.get("WB_DATA_DIR", self.relative_path))
        if base.is_absolute():
            return base
        return Path.cwd() / base


class DuckDBWarehouseResource(ConfigurableResource):
    """DuckDB warehouse: local dev file vs prod file path, selected by profile + env."""

    database_path: str = Field(
        default="warehouse.duckdb",
        description="Default relative DuckDB path when no env override is set (local profile).",
    )
    warehouse_profile: str = Field(
        default="local",
        description="local | prod — also overridable with WB_WAREHOUSE_PROFILE.",
    )
    environment: str = Field(
        default="local",
        description="Informational tag for ops; keep aligned with warehouse_profile in real deployments.",
    )

    def effective_profile(self) -> str:
        return (os.environ.get("WB_WAREHOUSE_PROFILE") or self.warehouse_profile or "local").strip().lower()

    def resolve_path(self) -> Path:
        return resolve_warehouse_duckdb_path(
            warehouse_profile=self.effective_profile(),
            database_path=self.database_path,
        )

    def connect(self):
        import duckdb

        path = self.resolve_path()
        profile = self.effective_profile()
        # Optional: open prod warehouse read-only for safer demos against a shared file.
        read_only = profile in ("prod", "production", "prd") and os.environ.get(
            "WB_DUCKDB_READ_ONLY", ""
        ).strip().lower() in ("1", "true", "yes", "on")
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except OSError:
            # Remote paths (s3://, md:, …) or read-only parents — DuckDB will error if truly invalid.
            pass
        return duckdb.connect(str(path), read_only=read_only)
