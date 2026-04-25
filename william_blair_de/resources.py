import os
from pathlib import Path

from dagster import ConfigurableResource
from pydantic import Field


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


class DataDirResource(ConfigurableResource):
    """Directory containing source CSV files (default: ./data under repo root)."""

    relative_path: str = Field(default="data", description="Path relative to cwd (repo root when using dagster dev).")

    def path(self) -> Path:
        base = Path(os.environ.get("WB_DATA_DIR", self.relative_path))
        if base.is_absolute():
            return base
        return Path.cwd() / base


class DuckDBWarehouseResource(ConfigurableResource):
    """DuckDB file backend; swap path via env for a hypothetical prod lakehouse."""

    database_path: str = Field(
        default="warehouse.duckdb",
        description="Path to DuckDB file. Override with WB_DUCKDB_PATH in production.",
    )
    environment: str = Field(default="local", description="local | prod — informational metadata.")

    def resolve_path(self) -> Path:
        env_path = os.environ.get("WB_DUCKDB_PATH")
        if env_path:
            return Path(env_path)
        p = Path(self.database_path)
        return p if p.is_absolute() else Path.cwd() / p

    def connect(self):
        import duckdb

        path = self.resolve_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(path))
