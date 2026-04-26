"""Drop raw, staging, and analytics schemas in the DuckDB warehouse (fresh materialize).

Uses the same path resolution as DuckDBWarehouseResource: WB_DUCKDB_PATH env, else
warehouse.duckdb under the current working directory.

  python scripts/reset_warehouse.py
"""

import os
from pathlib import Path

import duckdb


def _db_path() -> Path:
    env = os.environ.get("WB_DUCKDB_PATH")
    if env:
        return Path(env)
    p = Path("warehouse.duckdb")
    return p if p.is_absolute() else Path.cwd() / p


def main() -> None:
    path = _db_path()
    if not path.exists():
        print(f"No database at {path}; nothing to reset.")
        return
    con = duckdb.connect(str(path))
    for schema in ("analytics", "staging", "raw"):
        con.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    con.close()
    print(f"Dropped schemas raw, staging, analytics in {path}")


if __name__ == "__main__":
    main()
