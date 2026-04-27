"""Drop raw, staging, and analytics schemas in the DuckDB warehouse (fresh materialize).

Uses the same path resolution as ``DuckDBWarehouseResource`` / ``resolve_warehouse_duckdb_path``
(``WB_WAREHOUSE_PROFILE``, ``WB_LOCAL_DUCKDB_PATH``, ``WB_PROD_DUCKDB_PATH``, ``WB_DUCKDB_PATH``).

Run from repo root:

  python scripts/reset_warehouse.py
"""

import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from william_blair_de.resources import resolve_warehouse_duckdb_path


def _db_path() -> Path:
    return resolve_warehouse_duckdb_path()


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
