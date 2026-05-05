"""Raw ingest: load each CSV into DuckDB ``raw`` schema with ``all_varchar=true`` (no casting).

Preserves source fidelity; typing and cleaning happen in staging.

Each ``raw_*`` asset: resolve path → ensure schema → CREATE OR REPLACE TABLE AS read_csv_auto
→ count rows → return metadata for the Dagster UI.
"""

# Dagster primitives: context for logging/metadata, MaterializeResult for run output, @asset decorator.
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

# Resources supply filesystem path to CSVs and DuckDB connection settings.
from william_blair_de.resources import DataDirResource, DuckDBWarehouseResource


def _load_csv_as_varchar(
    context: AssetExecutionContext,
    warehouse: DuckDBWarehouseResource,
    data_dir: DataDirResource,
    sql_table: str,
    csv_name: str,
) -> MaterializeResult:
    """Ingest: preserve source strings (all_varchar) — no business transforms.

    Steps:
    1. Build absolute path to the CSV under ``data_dir``.
    2. Fail fast if the file is missing (clear operator error).
    3. Open DuckDB, ensure ``raw`` schema exists.
    4. Replace ``raw.<sql_table>`` with a full read of the CSV (all columns VARCHAR).
    5. Count rows for Dagster metadata and close the connection.
    """
    # Join data_dir root with the fixed filename for this asset.
    path = data_dir.path() / csv_name
    # Avoid silent empty loads: surface a FileNotFoundError in the run logs.
    if not path.exists():
        raise FileNotFoundError(f"Missing source file: {path}")

    # ``connect()`` applies profile/env path logic from DuckDBWarehouseResource.
    conn = warehouse.connect()
    # Schema holds all landing tables; IF NOT EXISTS is idempotent across runs.
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    # sql_table is fixed per call site (no user input) — safe to interpolate into DDL.
    # read_csv_auto infers structure but all_varchar=true keeps every column as text until staging.
    conn.execute(
        f"""
        CREATE OR REPLACE TABLE raw.{sql_table} AS
        SELECT * FROM read_csv_auto(?, header=true, all_varchar=true)
        """,
        [str(path.resolve())],
    )
    # Single aggregate for UI metadata (row_count).
    n = conn.execute(f"SELECT COUNT(*) FROM raw.{sql_table}").fetchone()[0]
    conn.close()

    return MaterializeResult(
        metadata={
            "row_count": MetadataValue.int(n),
            "source_path": MetadataValue.path(str(path)),
        }
    )


@asset(group_name="ingest")
def raw_acquirers(
    context: AssetExecutionContext,
    warehouse: DuckDBWarehouseResource,
    data_dir: DataDirResource,
) -> MaterializeResult:
    """Load acquirers.csv → raw.acquirers (all varchar)."""
    return _load_csv_as_varchar(context, warehouse, data_dir, "acquirers", "acquirers.csv")


@asset(group_name="ingest")
def raw_targets(
    context: AssetExecutionContext,
    warehouse: DuckDBWarehouseResource,
    data_dir: DataDirResource,
) -> MaterializeResult:
    """Load targets.csv → raw.targets (all varchar)."""
    return _load_csv_as_varchar(context, warehouse, data_dir, "targets", "targets.csv")


@asset(group_name="ingest")
def raw_transactions(
    context: AssetExecutionContext,
    warehouse: DuckDBWarehouseResource,
    data_dir: DataDirResource,
) -> MaterializeResult:
    """Load transactions.csv → raw.transactions (all varchar)."""
    return _load_csv_as_varchar(context, warehouse, data_dir, "transactions", "transactions.csv")


@asset(group_name="ingest")
def raw_acquirer_financials(
    context: AssetExecutionContext,
    warehouse: DuckDBWarehouseResource,
    data_dir: DataDirResource,
) -> MaterializeResult:
    """Load acquirer_financials.csv → raw.acquirer_financials (all varchar)."""
    return _load_csv_as_varchar(context, warehouse, data_dir, "acquirer_financials", "acquirer_financials.csv")


@asset(group_name="ingest")
def raw_sector_multiples(
    context: AssetExecutionContext,
    warehouse: DuckDBWarehouseResource,
    data_dir: DataDirResource,
) -> MaterializeResult:
    """Load sector_multiples.csv → raw.sector_multiples (all varchar)."""
    return _load_csv_as_varchar(context, warehouse, data_dir, "sector_multiples", "sector_multiples.csv")
