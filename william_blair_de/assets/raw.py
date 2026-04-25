from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from william_blair_de.resources import DataDirResource, DuckDBWarehouseResource


def _load_csv_as_varchar(
    context: AssetExecutionContext,
    warehouse: DuckDBWarehouseResource,
    data_dir: DataDirResource,
    sql_table: str,
    csv_name: str,
) -> MaterializeResult:
    """Ingest: preserve source strings (all_varchar) — no business transforms."""
    path = data_dir.path() / csv_name
    if not path.exists():
        raise FileNotFoundError(f"Missing source file: {path}")

    conn = warehouse.connect()
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    # sql_table is fixed per call site (no user input).
    conn.execute(
        f"""
        CREATE OR REPLACE TABLE raw.{sql_table} AS
        SELECT * FROM read_csv_auto(?, header=true, all_varchar=true)
        """,
        [str(path.resolve())],
    )
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
    return _load_csv_as_varchar(context, warehouse, data_dir, "acquirers", "acquirers.csv")


@asset(group_name="ingest")
def raw_targets(
    context: AssetExecutionContext,
    warehouse: DuckDBWarehouseResource,
    data_dir: DataDirResource,
) -> MaterializeResult:
    return _load_csv_as_varchar(context, warehouse, data_dir, "targets", "targets.csv")


@asset(group_name="ingest")
def raw_transactions(
    context: AssetExecutionContext,
    warehouse: DuckDBWarehouseResource,
    data_dir: DataDirResource,
) -> MaterializeResult:
    return _load_csv_as_varchar(context, warehouse, data_dir, "transactions", "transactions.csv")


@asset(group_name="ingest")
def raw_acquirer_financials(
    context: AssetExecutionContext,
    warehouse: DuckDBWarehouseResource,
    data_dir: DataDirResource,
) -> MaterializeResult:
    return _load_csv_as_varchar(context, warehouse, data_dir, "acquirer_financials", "acquirer_financials.csv")


@asset(group_name="ingest")
def raw_sector_multiples(
    context: AssetExecutionContext,
    warehouse: DuckDBWarehouseResource,
    data_dir: DataDirResource,
) -> MaterializeResult:
    return _load_csv_as_varchar(context, warehouse, data_dir, "sector_multiples", "sector_multiples.csv")
