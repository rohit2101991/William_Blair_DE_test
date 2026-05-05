"""SCD Type 1 entity dimensions: merge staging into analytics with optional hash-gated updates.

Staging carries ``row_content_hash`` (MD5 of non-key attributes). ``merge_scd1`` updates a matched row
only when the hash differs, avoiding no-op writes. Rows that disappear from staging are **not**
deleted from the dimension (orphans retained).
"""

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from william_blair_de.assets.staging import stg_acquirers, stg_targets
from william_blair_de.materialization_context import (
    ensure_analytics_data_date_column,
    ensure_analytics_row_content_hash_column,
    materialization_data_date,
    sql_data_date_literal,
)
from william_blair_de.resources import DuckDBWarehouseResource


def analytics_table_exists(conn, table: str) -> bool:
    n = conn.execute(
        """
        SELECT COUNT(*) FROM duckdb_tables()
        WHERE schema_name = 'analytics' AND table_name = ?
        """,
        [table],
    ).fetchone()[0]
    return n > 0


def merge_scd1(
    conn,
    *,
    target_table: str,
    source_sql: str,
    pk_columns: tuple[str, ...],
    all_columns: tuple[str, ...],
    content_hash_column: str | None = "row_content_hash",
) -> None:
    """Upsert by PK; update only when ``content_hash_column`` differs (if present in ``all_columns``)."""
    t = f'analytics."{target_table}"'
    on = " AND ".join(f"t.{k} = s.{k}" for k in pk_columns)
    update_cols = tuple(c for c in all_columns if c not in pk_columns)
    set_clause = ", ".join(f"{c} = s.{c}" for c in update_cols)
    ins_cols = ", ".join(all_columns)
    vals = ", ".join(f"s.{c}" for c in all_columns)
    if content_hash_column and content_hash_column in all_columns:
        matched = (
            f"WHEN MATCHED AND (t.{content_hash_column} IS DISTINCT FROM s.{content_hash_column}) "
            f"THEN UPDATE SET {set_clause}"
        )
    else:
        matched = f"WHEN MATCHED THEN UPDATE SET {set_clause}"
    conn.execute(
        f"""
        MERGE INTO {t} AS t
        USING ({source_sql}) AS s
        ON {on}
        {matched}
        WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({vals})
        """
    )


_DIM_ACQUIRER_COLS = (
    "acquirer_id",
    "acquirer_name",
    "acquirer_type",
    "primary_sector_focus",
    "headquarters",
    "ticker_or_status",
    "employee_count",
    "annual_revenue_mm",
    "founded_date",
    "geographic_reach",
    "row_content_hash",
    "dw_updated_at",
    "data_date",
)

_DIM_TARGET_COLS = (
    "target_id",
    "target_name",
    "sector",
    "sub_sector",
    "geography",
    "ownership_type",
    "founded_date",
    "employee_count",
    "ltm_revenue_mm",
    "ebitda_margin_pct",
    "revenue_growth_pct",
    "row_content_hash",
    "dw_updated_at",
    "data_date",
)


@asset(deps=[stg_acquirers], group_name="model")
def dim_acquirer(context: AssetExecutionContext, warehouse: DuckDBWarehouseResource) -> MaterializeResult:
    """SCD1 current-state acquirer row per acquirer_id (merge from staging.acquirers)."""
    d = materialization_data_date(context)
    lit = sql_data_date_literal(d)
    conn = warehouse.connect()
    conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")
    src = f"""
        SELECT
          acquirer_id,
          acquirer_name,
          acquirer_type,
          primary_sector_focus,
          headquarters,
          ticker_or_status,
          employee_count,
          annual_revenue_mm,
          founded_date,
          geographic_reach,
          row_content_hash,
          CURRENT_TIMESTAMP AS dw_updated_at,
          {lit} AS data_date
        FROM staging.acquirers
    """
    if not analytics_table_exists(conn, "dim_acquirer"):
        conn.execute(
            f"""
            CREATE TABLE analytics.dim_acquirer AS
            {src}
            """
        )
    else:
        ensure_analytics_data_date_column(conn, "dim_acquirer")
        ensure_analytics_row_content_hash_column(conn, "dim_acquirer")
        merge_scd1(
            conn,
            target_table="dim_acquirer",
            source_sql=src,
            pk_columns=("acquirer_id",),
            all_columns=_DIM_ACQUIRER_COLS,
        )
    n = conn.execute("SELECT COUNT(*) FROM analytics.dim_acquirer").fetchone()[0]
    conn.close()
    return MaterializeResult(metadata={"row_count": MetadataValue.int(n)})


@asset(deps=[stg_targets], group_name="model")
def dim_target(context: AssetExecutionContext, warehouse: DuckDBWarehouseResource) -> MaterializeResult:
    """SCD1 current-state target row per target_id (merge from staging.targets)."""
    d = materialization_data_date(context)
    lit = sql_data_date_literal(d)
    conn = warehouse.connect()
    conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")
    src = f"""
        SELECT
          target_id,
          target_name,
          sector,
          sub_sector,
          geography,
          ownership_type,
          founded_date,
          employee_count,
          ltm_revenue_mm,
          ebitda_margin_pct,
          revenue_growth_pct,
          row_content_hash,
          CURRENT_TIMESTAMP AS dw_updated_at,
          {lit} AS data_date
        FROM staging.targets
    """
    if not analytics_table_exists(conn, "dim_target"):
        conn.execute(
            f"""
            CREATE TABLE analytics.dim_target AS
            {src}
            """
        )
    else:
        ensure_analytics_data_date_column(conn, "dim_target")
        ensure_analytics_row_content_hash_column(conn, "dim_target")
        merge_scd1(
            conn,
            target_table="dim_target",
            source_sql=src,
            pk_columns=("target_id",),
            all_columns=_DIM_TARGET_COLS,
        )
    n = conn.execute("SELECT COUNT(*) FROM analytics.dim_target").fetchone()[0]
    conn.close()
    return MaterializeResult(metadata={"row_count": MetadataValue.int(n)})
