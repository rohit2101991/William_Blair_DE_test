"""Analytics layer: partitioned fact + SCD1 dims + reporting rollups."""

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from william_blair_de.assets.dimensions import (
    analytics_table_exists,
    dim_acquirer,
    dim_target,
    merge_scd1,
)
from william_blair_de.assets.staging import (
    stg_acquirer_financials,
    stg_sector_multiples,
    stg_transactions,
)
from william_blair_de.partitions import DEAL_YEAR_PARTITIONS
from william_blair_de.resources import DuckDBWarehouseResource


def _fct_select_sql() -> str:
    return """
    SELECT
      t.transaction_id,
      t.target_id,
      t.acquirer_id,
      t.announce_date,
      t.close_date,
      t.deal_year,
      t.deal_quarter,
      t.deal_type,
      t.financing_type,
      t.deal_size_mm,
      t.ev_ebitda_multiple,
      t.ev_revenue_multiple,
      t.synergy_pct_of_deal,
      t.outcome,
      t.strategic_rationale_tags,
      t.num_bidders,
      t.days_to_close,
      tg.target_name,
      tg.sector AS target_sector,
      tg.sub_sector AS target_sub_sector,
      tg.geography AS target_geography,
      tg.ownership_type AS target_ownership_type,
      tg.ltm_revenue_mm AS target_ltm_revenue_mm,
      tg.ebitda_margin_pct AS target_ebitda_margin_pct,
      aq.acquirer_name,
      aq.acquirer_type,
      aq.primary_sector_focus AS acquirer_sector_focus,
      aq.geographic_reach AS acquirer_geographic_reach,
      sm.median_ev_ebitda AS sector_median_ev_ebitda,
      sm.median_ev_revenue AS sector_median_ev_revenue,
      CASE
        WHEN sm.median_ev_ebitda IS NOT NULL AND abs(sm.median_ev_ebitda) > 1e-9 AND t.ev_ebitda_multiple IS NOT NULL
          THEN (t.ev_ebitda_multiple - sm.median_ev_ebitda) / sm.median_ev_ebitda * 100.0
      END AS ev_ebitda_pct_vs_sector_median,
      CASE
        WHEN sm.median_ev_revenue IS NOT NULL AND abs(sm.median_ev_revenue) > 1e-9 AND t.ev_revenue_multiple IS NOT NULL
          THEN (t.ev_revenue_multiple - sm.median_ev_revenue) / sm.median_ev_revenue * 100.0
      END AS ev_revenue_pct_vs_sector_median,
      af_match.revenue_mm AS acquirer_fiscal_revenue_mm,
      af_match.ebitda_mm AS acquirer_fiscal_ebitda_mm,
      af_match.net_debt_mm AS acquirer_net_debt_mm,
      af_match.cash_on_hand_mm AS acquirer_cash_mm,
      af_match.total_assets_mm AS acquirer_total_assets_mm,
      CASE
        WHEN t.deal_size_mm IS NULL THEN NULL
        WHEN t.deal_size_mm < 100 THEN 'Small'
        WHEN t.deal_size_mm < 500 THEN 'Mid-Market'
        WHEN t.deal_size_mm < 2000 THEN 'Large'
        ELSE 'Mega'
      END AS deal_size_tier
    FROM staging.transactions t
    LEFT JOIN analytics.dim_target tg ON tg.target_id = t.target_id
    LEFT JOIN analytics.dim_acquirer aq ON aq.acquirer_id = t.acquirer_id
    LEFT JOIN staging.sector_multiples sm
      ON sm.sector = tg.sector
     AND sm.fiscal_year = t.deal_year
     AND sm.fiscal_quarter = t.deal_quarter
    LEFT JOIN staging.acquirer_financials af_match
      ON af_match.acquirer_id = t.acquirer_id
     AND af_match.fiscal_year = CAST(EXTRACT(YEAR FROM t.announce_date) AS INTEGER)
    """


@asset(
    partitions_def=DEAL_YEAR_PARTITIONS,
    deps=[
        stg_transactions,
        dim_target,
        dim_acquirer,
        stg_sector_multiples,
        stg_acquirer_financials,
    ],
    group_name="model",
)
def fct_transactions(context: AssetExecutionContext, warehouse: DuckDBWarehouseResource) -> MaterializeResult:
    """Denormalized transaction fact; materialize one `deal_year` partition at a time (merge into analytics.fct_transactions)."""
    year = int(context.partition_key)
    conn = warehouse.connect()
    conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")

    base = _fct_select_sql()
    exists = conn.execute(
        """
        SELECT COUNT(*) FROM duckdb_tables()
        WHERE schema_name = 'analytics' AND table_name = 'fct_transactions'
        """
    ).fetchone()[0]

    filter_clause = "WHERE t.deal_year = ?"
    # First run creates the table; later runs upsert by partition via delete+insert.
    if exists == 0:
        conn.execute(
            f"CREATE TABLE analytics.fct_transactions AS {base} {filter_clause}",
            [year],
        )
    else:
        conn.execute("DELETE FROM analytics.fct_transactions WHERE deal_year = ?", [year])
        conn.execute(
            f"INSERT INTO analytics.fct_transactions {base} {filter_clause}",
            [year],
        )

    n = conn.execute("SELECT COUNT(*) FROM analytics.fct_transactions WHERE deal_year = ?", [year]).fetchone()[0]
    conn.close()
    return MaterializeResult(
        metadata={"partition": MetadataValue.text(context.partition_key), "rows_for_year": MetadataValue.int(n)}
    )


_DIM_ACQUIRER_ACTIVITY_INNER = """
WITH enriched AS (
  SELECT
    t.acquirer_id,
    t.transaction_id,
    t.deal_year,
    t.deal_type,
    t.deal_size_mm,
    t.ev_ebitda_multiple,
    t.ev_revenue_multiple,
    t.announce_date,
    tg.sector AS target_sector
  FROM staging.transactions t
  LEFT JOIN analytics.dim_target tg ON tg.target_id = t.target_id
),
span AS (
  SELECT acquirer_id,
         MAX(deal_year) - MIN(deal_year) + 1 AS year_span
  FROM enriched
  GROUP BY 1
)
SELECT
  e.acquirer_id,
  aq.acquirer_name,
  aq.acquirer_type,
  COUNT(*) AS deal_count,
  SUM(e.deal_size_mm) AS total_deal_volume_mm,
  AVG(e.ev_ebitda_multiple) AS avg_ev_ebitda_multiple,
  AVG(e.ev_revenue_multiple) AS avg_ev_revenue_multiple,
  COUNT(DISTINCT e.target_sector) AS distinct_target_sectors,
  string_agg(e.deal_type, ' | ') AS deal_types_observed,
  string_agg(coalesce(e.target_sector, 'Unknown'), ' | ') AS sectors_touched,
  MIN(e.announce_date) AS first_transaction_date,
  MAX(e.announce_date) AS most_recent_transaction_date,
  CASE WHEN s.year_span IS NULL OR s.year_span = 0 THEN NULL
       ELSE CAST(COUNT(*) AS DOUBLE) / s.year_span END AS avg_deals_per_active_year
FROM enriched e
LEFT JOIN analytics.dim_acquirer aq ON aq.acquirer_id = e.acquirer_id
LEFT JOIN span s ON s.acquirer_id = e.acquirer_id
GROUP BY e.acquirer_id, aq.acquirer_name, aq.acquirer_type, s.year_span
"""

_DIM_ACQUIRER_ACTIVITY_COLS = (
    "acquirer_id",
    "acquirer_name",
    "acquirer_type",
    "deal_count",
    "total_deal_volume_mm",
    "avg_ev_ebitda_multiple",
    "avg_ev_revenue_multiple",
    "distinct_target_sectors",
    "deal_types_observed",
    "sectors_touched",
    "first_transaction_date",
    "most_recent_transaction_date",
    "avg_deals_per_active_year",
    "dw_updated_at",
)


@asset(
    deps=[stg_transactions, dim_target, dim_acquirer],
    group_name="model",
)
def dim_acquirer_activity(context: AssetExecutionContext, warehouse: DuckDBWarehouseResource) -> MaterializeResult:
    """Acquirer rollup; SCD1 merge on acquirer_id from current facts × dims."""
    conn = warehouse.connect()
    conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")
    activity_src = f"""
        SELECT *, CURRENT_TIMESTAMP AS dw_updated_at
        FROM ({_DIM_ACQUIRER_ACTIVITY_INNER}) AS act_inner
    """
    # Same pattern as other dims: bootstrap once, then SCD1 merge.
    if not analytics_table_exists(conn, "dim_acquirer_activity"):
        conn.execute(
            f"""
            CREATE TABLE analytics.dim_acquirer_activity AS
            {activity_src}
            """
        )
    else:
        merge_scd1(
            conn,
            target_table="dim_acquirer_activity",
            source_sql=activity_src,
            pk_columns=("acquirer_id",),
            all_columns=_DIM_ACQUIRER_ACTIVITY_COLS,
        )
    n = conn.execute("SELECT COUNT(*) FROM analytics.dim_acquirer_activity").fetchone()[0]
    conn.close()
    return MaterializeResult(metadata={"row_count": MetadataValue.int(n)})


@asset(
    deps=[stg_transactions, dim_target],
    group_name="model",
)
def rpt_sector_trend_summary(context: AssetExecutionContext, warehouse: DuckDBWarehouseResource) -> MaterializeResult:
    """Extra modeled table: sector × year rollups for market activity vs targets in this dataset."""
    conn = warehouse.connect()
    conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")
    conn.execute(
        """
        CREATE OR REPLACE TABLE analytics.rpt_sector_trend_summary AS
        SELECT
          tg.sector,
          t.deal_year,
          COUNT(*) AS transaction_count,
          SUM(t.deal_size_mm) AS total_deal_size_mm,
          AVG(t.ev_ebitda_multiple) AS avg_ev_ebitda_multiple,
          AVG(t.ev_revenue_multiple) AS avg_ev_revenue_multiple,
          SUM(CASE WHEN t.outcome ILIKE 'Closed%' THEN 1 ELSE 0 END) AS closed_deal_count,
          SUM(CASE WHEN t.outcome ILIKE 'Pending%' THEN 1 ELSE 0 END) AS pending_deal_count
        FROM staging.transactions t
        INNER JOIN analytics.dim_target tg ON tg.target_id = t.target_id
        GROUP BY tg.sector, t.deal_year
        ORDER BY tg.sector, t.deal_year
        """
    )
    n = conn.execute("SELECT COUNT(*) FROM analytics.rpt_sector_trend_summary").fetchone()[0]
    conn.close()
    return MaterializeResult(metadata={"row_count": MetadataValue.int(n)})


@asset(
    deps=[stg_transactions, dim_target],
    group_name="model",
)
def fct_target_deal_sequence(
    context: AssetExecutionContext, warehouse: DuckDBWarehouseResource
) -> MaterializeResult:
    """Per-target deal timeline: order transactions by announce_date and compare to prior deal on same target.

    Grain: (target_id, deal_sequence_number) — one row per transaction with sequence and lag deltas.
    """
    conn = warehouse.connect()
    conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")
    conn.execute(
        """
        CREATE OR REPLACE TABLE analytics.fct_target_deal_sequence AS
        WITH ordered AS (
          SELECT
            t.transaction_id,
            t.target_id,
            t.acquirer_id,
            t.announce_date,
            t.close_date,
            t.deal_year,
            t.deal_quarter,
            t.deal_type,
            t.financing_type,
            t.deal_size_mm,
            t.ev_ebitda_multiple,
            t.ev_revenue_multiple,
            t.outcome,
            t.num_bidders,
            t.days_to_close,
            tg.target_name,
            tg.sector AS target_sector,
            ROW_NUMBER() OVER (
              PARTITION BY t.target_id
              ORDER BY t.announce_date ASC NULLS LAST, t.transaction_id
            ) AS deal_sequence_number
          FROM staging.transactions t
          LEFT JOIN analytics.dim_target tg ON tg.target_id = t.target_id
          WHERE t.target_id IS NOT NULL
        ),
        lagged AS (
          SELECT
            *,
            LAG(transaction_id) OVER (PARTITION BY target_id ORDER BY deal_sequence_number)
              AS prior_transaction_id,
            LAG(announce_date) OVER (PARTITION BY target_id ORDER BY deal_sequence_number)
              AS prior_announce_date,
            LAG(close_date) OVER (PARTITION BY target_id ORDER BY deal_sequence_number)
              AS prior_close_date,
            LAG(outcome) OVER (PARTITION BY target_id ORDER BY deal_sequence_number)
              AS prior_outcome,
            LAG(deal_size_mm) OVER (PARTITION BY target_id ORDER BY deal_sequence_number)
              AS prior_deal_size_mm,
            LAG(ev_ebitda_multiple) OVER (PARTITION BY target_id ORDER BY deal_sequence_number)
              AS prior_ev_ebitda_multiple,
            LAG(acquirer_id) OVER (PARTITION BY target_id ORDER BY deal_sequence_number)
              AS prior_acquirer_id
          FROM ordered
        )
        SELECT
          target_id,
          deal_sequence_number,
          transaction_id,
          acquirer_id,
          announce_date,
          close_date,
          deal_year,
          deal_quarter,
          deal_type,
          financing_type,
          outcome,
          deal_size_mm,
          ev_ebitda_multiple,
          ev_revenue_multiple,
          num_bidders,
          days_to_close,
          target_name,
          target_sector,
          prior_transaction_id,
          prior_announce_date,
          prior_close_date,
          prior_outcome,
          prior_deal_size_mm,
          prior_ev_ebitda_multiple,
          prior_acquirer_id,
          CASE
            WHEN prior_announce_date IS NOT NULL AND announce_date IS NOT NULL
              THEN DATE_DIFF('day', prior_announce_date, announce_date)
          END AS days_since_prior_announce,
          CASE
            WHEN prior_deal_size_mm IS NOT NULL AND deal_size_mm IS NOT NULL
              THEN deal_size_mm - prior_deal_size_mm
          END AS deal_size_mm_delta_vs_prior,
          CASE
            WHEN prior_ev_ebitda_multiple IS NOT NULL AND ev_ebitda_multiple IS NOT NULL
              THEN ev_ebitda_multiple - prior_ev_ebitda_multiple
          END AS ev_ebitda_multiple_delta_vs_prior,
          (prior_acquirer_id IS NOT NULL AND acquirer_id = prior_acquirer_id) AS same_acquirer_as_prior
        FROM lagged
        """
    )
    n = conn.execute("SELECT COUNT(*) FROM analytics.fct_target_deal_sequence").fetchone()[0]
    conn.close()
    return MaterializeResult(metadata={"row_count": MetadataValue.int(n)})
