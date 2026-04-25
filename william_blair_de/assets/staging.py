from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from william_blair_de.assets.raw import (
    raw_acquirer_financials,
    raw_acquirers,
    raw_sector_multiples,
    raw_targets,
    raw_transactions,
)
from william_blair_de.resources import DuckDBWarehouseResource


@asset(deps=[raw_acquirers], group_name="stage")
def stg_acquirers(context: AssetExecutionContext, warehouse: DuckDBWarehouseResource) -> MaterializeResult:
    conn = warehouse.connect()
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute(
        """
        CREATE OR REPLACE TABLE staging.acquirers AS
        SELECT
          upper(trim(acquirer_id)) AS acquirer_id,
          trim(acquirer_name) AS acquirer_name,
          CASE lower(trim(coalesce(acquirer_type, '')))
            WHEN 'strategic' THEN 'Strategic'
            WHEN 'financial sponsor' THEN 'Financial Sponsor'
            ELSE trim(acquirer_type)
          END AS acquirer_type,
          trim(primary_sector_focus) AS primary_sector_focus,
          trim(headquarters) AS headquarters,
          trim(ticker_or_status) AS ticker_or_status,
          TRY_CAST(employee_count AS INTEGER) AS employee_count,
          TRY_CAST(annual_revenue_mm AS DOUBLE) AS annual_revenue_mm,
          TRY_CAST(
            replace(trim(founded_date), '/', '-') AS DATE
          ) AS founded_date,
          trim(geographic_reach) AS geographic_reach
        FROM raw.acquirers
        WHERE acquirer_id IS NOT NULL AND trim(acquirer_id) <> ''
        """
    )
    n = conn.execute("SELECT COUNT(*) FROM staging.acquirers").fetchone()[0]
    conn.close()
    return MaterializeResult(metadata={"row_count": MetadataValue.int(n)})


@asset(deps=[raw_targets], group_name="stage")
def stg_targets(context: AssetExecutionContext, warehouse: DuckDBWarehouseResource) -> MaterializeResult:
    conn = warehouse.connect()
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute(
        """
        CREATE OR REPLACE TABLE staging.targets AS
        SELECT
          upper(trim(target_id)) AS target_id,
          trim(target_name) AS target_name,
          trim(sector) AS sector,
          trim(sub_sector) AS sub_sector,
          trim(geography) AS geography,
          trim(ownership_type) AS ownership_type,
          TRY_CAST(replace(trim(founded_date), '/', '-') AS DATE) AS founded_date,
          TRY_CAST(employee_count AS INTEGER) AS employee_count,
          TRY_CAST(ltm_revenue_mm AS DOUBLE) AS ltm_revenue_mm,
          TRY_CAST(ebitda_margin_pct AS DOUBLE) AS ebitda_margin_pct,
          TRY_CAST(revenue_growth_pct AS DOUBLE) AS revenue_growth_pct
        FROM raw.targets
        WHERE target_id IS NOT NULL AND trim(target_id) <> ''
        """
    )
    n = conn.execute("SELECT COUNT(*) FROM staging.targets").fetchone()[0]
    conn.close()
    return MaterializeResult(metadata={"row_count": MetadataValue.int(n)})


@asset(deps=[raw_transactions], group_name="stage")
def stg_transactions(context: AssetExecutionContext, warehouse: DuckDBWarehouseResource) -> MaterializeResult:
    conn = warehouse.connect()
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute(
        """
        CREATE OR REPLACE TABLE staging.transactions_quarantine AS
        SELECT r.*,
          CASE
            WHEN trim(coalesce(transaction_id, '')) = '' THEN 'missing_transaction_id'
            WHEN TRY_CAST(deal_size_mm AS DOUBLE) IS NOT NULL AND TRY_CAST(deal_size_mm AS DOUBLE) < 0
              THEN 'negative_deal_size'
            ELSE 'other'
          END AS quarantine_reason
        FROM raw.transactions r
        WHERE trim(coalesce(transaction_id, '')) = ''
           OR (TRY_CAST(deal_size_mm AS DOUBLE) IS NOT NULL AND TRY_CAST(deal_size_mm AS DOUBLE) < 0)
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TABLE staging.transactions AS
        SELECT
          upper(trim(transaction_id)) AS transaction_id,
          upper(trim(target_id)) AS target_id,
          upper(trim(acquirer_id)) AS acquirer_id,
          TRY_CAST(replace(trim(announce_date), '/', '-') AS DATE) AS announce_date,
          CASE WHEN trim(coalesce(close_date, '')) = '' THEN NULL
               ELSE TRY_CAST(replace(trim(close_date), '/', '-') AS DATE) END AS close_date,
          TRY_CAST(deal_year AS INTEGER) AS deal_year,
          trim(deal_quarter) AS deal_quarter,
          trim(deal_type) AS deal_type,
          trim(financing_type) AS financing_type,
          TRY_CAST(deal_size_mm AS DOUBLE) AS deal_size_mm,
          TRY_CAST(ev_ebitda_multiple AS DOUBLE) AS ev_ebitda_multiple,
          TRY_CAST(ev_revenue_multiple AS DOUBLE) AS ev_revenue_multiple,
          TRY_CAST(synergy_pct_of_deal AS DOUBLE) AS synergy_pct_of_deal,
          trim(outcome) AS outcome,
          trim(strategic_rationale_tags) AS strategic_rationale_tags,
          TRY_CAST(num_bidders AS INTEGER) AS num_bidders,
          TRY_CAST(days_to_close AS INTEGER) AS days_to_close
        FROM raw.transactions
        WHERE trim(coalesce(transaction_id, '')) <> ''
          AND NOT (
            TRY_CAST(deal_size_mm AS DOUBLE) IS NOT NULL AND TRY_CAST(deal_size_mm AS DOUBLE) < 0
          )
        """
    )
    good = conn.execute("SELECT COUNT(*) FROM staging.transactions").fetchone()[0]
    bad = conn.execute("SELECT COUNT(*) FROM staging.transactions_quarantine").fetchone()[0]
    conn.close()
    return MaterializeResult(
        metadata={
            "row_count": MetadataValue.int(good),
            "quarantined_rows": MetadataValue.int(bad),
        }
    )


@asset(deps=[raw_acquirer_financials], group_name="stage")
def stg_acquirer_financials(
    context: AssetExecutionContext, warehouse: DuckDBWarehouseResource
) -> MaterializeResult:
    conn = warehouse.connect()
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute(
        """
        CREATE OR REPLACE TABLE staging.acquirer_financials AS
        SELECT
          upper(trim(acquirer_id)) AS acquirer_id,
          TRY_CAST(fiscal_year AS INTEGER) AS fiscal_year,
          TRY_CAST(revenue_mm AS DOUBLE) AS revenue_mm,
          TRY_CAST(ebitda_mm AS DOUBLE) AS ebitda_mm,
          TRY_CAST(ebitda_margin_pct AS DOUBLE) AS ebitda_margin_pct,
          TRY_CAST(net_debt_mm AS DOUBLE) AS net_debt_mm,
          TRY_CAST(cash_on_hand_mm AS DOUBLE) AS cash_on_hand_mm,
          TRY_CAST(total_assets_mm AS DOUBLE) AS total_assets_mm,
          TRY_CAST(market_cap_mm AS DOUBLE) AS market_cap_mm,
          TRY_CAST(employee_count AS INTEGER) AS employee_count
        FROM raw.acquirer_financials
        WHERE acquirer_id IS NOT NULL AND trim(acquirer_id) <> ''
        """
    )
    n = conn.execute("SELECT COUNT(*) FROM staging.acquirer_financials").fetchone()[0]
    conn.close()
    return MaterializeResult(metadata={"row_count": MetadataValue.int(n)})


@asset(deps=[raw_sector_multiples], group_name="stage")
def stg_sector_multiples(context: AssetExecutionContext, warehouse: DuckDBWarehouseResource) -> MaterializeResult:
    conn = warehouse.connect()
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute(
        """
        CREATE OR REPLACE TABLE staging.sector_multiples AS
        SELECT
          trim(sector) AS sector,
          TRY_CAST(fiscal_year AS INTEGER) AS fiscal_year,
          trim(fiscal_quarter) AS fiscal_quarter,
          TRY_CAST(median_ev_ebitda AS DOUBLE) AS median_ev_ebitda,
          TRY_CAST(p25_ev_ebitda AS DOUBLE) AS p25_ev_ebitda,
          TRY_CAST(p75_ev_ebitda AS DOUBLE) AS p75_ev_ebitda,
          TRY_CAST(median_ev_revenue AS DOUBLE) AS median_ev_revenue,
          TRY_CAST(p25_ev_revenue AS DOUBLE) AS p25_ev_revenue,
          TRY_CAST(p75_ev_revenue AS DOUBLE) AS p75_ev_revenue,
          TRY_CAST(deal_count AS INTEGER) AS deal_count
        FROM raw.sector_multiples
        """
    )
    n = conn.execute("SELECT COUNT(*) FROM staging.sector_multiples").fetchone()[0]
    conn.close()
    return MaterializeResult(metadata={"row_count": MetadataValue.int(n)})
