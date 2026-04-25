"""Business-rule checks: non-negative / physically plausible lower bounds on staged numerics."""

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    MetadataValue,
    asset_check,
)

from william_blair_de.assets.staging import (
    stg_acquirer_financials,
    stg_acquirers,
    stg_sector_multiples,
    stg_targets,
    stg_transactions,
)
from william_blair_de.resources import DuckDBWarehouseResource


@asset_check(
    asset=stg_acquirers,
    description="Business rule: employee_count and annual_revenue_mm must be null or >= 0 (counts and $MM cannot be negative).",
)
def stg_acquirers_non_negative_metrics(
    context: AssetCheckExecutionContext, warehouse: DuckDBWarehouseResource
) -> AssetCheckResult:
    conn = warehouse.connect()
    bad = conn.execute(
        """
        SELECT COUNT(*) FROM staging.acquirers
        WHERE (employee_count IS NOT NULL AND employee_count < 0)
           OR (annual_revenue_mm IS NOT NULL AND annual_revenue_mm < 0)
        """
    ).fetchone()[0]
    conn.close()
    return AssetCheckResult(
        passed=bad == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"violating_rows": MetadataValue.int(int(bad))},
    )


@asset_check(
    asset=stg_targets,
    description="Business rule: employee_count and ltm_revenue_mm must be null or >= 0 (margins/growth may be negative and are not checked here).",
)
def stg_targets_non_negative_metrics(
    context: AssetCheckExecutionContext, warehouse: DuckDBWarehouseResource
) -> AssetCheckResult:
    conn = warehouse.connect()
    bad = conn.execute(
        """
        SELECT COUNT(*) FROM staging.targets
        WHERE (employee_count IS NOT NULL AND employee_count < 0)
           OR (ltm_revenue_mm IS NOT NULL AND ltm_revenue_mm < 0)
        """
    ).fetchone()[0]
    conn.close()
    return AssetCheckResult(
        passed=bad == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"violating_rows": MetadataValue.int(int(bad))},
    )


@asset_check(
    asset=stg_transactions,
    description="Business rule: deal_size_mm, valuation multiples, bidder count, days to close, synergy % must be null or >= 0.",
)
def stg_transactions_non_negative_metrics(
    context: AssetCheckExecutionContext, warehouse: DuckDBWarehouseResource
) -> AssetCheckResult:
    conn = warehouse.connect()
    bad = conn.execute(
        """
        SELECT COUNT(*) FROM staging.transactions
        WHERE (deal_size_mm IS NOT NULL AND deal_size_mm < 0)
           OR (ev_ebitda_multiple IS NOT NULL AND ev_ebitda_multiple < 0)
           OR (ev_revenue_multiple IS NOT NULL AND ev_revenue_multiple < 0)
           OR (synergy_pct_of_deal IS NOT NULL AND synergy_pct_of_deal < 0)
           OR (num_bidders IS NOT NULL AND num_bidders < 0)
           OR (days_to_close IS NOT NULL AND days_to_close < 0)
        """
    ).fetchone()[0]
    conn.close()
    return AssetCheckResult(
        passed=bad == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"violating_rows": MetadataValue.int(int(bad))},
    )


@asset_check(
    asset=stg_acquirer_financials,
    description="Business rule: headcount, revenue, EBITDA, cash, assets, market cap null or >= 0; excludes net_debt (can be net cash) and margin %.",
)
def stg_acquirer_financials_non_negative_metrics(
    context: AssetCheckExecutionContext, warehouse: DuckDBWarehouseResource
) -> AssetCheckResult:
    conn = warehouse.connect()
    bad = conn.execute(
        """
        SELECT COUNT(*) FROM staging.acquirer_financials
        WHERE (employee_count IS NOT NULL AND employee_count < 0)
           OR (revenue_mm IS NOT NULL AND revenue_mm < 0)
           OR (ebitda_mm IS NOT NULL AND ebitda_mm < 0)
           OR (cash_on_hand_mm IS NOT NULL AND cash_on_hand_mm < 0)
           OR (total_assets_mm IS NOT NULL AND total_assets_mm < 0)
           OR (market_cap_mm IS NOT NULL AND market_cap_mm < 0)
        """
    ).fetchone()[0]
    conn.close()
    return AssetCheckResult(
        passed=bad == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"violating_rows": MetadataValue.int(int(bad))},
    )


@asset_check(
    asset=stg_sector_multiples,
    description="Business rule: sector benchmark multiples and deal_count must be null or >= 0.",
)
def stg_sector_multiples_non_negative_metrics(
    context: AssetCheckExecutionContext, warehouse: DuckDBWarehouseResource
) -> AssetCheckResult:
    conn = warehouse.connect()
    bad = conn.execute(
        """
        SELECT COUNT(*) FROM staging.sector_multiples
        WHERE (median_ev_ebitda IS NOT NULL AND median_ev_ebitda < 0)
           OR (p25_ev_ebitda IS NOT NULL AND p25_ev_ebitda < 0)
           OR (p75_ev_ebitda IS NOT NULL AND p75_ev_ebitda < 0)
           OR (median_ev_revenue IS NOT NULL AND median_ev_revenue < 0)
           OR (p25_ev_revenue IS NOT NULL AND p25_ev_revenue < 0)
           OR (p75_ev_revenue IS NOT NULL AND p75_ev_revenue < 0)
           OR (deal_count IS NOT NULL AND deal_count < 0)
        """
    ).fetchone()[0]
    conn.close()
    return AssetCheckResult(
        passed=bad == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"violating_rows": MetadataValue.int(int(bad))},
    )
