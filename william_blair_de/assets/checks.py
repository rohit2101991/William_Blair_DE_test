"""Structural and business validation checks on staging tables."""

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    MetadataValue,
    asset_check,
)

from william_blair_de.assets.staging import stg_acquirers, stg_targets, stg_transactions
from william_blair_de.resources import DuckDBWarehouseResource


@asset_check(asset=stg_transactions, description="transaction_id is unique in staging.")
def stg_transactions_unique_ids(
    context: AssetCheckExecutionContext, warehouse: DuckDBWarehouseResource
) -> AssetCheckResult:
    # Each check opens a short-lived connection to keep checks independent.
    conn = warehouse.connect()
    dupes = conn.execute(
        """
        SELECT COUNT(*) FROM (
          SELECT transaction_id FROM staging.transactions
          GROUP BY 1 HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    conn.close()
    return AssetCheckResult(
        passed=dupes == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"duplicate_id_groups": MetadataValue.int(int(dupes))},
    )


@asset_check(asset=stg_transactions, description="Every acquirer_id exists in stg_acquirers.")
def stg_transactions_fk_acquirer(
    context: AssetCheckExecutionContext, warehouse: DuckDBWarehouseResource
) -> AssetCheckResult:
    conn = warehouse.connect()
    orphans = conn.execute(
        """
        SELECT COUNT(*) FROM staging.transactions t
        WHERE NOT EXISTS (
          SELECT 1 FROM staging.acquirers a WHERE a.acquirer_id = t.acquirer_id
        )
        """
    ).fetchone()[0]
    conn.close()
    return AssetCheckResult(
        passed=orphans == 0,
        metadata={"orphan_acquirer_rows": MetadataValue.int(int(orphans))},
    )


@asset_check(asset=stg_transactions, description="Every target_id exists in stg_targets.")
def stg_transactions_fk_target(
    context: AssetCheckExecutionContext, warehouse: DuckDBWarehouseResource
) -> AssetCheckResult:
    conn = warehouse.connect()
    orphans = conn.execute(
        """
        SELECT COUNT(*) FROM staging.transactions t
        WHERE NOT EXISTS (
          SELECT 1 FROM staging.targets x WHERE x.target_id = t.target_id
        )
        """
    ).fetchone()[0]
    conn.close()
    return AssetCheckResult(
        passed=orphans == 0,
        metadata={"orphan_target_rows": MetadataValue.int(int(orphans))},
    )


@asset_check(asset=stg_transactions, description="close_date >= announce_date when both set.")
def stg_transactions_date_order(
    context: AssetCheckExecutionContext, warehouse: DuckDBWarehouseResource
) -> AssetCheckResult:
    conn = warehouse.connect()
    bad = conn.execute(
        """
        SELECT COUNT(*) FROM staging.transactions
        WHERE announce_date IS NOT NULL AND close_date IS NOT NULL
          AND close_date < announce_date
        """
    ).fetchone()[0]
    conn.close()
    return AssetCheckResult(
        passed=bad == 0,
        metadata={"invalid_date_pairs": MetadataValue.int(int(bad))},
    )


@asset_check(
    asset=stg_transactions,
    description="Business rule DQ: if outcome is Closed, close_date must be populated (deal completed).",
)
def stg_transactions_closed_requires_close_date(
    context: AssetCheckExecutionContext, warehouse: DuckDBWarehouseResource
) -> AssetCheckResult:
    conn = warehouse.connect()
    bad = conn.execute(
        """
        SELECT COUNT(*) FROM staging.transactions
        WHERE lower(trim(coalesce(outcome, ''))) = 'closed'
          AND close_date IS NULL
        """
    ).fetchone()[0]
    conn.close()
    return AssetCheckResult(
        passed=bad == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"closed_without_close_date": MetadataValue.int(int(bad))},
    )


@asset_check(
    asset=stg_transactions,
    description="Business rule DQ: when announce_date and close_date are both set, days_to_close must be populated.",
)
def stg_transactions_dates_require_days_to_close(
    context: AssetCheckExecutionContext, warehouse: DuckDBWarehouseResource
) -> AssetCheckResult:
    conn = warehouse.connect()
    bad = conn.execute(
        """
        SELECT COUNT(*) FROM staging.transactions
        WHERE announce_date IS NOT NULL
          AND close_date IS NOT NULL
          AND days_to_close IS NULL
        """
    ).fetchone()[0]
    conn.close()
    return AssetCheckResult(
        passed=bad == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"both_dates_but_null_days_to_close": MetadataValue.int(int(bad))},
    )


@asset_check(asset=stg_targets, description="target_id unique.")
def stg_targets_unique_ids(
    context: AssetCheckExecutionContext, warehouse: DuckDBWarehouseResource
) -> AssetCheckResult:
    conn = warehouse.connect()
    dupes = conn.execute(
        """
        SELECT COUNT(*) FROM (
          SELECT target_id FROM staging.targets GROUP BY 1 HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    conn.close()
    return AssetCheckResult(passed=dupes == 0, metadata={"dup_groups": MetadataValue.int(int(dupes))})


@asset_check(asset=stg_acquirers, description="acquirer_id unique.")
def stg_acquirers_unique_ids(
    context: AssetCheckExecutionContext, warehouse: DuckDBWarehouseResource
) -> AssetCheckResult:
    conn = warehouse.connect()
    dupes = conn.execute(
        """
        SELECT COUNT(*) FROM (
          SELECT acquirer_id FROM staging.acquirers GROUP BY 1 HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    conn.close()
    return AssetCheckResult(passed=dupes == 0, metadata={"dup_groups": MetadataValue.int(int(dupes))})
