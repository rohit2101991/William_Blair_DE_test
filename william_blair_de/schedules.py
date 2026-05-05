"""Time-based automation: daily job for core raw→stage→model (excludes partitioned fct_transactions).

``daily_core_ct`` runs at 03:00 America/Chicago. Default status is STOPPED — enable in Dagster UI.

The job intentionally omits ``fct_transactions`` because that asset requires partition keys; use a
partitioned schedule or manual backfill for facts. See ``sensors.py`` for file-driven runs that
include all ``deal_year`` partitions.
"""

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    ScheduleDefinition,
    define_asset_job,
)

from william_blair_de.assets.analytics import (
    dim_acquirer_activity,
    fct_target_deal_sequence,
    rpt_sector_trend_summary,
)
from william_blair_de.assets.dimensions import dim_acquirer, dim_target
from william_blair_de.assets.raw import (
    raw_acquirer_financials,
    raw_acquirers,
    raw_sector_multiples,
    raw_targets,
    raw_transactions,
)
from william_blair_de.assets.staging import (
    stg_acquirer_financials,
    stg_acquirers,
    stg_sector_multiples,
    stg_targets,
    stg_transactions,
)

# Single job wrapping the non-partitioned spine of the DAG.
daily_core_refresh_job = define_asset_job(
    name="daily_core_refresh",
    selection=AssetSelection.assets(
        raw_acquirers,
        raw_targets,
        raw_transactions,
        raw_acquirer_financials,
        raw_sector_multiples,
        stg_acquirers,
        stg_targets,
        stg_transactions,
        stg_acquirer_financials,
        stg_sector_multiples,
        dim_acquirer,
        dim_target,
        dim_acquirer_activity,
        rpt_sector_trend_summary,
        fct_target_deal_sequence,
    ),
)

# Cron expression: minute hour day-of-month month day-of-week (standard 5-field cron).
daily_core_ct_schedule = ScheduleDefinition(
    name="daily_core_ct",
    job=daily_core_refresh_job,
    cron_schedule="0 3 * * *",
    execution_timezone="America/Chicago",
    default_status=DefaultScheduleStatus.STOPPED,
)
