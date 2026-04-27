"""Cron schedules (interviewers often look for schedule + sensor)."""

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

# Partitioned fct_transactions is omitted: a cron run would need partition keys or a
# separate backfill. This job refreshes ingest → stage → non-partitioned models daily.
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

# 03:00 America/Chicago daily. STOPPED by default: turn on in Dagster UI → Schedules for demos.
daily_core_ct_schedule = ScheduleDefinition(
    name="daily_core_ct",
    job=daily_core_refresh_job,
    cron_schedule="0 3 * * *",
    execution_timezone="America/Chicago",
    default_status=DefaultScheduleStatus.STOPPED,
)
