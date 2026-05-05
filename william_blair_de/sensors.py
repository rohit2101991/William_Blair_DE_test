"""File-change sensor: CSV mtime changes request downstream runs.

Runs are tagged ``wb_data_refresh=1``. While any prior sensor-launched run is still
queued or executing, we skip yielding new requests so refreshes do not pile up.
"""

import json
import os
from pathlib import Path

from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunRequest,
    RunsFilter,
    SensorEvaluationContext,
    sensor,
)

from william_blair_de.assets.analytics import (
    dim_acquirer_activity,
    fct_transactions,
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
from william_blair_de.partitions import DEAL_YEAR_PARTITIONS

# Attached to every RunRequest from this sensor so we can detect in-flight refreshes.
WB_DATA_REFRESH_TAG_KEY = "wb_data_refresh"
WB_DATA_REFRESH_TAG_VAL = "1"


def _sensor_refresh_in_flight(context: SensorEvaluationContext) -> bool:
    """True if a prior sensor-triggered run is still queued or running."""
    recs = context.instance.get_run_records(
        filters=RunsFilter(
            tags={WB_DATA_REFRESH_TAG_KEY: WB_DATA_REFRESH_TAG_VAL},
            statuses=[
                DagsterRunStatus.QUEUED,
                DagsterRunStatus.NOT_STARTED,
                DagsterRunStatus.STARTING,
                DagsterRunStatus.STARTED,
                DagsterRunStatus.CANCELING,
            ],
        ),
        limit=1,
    )
    return len(recs) > 0


def _data_dir_path() -> Path:
    rel = os.environ.get("WB_DATA_DIR", "data")
    p = Path(rel)
    return p if p.is_absolute() else Path.cwd() / p


def _snapshot_mtimes() -> dict[str, float]:
    out: dict[str, float] = {}
    d = _data_dir_path()
    if not d.is_dir():
        return out
    for name in (
        "acquirers.csv",
        "targets.csv",
        "transactions.csv",
        "acquirer_financials.csv",
        "sector_multiples.csv",
    ):
        fp = d / name
        if fp.exists():
            out[name] = fp.stat().st_mtime
    return out


@sensor(
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
    description="When CSV mtimes under ./data change, request a full raw→stage→model run.",
)
def data_files_changed_sensor(context: SensorEvaluationContext):
    snap = _snapshot_mtimes()
    if not snap:
        return

    # Cursor stores prior mtime snapshot so we only trigger on change.
    state = json.loads(context.cursor) if context.cursor else {}
    if state.get("mtimes") == snap:
        return

    if _sensor_refresh_in_flight(context):
        return

    context.update_cursor(json.dumps({"mtimes": snap}))

    # Stable hash becomes part of run_key for idempotent scheduling.
    h = abs(hash(tuple(sorted(snap.items()))))
    run_tags = {WB_DATA_REFRESH_TAG_KEY: WB_DATA_REFRESH_TAG_VAL}
    yield RunRequest(
        run_key=f"data_refresh_core_{h}",
        tags=run_tags,
        asset_selection=[
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
        ],
    )
    for pk in DEAL_YEAR_PARTITIONS.get_partition_keys():
        yield RunRequest(
            run_key=f"data_refresh_fct_{pk}_{h}",
            tags=run_tags,
            partition_key=pk,
            asset_selection=[fct_transactions],
        )
