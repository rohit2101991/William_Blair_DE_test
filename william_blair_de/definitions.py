"""Dagster code location wiring: assets, checks, resources, schedule, sensor."""

from dagster import Definitions, in_process_executor, load_asset_checks_from_modules, load_assets_from_modules

from william_blair_de.assets import analytics, business_checks, checks, dimensions, raw, staging
from william_blair_de.resources import DataDirResource, DuckDBWarehouseResource
from william_blair_de.schedules import daily_core_ct_schedule
from william_blair_de.sensors import data_files_changed_sensor

all_assets = load_assets_from_modules([raw, staging, dimensions, analytics])
# Keep checks split by theme: structural checks + business semantics.
all_checks = load_asset_checks_from_modules([checks, business_checks])

# Single Definitions object exported for Dagster to load.
defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    resources={
        "warehouse": DuckDBWarehouseResource(),
        "data_dir": DataDirResource(),
    },
    schedules=[daily_core_ct_schedule],
    sensors=[data_files_changed_sensor],
    # Single DuckDB file cannot be written from multiple processes (default multiprocess executor).
    executor=in_process_executor,
)
