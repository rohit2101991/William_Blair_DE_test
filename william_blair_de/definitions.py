"""Dagster code location: register assets, checks, resources, automation, and executor.

This module is the single entrypoint Dagster loads (``defs``). It wires:

- **Assets**: raw CSV ingest → staging transforms → analytics dimensions/facts/reports.
- **Asset checks**: run after selected assets; failures show on that asset in the UI.
- **Resources**: ``warehouse`` (DuckDB file) and ``data_dir`` (CSV folder).
- **Schedules / sensors**: optional automation (both stopped by default in this repo).
- **Executor**: ``in_process_executor`` so only one process writes DuckDB (file lock).

``load_assets_from_modules`` order is not execution order; the asset graph defines run order.
``in_process_executor`` keeps a single DuckDB writer (one process holds the file lock).
"""

# Definitions, executor, and helpers to load assets/checks from Python modules by package path.
from dagster import Definitions, in_process_executor, load_asset_checks_from_modules, load_assets_from_modules

# Asset modules: each file defines @asset callables; Dagster builds the dependency graph from deps=.
from william_blair_de.assets import analytics, business_checks, checks, dimensions, raw, staging
# ConfigurableResource subclasses for warehouse path and CSV directory.
from william_blair_de.resources import DataDirResource, DuckDBWarehouseResource
# Time-based job (daily core refresh); default OFF until enabled in UI.
from william_blair_de.schedules import daily_core_ct_schedule
# File-mtime sensor that can enqueue runs when ./data CSVs change; default OFF in UI.
from william_blair_de.sensors import data_files_changed_sensor

# Discover every @asset in the listed modules and register them with Dagster.
all_assets = load_assets_from_modules([raw, staging, dimensions, analytics])
# Load @asset_check callables from checks (structural/FK) and business_checks (plausibility).
all_checks = load_asset_checks_from_modules([checks, business_checks])

# Single Definitions object exported for Dagster to load (``dagster dev`` / code locations).
defs = Definitions(
    # All tables/models in the pipeline.
    assets=all_assets,
    # Quality gates attached to staging (and similar) assets.
    asset_checks=all_checks,
    # Injected into every asset/check that declares a matching resource parameter name.
    resources={
        "warehouse": DuckDBWarehouseResource(),  # DuckDB connection factory + path resolution.
        "data_dir": DataDirResource(),  # Root folder for read_csv_auto source files.
    },
    # Cron schedule(s); sensor(s) for event-driven runs.
    schedules=[daily_core_ct_schedule],
    sensors=[data_files_changed_sensor],
    # Single DuckDB file cannot be written from multiple processes (default multiprocess executor).
    executor=in_process_executor,
)
