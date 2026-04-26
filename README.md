# William Blair DE take-home (Dagster + DuckDB)

Runnable Dagster OSS project: **ingest → stage → model** for the five M&A CSVs, with **asset checks**, **`deal_year` partitions** on `fct_transactions`, a **sensor** on `./data` CSV mtimes, and **environment-based** DuckDB path configuration.

## Quick start

```bash
cd William_Blair_DE_test
python3 -m venv .venv && source .venv/bin/activate
pip install -e .
dagster dev
```

**Runs in the UI:** Recent Dagster versions use a **throwaway storage folder** when `DAGSTER_HOME` is unset, so terminal `dagster asset materialize` runs may not show up next to `dagster dev`. Use the same metadata for both, e.g. `export DAGSTER_HOME="$HOME/.dagster"` in the shell before `dagster dev` and before any CLI materialize (a `dagster.yaml` there is optional; telemetry can be disabled there).

From the UI, materialize **raw → staging →** `dim_acquirer` / `dim_target` (SCD1 merge) **→** `dim_acquirer_activity` and `rpt_sector_trend_summary`, then materialize **`fct_transactions` for each `deal_year` partition** (2015–2024), or use the CLI:

```bash
dagster asset materialize -m william_blair_de.definitions --select "raw_acquirers,raw_targets,raw_transactions,raw_acquirer_financials,raw_sector_multiples,stg_acquirers,stg_targets,stg_transactions,stg_acquirer_financials,stg_sector_multiples,dim_acquirer,dim_target,dim_acquirer_activity,rpt_sector_trend_summary"
for y in 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024; do
  dagster asset materialize -m william_blair_de.definitions --select fct_transactions --partition "$y"
done
```

**Data:** place the five CSVs under `./data` (already copied for local dev). Override directory with env `WB_DATA_DIR`.

**Warehouse:** default file `./warehouse.duckdb`. Override with `WB_DUCKDB_PATH` for a “production” path without changing code.

**Reset warehouse (drop all loaded schemas):** `python scripts/reset_warehouse.py` — then materialize again from raw. Uses the same path rules as `WB_DUCKDB_PATH` / `warehouse.duckdb`.

## Sample output data (for reviewers)

The folder **`sample_output_data/`** holds **CSV snapshots** of the three **modeled `analytics` tables** so interviewers can inspect results **without** opening DuckDB first:

| File | DuckDB source | Role |
|------|----------------|------|
| `fct_transactions.csv` | `analytics.fct_transactions` | Denormalized **fact** table (transactions + target + acquirer + benchmarks). |
| `dim_acquirer_activity.csv` | `analytics.dim_acquirer_activity` | **Acquirer-level** summary (deal counts, volumes, sectors, dates). |
| `rpt_sector_trend_summary.csv` | `analytics.rpt_sector_trend_summary` | **Sector × year** activity rollup. |

**How to view**

- **Spreadsheet:** Open any `.csv` in Excel, Google Sheets, or Apple Numbers (UTF-8, comma-separated, header row).
- **CLI:** `head -5 sample_output_data/fct_transactions.csv`
- **DuckDB (read-only on the CSV):**  
  `duckdb -c "SELECT * FROM read_csv_auto('sample_output_data/fct_transactions.csv') LIMIT 10"`
- **Python:**  
  `import pandas as pd; pd.read_csv("sample_output_data/fct_transactions.csv").head()`

**Regenerate after you change model SQL or data**

1. Materialize all assets (see **Quick start** loop above so every `deal_year` partition of `fct_transactions` is built).
2. Run:

```bash
python scripts/export_sample_outputs.py
```

That reads `./warehouse.duckdb` and overwrites the CSVs in `sample_output_data/`. The DuckDB file itself stays **gitignored**; these CSVs are **committed** so the repo carries a static preview aligned with the submission instructions (“sample output or materialized data you want us to see”).

## Why DuckDB (and not Delta for this repo)

**DuckDB** is an **embedded analytical database**: a library you link from Python, storing tables in a **single file** (or in memory). You run **SQL** locally with no database server, which matches the take-home constraint (“no Postgres / Snowflake”). It is strong for **joining staged CSV-derived tables**, **aggregations**, and **ad hoc QA** with minimal setup.

**Delta Lake** is a **table format** (Parquet + transaction log) with **ACID writes, time travel, and scalable incremental** processing. It is a good fit when many writers/readers hit object storage (e.g. S3) or Spark/Databricks is in play. For this assessment, Delta adds **dependencies and I/O patterns** (`deltalake` / Spark) that are not required to demonstrate Dagster; **DuckDB keeps the stack small** and avoids file-lock issues with multiple writers unless you add a careful pattern.

**Verdict:** You *can* use Delta for stage/gold (e.g. `deltalake.write_deltalake` per asset) entirely locally; it is valid but **heavier** than DuckDB for a 4–5 hour exercise. This project uses **DuckDB schemas** `raw`, `staging`, and `analytics` inside one file for clarity.

## Architecture

| Layer    | Dagster `group_name` | DuckDB schema   | Notes |
|----------|----------------------|-----------------|--------|
| Ingest   | `ingest`             | `raw`           | `read_csv_auto(..., all_varchar=true)` — preserve source strings. |
| Stage    | `stage`              | `staging`       | Casts, trims, quarantine table for bad transaction rows. |
| Model    | `model`              | `analytics`     | **SCD1:** `dim_acquirer`, `dim_target`, `dim_acquirer_activity` (merge/upsert on business keys). **Facts / reports:** `fct_transactions` (partitioned by `deal_year`), `rpt_sector_trend_summary` (full refresh). |

**Executor:** `Definitions(executor=in_process_executor)` so **one process** holds the DuckDB file lock (default multiprocess executor conflicts with a single-writer DuckDB file).

**Partitions:** `fct_transactions` uses `StaticPartitionsDefinition` on calendar years 2015–2024. Each run **deletes then inserts** rows for that year into `analytics.fct_transactions` so you can refresh one year without rebuilding others.

**Sensor:** `data_files_changed_sensor` compares each CSV’s **mtime** (last modified time on disk) under `./data`. When any of those mtimes **change** (file replaced, saved, or copied in), the sensor infers “new data” and can request refresh runs (core assets + one run per `deal_year` for `fct_transactions`). It ships **`STOPPED` by default** — enable under **Overview → Sensors** when demoing.

**Schedule:** `daily_core_ct` runs job **`daily_core_refresh`** at **03:00 America/Chicago** (`0 3 * * *`). It materializes **ingest → stage →** `dim_acquirer`, `dim_target`, `dim_acquirer_activity`, and `rpt_sector_trend_summary` (not **`fct_transactions`**, which needs partition keys—materialize that from the UI or CLI backfill). The schedule is **`STOPPED` by default**; turn it on under **Overview → Schedules** for walkthroughs so interviewers see a cron definition without surprise overnight runs on a fresh clone.

## Data quality (high level)

- **Staging quarantine:** `staging.transactions_quarantine` captures rows with missing `transaction_id` or negative `deal_size_mm`.
- **Asset checks:** uniqueness on dimension IDs, FK presence from `stg_transactions` to acquirers/targets, date ordering `close_date >= announce_date`, and **business-rule DQ**: `Closed` outcome requires `close_date`; if both `announce_date` and `close_date` are set, `days_to_close` must be populated (`william_blair_de/assets/checks.py`). **Blank `close_date` for non-Closed deals** (e.g. Pending) is normal, not an error.
- **Inventory (Excel):** see `DATA_QUALITY.xlsx` (sheets: **DQ inventory**, **Raw ingest fidelity**, **Post-materialization SQL**). Regenerate after edits: `python scripts/build_data_quality_xlsx.py`.
- **Non-negative / sign business checks:** `william_blair_de/assets/business_checks.py` plus column rationale and stakeholder semantic ideas in `BUSINESS_CHECKS.md`.

## Known limitations

- **DuckDB + multiprocess:** do not remove `in_process_executor` unless you switch storage (e.g. per-run temp DBs merged externally, or Delta with proper concurrent-write semantics).
- **`fct_transactions` CLI:** `dagster asset materialize --select "*"` fails because partitions must be supplied; use the loop above or the UI backfill.
- **Acquirer financials join:** matched on `fiscal_year = EXTRACT(YEAR FROM announce_date)`; misaligned fiscal vs calendar years are a known simplification.

## References

- [Dagster OSS](https://github.com/dagster-io/dagster)
- [Dagster quickstart](https://docs.dagster.io/getting-started/quickstart)
- [DuckDB](https://duckdb.org/docs/)
