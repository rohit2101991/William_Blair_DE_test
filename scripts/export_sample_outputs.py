"""Export analytics-layer tables from the configured DuckDB warehouse into sample_output_data/ as CSV.

Steps performed by ``main()``:
1. Resolve warehouse path via ``resolve_warehouse_duckdb_path`` (env-driven, same as Dagster resource).
2. Fail fast if the file does not exist (nothing materialized yet).
3. Ensure ``sample_output_data/`` exists.
4. For each (relation, filename) in ``EXPORTS``, run ``COPY (SELECT * ...) TO`` as CSV with header.
5. Append row counts to a small ``MANIFEST.txt`` for reviewers.

Run from repo root after materializing assets:
  python scripts/export_sample_outputs.py

Requires: resolved warehouse file with analytics.fct_transactions,
          analytics.dim_acquirer_activity, analytics.rpt_sector_trend_summary
"""

import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from william_blair_de.resources import resolve_warehouse_duckdb_path

OUT_DIR = ROOT / "sample_output_data"

EXPORTS = [
    ("analytics.fct_transactions", "fct_transactions.csv"),
    ("analytics.dim_acquirer_activity", "dim_acquirer_activity.csv"),
    ("analytics.rpt_sector_trend_summary", "rpt_sector_trend_summary.csv"),
]


def main() -> None:
    # ``resolve_warehouse_duckdb_path`` reads WB_WAREHOUSE_PROFILE and path overrides (see resources.py).
    db_path = resolve_warehouse_duckdb_path()
    if not db_path.exists():
        raise SystemExit(
            f"Missing {db_path}. Materialize assets first (dagster dev or CLI), then re-run this script."
        )
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    # Human-readable manifest lines collected while exporting.
    lines = [
        "sample_output_data/: CSV snapshots of analytics.* for reviewers (see README).",
        "Regenerate: materialize all assets, then: python scripts/export_sample_outputs.py",
        "",
    ]
    # read_only avoids accidental locks during review exports.
    con = duckdb.connect(str(db_path), read_only=True)
    for relation, filename in EXPORTS:
        dest = OUT_DIR / filename
        con.execute(
            f"COPY (SELECT * FROM {relation}) TO ? (HEADER, DELIMITER ',')",
            [str(dest)],
        )
        n = con.execute(f"SELECT COUNT(*) FROM {relation}").fetchone()[0]
        print(f"Wrote {dest} ({n} rows)")
        lines.append(f"  {filename}  <-  {relation}  ({n} rows)")
    con.close()
    manifest = OUT_DIR / "MANIFEST.txt"
    manifest.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {manifest}")


if __name__ == "__main__":
    main()
