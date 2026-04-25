"""Export analytics-layer tables from warehouse.duckdb into sample_output_data/ as CSV.

Run from repo root after materializing assets:
  python scripts/export_sample_outputs.py

Requires: ./warehouse.duckdb with schemas analytics.fct_transactions,
          analytics.dim_acquirer_activity, analytics.rpt_sector_trend_summary
"""

from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "warehouse.duckdb"
OUT_DIR = ROOT / "sample_output_data"

EXPORTS = [
    ("analytics.fct_transactions", "fct_transactions.csv"),
    ("analytics.dim_acquirer_activity", "dim_acquirer_activity.csv"),
    ("analytics.rpt_sector_trend_summary", "rpt_sector_trend_summary.csv"),
]


def main() -> None:
    if not DB.exists():
        raise SystemExit(
            f"Missing {DB}. Materialize assets first (dagster dev or CLI), then re-run this script."
        )
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    lines = [
        "sample_output_data/: CSV snapshots of analytics.* for reviewers (see README).",
        "Regenerate: materialize all assets, then: python scripts/export_sample_outputs.py",
        "",
    ]
    con = duckdb.connect(str(DB), read_only=True)
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
