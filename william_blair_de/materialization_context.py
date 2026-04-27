"""Run-level values for materialized tables (e.g. load / data date from Dagster)."""

from __future__ import annotations

from datetime import date, datetime, timezone

from dagster import AssetExecutionContext


def materialization_data_date(context: AssetExecutionContext) -> date:
    """Calendar date to stamp model rows: prefer this run's start time in UTC, else run creation, else today.

    Scheduled daily runs (e.g. ``daily_core_ct``) will usually have ``RunRecord.start_time`` set once the
    run starts. Ad-hoc or test materializations may fall back to ``create_timestamp`` or the local date.
    """
    try:
        inst = context.instance
        rid = context.run_id
        if inst is not None and rid:
            rec = inst.get_run_record_by_id(rid)
            if rec is not None:
                if rec.start_time is not None:
                    return datetime.fromtimestamp(rec.start_time, tz=timezone.utc).date()
                ct = rec.create_timestamp
                if ct is not None:
                    if ct.tzinfo is not None:
                        return ct.astimezone(timezone.utc).date()
                    return ct.date()
    except Exception:
        pass
    return date.today()


def sql_data_date_literal(d: date) -> str:
    """Safe single-date literal for DuckDB SQL (value is a Python ``date``, not user text)."""
    return f"DATE '{d.isoformat()}'"


def ensure_analytics_data_date_column(conn, table: str) -> None:
    """Add ``data_date`` to an existing analytics table if missing (upgrades in-place)."""
    row = conn.execute(
        """
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'analytics' AND table_name = ? AND column_name = 'data_date'
        """,
        [table],
    ).fetchone()
    if row is None:
        conn.execute(f"ALTER TABLE analytics.{table} ADD COLUMN data_date DATE")
