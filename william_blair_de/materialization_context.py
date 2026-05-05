"""Run-level values for materialized tables (e.g. load / data date from Dagster).

``materialization_data_date`` answers: "what calendar date should we stamp on rows for this run?"
It prefers the Dagster run record's timestamps so scheduled jobs get consistent dates.

``ensure_analytics_*`` helpers support in-place warehouse upgrades when new columns are added.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

from dagster import AssetExecutionContext


def materialization_data_date(context: AssetExecutionContext) -> date:
    """Calendar date to stamp model rows: prefer this run's start time in UTC, else run creation, else today.

    Scheduled daily runs (e.g. ``daily_core_ct``) will usually have ``RunRecord.start_time`` set once the
    run starts. Ad-hoc or test materializations may fall back to ``create_timestamp`` or the local date.
    """
    try:
        # DagsterInstance may be None in some test harnesses — guard access.
        inst = context.instance
        rid = context.run_id
        if inst is not None and rid:
            rec = inst.get_run_record_by_id(rid)
            if rec is not None:
                # Primary: wall-clock start of the run (UTC date).
                if rec.start_time is not None:
                    return datetime.fromtimestamp(rec.start_time, tz=timezone.utc).date()
                ct = rec.create_timestamp
                if ct is not None:
                    # Fallback: enqueue time — normalize to UTC if timezone-aware.
                    if ct.tzinfo is not None:
                        return ct.astimezone(timezone.utc).date()
                    return ct.date()
    except Exception:
        # Any catalog/API failure should not break asset materialization — use today.
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


def ensure_analytics_row_content_hash_column(conn, table: str) -> None:
    """Add ``row_content_hash`` (MD5 of business attributes) if the analytics table predates hashing."""
    row = conn.execute(
        """
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'analytics' AND table_name = ? AND column_name = 'row_content_hash'
        """,
        [table],
    ).fetchone()
    if row is None:
        conn.execute(f"ALTER TABLE analytics.{table} ADD COLUMN row_content_hash VARCHAR")
