"""Partition keys for ``fct_transactions`` (one Dagster run per calendar year)."""

from dagster import StaticPartitionsDefinition

# Static keys 2015–2024: each backfill/materialize supplies a single ``deal_year`` partition.
DEAL_YEAR_PARTITIONS = StaticPartitionsDefinition([str(y) for y in range(2015, 2025)])
