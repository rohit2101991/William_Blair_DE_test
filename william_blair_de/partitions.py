"""Partition keys for ``fct_transactions`` (one Dagster run per calendar year).

``StaticPartitionsDefinition`` fixes the set of keys at code load time. Each key is a string year.
Downstream: ``context.partition_key`` in ``fct_transactions`` becomes ``deal_year`` filter input.

The sensor and any backfill iterate ``DEAL_YEAR_PARTITIONS.get_partition_keys()`` to enqueue one
materialization per year (2015 through 2024 inclusive).
"""

from dagster import StaticPartitionsDefinition

# Static keys 2015–2024: each backfill/materialize supplies a single ``deal_year`` partition.
DEAL_YEAR_PARTITIONS = StaticPartitionsDefinition([str(y) for y in range(2015, 2025)])
