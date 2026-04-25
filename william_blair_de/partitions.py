from dagster import StaticPartitionsDefinition

# Distinct deal years present in the provided transactions extract.
DEAL_YEAR_PARTITIONS = StaticPartitionsDefinition([str(y) for y in range(2015, 2025)])
