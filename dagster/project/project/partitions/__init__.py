import os

from dagster import StaticPartitionsDefinition

salaries_partition = StaticPartitionsDefinition(os.getenv("COUNTRIES").split(","))
