from dagster import (
    Definitions,
    load_assets_from_modules,
    AssetSelection,
    define_asset_job,
    ScheduleDefinition,
    FilesystemIOManager,
    EnvVar
)
from dagster_duckdb_pandas import DuckDBPandasIOManager

from .resources import SalaryAPIResource
from . import assets

all_assets = load_assets_from_modules([assets])

salary_api = SalaryAPIResource(countries=EnvVar("COUNTRIES"))
salaries_job = define_asset_job("salaries_job", selection=AssetSelection.all())
salaries_schedule = ScheduleDefinition(job=salaries_job, cron_schedule="0 * * * *")
io_manager = FilesystemIOManager(base_dir="data")
db_io_manager = DuckDBPandasIOManager(database="data/salaries.duckdb")

defs = Definitions(
    assets=all_assets,
    schedules=[salaries_schedule],
    resources={"io_manager": io_manager, "db_io_manager": db_io_manager, "salary_api": salary_api},
)
