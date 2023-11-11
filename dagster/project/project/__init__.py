import os

from dagster_duckdb_pandas import DuckDBPandasIOManager

from dagster import Definitions, FilesystemIOManager, load_assets_from_modules

from .assets import salaries
from .jobs import salaries_job
from .resources import SalaryAPIResource
from .schedules import salaries_schedule

salary_assets = load_assets_from_modules([salaries])

deployment_name = os.getenv("DAGSTER_DEPLOYMENT", "local")
deployment_resources = {
    "local": {
        "salary_api": SalaryAPIResource(),
        "io_manager": FilesystemIOManager(base_dir="data"),
        "db_io_manager": DuckDBPandasIOManager(
            database="data/salaries.duckdb", schema="hr"
        ),
    },
    "production": {
        "salary_api": SalaryAPIResource(),
        "io_manager": FilesystemIOManager(base_dir="data"),
        "db_io_manager": DuckDBPandasIOManager(
            database="data/salaries.duckdb", schema="hr"
        ),
    },
}

defs = Definitions(
    assets=salary_assets,
    schedules=[salaries_schedule],
    resources=deployment_resources[deployment_name],
    jobs=[salaries_job],
)
