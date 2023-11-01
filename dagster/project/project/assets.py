import pandas as pd
from dagster import asset, get_dagster_logger, AssetExecutionContext, MetadataValue
from .resources import SalaryAPIResource, Salary
import time
from typing import List


@asset(
    group_name="salaries",
    io_manager_key="io_manager",
)
def salaries(salary_api: SalaryAPIResource) -> List[Salary]:
    salaries = salary_api.get_salaries_for_countries()
    return salaries


@asset(
    group_name="salaries",
    io_manager_key="db_io_manager",
)
def load_salaries(context: AssetExecutionContext, salaries: List[Salary]) -> pd.DataFrame:
    logger = get_dagster_logger()

    results = []
    for salary in salaries:
        logger.info(f"reading: {salary.title} info")
        results.append(salary.to_dict())

    df = pd.DataFrame(results)
    context.add_output_metadata(
        metadata={
            "head": MetadataValue.md(df.head().to_markdown()),
        }
    )

    return df
