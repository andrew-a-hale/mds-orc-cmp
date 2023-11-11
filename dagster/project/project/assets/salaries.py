import os
import base64 as b64
from typing import List

import pandas as pd
import plotly.express as px
import plotly.io as pio

from dagster import AssetExecutionContext, MetadataValue, asset, get_dagster_logger

from ..partitions import salaries_partition
from ..resources import Salary, SalaryAPIResource


@asset(
    group_name="salaries",
    io_manager_key="io_manager",
    partitions_def=salaries_partition,
)
def salaries_file(
    context: AssetExecutionContext, salary_api: SalaryAPIResource
) -> List[Salary]:
    country = context.asset_partition_key_for_output()
    salaries = salary_api.get_salaries(country)
    return salaries


@asset(
    group_name="salaries",
    io_manager_key="db_io_manager",
)
def salaries(context: AssetExecutionContext, salaries_file: dict) -> pd.DataFrame:
    logger = get_dagster_logger()

    results = []
    for partition, salaries in salaries_file.items():
        logger.info(f"reading: {partition}")
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


@asset(
    group_name="salaries",
    io_manager_key="db_io_manager",
)
def salaries_by_country(salaries: pd.DataFrame) -> pd.DataFrame:
    return salaries.groupby("country", as_index=False).mean(numeric_only=True)


@asset(
    group_name="salaries",
    io_manager_key="db_io_manager",
)
def salaries_plot(
    context: AssetExecutionContext, salaries_by_country: pd.DataFrame
) -> None:
    fig = px.bar(
        salaries_by_country,
        x="country",
        y="p50",
        title="salary by country",
    )

    file_path = os.path.join("data/salaries.jpeg")
    pio.write_image(fig, file_path)

    with open(file_path, "rb") as f:
        image_data = f.read()

    b64_data = b64.b64encode(image_data).decode("utf-8")
    md = f"![Image](data:image/jpeg;base64,{b64_data})"
    context.add_output_metadata({"preview": MetadataValue.md(md)})
