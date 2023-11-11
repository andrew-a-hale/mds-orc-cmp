from dagster import AssetSelection, define_asset_job

salaries_job = define_asset_job(
    name="salaries_job", selection=AssetSelection.groups("salaries")
)
