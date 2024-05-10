from dagster import (Definitions,
                     load_assets_from_package_module,
                     define_asset_job,
                     ScheduleDefinition)
from dagster_dbt import dbt_cli_resource
from dagster_snowflake_pandas import snowflake_pandas_io_manager
from .assets import (dbt as dbt_assets,
                     ipttc as ipttc_api_assets)
from .utils.constants import (DBT_CONFIG,
                              SNOWFLAKE_RAW_CONFIG,
                              SNOWFLAKE_TRANSFORM_CONFIG)


all_assets = [
    *load_assets_from_package_module(dbt_assets),
    *load_assets_from_package_module(ipttc_api_assets)
]

materialise_all_job = define_asset_job("all_assets", selection="*")

defs = Definitions(
    assets=all_assets,
    jobs=[materialise_all_job],
    schedules=[
        ScheduleDefinition(
            job=materialise_all_job,
            cron_schedule="0 18 * * 3",
            execution_timezone="Australia/Sydney"
        ),
    ],
    resources={
        # "airbyte":airbyte_instance,
        "dbt": dbt_cli_resource.configured(DBT_CONFIG),
        "snowflake_raw":
        snowflake_pandas_io_manager.configured(SNOWFLAKE_RAW_CONFIG),
        "snowflake_transform":
        snowflake_pandas_io_manager.configured(SNOWFLAKE_TRANSFORM_CONFIG)
    }
)
