from dagster import file_relative_path
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project
from ...utils.constants import DBT_PROJECT_DIR, DBT_PROFILES_DIR


assets = load_assets_from_dbt_project(
    DBT_PROJECT_DIR,
    DBT_PROFILES_DIR,
    source_key_prefix=["raw"],
    select="tag:ipttc"
)
