import json
import os
from pathlib import Path

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

from ..partitions import daily_partition
from ..resources import dbt_resource
from .constants import DBT_DIRECTORY

INCREMENTAL_SELECTOR = "config.materialized:incremental"

class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        dbt_resource_type = dbt_resource_props["resource_type"]
        if dbt_resource_type != "source":
            return super().get_asset_key(dbt_resource_props)
        dbt_resource_name = dbt_resource_props["name"]
        return AssetKey(f"taxi_{dbt_resource_name}")

    def get_group_name(self, dbt_resource_props):
        return dbt_resource_props["fqn"][1]


if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
    dbt_manifest_path = (
        dbt_resource.cli(
            ["--quiet", "parse"],
            target_path=Path("target"),
        )
        .wait()
        .target_path.joinpath("manifest.json")
    )
else:
    dbt_manifest_path = os.path.join(DBT_DIRECTORY, "target", "manifest.json")


@dbt_assets(
    manifest=dbt_manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
    exclude=INCREMENTAL_SELECTOR,
)
def dbt_analytics(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


@dbt_assets(
    manifest=dbt_manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
    select=INCREMENTAL_SELECTOR,     # select only models with INCREMENTAL_SELECTOR
    partitions_def=daily_partition   # partition those models using daily_partition
)
def incremental_dbt_models(
    context: AssetExecutionContext,
    dbt: DbtCliResource
):
    time_window = context.partition_time_window
    dbt_vars = {
        "min_date": time_window.start.isoformat(),
        "max_date": time_window.end.isoformat()
    }
    yield from dbt.cli(["build", "--vars", json.dumps(dbt_vars)], context=context).stream()
