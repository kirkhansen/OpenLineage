# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Optional

import requests

from dagster import (
    AssetKey,
    DagsterInstance,
    EventLogRecord,
    EventRecordsFilter,
)
from dagster._core.events import DagsterEventType  # type: ignore
from openlineage.client.run import InputDataset, OutputDataset, Dataset


NOMINAL_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

# TODO: should be able: int | None = None to glean this based on environment/DagsterInstance context
GRAPHQL_HOST = "http://localhost:3001/graphql"

ASSET_NODE_QUERY = """
query AssetNodes($pipelineSelector: PipelineSelector!) {
  assetNodes(
    pipeline: $pipelineSelector
  ) {
    id
    ...assetPath
    dependedBy {
      asset{
        ...assetPath
      }
    }
    dependencies {
      asset {
        ...assetPath
      }
    }
  }
}

fragment assetPath on AssetNode {
  assetKey {
    path
  }
}
"""


@dataclass
class Repository:
    name: str | None
    location: str | None


def to_utc_iso_8601(timestamp: float) -> str:
    return datetime.utcfromtimestamp(timestamp).strftime(NOMINAL_TIME_FORMAT)


def make_step_run_id() -> str:
    return str(uuid.uuid4())


def make_step_job_name(pipeline_name: str, step_key: str) -> str:
    return f"{pipeline_name}.{step_key}"

def get_asset_record_dependencies(repository_name: str, repository_location: str, pipeline_name: str,) -> dict:
    """
    Hits graphql endpoint to fetch the asset records and their dependencies to
    add on to the dagster events for dataset definitions with ins/outs.
    """
    # TODO:  filter out based on running pipelines ?
    # pipeline_ids = running_pipelines.keys()
    # TODO: cache grabbing asset nodes, or turn into single queries per asset?
    # TODO: should inputs and outputs be openlineage Dataset objects? i think so. simplifies some things downstream, i think
    query_params = {"pipelineSelector": {"pipelineName": pipeline_name, "repositoryName": repository_name, "repositoryLocationName": repository_location}}
    assets_nodes_response = requests.post(GRAPHQL_HOST, json={"query": ASSET_NODE_QUERY, "variables": query_params}).json()
    asset_nodes = assets_nodes_response["data"]["assetNodes"]
    asset_node_lookup = {
            AssetKey(asset_node["assetKey"]["path"]): {
                "input_datasets": [Dataset(namespace=repository_name, name=AssetKey(dep["asset"]["assetKey"]["path"]).to_python_identifier()) for dep in asset_node["dependencies"]],
                "output_datasets": [Dataset(namespace=repository_name, name=AssetKey(asset_node["assetKey"]["path"]).to_python_identifier())]
            }
        for asset_node in asset_nodes
    }
    return asset_node_lookup


def get_event_log_records(
    instance: DagsterInstance,
    event_types: set[DagsterEventType],
    run_updated_after: float,
    record_filter_limit: Optional[int] = None,
) -> Iterable[EventLogRecord]:
    """Returns a list of Dagster event log records in ascending order
    from the instance's event log storage.
    :param instance: active instance to get records from
    :param event_type: Event type to filter out
    :param record_filter_limit: maximum number of event logs to retrieve
    :return: iterable of Dagster event log records
    """
    for event_type in event_types:
        for event_record in instance.get_event_records(
            EventRecordsFilter(
                event_type=event_type,
                after_timestamp=run_updated_after,
            ),
            limit=record_filter_limit,
            ascending=True,
        ):
            yield event_record


def get_repository(
    instance: DagsterInstance, pipeline_run_id: str
) -> Repository:
    """Returns a Repository dataclass
    :param instance: active instance to get the pipeline run
    :param pipeline_run_id: run id to look up its run
    :return: Repository dataclass
    """
    pipeline_run = instance.get_run_by_id(pipeline_run_id)
    name = None
    location = None
    if pipeline_run:
        ext_pipeline_origin = pipeline_run.external_pipeline_origin
        if ext_pipeline_origin and ext_pipeline_origin.external_repository_origin:
            name = ext_pipeline_origin.external_repository_origin.repository_name
            location = ext_pipeline_origin.external_repository_origin.repository_location_origin.location_name
    return Repository(name=name, location=location)
