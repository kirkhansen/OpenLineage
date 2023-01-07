# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Optional
from urllib.parse import urlparse

import requests

from dagster import (
    AssetKey,
    DagsterInstance,
    EventLogRecord,
    EventRecordsFilter,
)
from dagster._core.events import DagsterEventType  # type: ignore
from openlineage.client.facet import DocumentationDatasetFacet, SchemaDatasetFacet, SchemaField
from openlineage.client.run import Dataset


NOMINAL_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


PIPELINE_ASSET_QUERY = """
query PipelineAssetPaths($runId: String!) {
  pipelineRunsOrError(filter: {runIds: [$runId]}) {
    ... on Runs {
      results {
        assets {
          key {
            path
          }
        }
      }
    }
  }
}

"""


ASSET_INFO_QUERY = """
query AssetByKey($assetPath: [String!]!) {
  assetOrError(assetKey: { path: $assetPath }) {
    ... on Asset {
      definition {
        description
        metadataEntries {
          ...MetadataEntryFragment
        }
        dependencies {
          asset {
            ...AssetNodePathFragment
            assetMaterializations {
              metadataEntries {
                ...MetadataEntryFragment
              }
            }
          }
        }
        type {
          description
          metadataEntries {
            ...MetadataEntryFragment
          }
        }
        assetMaterializations {
          metadataEntries {
            ...MetadataEntryFragment
          }
        }
      }
    }
  }
}

fragment AssetNodePathFragment on AssetNode {
  assetKey {
    path
  }
}

fragment MetadataEntryFragment on MetadataEntry {
  __typename
  label
  description
  ... on PathMetadataEntry {
    path
    __typename
  }
  ... on NotebookMetadataEntry {
    path
    __typename
  }
  ... on JsonMetadataEntry {
    jsonString
    __typename
  }
  ... on UrlMetadataEntry {
    url
    __typename
  }
  ... on TextMetadataEntry {
    text
    __typename
  }
  ... on MarkdownMetadataEntry {
    mdStr
    __typename
  }
  ... on PythonArtifactMetadataEntry {
    module
    name
    __typename
  }
  ... on FloatMetadataEntry {
    floatValue
    __typename
  }
  ... on IntMetadataEntry {
    intValue
    intRepr
    __typename
  }
  ... on BoolMetadataEntry {
    boolValue
    __typename
  }
  ... on PipelineRunMetadataEntry {
    runId
    __typename
  }
  ... on AssetMetadataEntry {
    assetKey {
      path
      __typename
    }
    __typename
  }
  ... on TableMetadataEntry {
    table {
      records
      schema {
        ...TableSchemaFragment
        __typename
      }
      __typename
    }
    __typename
  }
  ... on TableSchemaMetadataEntry {
    schema {
      ...TableSchemaFragment
      __typename
    }
    __typename
  }
}

fragment TableSchemaFragment on TableSchema {
  __typename
  columns {
    name
    description
    type
    constraints {
      nullable
      unique
      other
      __typename
    }
    __typename
  }
  constraints {
    other
    __typename
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


def _get_table_schema_facet(node_response: dict) -> dict:
    """build up a SchemaDatasetFacet if a TableSchemaMetadataEntry is present on the asset"""

    # should be one per node here
    for metadata in node_response["type"]["metadataEntries"]:
        if metadata["label"] == "schema":
            schema = metadata["schema"]
            schema["description"] = node_response["type"]["description"]
            schema_fields = [SchemaField(name=column["name"], type=column["type"], description=column["description"]) for column in schema["columns"]]
            schema_facet = SchemaDatasetFacet(fields=schema_fields)
            return {"schema": schema_facet}
    return {}


def _get_documentation_facet(node_response: dict) -> dict:
    description = node_response["description"]
    documentation_facet= DocumentationDatasetFacet(description=description)
    return {"documentation": documentation_facet}


def _get_namespace_and_name_from_materialization_metadata(asset: dict) -> dict:
    if asset["assetMaterializations"]:
        latest_materialization = asset["assetMaterializations"][0]
        for metadata in latest_materialization["metadataEntries"]:
            # TODO verify other io handler types to see what the metadata
            # entries look like for the storae loaction. in the case of s3, this shows
            # up as a path object with a uri label, and a description
            if metadata["__typename"] == "PathMetadataEntry":
                uri = urlparse(metadata["path"])
                return {"namespace": f"{uri.scheme}://{uri.netloc}",
                        "name": uri.path}
    return {}



def get_asset_record_dependencies(pipeline_run_id: str, graphql_uri: str) -> dict:
    """
    Hits graphql endpoint to fetch the asset records and their dependencies to
    add on to the dagster events for dataset definitions with ins/outs.
    """
    pipeline_assets_response = requests.post(graphql_uri, json={"query": PIPELINE_ASSET_QUERY, "variables": {"runId": pipeline_run_id}}).json()
    asset_dependencies_lookup = {}

    for asset in pipeline_assets_response["data"]["pipelineRunsOrError"]["results"][0]["assets"]:
        asset_key = AssetKey.from_graphql_input(asset["key"])
        asset_info_response = requests.post(graphql_uri, json={"query": ASSET_INFO_QUERY, "variables": {"assetPath": asset_key.path}}).json()
        asset_info = asset_info_response["data"]["assetOrError"]["definition"]
        input_datasets = [Dataset(**_get_namespace_and_name_from_materialization_metadata(dep["asset"])) for dep in asset_info["dependencies"]]
        output_datasets = [
            Dataset(
                **_get_namespace_and_name_from_materialization_metadata(asset_info),
                facets=_get_table_schema_facet(asset_info) | _get_documentation_facet(asset_info))
        ]

        asset_dependencies_lookup[asset_key] = {"input_datasets": input_datasets, "output_datasets": output_datasets}
    return asset_dependencies_lookup


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
