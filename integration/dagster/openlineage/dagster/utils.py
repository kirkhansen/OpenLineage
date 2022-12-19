# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import uuid
from datetime import datetime
from typing import Iterable, Optional

from dagster import (
    DagsterInstance,
    EventLogRecord,
    EventRecordsFilter,
    RunShardedEventsCursor,
)
from dagster._core.events import DagsterEventType  # type: ignore


NOMINAL_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def to_utc_iso_8601(timestamp: float) -> str:
    return datetime.utcfromtimestamp(timestamp).strftime(NOMINAL_TIME_FORMAT)


def make_step_run_id() -> str:
    return str(uuid.uuid4())


def make_step_job_name(pipeline_name: str, step_key: str) -> str:
    return f"{pipeline_name}.{step_key}"


def get_event_log_records(
    instance: DagsterInstance,
    event_types: set[DagsterEventType],
    last_storage_id: int,
    run_updated_after: datetime,
    record_filter_limit: Optional[int] = None,
) -> Iterable[EventLogRecord]:
    """Returns a list of Dagster event log records in ascending order
    from the instance's event log storage.
    :param instance: active instance to get records from
    :param event_type: Event type to filter out
    :param last_storage_id: storage id to use as after cursor filter
    :param record_filter_limit: maximum number of event logs to retrieve
    :return: iterable of Dagster event log records
    """
    for event_type in event_types:
        for event_record in instance.get_event_records(
            EventRecordsFilter(
                event_type=event_type,
                after_cursor=RunShardedEventsCursor(last_storage_id, run_updated_after),
            ),
            limit=record_filter_limit,
            ascending=True,
        ):
            yield event_record


def get_repository_name(
    instance: DagsterInstance, pipeline_run_id: str
) -> Optional[str]:
    """Returns an optional repository name
    :param instance: active instance to get the pipeline run
    :param pipeline_run_id: run id to look up its run
    :return: optional repository name
    """
    pipeline_run = instance.get_run_by_id(pipeline_run_id)
    repository_name = None
    if pipeline_run:
        ext_pipeline_origin = pipeline_run.external_pipeline_origin
        if ext_pipeline_origin:
            ext_repository_origin = ext_pipeline_origin.external_repository_origin
            if ext_repository_origin:
                repository_name = ext_repository_origin.repository_name
    return repository_name
