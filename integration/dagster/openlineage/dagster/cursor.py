# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
import json
from typing import Optional, Dict, List

import attr
from openlineage.client.run import InputDataset, OutputDataset


@attr.s
class RunningStep:
    step_run_id: str = attr.ib()
    input_datasets: List[InputDataset] = attr.ib(factory=list)
    output_datasets: List[OutputDataset] = attr.ib(factory=list)


@attr.s
class RunningPipeline:
    running_steps: Dict[str, RunningStep] = attr.ib(factory=dict)
    repository_name: Optional[str] = attr.ib(default=None)


@attr.s
class OpenLineageCursor:
    last_storage_id: int = attr.ib()
    running_pipelines: Dict[str, RunningPipeline] = attr.ib(factory=dict)
    run_updated_after: datetime = attr.ib(default=datetime.now())

    def to_json(self):
        dict_repr = attr.asdict(self)
        dict_repr["run_updated_after"] = datetime.timestamp(
            dict_repr["run_updated_after"]
        )
        return json.dumps(dict_repr)

    @classmethod
    def from_json(cls, json_str: str):
        attrs = json.loads(json_str)
        try:
            attrs["run_updated_after"] = datetime.fromtimestamp(
                attrs["run_updated_after"]
            )
        except KeyError:
            attrs["run_updated_after"] = datetime.now()
        return cls(**attrs)
