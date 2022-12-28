# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
import json
from typing import Dict, List, Optional

import attr
import cattr
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
    repository_location: Optional[str] = attr.ib(default=None)


@attr.s
class OpenLineageCursor:
    last_storage_id: int = attr.ib()
    running_pipelines: Dict[str, RunningPipeline] = attr.ib(factory=dict)
    run_updated_after: float = attr.ib(default=datetime.now().timestamp())

    def to_json(self):
        return json.dumps(attr.asdict(self))

    @classmethod
    def from_json(cls, json_str: str):
        return cattr.structure(json.loads(json_str), cls)
