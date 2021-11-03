import copy
import datetime as dt
import logging
import typing as t
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

EtlPipelinePropertiesT = t.TypeVar("EtlPipelinePropertiesT")

EtlPipelineData = t.Optional[t.Dict[str, t.Any]]
EtlPipelineStepResult = t.Dict[str, t.Any]


@dataclass
class EtlPropertiesLocalSave:
    start: dt.date
    end: dt.date
    path: Path


class PipelineStep(t.Generic[EtlPipelinePropertiesT], metaclass=ABCMeta):
    @abstractmethod
    def execute(self, properties: EtlPipelinePropertiesT, data: EtlPipelineData = None) -> EtlPipelineStepResult:
        raise NotImplementedError


@dataclass
class EtlPipeline:
    __steps: t.Sequence[PipelineStep[EtlPipelinePropertiesT]]

    def execute(self, properties: EtlPipelinePropertiesT, data: EtlPipelineData = None):
        updated_data = copy.deepcopy(data)
        for step in self.__steps:
            new_data = step.execute(properties=properties, data=updated_data)
            updated_data = {**updated_data, **new_data}
        return updated_data
