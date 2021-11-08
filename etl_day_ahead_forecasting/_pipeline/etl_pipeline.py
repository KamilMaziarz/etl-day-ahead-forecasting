import copy
import typing as t
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass

import pydantic

from etl_day_ahead_forecasting._pipeline._etl_pipeline_models import ETLPipelinePropertiesT, ETLPipelineData


class PipelineStep(t.Generic[ETLPipelinePropertiesT], metaclass=ABCMeta):
    @pydantic.validate_arguments
    @abstractmethod
    def execute(self, properties: ETLPipelinePropertiesT, data: t.Optional[ETLPipelineData] = None) -> ETLPipelineData:
        raise NotImplementedError


@dataclass
class ETLPipeline:
    _steps: t.Sequence[PipelineStep[ETLPipelinePropertiesT]]

    @pydantic.validate_arguments
    def execute(self, properties: ETLPipelinePropertiesT, data: t.Optional[ETLPipelineData] = None):
        updated_data = copy.deepcopy(data) if data is not None else {}
        for step in self._steps:
            new_data = step.execute(properties=properties, data=updated_data)
            updated_data = {**updated_data, **new_data}
        return updated_data
