import copy
import typing as t
from abc import ABCMeta, abstractmethod

import pydantic

from etl_day_ahead_forecasting.pipeline.models import _ETLPipelinePropertiesT, _ETLPipelineData  # noqa


class PipelineStep(t.Generic[_ETLPipelinePropertiesT], metaclass=ABCMeta):
    @pydantic.validate_arguments
    @abstractmethod
    def execute(
            self,
            properties: _ETLPipelinePropertiesT,
            data: t.Optional[_ETLPipelineData] = None,
    ) -> _ETLPipelineData:
        raise NotImplementedError


class ETLPipeline(t.Generic[_ETLPipelinePropertiesT]):
    def __init__(self, steps: t.Sequence[PipelineStep[_ETLPipelinePropertiesT]]):
        self.steps = steps

    @pydantic.validate_arguments
    def execute(self, properties: _ETLPipelinePropertiesT, data: t.Optional[_ETLPipelineData] = None):
        updated_data = copy.deepcopy(data) if data is not None else {}
        for step in self.steps:
            new_data = step.execute(properties=properties, data=updated_data)
            updated_data = {**updated_data, **new_data}
        return updated_data
