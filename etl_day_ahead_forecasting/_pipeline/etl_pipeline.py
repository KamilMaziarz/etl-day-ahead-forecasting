import copy
import datetime as dt
import logging
import typing as t
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path

import pydantic

from etl_day_ahead_forecasting._exceptions import IncorrectSuffixError

logger = logging.getLogger(__name__)

ETLPipelinePropertiesT = t.TypeVar("ETLPipelinePropertiesT")


class ETLDataName(Enum):
    EXTRACTED_DATA = auto()
    TRANSFORMED_DATA = auto()


ETLPipelineData = t.Dict[ETLDataName, t.Any]


class ETLPropertiesDataTypeToLoad(pydantic.BaseModel):
    data_name: ETLDataName


class ETLPropertiesPath(pydantic.BaseModel):
    path: Path

    @pydantic.validator('path')
    def path_suffix_must_be_pkl(cls, value):  # noqa
        if value.suffix != '.pkl':
            raise IncorrectSuffixError(file_type=value.suffix)
        return value


class ETLPropertiesTimeRange(pydantic.BaseModel):
    start: dt.date
    end: dt.date


class ETLPropertiesLocalSave(ETLPropertiesPath, ETLPropertiesTimeRange):
    pass


class ETLPropertiesPathWithDataType(ETLPropertiesPath, ETLPropertiesDataTypeToLoad):
    pass


class ETLPropertiesLocalSaveWithDataType(ETLPropertiesLocalSave, ETLPropertiesDataTypeToLoad):
    pass


class PipelineStep(t.Generic[ETLPipelinePropertiesT], metaclass=ABCMeta):
    @pydantic.validate_arguments
    @abstractmethod
    def execute(self, properties: ETLPipelinePropertiesT, data: t.Optional[ETLPipelineData]) -> ETLPipelineData:
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
