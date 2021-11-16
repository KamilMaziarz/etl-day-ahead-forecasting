import datetime as dt
import typing as t
from enum import Enum, auto
from pathlib import Path

import pydantic

from etl_day_ahead_forecasting.exceptions import IncorrectSuffixError


class ETLDataName(Enum):
    EXTRACTED_DATA = auto()
    TRANSFORMED_DATA = auto()


ETLPipelineData = t.Dict[ETLDataName, t.Any]

ETLPipelinePropertiesT = t.TypeVar("ETLPipelinePropertiesT")


class ETLProperties(pydantic.BaseModel):
    pass


class ETLPropertiesPath(ETLProperties):
    path: Path

    @pydantic.validator('path')
    def path_suffix_must_be_pkl(cls, value):  # noqa
        if value.suffix != '.pkl':
            raise IncorrectSuffixError(file_type=value.suffix)
        return value


class ETLPropertiesLocalSave(ETLPropertiesPath):
    data_name: ETLDataName


class ETLPropertiesReadBackup(ETLProperties):
    pass


class ETLPropertiesTimeRange(ETLProperties):
    start: dt.date
    end: dt.date


class ETLPropertiesDataType(ETLProperties):
    data_type: ETLDataName


class ETLPropertiesTimeRangeWithDataType(ETLPropertiesTimeRange, ETLPropertiesDataType):
    pass
