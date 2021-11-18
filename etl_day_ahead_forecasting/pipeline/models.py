import datetime as dt
import typing as t
from enum import Enum, auto
from pathlib import Path

import pydantic

from etl_day_ahead_forecasting.exceptions import IncorrectSuffixError

__all__ = [
    'ETLDataType',
    'ETLPseProperties',
    'ETLPsePropertiesSaveLocally',
    'ETLPsePropertiesReadBackup',
    'ETLPsePropertiesReadBackupSaveLocally',
]


class ETLDataType(Enum):
    EXTRACTED_DATA = auto()
    TRANSFORMED_DATA = auto()


_ETLPipelineData = t.Dict[ETLDataType, t.Any]

_ETLPipelinePropertiesT = t.TypeVar("_ETLPipelinePropertiesT")


class _ETLProperties(pydantic.BaseModel):
    pass


class _ETLPropertiesPath(_ETLProperties):
    path: Path

    @pydantic.validator('path')
    def path_suffix_must_be_pkl(cls, value):  # noqa
        if value.suffix != '.pkl':
            raise IncorrectSuffixError(file_type=value.suffix)
        return value


class _ETLPropertiesSaveLocal(_ETLPropertiesPath):
    save_locally: bool = True
    local_data_type: ETLDataType = ETLDataType.EXTRACTED_DATA


class _ETLPropertiesReadBackup(_ETLProperties):
    read_backup: bool = True


class _ETLPropertiesTimeRange(_ETLProperties):
    start: dt.date
    end: dt.date


class _ETLPropertiesDataTypeToBeTransformed(_ETLProperties):
    data_type_to_be_transformed: ETLDataType = ETLDataType.EXTRACTED_DATA


class _ETLPropertiesTimeRangeWithDataType(_ETLPropertiesTimeRange, _ETLPropertiesDataTypeToBeTransformed):
    pass


class ETLPseProperties(_ETLPropertiesTimeRangeWithDataType):
    pass


class ETLPsePropertiesSaveLocally(ETLPseProperties, _ETLPropertiesSaveLocal):
    pass


class ETLPsePropertiesReadBackup(ETLPseProperties, _ETLPropertiesReadBackup):
    pass


class ETLPsePropertiesReadBackupSaveLocally(ETLPsePropertiesSaveLocally, _ETLPropertiesReadBackup):
    pass
