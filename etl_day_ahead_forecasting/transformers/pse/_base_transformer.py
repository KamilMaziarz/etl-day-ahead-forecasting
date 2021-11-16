import typing as t
from abc import ABCMeta, abstractmethod

from etl_day_ahead_forecasting.exceptions import ExpectedDataTypeNotFoundError
from etl_day_ahead_forecasting.pipeline.etl_pipeline import PipelineStep
from etl_day_ahead_forecasting.pipeline.models import (
    ETLPipelineData,
    ETLPropertiesTimeRangeWithDataType,
)
from etl_day_ahead_forecasting.transformers.pse._handlers import CommonTransformer

_PSE_POSSIBLE_PROPERTIES = t.Union[ETLPropertiesTimeRangeWithDataType]


class BasePseTransformer(PipelineStep[_PSE_POSSIBLE_PROPERTIES], metaclass=ABCMeta):
    def execute(
            self,
            properties: _PSE_POSSIBLE_PROPERTIES,
            data: t.Optional[ETLPipelineData] = None,
    ) -> ETLPipelineData:
        if data is None or properties.data_type not in data:
            raise ExpectedDataTypeNotFoundError(expected_data_type=properties.data_type.name)
        return CommonTransformer.transform(
            data=data[properties.data_type],
            start=properties.start,
            end=properties.end,
            column_mapping=self._column_names_mapping(),
            date_format=self._get_date_format(),
            group_by=self._get_group_by_columns(),
        )

    @staticmethod
    @abstractmethod
    def _column_names_mapping() -> t.Dict[str, str]:
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def _get_date_format() -> str:
        raise NotImplementedError

    @staticmethod
    def _get_group_by_columns() -> t.Optional[t.List[str]]:
        return None
