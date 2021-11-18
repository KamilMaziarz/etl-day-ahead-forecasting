import typing as t

import pandas as pd

from etl_day_ahead_forecasting.exceptions import ExpectedDataTypeNotFoundError
from etl_day_ahead_forecasting.pipeline._etl_pipeline import PipelineStep  # noqa
from etl_day_ahead_forecasting.pipeline.models import (
    _ETLPropertiesTimeRangeWithDataType,  # noqa
    _ETLPipelineData,  # noqa
    ETLDataType,
)

__all__ = ('EuaPriceTransformer',)


class EuaPriceTransformer(PipelineStep[_ETLPropertiesTimeRangeWithDataType]):
    def execute(
            self,
            properties: _ETLPropertiesTimeRangeWithDataType,
            data: t.Optional[_ETLPipelineData] = None,
    ) -> _ETLPipelineData:
        if data is None or properties.data_type_to_be_transformed not in data:
            raise ExpectedDataTypeNotFoundError(expected_data_type=properties.data_type_to_be_transformed.name)

        raw = data[properties.data_type_to_be_transformed]
        raw.index = pd.to_datetime(raw['Data'])
        raw.drop(columns=['Data'], inplace=True)
        filtered_data = raw[properties.start:properties.end]
        filtered_data.rename(columns=self._column_names_mapping(), inplace=True)
        return {ETLDataType.TRANSFORMED_DATA: filtered_data}

    @staticmethod
    def _column_names_mapping() -> t.Dict[str, str]:
        return {
            'RCCO2 [zł/Mg CO2]': 'eua_pln',
            'RCCO2 [EUR/Mg CO2]': 'eua_eur',
        }
