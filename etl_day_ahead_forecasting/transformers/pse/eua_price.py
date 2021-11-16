import typing as t

import pandas as pd

from etl_day_ahead_forecasting.exceptions import ExpectedDataTypeNotFoundError
from etl_day_ahead_forecasting.pipeline.etl_pipeline import PipelineStep
from etl_day_ahead_forecasting.pipeline.models import (
    ETLPropertiesTimeRangeWithDataType,
    ETLPipelineData,
    ETLDataName,
)

__all__ = ('EuaPriceTransformer',)


class EuaPriceTransformer(PipelineStep[ETLPropertiesTimeRangeWithDataType]):
    def execute(
            self,
            properties: ETLPropertiesTimeRangeWithDataType,
            data: t.Optional[ETLPipelineData] = None,
    ) -> ETLPipelineData:
        if data is None or properties.data_type not in data:
            raise ExpectedDataTypeNotFoundError(expected_data_type=properties.data_type.name)

        raw = data[properties.data_type]
        raw.index = pd.to_datetime(raw['Data'])
        raw.drop(columns=['Data'], inplace=True)
        filtered_data = raw[properties.start:properties.end]
        filtered_data.rename(columns=self._column_names_mapping(), inplace=True)
        return {ETLDataName.TRANSFORMED_DATA: filtered_data}

    @staticmethod
    def _column_names_mapping() -> t.Dict[str, str]:
        return {
            'RCCO2 [zł/Mg CO2]': 'eua_pln',
            'RCCO2 [EUR/Mg CO2]': 'eua_eur',
        }
