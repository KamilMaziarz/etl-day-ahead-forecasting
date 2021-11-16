import typing as t

import pandas as pd

from etl_day_ahead_forecasting.exceptions import ExpectedDataTypeNotFoundError
from etl_day_ahead_forecasting.pipeline.models import ETLPropertiesTimeRangeWithDataType, ETLPipelineData
from etl_day_ahead_forecasting.transformers.pse._base_transformer import BasePseTransformer

__all__ = ('UnitsGenerationTransformer',)


class UnitsGenerationTransformer(BasePseTransformer):

    def execute(
            self,
            properties: ETLPropertiesTimeRangeWithDataType,
            data: t.Optional[ETLPipelineData] = None,
    ) -> ETLPipelineData:
        if data is None or properties.data_type not in data:
            raise ExpectedDataTypeNotFoundError(expected_data_type=properties.data_type.name)
        data[properties.data_type] = self._get_cleaned_data(data=data[properties.data_type])
        return super().execute(properties=properties, data=data)

    @staticmethod
    def _get_cleaned_data(data: pd.DataFrame) -> pd.DataFrame:
        return pd.melt(
            data,
            id_vars=['Doba', 'Kod', 'Nazwa', 'Tryb pracy'],
            value_vars=[str(i) for i in range(1, 25)],
            var_name='Godzina',
            value_name='power',
        ).rename(columns={'Doba': 'Data'})

    @staticmethod
    def _get_date_format() -> str:
        return '%Y%m%d'

    @staticmethod
    def _column_names_mapping() -> t.Dict[str, str]:
        return {
            'Nazwa': 'unit_name',
            'Kod': 'unit_code',
            'Tryb pracy': 'work_mode',
        }

    @staticmethod
    def _get_group_by_columns() -> t.Optional[t.List[str]]:
        return ['Nazwa', 'Kod', 'Tryb pracy']
