from abc import abstractmethod, ABCMeta

import pandas as pd

from etl_day_ahead_forecasting._extractors.pse._client import PseClient
from etl_day_ahead_forecasting._pipeline.etl_pipeline import (  # noqa
    PipelineStep,
    EtlPropertiesLocalSave,
    EtlPipelinePropertiesT,
    EtlPipelineData,
    EtlPipelineStepResult,
)


class BasePseExtractor(PipelineStep[EtlPropertiesLocalSave], metaclass=ABCMeta):
    def execute(self, properties: EtlPipelinePropertiesT, data: EtlPipelineData = None) -> EtlPipelineStepResult:
        pse_client = PseClient()
        data_type = self._get_data_type()
        data_daily = [pse_client.extract(data_type=data_type, date=date)
                      for date in pd.date_range(properties.start, properties.end)]
        extraction_result = {'extracted_data': pd.concat(data_daily)}
        return extraction_result

    @abstractmethod
    def _get_data_type(self) -> str:
        raise NotImplementedError
