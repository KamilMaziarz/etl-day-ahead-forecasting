import io
from abc import abstractmethod, ABCMeta

import pandas as pd
from requests import Response

from etl_day_ahead_forecasting._extractors.pse._client import PseClient  # noqa
from etl_day_ahead_forecasting._pipeline.etl_pipeline import (  # noqa
    PipelineStep,
    ETLPropertiesTimeRange,
    ETLPipelineData,
    ETLDataName,
)


class BasePseExtractor(PipelineStep[ETLPropertiesTimeRange], metaclass=ABCMeta):
    def execute(self, properties: ETLPropertiesTimeRange, data: ETLPipelineData = None) -> ETLPipelineData:
        pse_client = PseClient()
        data_type = self._get_data_type()
        extract_periods = pse_client.divide_date_range_into_31_day_periods(start=properties.start, end=properties.end)
        responses = [pse_client.extract(data_type=data_type, start=start, end=end) for start, end in extract_periods]
        extracted = pd.concat([self._parse_response(response=r) for r in responses])
        return {ETLDataName.EXTRACTED_DATA: extracted}

    @staticmethod
    def _parse_response(response: Response) -> pd.DataFrame:
        encoded_data = response.content.decode('cp1250')
        return pd.read_csv(io.StringIO(encoded_data), sep=';', decimal=',', error_bad_lines=False)

    @abstractmethod
    def _get_data_type(self) -> str:
        raise NotImplementedError
