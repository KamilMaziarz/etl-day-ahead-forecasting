import datetime as dt
import io
import typing as t
from abc import abstractmethod, ABCMeta

import pandas as pd
from requests import Response

from etl_day_ahead_forecasting._extractors._backup_drive_reader import BackupDriveReader
from etl_day_ahead_forecasting._extractors.pse._client import PseClient  # noqa
from etl_day_ahead_forecasting._pipeline._etl_pipeline_models import (  # noqa
    ETLPropertiesTimeRange,
    ETLPropertiesReadBackup,
    ETLDataName,
    ETLPropertiesLocalSave,
    ETLPipelineData,
)
from etl_day_ahead_forecasting._pipeline.etl_pipeline import PipelineStep  # noqa
from etl_day_ahead_forecasting._utils.paths import get_backup_path  # noqa

_PSE_POSSIBLE_PROPERTIES = t.Union[ETLPropertiesTimeRange, ETLPropertiesReadBackup]


class BasePseExtractor(PipelineStep[_PSE_POSSIBLE_PROPERTIES], metaclass=ABCMeta):
    def execute(
            self,
            properties: _PSE_POSSIBLE_PROPERTIES,
            data: t.Optional[ETLPipelineData] = None,
    ) -> ETLPipelineData:
        if isinstance(properties, ETLPropertiesReadBackup):
            return self._get_backup_file()
        return self._extract_from_pse_website(properties=properties)

    def _get_backup_file(self) -> ETLPipelineData:
        path = get_backup_path(source='pse', extractor=self)
        backup_reader_properties = ETLPropertiesLocalSave(path=path, data_name=ETLDataName.EXTRACTED_DATA)
        return BackupDriveReader().execute(properties=backup_reader_properties)

    def _extract_from_pse_website(self, properties: _PSE_POSSIBLE_PROPERTIES) -> ETLPipelineData:
        pse_client = PseClient()
        extract_periods = self._get_periods_to_extract(start=properties.start, end=properties.end)
        responses = [
            pse_client.extract(data_type=self._get_data_type(), start=s, end=e) for s, e in extract_periods
        ]
        extracted = pd.concat([self._parse_response(response=r) for r in responses])
        return {ETLDataName.EXTRACTED_DATA: extracted}

    @staticmethod
    def _get_periods_to_extract(start: dt.date, end: dt.date):
        return PseClient.divide_date_range_into_31_day_periods(start=start, end=end)

    @staticmethod
    def _parse_response(response: Response) -> pd.DataFrame:
        encoded_data = response.content.decode('cp1250')
        return pd.read_csv(io.StringIO(encoded_data), sep=';', decimal=',', error_bad_lines=False)

    @abstractmethod
    def _get_data_type(self) -> str:
        raise NotImplementedError
