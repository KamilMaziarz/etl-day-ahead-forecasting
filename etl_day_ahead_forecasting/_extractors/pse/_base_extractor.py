import datetime as dt
import io
import typing as t
from abc import abstractmethod, ABCMeta

import pandas as pd
from requests import Response

from etl_day_ahead_forecasting._extractors._backup_drive_reader import BackupDriveReader
from etl_day_ahead_forecasting._extractors.pse._client import PseClient
from etl_day_ahead_forecasting.pipeline._etl_pipeline import PipelineStep  # noqa
from etl_day_ahead_forecasting.pipeline.models import (
    _ETLPropertiesTimeRange,  # noqa
    _ETLPropertiesReadBackup,  # noqa
    ETLDataType,
    _ETLPropertiesSaveLocal,  # noqa
    _ETLPipelineData,  # noqa
)
from etl_day_ahead_forecasting.utils.paths import get_backup_path

_PSE_POSSIBLE_PROPERTIES = t.Union[_ETLPropertiesTimeRange, _ETLPropertiesReadBackup]


class BasePseExtractor(PipelineStep[_PSE_POSSIBLE_PROPERTIES], metaclass=ABCMeta):
    def execute(
            self,
            properties: _PSE_POSSIBLE_PROPERTIES,
            data: t.Optional[_ETLPipelineData] = None,
    ) -> _ETLPipelineData:
        if hasattr(properties, 'read_backup') and properties.read_backup:
            return self._get_backup_file()
        return self._extract_from_pse_website(properties=properties)

    def _get_backup_file(self) -> _ETLPipelineData:
        backup_reader_properties = _ETLPropertiesSaveLocal(path=get_backup_path(source='pse', extractor=self))
        return BackupDriveReader().execute(properties=backup_reader_properties)

    def _extract_from_pse_website(self, properties: _PSE_POSSIBLE_PROPERTIES) -> _ETLPipelineData:
        pse_client = PseClient()
        extract_periods = self._get_periods_to_extract(start=properties.start, end=properties.end)
        responses = [
            pse_client.extract(data_type=self._get_data_type(), start=s, end=e) for s, e in extract_periods
        ]
        extracted = pd.concat([self._parse_response(response=r) for r in responses])
        return {ETLDataType.EXTRACTED_DATA: extracted}

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
