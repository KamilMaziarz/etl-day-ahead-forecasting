import io
import typing as t
from abc import abstractmethod, ABCMeta

import pandas as pd
from requests import Response

from etl_day_ahead_forecasting._extractors._backup_drive_reader import BackupDriveReader
from etl_day_ahead_forecasting._extractors.pse._client import PseClient  # noqa
from etl_day_ahead_forecasting._pipeline.etl_pipeline import (  # noqa
    PipelineStep,
    ETLPropertiesTimeRange,
    ETLPropertiesLocalSave,
    ETLPipelineData,
    ETLDataName,
)


class BasePseExtractor(PipelineStep[ETLPropertiesTimeRange], metaclass=ABCMeta):
    def execute(
            self,
            properties: ETLPropertiesTimeRange,
            read_backup: bool = False,
    ) -> ETLPipelineData:
        if read_backup:
            return self._read_backup_file(properties=properties)
        pse_client = PseClient()
        extract_periods = pse_client.divide_date_range_into_31_day_periods(start=properties.start, end=properties.end)
        responses = [
            pse_client.extract(data_type=self._get_data_type(), start=s, end=e) for s, e in extract_periods
        ]
        extracted = pd.concat([self._parse_response(response=r) for r in responses])
        return {ETLDataName.EXTRACTED_DATA: extracted}

    def _read_backup_file(self, properties: ETLPropertiesTimeRange) -> t.Any:
        reader = BackupDriveReader()
        path = reader.get_backup_file_path_by_extractor_name(extractor_name=self.__class__.__name__, source='pse')
        local_save_properties = ETLPropertiesLocalSave(path=path, start=properties.start, end=properties.end)
        return BackupDriveReader().execute(properties=local_save_properties)

    @staticmethod
    def _parse_response(response: Response) -> pd.DataFrame:
        encoded_data = response.content.decode('cp1250')
        return pd.read_csv(io.StringIO(encoded_data), sep=';', decimal=',', error_bad_lines=False)

    @abstractmethod
    def _get_data_type(self) -> str:
        raise NotImplementedError
