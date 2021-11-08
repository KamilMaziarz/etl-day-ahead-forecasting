import datetime as dt
import pickle
import re
import typing as t
from pathlib import Path

from etl_day_ahead_forecasting._pipeline.etl_pipeline import (  # noqa
    PipelineStep,
    ETLPropertiesPath,
    ETLPipelineData,
    ETLDataName,
)
from etl_day_ahead_forecasting._utils.paths import get_backup_path, _PATH_ELEMENT  # noqa

__all__ = ('BackupDriveReader',)


class BackupDriveReader(PipelineStep[ETLPropertiesPath]):
    def execute(
            self,
            properties: ETLPropertiesPath,
            data: t.Optional[ETLPipelineData] = None,
    ) -> ETLPipelineData:
        with open(properties.path, 'rb') as file:
            extracted = pickle.load(file)
        return {ETLDataName.EXTRACTED_DATA: extracted}

    def get_backup_file_path_by_extractor_name(self, extractor_name: str, source: _PATH_ELEMENT) -> Path:
        name = self._change_camel_case_to_snake_case(string=extractor_name.replace('Extractor', ''))
        return get_backup_path(source=source, name=name, start=dt.date(2018, 1, 1), end=dt.date(2020, 12, 31))

    @staticmethod
    def _change_camel_case_to_snake_case(string: str) -> str:
        string = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', string)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', string).lower()
