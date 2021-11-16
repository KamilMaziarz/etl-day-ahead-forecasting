import pickle
import typing as t

from etl_day_ahead_forecasting.pipeline.etl_pipeline import PipelineStep, ETLPipelineData
from etl_day_ahead_forecasting.pipeline.models import ETLPropertiesLocalSave

__all__ = ('BackupDriveReader',)


class BackupDriveReader(PipelineStep[ETLPropertiesLocalSave]):
    def execute(self, properties: ETLPropertiesLocalSave, data: t.Optional[ETLPipelineData] = None) -> ETLPipelineData:
        with open(properties.path, 'rb') as file:
            extracted = pickle.load(file)
        return {properties.data_name: extracted}
