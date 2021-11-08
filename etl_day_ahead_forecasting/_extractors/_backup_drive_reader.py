import pickle
import typing as t

from etl_day_ahead_forecasting._pipeline._etl_pipeline_models import ETLPropertiesLocalRead  # noqa
from etl_day_ahead_forecasting._pipeline.etl_pipeline import PipelineStep, ETLPipelineData  # noqa

__all__ = ('BackupDriveReader',)


class BackupDriveReader(PipelineStep[ETLPropertiesLocalRead]):
    def execute(self, properties: ETLPropertiesLocalRead, data: t.Optional[ETLPipelineData] = None) -> ETLPipelineData:
        with open(properties.path, 'rb') as file:
            extracted = pickle.load(file)
        return {properties.data_name: extracted}
