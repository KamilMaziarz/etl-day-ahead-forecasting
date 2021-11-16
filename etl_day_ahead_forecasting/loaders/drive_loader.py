import pickle

from etl_day_ahead_forecasting.pipeline.etl_pipeline import PipelineStep, ETLPipelineData
from etl_day_ahead_forecasting.pipeline.models import ETLPropertiesLocalSave

__all__ = ('DriveLoader',)


class DriveLoader(PipelineStep[ETLPropertiesLocalSave]):
    def execute(self, properties: ETLPropertiesLocalSave, data: ETLPipelineData = None) -> ETLPipelineData:
        if data is not None:
            with open(properties.path, 'wb') as file:
                pickle.dump(data[properties.data_name], file)
        return {}
