import pickle

from etl_day_ahead_forecasting._pipeline._etl_pipeline_models import ETLPropertiesLocalSave  # noqa
from etl_day_ahead_forecasting._pipeline.etl_pipeline import PipelineStep, ETLPipelineData  # noqa

__all__ = ('DriveLoader',)


class DriveLoader(PipelineStep[ETLPropertiesLocalSave]):
    def execute(self, properties: ETLPropertiesLocalSave, data: ETLPipelineData = None) -> ETLPipelineData:
        if data is not None:
            with open(properties.path, 'wb') as file:
                pickle.dump(data[properties.data_name], file)
        return {}
