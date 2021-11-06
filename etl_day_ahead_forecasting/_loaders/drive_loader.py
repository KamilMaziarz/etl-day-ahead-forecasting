import pickle

from etl_day_ahead_forecasting._pipeline.etl_pipeline import (  # noqa
    PipelineStep,
    ETLPropertiesPathWithDataType,
    ETLPipelineData,
)
from etl_day_ahead_forecasting._utils.paths import get_resources_path  # noqa

__all__ = ('DriveLoader',)


class DriveLoader(PipelineStep[ETLPropertiesPathWithDataType]):
    def execute(self, properties: ETLPropertiesPathWithDataType, data: ETLPipelineData) -> ETLPipelineData:
        if data is not None:
            with open(properties.path, 'wb') as file:
                pickle.dump(data[properties.data_name], file)
        return {}
