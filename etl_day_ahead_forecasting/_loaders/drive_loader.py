import logging
import pickle

from etl_day_ahead_forecasting.pipeline._etl_pipeline import PipelineStep, _ETLPipelineData  # noqa
from etl_day_ahead_forecasting.pipeline.models import _ETLPropertiesSaveLocal  # noqa

__all__ = ('DriveLoader',)

logger = logging.getLogger(__name__)


class DriveLoader(PipelineStep[_ETLPropertiesSaveLocal]):
    def execute(self, properties: _ETLPropertiesSaveLocal, data: _ETLPipelineData = None) -> _ETLPipelineData:
        if data is not None and hasattr(properties, 'save_locally') and properties.save_locally:
            properties.path.parent.mkdir(parents=True, exist_ok=True)
            with open(properties.path, 'wb') as file:
                pickle.dump(data[properties.local_data_type], file)
            logger.info(f'SAVED to path: {properties.path}')
        return {}
