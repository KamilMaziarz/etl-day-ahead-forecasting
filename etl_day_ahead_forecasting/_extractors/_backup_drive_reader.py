import logging
import pickle
import typing as t

from etl_day_ahead_forecasting.pipeline._etl_pipeline import PipelineStep, _ETLPipelineData  # noqa
from etl_day_ahead_forecasting.pipeline.models import _ETLPropertiesSaveLocal  # noqa

__all__ = ('BackupDriveReader',)

logger = logging.getLogger(__name__)


class BackupDriveReader(PipelineStep[_ETLPropertiesSaveLocal]):
    def execute(
            self,
            properties: _ETLPropertiesSaveLocal,
            data: t.Optional[_ETLPipelineData] = None,
    ) -> _ETLPipelineData:
        with open(properties.path, 'rb') as file:
            extracted = pickle.load(file)
        logger.info(f'EXTRACTED: Backup file from path: {properties.path}')
        return {properties.local_data_type: extracted}
