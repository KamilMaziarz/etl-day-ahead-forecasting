import datetime as dt

from etl_day_ahead_forecasting._extractors.pse.renewables_generation import RenewablesGenerationExtractor  # noqa
from etl_day_ahead_forecasting._loaders.drive_loader import DriveLoader  # noqa
from etl_day_ahead_forecasting._pipeline.etl_pipeline import (
    ETLPipeline,
    ETLPropertiesLocalSaveWithDataType,
    ETLDataName,
)
from etl_day_ahead_forecasting._utils.paths import get_backup_path  # noqa

properties = ETLPropertiesLocalSaveWithDataType(
    start=dt.date(2018, 1, 1),
    end=dt.date(2018, 2, 15),
    data_name=ETLDataName.EXTRACTED_DATA,
    path=get_backup_path(
        source='pse',
        name='renewables_generation',
        start=dt.date(2018, 1, 1),
        end=dt.date(2018, 1, 31),
    )
)

pipeline_steps = [
    RenewablesGenerationExtractor(),
    DriveLoader(),
]

pipeline = ETLPipeline(_steps=pipeline_steps)
pipeline.execute(properties=properties)
