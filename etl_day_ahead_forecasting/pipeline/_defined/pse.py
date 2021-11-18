import typing as t

from etl_day_ahead_forecasting._extractors import (  # noqa
    CrossBorderFlowsExtractor,
    EuaPriceExtractor,
    RenewablesGenerationExtractor,
    SystemOperationDataExtractor,
    UnitsGenerationExtractor,
    UnitsOutagesExtractor,
)
from etl_day_ahead_forecasting._loaders import DriveLoader  # noqa
from etl_day_ahead_forecasting._transformers import (  # noqa
    CrossBorderFlowsTransformer,
    EuaPriceTransformer,
    RenewablesGenerationTransformer,
    SystemOperationDataTransformer,
    UnitsGenerationTransformer,
    UnitsOutagesTransformer,
)
from etl_day_ahead_forecasting.pipeline._etl_pipeline import PipelineStep, ETLPipeline
from etl_day_ahead_forecasting.pipeline.models import (
    ETLPseProperties,
    ETLPsePropertiesSaveLocally,
    ETLPsePropertiesReadBackup,
    ETLPsePropertiesReadBackupSaveLocally,
)

__all__ = [
    'cross_border_pipeline',
    'eua_price_pipeline',
    'renewables_generation_pipeline',
    'system_operation_data_pipeline',
    'units_generation_pipeline',
    'units_outages_pipeline',
]

__PIPELINE_PROPERTIES_T = t.Union[
    ETLPseProperties,
    ETLPsePropertiesSaveLocally,
    ETLPsePropertiesReadBackup,
    ETLPsePropertiesReadBackupSaveLocally,
]


def __create_etl_pipeline_save_locally(
        extractor_type: t.Type[PipelineStep],
        transformer_type: t.Type[PipelineStep],
) -> ETLPipeline:
    return ETLPipeline[__PIPELINE_PROPERTIES_T]([extractor_type(), transformer_type(), DriveLoader()])


cross_border_pipeline = __create_etl_pipeline_save_locally(
    extractor_type=CrossBorderFlowsExtractor,
    transformer_type=CrossBorderFlowsTransformer,
)
eua_price_pipeline = __create_etl_pipeline_save_locally(
    extractor_type=EuaPriceExtractor,
    transformer_type=EuaPriceTransformer,
)
renewables_generation_pipeline = __create_etl_pipeline_save_locally(
    extractor_type=RenewablesGenerationExtractor,
    transformer_type=RenewablesGenerationTransformer,
)
system_operation_data_pipeline = __create_etl_pipeline_save_locally(
    extractor_type=SystemOperationDataExtractor,
    transformer_type=SystemOperationDataTransformer,
)
units_generation_pipeline = __create_etl_pipeline_save_locally(
    extractor_type=UnitsGenerationExtractor,
    transformer_type=UnitsGenerationTransformer,
)
units_outages_pipeline = __create_etl_pipeline_save_locally(
    extractor_type=UnitsOutagesExtractor,
    transformer_type=UnitsOutagesTransformer,
)
