from abc import ABCMeta

from etl_day_ahead_forecasting._pipeline.etl_pipeline import PipelineStep  # noqa


class BaseExtractor(PipelineStep, metaclass=ABCMeta):
    pass
