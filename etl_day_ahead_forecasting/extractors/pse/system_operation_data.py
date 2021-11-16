from etl_day_ahead_forecasting.extractors.pse._base_extractor import BasePseExtractor


__all__ = ('SystemOperationDataExtractor',)


class SystemOperationDataExtractor(BasePseExtractor):
    def _get_data_type(self) -> str:
        return 'PL_WYK_KSE'
