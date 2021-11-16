from etl_day_ahead_forecasting.extractors.pse._base_extractor import BasePseExtractor


__all__ = ('RenewablesGenerationExtractor',)


class RenewablesGenerationExtractor(BasePseExtractor):
    def _get_data_type(self) -> str:
        return 'PL_GEN_WIATR'
