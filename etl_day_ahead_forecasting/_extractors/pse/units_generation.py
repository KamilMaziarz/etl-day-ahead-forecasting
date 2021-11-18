from etl_day_ahead_forecasting._extractors.pse._base_extractor import BasePseExtractor


__all__ = ('UnitsGenerationExtractor',)


class UnitsGenerationExtractor(BasePseExtractor):
    def _get_data_type(self) -> str:
        return 'PL_GEN_MOC_JW_EPS'
