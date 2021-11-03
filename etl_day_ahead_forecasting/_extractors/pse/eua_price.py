from etl_day_ahead_forecasting._extractors.pse._base_extractor import BasePseExtractor


__all__ = ('EuaPriceExtractor',)


class EuaPriceExtractor(BasePseExtractor):
    def _get_data_type(self) -> str:
        return 'PL_CENY_ROZL_CO2'
