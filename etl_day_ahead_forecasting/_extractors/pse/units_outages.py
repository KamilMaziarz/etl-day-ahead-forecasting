from etl_day_ahead_forecasting._extractors.pse._base_extractor import BasePseExtractor


__all__ = ('UnitsOutagesExtractor',)


class UnitsOutagesExtractor(BasePseExtractor):
    def _get_data_type(self) -> str:
        return 'PL_WYK_UBYTKI'
