from etl_day_ahead_forecasting._extractors.pse._base_extractor import BasePseExtractor


__all__ = ('CrossBorderFlowsExtractor',)


class CrossBorderFlowsExtractor(BasePseExtractor):
    def _get_data_type(self) -> str:
        return 'PL_WYK_WYM'
