from etl_day_ahead_forecasting._extractors.pse._base_extractor import BasePseExtractor
import datetime as dt

__all__ = ('EuaPriceExtractor',)

from etl_day_ahead_forecasting._extractors.pse._client import PseClient


class EuaPriceExtractor(BasePseExtractor):
    def _get_data_type(self) -> str:
        return 'PL_CENY_ROZL_CO2'

    @staticmethod
    def _get_periods_to_extract(start: dt.date, end: dt.date):
        return PseClient.divide_date_range_into_daily_periods(start=start, end=end)
