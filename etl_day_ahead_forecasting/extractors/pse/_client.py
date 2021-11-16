import datetime as dt
import functools
import logging
import time
import typing as t
from dataclasses import dataclass

import pandas as pd
import pydantic
import requests
from requests import Response

from etl_day_ahead_forecasting.exceptions import PseTooBroadTimeRangeError

logger = logging.getLogger(__name__)


def _wait_if_there_was_extraction_recently(func):
    @functools.wraps(func)
    def inner(self, *agrs, **kwargs):
        time_since_last_call = dt.datetime.now() - self._last_called
        if time_since_last_call < dt.timedelta(seconds=30):
            seconds_to_wait = 30 - time_since_last_call.seconds
            logger.info('PseClient extract method has been called recently.'
                        f' Waiting for {seconds_to_wait} seconds to call it once again.')
            time.sleep(seconds_to_wait)
        response = func(self, *agrs, **kwargs)
        self._last_called = dt.datetime.now()
        return response
    return inner


@pydantic.dataclasses.dataclass(frozen=True)
class _PseDateRange:
    MAX_RANGE: dt.timedelta = dt.timedelta(30)

    def divide_date_range_into_31_day_periods(self, start: dt.date, end: dt.date) -> t.List[t.Tuple[dt.date, dt.date]]:
        period_starts = pd.date_range(start, end, freq=self.MAX_RANGE + dt.timedelta(1))
        periods_until_last = [
            (period_start.date(), (period_start + self.MAX_RANGE).date()) for period_start in period_starts[:-1]
        ]
        return [*periods_until_last, (period_starts[-1].date(), end)]

    def validate_date_range(self, start, end) -> None:
        if end - start > self.MAX_RANGE:
            raise PseTooBroadTimeRangeError(date_range=end-start)


@dataclass
class _PseClient:
    data_type: str
    start: dt.date
    end: dt.date
    _max_retires: int = 3
    _retries: int = 0

    def get(self) -> Response:
        response = requests.get(self._create_url())
        if response.ok:
            logger.info(f'EXTRACTED: data {self.data_type} for date range'
                        f' < {self.start.strftime("%Y-%m-%d")} : {self.end.strftime("%Y-%m-%d")} >')
            return response
        elif response.status_code == 429 and self._retries <= self._max_retires:
            logger.warning(f'{response.reason}. Waiting for 10 minutes and retrying.')
            time.sleep(600)
            self._retries += 1
            return self.get()
        else:
            response.raise_for_status()

    def _create_url(self) -> str:
        # date_from and date_to doesn't work on the PSE website. It returns just the date_from.
        if self.data_type == 'PL_CENY_ROZL_CO2':
            return 'https://www.pse.pl/getcsv/-/export/csv/PL_CENY_ROZL_CO2/data/' \
                   f'{self.start.strftime("%Y%m%d")}/datdo/{self.end.strftime("%Y-%m-%d")}'
        else:
            return f'https://www.pse.pl/getcsv/-/export/csv/{self.data_type}/' \
                   f'data_od/{self.start.strftime("%Y%m%d")}/data_do/{self.end.strftime("%Y%m%d")}'

    @pydantic.root_validator
    def _validate_date_range(cls, values) -> None:  # noqa
        _PseDateRange().validate_date_range(start=values.get('start'), end=values.get('end'))
        return values


class PseClient:
    def __init__(self):
        self._last_called: dt.datetime = dt.datetime.min

    @_wait_if_there_was_extraction_recently
    @pydantic.validate_arguments
    def extract(self, data_type: str, start: dt.date, end: dt.date) -> Response:
        client = _PseClient(data_type=data_type, start=start, end=end)
        return client.get()

    @classmethod
    def divide_date_range_into_31_day_periods(cls, start: dt.date, end: dt.date) -> t.List[t.Tuple[dt.date, dt.date]]:
        return _PseDateRange().divide_date_range_into_31_day_periods(start=start, end=end)

    @classmethod
    def divide_date_range_into_daily_periods(cls, start: dt.date, end: dt.date) -> t.List[t.Tuple[dt.date, dt.date]]:
        return [(d.date(), d.date()) for d in pd.date_range(start, end, freq='1D')]
