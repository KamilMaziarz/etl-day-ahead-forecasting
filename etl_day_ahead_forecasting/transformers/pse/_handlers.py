import datetime as dt
import typing as t

import pandas as pd

from etl_day_ahead_forecasting.pipeline.models import ETLDataName, ETLPipelineData


class TimeShiftHandler:
    @staticmethod
    def remove_additional_hour_rows(data: pd.DataFrame) -> pd.DataFrame:
        truncated = data[data['Godzina'] != '2A'].copy(deep=True)
        truncated['Godzina'] = truncated['Godzina'].astype(int)
        return truncated

    @staticmethod
    def _get_missing_rows(data: pd.DataFrame) -> pd.DatetimeIndex:
        expected_date_indexes = pd.date_range(data.index.min(), data.index.max(), freq='1H')
        return expected_date_indexes.difference(data.index)

    @staticmethod
    def add_missing_rows_as_mean_of_adjacent(
            data: pd.DataFrame,
            group_by: t.Optional[t.List[str]] = None,
    ) -> pd.DataFrame:
        filled = data.copy(deep=True)
        for row in TimeShiftHandler._get_missing_rows(data=data):
            adjacent_rows = [(row + pd.Timedelta(hours=i)).strftime('%Y-%m-%d %H:%M') for i in range(-1, 2, 2)]
            if group_by is None:
                filled.loc[row, :] = filled[adjacent_rows[0]:adjacent_rows[1]].mean()
            else:
                grouped = filled[adjacent_rows[0]:adjacent_rows[1]].groupby(group_by)
                averaged = grouped.mean().reset_index()
                averaged.index = [row] * len(averaged)
                filled = pd.concat([filled, averaged]).drop_duplicates()
        filled.sort_index(inplace=True)
        return filled


class DatetimeColumnCreator:
    @staticmethod
    def create(data: pd.DataFrame, date_column: str, hour_column: str, date_format: str) -> pd.Series:
        data[date_column] = pd.to_datetime(data[date_column], format=date_format)
        return data[date_column] + pd.to_timedelta(data[hour_column], unit='h') - pd.Timedelta(hours=1)


class CommonTransformer:
    @staticmethod
    def transform(
            data: pd.DataFrame,
            start: dt.date,
            end: dt.date,
            column_mapping: t.Dict[str, str],
            date_format: str,
            group_by: t.Union[t.List[str], None],
    ) -> ETLPipelineData:
        truncated_data = TimeShiftHandler.remove_additional_hour_rows(data=data)
        truncated_data.index = DatetimeColumnCreator.create(
            data=truncated_data,
            date_column='Data',
            hour_column='Godzina',
            date_format=date_format,
        )
        truncated_data.drop(columns=['Data', 'Godzina'], inplace=True)
        truncated_data.sort_index(inplace=True)
        filtered_data = truncated_data[start:dt.datetime.combine(end, dt.time(23))]
        filled_data = TimeShiftHandler.add_missing_rows_as_mean_of_adjacent(data=filtered_data, group_by=group_by)
        filled_data.rename(columns=column_mapping, inplace=True)
        return {ETLDataName.TRANSFORMED_DATA: filled_data}
