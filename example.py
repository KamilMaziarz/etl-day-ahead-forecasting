import datetime as dt

from etl_day_ahead_forecasting.pipeline import system_operation_data_pipeline, ETLPsePropertiesReadBackup, \
    renewables_generation_pipeline, ETLPsePropertiesSaveLocally
from etl_day_ahead_forecasting.utils import get_resources_path, initialize_logging

if __name__ == '__main__':
    initialize_logging()

    pipeline_data_1 = system_operation_data_pipeline.execute(
        ETLPsePropertiesReadBackup(start=dt.date(2018, 4, 15), end=dt.date(2020, 3, 2))
    )
    pipeline_data_2 = renewables_generation_pipeline.execute(
        ETLPsePropertiesSaveLocally(
            start=dt.date(2020, 2, 25),
            end=dt.date(2020, 3, 2),
            path=get_resources_path() / 'transformed' / 'res_test.pkl'
        )
    )
