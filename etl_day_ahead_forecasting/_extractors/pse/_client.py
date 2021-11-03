import datetime as dt
import io

import pandas as pd
import requests


class PseClient:
    @staticmethod
    def extract(data_type: str, date: dt.date) -> pd.DataFrame:
        url = f'https://www.pse.pl/getcsv/-/export/csv/{data_type}/data/{date.strftime("%Y%m%d")}'
        response = requests.get(url)
        if response.ok:
            encoded_data = response.content.decode('cp1250')
            data = pd.read_csv(io.StringIO(encoded_data), sep=';', decimal=',')
            return data
        else:
            response.raise_for_status()
