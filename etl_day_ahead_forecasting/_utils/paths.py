import datetime as dt
import re
from os import path
from pathlib import Path

import pydantic

from etl_day_ahead_forecasting._extractors._base_extractor import BaseExtractor  # noqa

_PATH_ELEMENT = pydantic.constr(regex='^[a-z_]+$')


def get_resources_path() -> Path:
    current_path = Path(path.dirname(path.realpath(__file__)))
    resources_path = current_path / ".." / ".." / "resources"
    resources_path.mkdir(parents=True, exist_ok=True)
    return resources_path


def change_camel_case_to_snake_case(string: str) -> str:
    string = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', string)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', string).lower()


@pydantic.validate_arguments
def get_backup_path(
        source: _PATH_ELEMENT,
        extractor: BaseExtractor,
        start: dt.date = dt.date(2018, 1, 1),
        end: dt.date = dt.date(2020, 12, 31),
) -> Path:
    resources_path = get_resources_path()
    extractor_name = extractor.__class__.__name__
    directory_name = change_camel_case_to_snake_case(string=extractor_name.replace('Extractor', ''))
    directory_path = resources_path / source / directory_name
    directory_path.mkdir(parents=True, exist_ok=True)
    file_name = '_'.join([start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')])
    return (directory_path / file_name).with_suffix('.pkl')
