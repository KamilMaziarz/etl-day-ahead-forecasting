import datetime as dt
from os import path
from pathlib import Path

import pydantic

_PATH_ELEMENT = pydantic.constr(regex='^[a-z_]+$')


def get_resources_path() -> Path:
    current_path = Path(path.dirname(path.realpath(__file__)))
    resources_path = current_path / ".." / ".." / "resources"
    resources_path.mkdir(parents=True, exist_ok=True)
    return resources_path


@pydantic.validate_arguments
def get_backup_path(source: _PATH_ELEMENT, name: _PATH_ELEMENT, start: dt.date, end: dt.date) -> Path:
    resources_path = get_resources_path()
    directory_path = resources_path / source / name
    directory_path.mkdir(parents=True, exist_ok=True)
    file_name = '_'.join([start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')])
    return (directory_path / file_name).with_suffix('.pkl')
