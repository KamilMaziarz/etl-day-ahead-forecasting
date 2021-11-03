import logging.config
from pathlib import Path

import coloredlogs
import yaml


def setup_logging():
    Path('logs').mkdir(parents=True, exist_ok=True)
    with open('logger_config.yml', 'rt') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
        coloredlogs.install()


setup_logging()
