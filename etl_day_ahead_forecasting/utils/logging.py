import logging.config

import coloredlogs


def initialize_logging():
    logging.config.fileConfig(fname='logger.conf', disable_existing_loggers=False)
    coloredlogs.install()
    logger = logging.getLogger(__name__)
    logger.info('Logger has been initiated.')
