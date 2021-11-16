import datetime as dt


class ETLError(Exception):
    pass


class PseTooBroadTimeRangeError(ETLError):
    def __init__(self, date_range: dt.timedelta):
        super().__init__(f'Maximum date range of 30 days has been exceeded: {date_range.days} days.')


class ETLExtractorError(ETLError):
    pass


class ETLTransformerError(ETLError):
    pass


class ExpectedDataTypeNotFoundError(ETLTransformerError):
    def __init__(self, expected_data_type: str):
        super().__init__(f'Expected data type "{expected_data_type}" not found in data.')


class ETLLoaderError(ETLError):
    pass


class IncorrectSuffixError(ETLLoaderError):
    def __init__(self, file_type: str):
        super().__init__(f'Path suffix must be ".pkl". Current suffix is "{file_type}".')
