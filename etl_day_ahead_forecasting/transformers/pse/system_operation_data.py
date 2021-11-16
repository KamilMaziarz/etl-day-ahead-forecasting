import typing as t

from etl_day_ahead_forecasting.transformers.pse._base_transformer import BasePseTransformer

__all__ = ('SystemOperationDataTransformer',)


class SystemOperationDataTransformer(BasePseTransformer):

    @staticmethod
    def _get_date_format() -> str:
        return '%Y-%m-%d'

    @staticmethod
    def _column_names_mapping() -> t.Dict[str, str]:
        return {
            'Krajowe zapotrzebowanie na moc': 'demand',
            'Sumaryczna generacja JWCD': 'dispatchable_units_generation',
            'Generacja PI': 'reserve_type_1_generation',
            'Generacja IRZ': 'reserve_type_2_generation',
            'Sumaryczna generacja nJWCD': 'nondispatchable_units_generation',
            'Krajowe saldo wymiany międzysystemowej równoległej': 'cross_border_flow_parallel',
            'Krajowe saldo wymiany międzysystemowej nierównoległej': 'cross_border_flow_nonparallel',
        }
