import typing as t

from etl_day_ahead_forecasting._transformers.pse._base_transformer import BasePseTransformer

__all__ = ('CrossBorderFlowsTransformer',)


class CrossBorderFlowsTransformer(BasePseTransformer):

    @staticmethod
    def _get_date_format() -> str:
        return '%Y%m%d'

    @staticmethod
    def _column_names_mapping() -> t.Dict[str, str]:
        return {
            'CEPS_EXP': 'czechia_export',
            'CEPS_IMP': 'czechia_import',
            'SEPS_EXP': 'slovakia_export',
            'SEPS_IMP': 'slovakia_import',
            '50HzT_EXP': 'germany_export',
            '50HzT_IMP': 'germany_import',
            'SVK_EXP': 'sweden_export',
            'SVK_IMP': 'sweden_import',
            'UA_EXP': 'ukraine_export',
            'UA_IMP': 'ukraine_import',
            'LIT_EXP': 'lithuania_export',
            'LIT_IMP': 'lithuania_import',
        }
