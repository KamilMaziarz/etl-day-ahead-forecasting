import typing as t

from etl_day_ahead_forecasting.transformers.pse._base_transformer import BasePseTransformer

__all__ = ('RenewablesGenerationTransformer',)


class RenewablesGenerationTransformer(BasePseTransformer):

    @staticmethod
    def _get_date_format() -> str:
        return '%Y-%m-%d'

    @staticmethod
    def _column_names_mapping() -> t.Dict[str, str]:
        return {
            'Generacja źródeł wiatrowych': 'wind_generation',
            'Generacja źródeł fotowoltaicznych': 'solar_generation',
        }
