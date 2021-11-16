import typing as t

from etl_day_ahead_forecasting.transformers.pse._base_transformer import BasePseTransformer

__all__ = ('UnitsOutagesTransformer',)


class UnitsOutagesTransformer(BasePseTransformer):

    @staticmethod
    def _get_date_format() -> str:
        return '%Y%m%d'

    @staticmethod
    def _column_names_mapping() -> t.Dict[str, str]:
        return {
            'Elektrownia': 'unit_name',
            'Kod JW': 'unit_code',
            'Wielkość ubytku elektrownianego': 'unit_outage',
            'Wielkość ubytku sieciowego': 'grid_caused_unit_outage',
            'Dostępne zdolności wytwórcze': 'unit_available_capacity',
        }

    @staticmethod
    def _get_group_by_columns() -> t.Optional[t.List[str]]:
        return ['Elektrownia', 'Kod JW']
