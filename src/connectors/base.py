import typing
from abc import ABC, abstractmethod

import pandas as pd

class Connector(ABC):
    @abstractmethod
    def get_tables() -> typing.List[str]:
        pass
    
    @abstractmethod
    def get_primary_keys(tables: typing.List[str]) -> typing.List[str]:
        pass
    
    @abstractmethod
    def get_data(table: str = None, query: str = None) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_schema(table: str) -> typing.Any:
        pass