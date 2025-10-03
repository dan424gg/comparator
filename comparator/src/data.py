import pandas as pd
import sqlalchemy


class DataSource:
    def load(self) -> pd.DataFrame:
        raise NotImplementedError

class CsvSource(DataSource):
    def __init__(self, path: str, **read_opts):
        self.path = path
        self.read_opts = read_opts
    def load(self) -> pd.DataFrame:
        return pd.read_csv(self.path, **self.read_opts)

class JsonSource(DataSource):
    def __init__(self, path: str, **read_opts):
        self.path = path
        self.read_opts = read_opts
    def load(self) -> pd.DataFrame:
        return pd.read_json(self.path, **self.read_opts)

class PostgresSource(DataSource):
    def __init__(self, conn_str: str, query: str):
        self.conn_str = conn_str
        self.query = sqlalchemy.text(query) if isinstance(query, str) else query
    def load(self) -> pd.DataFrame:
        from sqlalchemy import create_engine
        return pd.read_sql(self.query, create_engine(self.conn_str))

class Normalizer:
    def default_col_generalizer(col: str) -> str:
        return (
            col.strip()         # remove whitespace
            .lower()         # lowercase
            .replace(" ", "_")
            .replace("-", "_")
        )

    def __init__(self, col_generalizer=None, col_mapping=None, custom_rules=None):
        self.col_generalizer = col_generalizer or self.default_col_generalizer
        self.col_mapping = col_mapping or {}
        self.custom_rules = custom_rules or {}

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        if self.col_generalizer:
            df.columns = [self.col_generalizer(c) for c in df.columns]
        
        if self.col_mapping:
            df = df.rename(columns=self.col_mapping)

        for col, func in self.custom_rules.items():
            if col in df.columns:
                df[col] = df[col].apply(func)
        return df
