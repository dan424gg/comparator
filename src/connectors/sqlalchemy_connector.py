import typing
import pandas as pd
import sqlalchemy
from .base import Connector


class SQLAlchemyConnector(Connector):
    """Shared SQLAlchemy-based connector implementing common functionality.

    Subclasses should provide a connection string by calling super().__init__(conn_str).
    """

    def __init__(self, conn_str: str):
        self.conn_str = conn_str

    def _get_engine(self):
        return sqlalchemy.create_engine(self.conn_str)

    def get_tables(self, exclude_tables: list = []) -> list:
        engine = self._get_engine()
        inspector = sqlalchemy.inspect(engine)
        tables = [table for table in inspector.get_table_names() if table not in exclude_tables]
        return tables

    def get_data(self, table_name: str = None, query: str = None) -> pd.DataFrame:
        assert table_name or query, "Either table_name or query must be provided."
        engine = self._get_engine()
        with engine.connect() as connection:
            if not query:
                query = sqlalchemy.Table(table_name, sqlalchemy.MetaData(), autoload_with=engine).select()
            else:
                query = sqlalchemy.text(query) if isinstance(query, str) else query

            df = pd.read_sql_query(query, connection)

        return df

    def get_primary_keys(self, table_names: typing.List[str]) -> list:
        engine = self._get_engine()
        inspector = sqlalchemy.inspect(engine)
        primary_keys = []
        for table_name in table_names:
            primary_keys.append(inspector.get_pk_constraint(table_name)['constrained_columns'])
        return primary_keys

    def get_schema(self, table_name: str) -> list:
        engine = self._get_engine()
        inspector = sqlalchemy.inspect(engine)
        return inspector.get_columns(table_name)
