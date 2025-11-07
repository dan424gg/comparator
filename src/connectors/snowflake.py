

import typing
import pandas as pd
import sqlalchemy
from .base import Connector

class SnowflakeConnector(Connector):
    def __init__(self, username: str, password: str, account: str, database: str, schema: str, warehouse: str, role: str = None):
        # snowflake-sqlalchemy connection string
        self.conn_str = (
            f"snowflake://{username}:{password}@{account}/{database}/{schema}?warehouse={warehouse}"
        )
        if role:
            self.conn_str += f"&role={role}"
    
    def get_tables(self, exclude_tables: list = []) -> list:
        engine = sqlalchemy.create_engine(self.conn_str)
        inspector = sqlalchemy.inspect(engine)
        tables = [table for table in inspector.get_table_names() if table not in exclude_tables]
        return tables
    
    def get_data(self, table_name: str = None, query: str = None) -> pd.DataFrame:
        assert table_name or query, "Either table_name or query must be provided."
        engine = sqlalchemy.create_engine(self.conn_str)
        with engine.connect() as connection:
            if not query:
                query = sqlalchemy.text(f'SELECT * FROM "{table_name}";')
            else:
                query = sqlalchemy.text(query) if isinstance(query, str) else query
            df = pd.read_sql_query(query, connection)
        return df
    
    def get_primary_keys(self, table_names: typing.List[str]) -> list:
        engine = sqlalchemy.create_engine(self.conn_str)
        inspector = sqlalchemy.inspect(engine)
        primary_keys = []
        for table_name in table_names:
            pk = inspector.get_pk_constraint(table_name).get('constrained_columns', [])
            primary_keys.append(pk)
        return primary_keys
    
    def get_schema(self, table_name: str) -> list:
        engine = sqlalchemy.create_engine(self.conn_str)
        inspector = sqlalchemy.inspect(engine)
        return inspector.get_columns(table_name)