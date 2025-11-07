"""
test_connector_interface.py - Reusable tests for any Connector implementation

These tests use the parameterized 'connector' fixture to automatically
run against all connector implementations (MySQL, Postgres, etc.)
"""
import pytest
import pandas as pd


class TestGetTables:
    """Tests for the get_tables() method."""
    
    def test_get_tables_returns_list(self, connector):
        """Test that get_tables returns a list."""
        tables = connector.get_tables()
        assert isinstance(tables, list)
    
    def test_get_tables_contains_expected_tables(self, connector, expected_tables):
        """Test that get_tables returns all expected tables."""
        tables = connector.get_tables()
        
        for expected_table in expected_tables:
            assert expected_table in tables, f"Expected table '{expected_table}' not found"
    
    def test_get_tables_with_exclusion(self, connector):
        """Test that get_tables correctly excludes specified tables."""
        tables = connector.get_tables(exclude_tables=["users"])
        
        assert "users" not in tables
        assert "orders" in tables
        assert "products" in tables
    
    def test_get_tables_with_multiple_exclusions(self, connector):
        """Test excluding multiple tables."""
        tables = connector.get_tables(exclude_tables=["users", "orders"])
        
        assert "users" not in tables
        assert "orders" not in tables
        assert "products" in tables
    
    def test_get_tables_with_empty_exclusion_list(self, connector, expected_tables):
        """Test that empty exclusion list returns all tables."""
        tables = connector.get_tables(exclude_tables=[])
        
        assert len(tables) >= len(expected_tables)


class TestGetData:
    """Tests for the get_data() method."""
    
    def test_get_data_by_table_name(self, connector, expected_user_columns):
        """Test getting data by table name."""
        df = connector.get_data(table_name="users")
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert list(df.columns) == expected_user_columns
    
    def test_get_data_returns_correct_rows(self, connector):
        """Test that get_data returns the correct data."""
        df = connector.get_data(table_name="users")
        
        assert "Alice" in df["name"].values
        assert "Bob" in df["name"].values
        assert "Charlie" in df["name"].values
    
    def test_get_data_by_custom_query(self, connector):
        """Test getting data using a custom query."""
        query = "SELECT name, email FROM users WHERE age > 25"
        df = connector.get_data(query=query)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2  # Alice (30) and Charlie (35)
        assert list(df.columns) == ["name", "email"]
        assert "Bob" not in df["name"].values
    
    def test_get_data_with_where_clause(self, connector):
        """Test query with WHERE clause."""
        query = "SELECT * FROM users WHERE name = 'Alice'"
        df = connector.get_data(query=query)
        
        assert len(df) == 1
        assert df.iloc[0]["name"] == "Alice"
        assert df.iloc[0]["age"] == 30
    
    def test_get_data_with_join(self, connector):
        """Test getting data with a JOIN query."""
        query = """
            SELECT u.name, o.product, o.amount 
            FROM users u 
            JOIN orders o ON u.id = o.user_id
            WHERE u.name = 'Alice'
        """
        df = connector.get_data(query=query)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2  # Alice has 2 orders
        assert "Alice" in df["name"].values
        assert "Laptop" in df["product"].values
        assert "Keyboard" in df["product"].values
    
    def test_get_data_with_aggregation(self, connector):
        """Test query with aggregation."""
        query = "SELECT COUNT(*) as user_count FROM users"
        df = connector.get_data(query=query)
        
        assert len(df) == 1
        assert df.iloc[0]["user_count"] == 3
    
    def test_get_data_with_order_by(self, connector):
        """Test query with ORDER BY clause."""
        query = "SELECT name FROM users ORDER BY age DESC"
        df = connector.get_data(query=query)
        
        assert df.iloc[0]["name"] == "Charlie"  # age 35
        assert df.iloc[1]["name"] == "Alice"    # age 30
        assert df.iloc[2]["name"] == "Bob"      # age 25
    
    def test_get_data_empty_result(self, connector):
        """Test getting data with a query that returns no results."""
        query = "SELECT * FROM users WHERE age > 100"
        df = connector.get_data(query=query)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        assert list(df.columns) == ["id", "name", "email", "age"]
    
    def test_get_data_without_parameters_raises_error(self, connector):
        """Test that assertion error is raised when neither table_name nor query provided."""
        with pytest.raises(AssertionError):
            connector.get_data()
    
    def test_get_data_none_parameters_raises_error(self, connector):
        """Test error when both parameters are explicitly None."""
        with pytest.raises(AssertionError):
            connector.get_data(table_name=None, query=None)


class TestGetPrimaryKeys:
    """Tests for the get_primary_keys() method."""
    
    def test_get_primary_keys_returns_list(self, connector):
        """Test that get_primary_keys returns a list."""
        primary_keys = connector.get_primary_keys(["users"])
        
        assert isinstance(primary_keys, list)
    
    def test_get_primary_keys_single_table(self, connector):
        """Test getting primary key for a single table."""
        primary_keys = connector.get_primary_keys(["users"])
        
        assert len(primary_keys) == 1
        assert len(primary_keys[0]) > 0
        assert "id" in primary_keys[0] or primary_keys[0] == ["id"]
    
    def test_get_primary_keys_multiple_tables(self, connector):
        """Test getting primary keys for multiple tables."""
        primary_keys = connector.get_primary_keys(["users", "orders"])
        
        assert len(primary_keys) == 2
        assert "id" in primary_keys[0] or primary_keys[0] == ["id"]
        assert "order_id" in primary_keys[1] or primary_keys[1] == ["order_id"]
    
    def test_get_primary_keys_all_tables(self, connector, expected_tables):
        """Test getting primary keys for all tables."""
        primary_keys = connector.get_primary_keys(expected_tables)
        
        assert len(primary_keys) == len(expected_tables)
        for pk in primary_keys:
            assert isinstance(pk, list)
            assert len(pk) > 0
    
    def test_get_primary_keys_empty_list(self, connector):
        """Test that empty table list returns empty primary keys list."""
        primary_keys = connector.get_primary_keys([])
        
        assert isinstance(primary_keys, list)
        assert len(primary_keys) == 0


class TestGetSchema:
    """Tests for the get_schema() method."""
    
    def test_get_schema_returns_list(self, connector):
        """Test that get_schema returns a list."""
        schema = connector.get_schema("users")
        
        assert isinstance(schema, list)
    
    def test_get_schema_contains_all_columns(self, connector, expected_user_columns):
        """Test that schema contains all expected columns."""
        schema = connector.get_schema("users")
        
        column_names = [col["name"] for col in schema]
        
        for expected_col in expected_user_columns:
            assert expected_col in column_names
    
    def test_get_schema_column_structure(self, connector):
        """Test that schema columns have expected structure."""
        schema = connector.get_schema("users")
        
        assert len(schema) > 0
        
        # Check first column has expected keys
        first_col = schema[0]
        assert "name" in first_col
        assert isinstance(first_col["name"], str)
    
    def test_get_schema_different_tables(self, connector):
        """Test getting schema for different tables."""
        users_schema = connector.get_schema("users")
        orders_schema = connector.get_schema("orders")
        
        users_columns = [col["name"] for col in users_schema]
        orders_columns = [col["name"] for col in orders_schema]
        
        assert "name" in users_columns
        assert "product" in orders_columns
        assert "name" not in orders_columns
        assert "product" not in users_columns


class TestDataTypes:
    """Tests for data type preservation."""
    
    def test_integer_types_preserved(self, connector):
        """Test that integer types are properly preserved."""
        df = connector.get_data(table_name="users")
        
        assert pd.api.types.is_integer_dtype(df["id"])
        assert pd.api.types.is_integer_dtype(df["age"])
    
    def test_string_types_preserved(self, connector):
        """Test that string types are properly preserved."""
        df = connector.get_data(table_name="users")
        
        assert pd.api.types.is_object_dtype(df["name"])
        assert pd.api.types.is_object_dtype(df["email"])
    
    def test_decimal_types_preserved(self, connector):
        """Test that decimal/numeric types are properly preserved."""
        df = connector.get_data(table_name="orders")
        
        # Should be numeric type (float or decimal)
        assert pd.api.types.is_numeric_dtype(df["amount"])


class TestIntegration:
    """Integration tests combining multiple methods."""
    
    def test_full_discovery_workflow(self, connector):
        """Test a complete workflow: discover tables, get schema, query data."""
        # Step 1: Get all tables
        tables = connector.get_tables()
        assert len(tables) >= 3
        
        # Step 2: Get schema for first table
        first_table = tables[0]
        schema = connector.get_schema(first_table)
        assert len(schema) > 0
        
        # Step 3: Get primary keys
        primary_keys = connector.get_primary_keys([first_table])
        assert len(primary_keys) == 1
        
        # Step 4: Query data
        df = connector.get_data(table_name=first_table)
        assert len(df) > 0
    
    def test_filter_and_query_workflow(self, connector):
        """Test filtering tables and querying specific data."""
        # Get tables excluding orders
        tables = connector.get_tables(exclude_tables=["orders"])
        
        # Query users table
        df = connector.get_data(table_name="users", query=None)
        
        # Verify we can work with the data
        assert len(df) > 0
        assert "users" in tables
    
    def test_schema_matches_data(self, connector):
        """Test that schema column names match actual data columns."""
        schema = connector.get_schema("users")
        df = connector.get_data(table_name="users")
        
        schema_columns = sorted([col["name"] for col in schema])
        data_columns = sorted(df.columns.tolist())
        
        assert schema_columns == data_columns