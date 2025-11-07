"""
test_postgres_connector.py - PostgreSQL-specific tests

These tests only run against the PostgreSQL connector and test
Postgres-specific features and behaviors.
"""
import pytest


class TestPostgresSpecificQueries:
    """Tests for Postgres-specific query syntax."""
    
    def test_no_backticks_required(self, postgres_connector):
        """Test that Postgres doesn't require backticks for table names."""
        df = postgres_connector.get_data(table_name="users")
        
        # Should work without errors
        assert len(df) > 0
    
    def test_postgres_specific_functions(self, postgres_connector):
        """Test Postgres-specific SQL functions."""
        query = "SELECT name || ' - ' || email as full_info FROM users"
        df = postgres_connector.get_data(query=query)
        
        assert len(df) == 3
        assert " - " in df.iloc[0]["full_info"]
    
    def test_limit_clause(self, postgres_connector):
        """Test Postgres LIMIT clause."""
        query = "SELECT * FROM users LIMIT 2"
        df = postgres_connector.get_data(query=query)
        
        assert len(df) == 2
    
    def test_serial_primary_key(self, postgres_connector):
        """Test that SERIAL primary keys work correctly."""
        primary_keys = postgres_connector.get_primary_keys(["users"])
        
        assert len(primary_keys) == 1
        assert "id" in primary_keys[0] or primary_keys[0] == ["id"]
    
    def test_case_sensitivity(self, postgres_connector):
        """Test Postgres case sensitivity in queries."""
        # Postgres is case-insensitive for unquoted identifiers
        query = "SELECT NAME, EMAIL FROM USERS LIMIT 1"
        df = postgres_connector.get_data(query=query)
        
        assert len(df) == 1
        assert "name" in df.columns or "NAME" in df.columns


class TestPostgresDataTypes:
    """Tests for Postgres-specific data types."""
    
    def test_varchar_handling(self, postgres_connector):
        """Test VARCHAR data type handling."""
        schema = postgres_connector.get_schema("users")
        
        name_col = next(col for col in schema if col["name"] == "name")
        assert name_col is not None
    
    def test_numeric_handling(self, postgres_connector):
        """Test NUMERIC/DECIMAL data type handling."""
        schema = postgres_connector.get_schema("orders")
        
        amount_col = next(col for col in schema if col["name"] == "amount")
        assert amount_col is not None
    
    def test_integer_handling(self, postgres_connector):
        """Test INTEGER data type handling."""
        schema = postgres_connector.get_schema("users")
        
        age_col = next(col for col in schema if col["name"] == "age")
        assert age_col is not None
    
    def test_serial_type_in_schema(self, postgres_connector):
        """Test that SERIAL type is properly represented in schema."""
        schema = postgres_connector.get_schema("users")
        
        id_col = next(col for col in schema if col["name"] == "id")
        assert id_col is not None


class TestPostgresSpecificFeatures:
    """Tests for Postgres-specific features."""
    
    def test_returning_clause(self, postgres_connector):
        """Test that Postgres RETURNING clause works in queries."""
        # Note: This would require INSERT/UPDATE, skipping for read-only tests
        pass
    
    def test_array_types(self, postgres_connector):
        """Test handling of arrays (Postgres-specific)."""
        # Would need a table with array columns to test properly
        # Placeholder for future array support testing
        pass
    
    def test_json_support(self, postgres_connector):
        """Test JSON/JSONB support (Postgres-specific)."""
        # Would need a table with JSON columns to test properly
        # Placeholder for future JSON support testing
        pass


class TestPostgresErrorHandling:
    """Tests for Postgres-specific error handling."""
    
    def test_invalid_table_name(self, postgres_connector):
        """Test handling of invalid table name."""
        with pytest.raises(Exception):
            postgres_connector.get_data(table_name="nonexistent_table")
    
    def test_invalid_query_syntax(self, postgres_connector):
        """Test handling of invalid SQL syntax."""
        with pytest.raises(Exception):
            postgres_connector.get_data(query="INVALID SQL SYNTAX HERE")
    
    def test_invalid_column_in_query(self, postgres_connector):
        """Test querying non-existent column."""
        with pytest.raises(Exception):
            postgres_connector.get_data(query="SELECT nonexistent_column FROM users")
    
    def test_schema_for_nonexistent_table(self, postgres_connector):
        """Test getting schema for non-existent table."""
        with pytest.raises(Exception):
            postgres_connector.get_schema("nonexistent_table")


class TestPostgresPerformance:
    """Tests for Postgres-specific performance considerations."""
    
    def test_large_result_set(self, postgres_connector):
        """Test handling of larger result sets."""
        # Query all tables' data
        tables = postgres_connector.get_tables()
        
        for table in tables:
            df = postgres_connector.get_data(table_name=table)
            assert df is not None
    
    def test_connection_reuse(self, postgres_connector):
        """Test that multiple queries work correctly."""
        # Execute multiple queries to ensure connection handling
        df1 = postgres_connector.get_data(table_name="users")
        df2 = postgres_connector.get_data(table_name="orders")
        df3 = postgres_connector.get_data(table_name="products")
        
        assert len(df1) > 0
        assert len(df2) > 0
        assert len(df3) > 0
    
    def test_concurrent_operations(self, postgres_connector):
        """Test multiple operations in sequence."""
        # Get tables
        tables = postgres_connector.get_tables()
        
        # Get schema for each table
        schemas = [postgres_connector.get_schema(table) for table in tables[:3]]
        
        # Get primary keys
        pks = postgres_connector.get_primary_keys(tables[:3])
        
        assert len(schemas) == 3
        assert len(pks) == 3


class TestPostgresTransactions:
    """Tests for Postgres transaction behavior."""
    
    def test_read_operations_dont_require_commit(self, postgres_connector):
        """Test that read operations work without explicit commits."""
        # Multiple reads should work fine
        df1 = postgres_connector.get_data(table_name="users")
        df2 = postgres_connector.get_data(table_name="orders")
        
        assert len(df1) > 0
        assert len(df2) > 0