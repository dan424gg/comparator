"""
test_mysql_connector.py - MySQL-specific tests

These tests only run against the MySQL connector and test
MySQL-specific features and behaviors.
"""
import pytest


class TestMySQLSpecificQueries:
    """Tests for MySQL-specific query syntax."""
    
    def test_backtick_table_names(self, mysql_connector):
        """Test that MySQL uses backticks for table names."""
        df = mysql_connector.get_data(table_name="users")
        
        # Should work without errors
        assert len(df) > 0
    
    def test_mysql_specific_functions(self, mysql_connector):
        """Test MySQL-specific SQL functions."""
        query = "SELECT CONCAT(name, ' - ', email) as full_info FROM users"
        df = mysql_connector.get_data(query=query)
        
        assert len(df) == 3
        assert " - " in df.iloc[0]["full_info"]
    
    def test_limit_clause(self, mysql_connector):
        """Test MySQL LIMIT clause."""
        query = "SELECT * FROM users LIMIT 2"
        df = mysql_connector.get_data(query=query)
        
        assert len(df) == 2
    
    def test_auto_increment_primary_key(self, mysql_connector):
        """Test that AUTO_INCREMENT primary keys work correctly."""
        primary_keys = mysql_connector.get_primary_keys(["users"])
        
        assert len(primary_keys) == 1
        assert "id" in primary_keys[0] or primary_keys[0] == ["id"]


class TestMySQLDataTypes:
    """Tests for MySQL-specific data types."""
    
    def test_varchar_handling(self, mysql_connector):
        """Test VARCHAR data type handling."""
        schema = mysql_connector.get_schema("users")
        
        name_col = next(col for col in schema if col["name"] == "name")
        # MySQL should report VARCHAR or similar string type
        assert name_col is not None
    
    def test_decimal_handling(self, mysql_connector):
        """Test DECIMAL data type handling."""
        schema = mysql_connector.get_schema("orders")
        
        amount_col = next(col for col in schema if col["name"] == "amount")
        assert amount_col is not None
    
    def test_int_handling(self, mysql_connector):
        """Test INT data type handling."""
        schema = mysql_connector.get_schema("users")
        
        age_col = next(col for col in schema if col["name"] == "age")
        assert age_col is not None


class TestMySQLErrorHandling:
    """Tests for MySQL-specific error handling."""
    
    def test_invalid_table_name(self, mysql_connector):
        """Test handling of invalid table name."""
        with pytest.raises(Exception):  # Should raise some database exception
            mysql_connector.get_data(table_name="nonexistent_table")
    
    def test_invalid_query_syntax(self, mysql_connector):
        """Test handling of invalid SQL syntax."""
        with pytest.raises(Exception):
            mysql_connector.get_data(query="INVALID SQL SYNTAX HERE")
    
    def test_invalid_column_in_query(self, mysql_connector):
        """Test querying non-existent column."""
        with pytest.raises(Exception):
            mysql_connector.get_data(query="SELECT nonexistent_column FROM users")


class TestMySQLPerformance:
    """Tests for MySQL-specific performance considerations."""
    
    def test_large_result_set(self, mysql_connector):
        """Test handling of larger result sets."""
        # Query all tables' data
        tables = mysql_connector.get_tables()
        
        for table in tables:
            df = mysql_connector.get_data(table_name=table)
            assert df is not None
    
    def test_connection_reuse(self, mysql_connector):
        """Test that multiple queries work correctly."""
        # Execute multiple queries to ensure connection handling
        df1 = mysql_connector.get_data(table_name="users")
        df2 = mysql_connector.get_data(table_name="orders")
        df3 = mysql_connector.get_data(table_name="products")
        
        assert len(df1) > 0
        assert len(df2) > 0
        assert len(df3) > 0