import pytest
from comparator.src.datasource import DataSource
import pandas as pd
import tempfile
import os

def test_database_source_loading(db_data):
    """Test loading data from a database source"""
    source = DataSource.from_sql(
        "SELECT * FROM source_table",
        connection=db_data
    )
    df = source.get_data()
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert set(df['name']) == {'Alice', 'Bob', 'Charlie'}

def test_database_target_loading(db_data):
    """Test loading data from a database target"""
    target = DataSource.from_sql(
        "SELECT * FROM target_table",
        connection=db_data
    )
    df = target.get_data()
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert set(df['name']) == {'Alice', 'Bob', 'David'}

def test_csv_persistence(db_data):
    """Test loading from database and saving to CSV"""
    # Load from database
    source = DataSource.from_sql(
        "SELECT * FROM source_table",
        connection=db_data
    )
    df = source.get_data()
    
    # Save to temporary CSV
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
        df.to_csv(tmp.name, index=False)
        
        # Load back from CSV
        csv_source = DataSource.from_csv(tmp.name)
        csv_df = csv_source.get_data()
        
        # Compare the data
        pd.testing.assert_frame_equal(
            df.sort_values('id').reset_index(drop=True),
            csv_df.sort_values('id').reset_index(drop=True),
            check_dtype=False  # CSV might change some dtypes
        )
    
    os.unlink(tmp.name)

def test_filtered_sql_query(db_data):
    """Test loading filtered data from SQL"""
    source = DataSource.from_sql(
        "SELECT * FROM source_table WHERE value > 15",
        connection=db_data
    )
    df = source.get_data()
    
    assert len(df) == 2  # Should only get Bob and Charlie
    assert all(value > 15 for value in df['value'])

def test_complex_sql_query(db_data):
    """Test loading data with a complex SQL query"""
    query = """
        SELECT 
            t1.id,
            t1.name,
            t1.value as source_value,
            t2.value as target_value,
            t1.updated_at
        FROM source_table t1
        LEFT JOIN target_table t2 ON t1.id = t2.id
        WHERE t1.value != COALESCE(t2.value, -1)
    """
    
    source = DataSource.from_sql(query, connection=db_data)
    df = source.get_data()
    
    assert 'source_value' in df.columns
    assert 'target_value' in df.columns
    assert len(df) > 0  # Should find at least one difference

def test_error_handling(db_data):
    """Test error handling for invalid SQL queries"""
    with pytest.raises(Exception):
        DataSource.from_sql(
            "SELECT * FROM nonexistent_table",
            connection=db_data
        )

    with pytest.raises(Exception):
        DataSource.from_sql(
            "INVALID SQL QUERY",
            connection=db_data
        )