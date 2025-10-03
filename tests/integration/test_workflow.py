import pytest
from comparator.src.comparator import Comparator
from comparator.src.datasource import DataSource
from comparator.src.normalizer import Normalizer
from comparator.src.reportexporter import ReportExporter
import pandas as pd
import os
import tempfile

def test_end_to_end_workflow(db_data, tmp_path):
    """
    Test the complete workflow from data loading to report generation
    """
    # 1. Load source and target data
    source = DataSource.from_sql(
        "SELECT * FROM source_table",
        connection=db_data
    )
    target = DataSource.from_sql(
        "SELECT * FROM target_table",
        connection=db_data
    )
    
    # 2. Get DataFrames
    source_df = source.get_data()
    target_df = target.get_data()
    
    # 3. Normalize data
    normalizer = Normalizer()
    source_df = normalizer.normalize(source_df)
    target_df = normalizer.normalize(target_df)
    
    # 4. Compare data
    comparator = Comparator(
        pk=['id'],
        ignore_cols=['updated_at']
    )
    result = comparator.compare(source_df, target_df)
    
    # 5. Generate and validate report
    report_path = tmp_path / "comparison_report.xlsx"
    exporter = ReportExporter(result)
    exporter.to_excel(str(report_path))
    
    # Validate results
    assert os.path.exists(report_path)
    assert len(result['matching']) == 1  # Only Alice should match exactly
    assert len(result['mismatched']) == 1  # Bob has different values
    assert len(result['only_in_source']) == 1  # Charlie only in source
    assert len(result['only_in_target']) == 1  # David only in target

def test_workflow_with_different_column_order(db_data):
    """
    Test workflow when source and target have different column orders
    """
    # Create DataFrames with different column orders
    source_df = pd.read_sql(
        "SELECT id, name, value, updated_at FROM source_table",
        db_data
    )
    target_df = pd.read_sql(
        "SELECT updated_at, id, value, name FROM target_table",
        db_data
    )
    
    comparator = Comparator(pk=['id'])
    result = comparator.compare(source_df, target_df)
    
    assert isinstance(result, dict)
    assert all(k in result for k in ['matching', 'mismatched', 'only_in_source', 'only_in_target'])

def test_workflow_with_csv_and_db(db_data, tmp_path):
    """
    Test workflow combining CSV and database sources
    """
    # Get database data
    db_df = pd.read_sql("SELECT * FROM source_table", db_data)
    
    # Save to CSV
    csv_path = tmp_path / "test_data.csv"
    db_df.to_csv(csv_path, index=False)
    
    # Create data sources
    source = DataSource.from_sql(
        "SELECT * FROM target_table",
        connection=db_data
    )
    target = DataSource.from_csv(str(csv_path))
    
    # Compare
    comparator = Comparator(pk=['id'])
    result = comparator.compare(source.get_data(), target.get_data())
    
    assert isinstance(result, dict)
    assert all(k in result for k in ['matching', 'mismatched', 'only_in_source', 'only_in_target'])

def test_workflow_with_data_normalization(db_data):
    """
    Test workflow with specific focus on data normalization
    """
    # Create test data with normalization needs
    with db_data.begin() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS messy_data (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100),
                value VARCHAR(100)
            )
        """)
        conn.execute("""
            INSERT INTO messy_data VALUES
                (1, '  Alice  ', '1,234.56'),
                (2, 'BOB', '$2,000.00'),
                (3, 'charlie', 'N/A')
        """)
    
    # Load and normalize data
    messy_df = pd.read_sql("SELECT * FROM messy_data", db_data)
    normalizer = Normalizer()
    clean_df = normalizer.normalize(messy_df)
    
    # Verify normalization
    assert clean_df['name'].tolist() == ['ALICE', 'BOB', 'CHARLIE']
    assert clean_df['value'].dtype == float
    assert pd.isna(clean_df['value'].iloc[2])  # N/A should be converted to NaN

def test_error_handling_in_workflow(db_data):
    """
    Test error handling throughout the workflow
    """
    # Test with invalid SQL
    with pytest.raises(Exception):
        DataSource.from_sql("INVALID SQL", db_data)
    
    # Test with invalid CSV
    with pytest.raises(Exception):
        DataSource.from_csv("nonexistent.csv")
    
    # Test with invalid primary key
    source_df = pd.read_sql("SELECT * FROM source_table", db_data)
    target_df = pd.read_sql("SELECT * FROM target_table", db_data)
    
    with pytest.raises(Exception):
        comparator = Comparator(pk=['nonexistent_column'])
        comparator.compare(source_df, target_df)