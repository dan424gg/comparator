import pytest
import pandas as pd
from comparator.src.datasource import DataSource
from pathlib import Path
import tempfile
import os

@pytest.fixture
def sample_csv_file():
    """Create a temporary CSV file for testing"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write('id,name,value\n')
        f.write('1,Alice,10\n')
        f.write('2,Bob,20\n')
    yield f.name
    os.unlink(f.name)

def test_from_csv(sample_csv_file):
    """Test loading data from CSV file"""
    ds = DataSource.from_csv(sample_csv_file)
    df = ds.get_data()
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ['id', 'name', 'value']
    assert df.iloc[0]['name'] == 'Alice'

def test_from_dataframe():
    """Test creating DataSource from DataFrame"""
    df = pd.DataFrame({
        'id': [1, 2],
        'name': ['Alice', 'Bob'],
        'value': [10, 20]
    })
    
    ds = DataSource.from_dataframe(df)
    result = ds.get_data()
    
    assert isinstance(result, pd.DataFrame)
    pd.testing.assert_frame_equal(df, result)

def test_invalid_csv_path():
    """Test error handling for invalid CSV file path"""
    with pytest.raises(FileNotFoundError):
        DataSource.from_csv('nonexistent.csv')

def test_empty_dataframe():
    """Test handling of empty DataFrame"""
    df = pd.DataFrame(columns=['id', 'name', 'value'])
    ds = DataSource.from_dataframe(df)
    result = ds.get_data()
    
    assert len(result) == 0
    assert list(result.columns) == ['id', 'name', 'value']

def test_csv_with_missing_values(tmp_path):
    """Test handling of CSV with missing values"""
    csv_path = tmp_path / "test.csv"
    with open(csv_path, 'w') as f:
        f.write('id,name,value\n')
        f.write('1,Alice,\n')
        f.write('2,,20\n')
    
    ds = DataSource.from_csv(str(csv_path))
    df = ds.get_data()
    
    assert df['value'].iloc[0] != df['value'].iloc[0]  # Check if NaN
    assert pd.isna(df['name'].iloc[1])

def test_csv_with_custom_options(sample_csv_file):
    """Test CSV loading with custom options"""
    ds = DataSource.from_csv(
        sample_csv_file,
        options={
            'dtype': {'id': int, 'value': float},
            'parse_dates': False
        }
    )
    df = ds.get_data()
    
    assert df['id'].dtype == int
    assert df['value'].dtype == float