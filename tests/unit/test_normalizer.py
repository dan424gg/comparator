import pytest
import pandas as pd
import numpy as np
from comparator.src.normalizer import Normalizer

def test_normalize_string_case():
    """Test string case normalization"""
    df = pd.DataFrame({
        'id': [1, 2],
        'name': ['Alice', 'bob']
    })
    
    normalizer = Normalizer()
    result = normalizer.normalize_string_case(df, ['name'])
    
    assert result['name'].tolist() == ['ALICE', 'BOB']

def test_normalize_whitespace():
    """Test whitespace normalization"""
    df = pd.DataFrame({
        'id': [1, 2],
        'text': ['  hello  world  ', 'multiple    spaces']
    })
    
    normalizer = Normalizer()
    result = normalizer.normalize_whitespace(df, ['text'])
    
    assert result['text'].tolist() == ['hello world', 'multiple spaces']

def test_normalize_dates():
    """Test date normalization"""
    df = pd.DataFrame({
        'id': [1, 2],
        'date': ['2025-01-01', '01/02/2025']
    })
    
    normalizer = Normalizer()
    result = normalizer.normalize_dates(df, ['date'])
    
    # Check that dates are normalized to ISO format
    assert result['date'].tolist() == ['2025-01-01', '2025-01-02']

def test_normalize_numeric():
    """Test numeric value normalization"""
    df = pd.DataFrame({
        'id': [1, 2],
        'value': ['1,234.56', '$2,000']
    })
    
    normalizer = Normalizer()
    result = normalizer.normalize_numeric(df, ['value'])
    
    assert result['value'].dtype == float
    assert result['value'].tolist() == [1234.56, 2000.0]

def test_normalize_null_values():
    """Test null value normalization"""
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'value': ['N/A', '', np.nan]
    })
    
    normalizer = Normalizer()
    result = normalizer.normalize_null_values(df, ['value'])
    
    assert pd.isna(result['value']).all()

def test_full_normalization():
    """Test complete normalization pipeline"""
    df = pd.DataFrame({
        'id': [1, 2],
        'name': ['  Alice  ', 'BOB'],
        'value': ['1,234.56', 'N/A'],
        'date': ['2025-01-01', '01/02/2025']
    })
    
    normalizer = Normalizer()
    result = normalizer.normalize(df)
    
    assert result['name'].tolist() == ['ALICE', 'BOB']
    assert result['value'].dtype == float
    assert pd.isna(result['value'].iloc[1])
    assert result['date'].tolist() == ['2025-01-01', '2025-01-02']

def test_normalize_empty_dataframe():
    """Test normalization of empty DataFrame"""
    df = pd.DataFrame(columns=['id', 'name', 'value'])
    
    normalizer = Normalizer()
    result = normalizer.normalize(df)
    
    assert len(result) == 0
    assert list(result.columns) == ['id', 'name', 'value']

def test_normalize_invalid_column():
    """Test error handling for invalid column names"""
    df = pd.DataFrame({'id': [1, 2]})
    
    normalizer = Normalizer()
    with pytest.raises(KeyError):
        normalizer.normalize_string_case(df, ['invalid_column'])