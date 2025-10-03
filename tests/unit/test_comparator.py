import pytest
from comparator.src.comparator import Comparator
import pandas as pd
import numpy as np

def test_comparator_initialization():
    """Test that Comparator initializes correctly with various parameters"""
    # Basic initialization
    comparator = Comparator(pk=['id'])
    assert comparator.pk == ['id']
    assert comparator.ignore_cols == []

    # Initialization with ignore columns
    comparator = Comparator(pk=['id'], ignore_cols=['updated_at'])
    assert comparator.pk == ['id']
    assert comparator.ignore_cols == ['updated_at']

def test_basic_comparison(sample_data_source, sample_data_target, sample_comparison_result):
    """Test basic comparison functionality with sample data"""
    comparator = Comparator(pk=['id'], ignore_cols=['updated_at'])
    result = comparator.compare(sample_data_source, sample_data_target)
    
    assert set(result['matching']) == set(sample_comparison_result['matching'])
    assert set(result['mismatched']) == set(sample_comparison_result['mismatched'])
    assert set(result['only_in_source']) == set(sample_comparison_result['only_in_source'])
    assert set(result['only_in_target']) == set(sample_comparison_result['only_in_target'])

def test_comparison_with_nulls():
    """Test comparison handling of null values"""
    source_df = pd.DataFrame({
        'id': [1, 2, 3],
        'value': [10, None, 30]
    })
    target_df = pd.DataFrame({
        'id': [1, 2, 3],
        'value': [10, np.nan, 35]
    })

    comparator = Comparator(pk=['id'])
    result = comparator.compare(source_df, target_df)

    assert set(result['matching']) == {1}
    assert set(result['mismatched']) == {2, 3}

def test_comparison_with_different_column_orders():
    """Test that comparison works regardless of column order"""
    source_df = pd.DataFrame({
        'id': [1, 2],
        'name': ['Alice', 'Bob'],
        'age': [25, 30]
    })
    target_df = pd.DataFrame({
        'age': [25, 30],
        'id': [1, 2],
        'name': ['Alice', 'Bob']
    })

    comparator = Comparator(pk=['id'])
    result = comparator.compare(source_df, target_df)
    
    assert set(result['matching']) == {1, 2}
    assert len(result['mismatched']) == 0

def test_invalid_primary_key():
    """Test error handling for invalid primary key"""
    source_df = pd.DataFrame({'id': [1, 2], 'value': [10, 20]})
    target_df = pd.DataFrame({'id': [1, 2], 'value': [10, 20]})

    with pytest.raises(KeyError):
        comparator = Comparator(pk=['invalid_key'])
        comparator.compare(source_df, target_df)

def test_duplicate_primary_keys():
    """Test handling of duplicate primary keys"""
    source_df = pd.DataFrame({
        'id': [1, 1, 2],
        'value': [10, 11, 20]
    })
    target_df = pd.DataFrame({
        'id': [1, 2, 2],
        'value': [10, 20, 21]
    })

    comparator = Comparator(pk=['id'])
    result = comparator.compare(source_df, target_df)

    # Both records with id=1 and id=2 should be marked as duplicates
    assert set(result.get('duplicate_keys', [])) == {1, 2}