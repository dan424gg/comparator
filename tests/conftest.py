import pytest
import pandas as pd
from typing import Dict, Any

@pytest.fixture
def sample_data_source() -> pd.DataFrame:
    """
    Fixture providing a sample source DataFrame for testing
    """
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 40, 45],
        'updated_at': ['2025-01-01', '2025-01-02', '2025-01-03', '2025-01-04', '2025-01-05']
    })

@pytest.fixture
def sample_data_target() -> pd.DataFrame:
    """
    Fixture providing a sample target DataFrame for testing with some differences
    """
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 6],  # Changed last ID from 5 to 6
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Frank'],  # Changed name
        'age': [25, 31, 35, 40, 50],  # Changed one age
        'updated_at': ['2025-01-01', '2025-01-02', '2025-01-03', '2025-01-04', '2025-01-06']
    })

@pytest.fixture
def sample_comparison_result() -> Dict[str, Any]:
    """
    Fixture providing an expected comparison result structure
    """
    return {
        'matching': [1, 3, 4],  # IDs of matching records
        'mismatched': [2],      # IDs of records with differences
        'only_in_source': [5],  # IDs only in source
        'only_in_target': [6],  # IDs only in target
        'all_records': range(1, 7)
    }

@pytest.fixture
def db_connection_string(postgres_container) -> str:
    """
    Fixture providing a connection string to a test database
    Requires testcontainers-postgres to be running
    """
    return f"postgresql://test:test@localhost:{postgres_container.get_exposed_port(5432)}/test"