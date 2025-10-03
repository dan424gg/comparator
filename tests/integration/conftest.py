import pytest
from testcontainers.postgres import PostgresContainer
import sqlalchemy as sa

@pytest.fixture(scope="session")
def postgres_container():
    """
    Create a PostgreSQL container for testing
    """
    with PostgresContainer(
        image="postgres:14",
        user="test",
        password="test",
        dbname="test"
    ) as container:
        container.start()
        yield container

@pytest.fixture
def db_engine(postgres_container):
    """
    Create a SQLAlchemy engine for the test database
    """
    connection_string = (
        f"postgresql://test:test@localhost:"
        f"{postgres_container.get_exposed_port(5432)}/test"
    )
    engine = sa.create_engine(connection_string)
    
    # Create test tables
    metadata = sa.MetaData()
    
    sa.Table(
        'source_table',
        metadata,
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('name', sa.String),
        sa.Column('value', sa.Float),
        sa.Column('updated_at', sa.DateTime)
    )
    
    sa.Table(
        'target_table',
        metadata,
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('name', sa.String),
        sa.Column('value', sa.Float),
        sa.Column('updated_at', sa.DateTime)
    )
    
    metadata.create_all(engine)
    
    yield engine
    
    # Clean up
    metadata.drop_all(engine)

@pytest.fixture
def db_data(db_engine):
    """
    Insert test data into the database
    """
    with db_engine.begin() as conn:
        # Insert source data
        conn.execute(sa.text("""
            INSERT INTO source_table (id, name, value, updated_at)
            VALUES 
                (1, 'Alice', 10.0, '2025-01-01'),
                (2, 'Bob', 20.0, '2025-01-02'),
                (3, 'Charlie', 30.0, '2025-01-03')
        """))
        
        # Insert target data with some differences
        conn.execute(sa.text("""
            INSERT INTO target_table (id, name, value, updated_at)
            VALUES 
                (1, 'Alice', 10.0, '2025-01-01'),
                (2, 'Bob', 25.0, '2025-01-02'),
                (4, 'David', 40.0, '2025-01-04')
        """))
    
    return db_engine