"""
conftest.py - Shared fixtures for database connector tests

Session-scoped: Containers and engines (expensive to create)
Function-scoped: Test data (cheap to recreate, ensures isolation)
"""
import os
import sys
import pytest
from testcontainers.mysql import MySqlContainer
from testcontainers.postgres import PostgresContainer
from typing import Generator
import sqlalchemy

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
SRC_PATH = os.path.join(ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

from connectors.mysql import MySQLConnector
from connectors.postgres import PostgresConnector


# ==================== Session-Scoped Container Fixtures ====================

@pytest.fixture(scope="session")
def mysql_container() -> Generator[MySqlContainer, None, None]:
    """
    Session-scoped MySQL container.
    Starts once per test session and reused across all tests.
    This is expensive to create, so we only do it once.
    """
    mysql = MySqlContainer("mysql:8.0")
    mysql.start()
    yield mysql
    mysql.stop()


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """
    Session-scoped PostgreSQL container.
    Starts once per test session and reused across all tests.
    This is expensive to create, so we only do it once.
    """
    postgres = PostgresContainer("postgres:15-alpine")
    postgres.start()
    yield postgres
    postgres.stop()


# ==================== Session-Scoped Engine Fixtures ====================

@pytest.fixture(scope="session")
def mysql_engine(mysql_container):
    """
    Session-scoped MySQL engine.
    Created once and reused for all MySQL operations.
    """
    engine = sqlalchemy.create_engine(
        f"mysql+pymysql://{mysql_container.username}:{mysql_container.password}"
        f"@{mysql_container.get_container_host_ip()}:{mysql_container.get_exposed_port(3306)}"
        f"/{mysql_container.dbname}"
    )
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def postgres_engine(postgres_container):
    """
    Session-scoped PostgreSQL engine.
    Created once and reused for all Postgres operations.
    """
    engine = sqlalchemy.create_engine(
        f"postgresql+psycopg2://{postgres_container.username}:{postgres_container.password}"
        f"@{postgres_container.get_container_host_ip()}:{postgres_container.get_exposed_port(5432)}"
        f"/{postgres_container.dbname}"
    )
    yield engine
    engine.dispose()


# ==================== Function-Scoped Setup/Cleanup Fixtures ====================

@pytest.fixture(scope="function")
def clean_mysql_db(mysql_engine):
    """
    Function-scoped fixture that ensures clean MySQL database state.
    
    Setup: Creates tables and inserts test data
    Teardown: Drops all tables for next test
    
    This runs before/after EACH test function for complete isolation.
    """
    with mysql_engine.connect() as conn:
        # Setup: Create tables
        conn.execute(sqlalchemy.text("""
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                age INT
            )
        """))
        
        conn.execute(sqlalchemy.text("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                product VARCHAR(100),
                amount DECIMAL(10, 2)
            )
        """))
        
        conn.execute(sqlalchemy.text("""
            CREATE TABLE IF NOT EXISTS products (
                product_id INT AUTO_INCREMENT PRIMARY KEY,
                product_name VARCHAR(100),
                price DECIMAL(10, 2)
            )
        """))
        
        # Setup: Insert test data
        conn.execute(sqlalchemy.text("""
            INSERT INTO users (name, email, age) VALUES
            ('Alice', 'alice@example.com', 30),
            ('Bob', 'bob@example.com', 25),
            ('Charlie', 'charlie@example.com', 35)
        """))
        
        conn.execute(sqlalchemy.text("""
            INSERT INTO orders (user_id, product, amount) VALUES
            (1, 'Laptop', 999.99),
            (2, 'Mouse', 29.99),
            (1, 'Keyboard', 79.99)
        """))
        
        conn.execute(sqlalchemy.text("""
            INSERT INTO products (product_name, price) VALUES
            ('Laptop', 999.99),
            ('Mouse', 29.99),
            ('Keyboard', 79.99)
        """))
        
        conn.commit()
    
    # Test runs here
    yield
    
    # Teardown: Clean up tables
    with mysql_engine.connect() as conn:
        conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS orders"))
        conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS users"))
        conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS products"))
        conn.commit()


@pytest.fixture(scope="function")
def clean_postgres_db(postgres_engine):
    """
    Function-scoped fixture that ensures clean Postgres database state.
    
    Setup: Creates tables and inserts test data
    Teardown: Drops all tables for next test
    
    This runs before/after EACH test function for complete isolation.
    """
    with postgres_engine.connect() as conn:
        # Setup: Create tables
        conn.execute(sqlalchemy.text("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                age INT
            )
        """))
        
        conn.execute(sqlalchemy.text("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id SERIAL PRIMARY KEY,
                user_id INT,
                product VARCHAR(100),
                amount DECIMAL(10, 2)
            )
        """))
        
        conn.execute(sqlalchemy.text("""
            CREATE TABLE IF NOT EXISTS products (
                product_id SERIAL PRIMARY KEY,
                product_name VARCHAR(100),
                price DECIMAL(10, 2)
            )
        """))
        
        # Setup: Insert test data
        conn.execute(sqlalchemy.text("""
            INSERT INTO users (name, email, age) VALUES
            ('Alice', 'alice@example.com', 30),
            ('Bob', 'bob@example.com', 25),
            ('Charlie', 'charlie@example.com', 35)
        """))
        
        conn.execute(sqlalchemy.text("""
            INSERT INTO orders (user_id, product, amount) VALUES
            (1, 'Laptop', 999.99),
            (2, 'Mouse', 29.99),
            (1, 'Keyboard', 79.99)
        """))
        
        conn.execute(sqlalchemy.text("""
            INSERT INTO products (product_name, price) VALUES
            ('Laptop', 999.99),
            ('Mouse', 29.99),
            ('Keyboard', 79.99)
        """))
        
        conn.commit()
    
    # Test runs here
    yield
    
    # Teardown: Clean up tables
    with postgres_engine.connect() as conn:
        conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS orders"))
        conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS users"))
        conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS products"))
        conn.commit()


# ==================== Function-Scoped Connector Fixtures ====================

@pytest.fixture
def mysql_connector(mysql_container, clean_mysql_db):
    """
    Function-scoped MySQLConnector instance.
    Depends on clean_mysql_db to ensure fresh data for each test.
    """
    return MySQLConnector(
        username=mysql_container.username,
        password=mysql_container.password,
        host=mysql_container.get_container_host_ip(),
        database=mysql_container.dbname,
        port=mysql_container.get_exposed_port(3306)
    )


@pytest.fixture
def postgres_connector(postgres_container, clean_postgres_db):
    """
    Function-scoped PostgresConnector instance.
    Depends on clean_postgres_db to ensure fresh data for each test.
    """
    return PostgresConnector(
        username=postgres_container.username,
        password=postgres_container.password,
        host=postgres_container.get_container_host_ip(),
        database=postgres_container.dbname,
        port=postgres_container.get_exposed_port(5432)
    )


@pytest.fixture(params=["mysql", "postgres"])
def connector(request, mysql_connector, postgres_connector):
    """
    Parameterized fixture that yields each connector type.
    Tests using this fixture will run once for MySQL and once for Postgres.
    
    Each test gets a fresh connector with clean database state.
    """
    if request.param == "mysql":
        return mysql_connector
    elif request.param == "postgres":
        return postgres_connector


# ==================== Alternative: Transaction-Based Cleanup ====================

@pytest.fixture(scope="function")
def mysql_transaction(mysql_engine, clean_mysql_db):
    """
    Alternative fixture using transaction rollback for cleanup.
    Faster than dropping tables, but test must not commit.
    
    Usage: Only use this if your tests don't call commit()
    """
    connection = mysql_engine.connect()
    transaction = connection.begin()
    
    yield connection
    
    transaction.rollback()
    connection.close()


@pytest.fixture(scope="function")
def postgres_transaction(postgres_engine, clean_postgres_db):
    """
    Alternative fixture using transaction rollback for cleanup.
    Faster than dropping tables, but test must not commit.
    
    Usage: Only use this if your tests don't call commit()
    """
    connection = postgres_engine.connect()
    transaction = connection.begin()
    
    yield connection
    
    transaction.rollback()
    connection.close()


# ==================== Test Data Constants ====================

@pytest.fixture
def expected_tables():
    """Expected table names in test database."""
    return ["users", "orders", "products"]


@pytest.fixture
def expected_user_columns():
    """Expected column names in users table."""
    return ["id", "name", "email", "age"]


@pytest.fixture
def expected_order_columns():
    """Expected column names in orders table."""
    return ["order_id", "user_id", "product", "amount"]


@pytest.fixture
def expected_product_columns():
    """Expected column names in products table."""
    return ["product_id", "product_name", "price"]


# ==================== Helper Fixtures ====================

@pytest.fixture
def sample_users():
    """Sample user data for testing."""
    return [
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
        {"name": "Charlie", "email": "charlie@example.com", "age": 35},
    ]


@pytest.fixture
def sample_orders():
    """Sample order data for testing."""
    return [
        {"user_id": 1, "product": "Laptop", "amount": 999.99},
        {"user_id": 2, "product": "Mouse", "amount": 29.99},
        {"user_id": 1, "product": "Keyboard", "amount": 79.99},
    ]