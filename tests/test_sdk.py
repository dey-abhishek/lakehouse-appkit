"""
Tests for SDK components.
"""
import pytest
from datetime import datetime
from unittest.mock import Mock

from lakehouse_appkit.sdk.models import BaseModel
from lakehouse_appkit.sdk.query import QueryBuilder
from lakehouse_appkit.sdk.utils import sanitize_sql_identifier, format_sql_value
from lakehouse_appkit.sdk.exceptions import (
    QueryError,
    ConnectionError,
    AuthenticationError,
)


# ============================================================================
# Model Tests
# ============================================================================

@pytest.mark.unit
class TestSDKModels:
    """Test SDK Pydantic models."""
    
    def test_base_model_creation(self):
        """Test base model creation."""
        from pydantic import BaseModel, Field
        
        class TestModel(BaseModel):
            name: str
            value: int
        
        model = TestModel(name="test", value=42)
        assert model.name == "test"
        assert model.value == 42
    
    def test_model_validation(self):
        """Test model validation."""
        from pydantic import BaseModel, Field, ValidationError
        
        class TestModel(BaseModel):
            age: int = Field(ge=0, le=150)
        
        # Valid
        model = TestModel(age=25)
        assert model.age == 25
        
        # Invalid
        with pytest.raises(ValidationError):
            TestModel(age=-1)
        
        with pytest.raises(ValidationError):
            TestModel(age=200)
    
    def test_model_serialization(self):
        """Test model serialization."""
        from pydantic import BaseModel
        
        class TestModel(BaseModel):
            name: str
            created_at: datetime
        
        now = datetime.now()
        model = TestModel(name="test", created_at=now)
        
        # To dict
        data = model.dict()
        assert data["name"] == "test"
        assert isinstance(data["created_at"], datetime)
        
        # To JSON
        json_str = model.json()
        assert isinstance(json_str, str)
        assert "test" in json_str


# ============================================================================
# Query Builder Tests
# ============================================================================

@pytest.mark.unit
class TestQueryBuilder:
    """Test SQL query builder."""
    
    def test_query_builder_initialization(self):
        """Test query builder initialization."""
        qb = QueryBuilder()
        assert qb is not None
    
    def test_select_query(self):
        """Test SELECT query building."""
        qb = QueryBuilder()
        query = qb.select("*").from_table("users").build()
        
        assert "SELECT *" in query
        assert "FROM users" in query
    
    def test_select_with_columns(self):
        """Test SELECT with specific columns."""
        qb = QueryBuilder()
        query = qb.select("id", "name", "email").from_table("users").build()
        
        assert "SELECT id, name, email" in query
    
    def test_where_clause(self):
        """Test WHERE clause."""
        qb = QueryBuilder()
        query = qb.select("*").from_table("users").where("age > 18").build()
        
        assert "WHERE age > 18" in query
    
    def test_multiple_where_clauses(self):
        """Test multiple WHERE clauses."""
        qb = QueryBuilder()
        query = (
            qb.select("*")
            .from_table("users")
            .where("age > 18")
            .where("status = 'active'")
            .build()
        )
        
        assert "WHERE" in query
        assert "age > 18" in query
        assert "status = 'active'" in query
    
    def test_order_by(self):
        """Test ORDER BY clause."""
        qb = QueryBuilder()
        query = qb.select("*").from_table("users").order_by("created_at DESC").build()
        
        assert "ORDER BY created_at DESC" in query
    
    def test_limit(self):
        """Test LIMIT clause."""
        qb = QueryBuilder()
        query = qb.select("*").from_table("users").limit(10).build()
        
        assert "LIMIT 10" in query
    
    def test_join(self):
        """Test JOIN clause."""
        qb = QueryBuilder()
        query = (
            qb.select("u.*, o.order_id")
            .from_table("users u")
            .join("orders o", "u.id = o.user_id")
            .build()
        )
        
        assert "JOIN orders o ON u.id = o.user_id" in query
    
    def test_complex_query(self):
        """Test complex query with multiple clauses."""
        qb = QueryBuilder()
        query = (
            qb.select("u.id", "u.name", "COUNT(o.id) as order_count")
            .from_table("users u")
            .join("orders o", "u.id = o.user_id")
            .where("u.status = 'active'")
            .group_by("u.id", "u.name")
            .having("COUNT(o.id) > 5")
            .order_by("order_count DESC")
            .limit(100)
            .build()
        )
        
        assert "SELECT u.id, u.name, COUNT(o.id) as order_count" in query
        assert "JOIN orders o" in query
        assert "WHERE u.status = 'active'" in query
        assert "GROUP BY u.id, u.name" in query
        assert "HAVING COUNT(o.id) > 5" in query
        assert "ORDER BY order_count DESC" in query
        assert "LIMIT 100" in query


# ============================================================================
# Utility Function Tests
# ============================================================================

@pytest.mark.unit
class TestSDKUtils:
    """Test SDK utility functions."""
    
    def test_sanitize_sql_identifier(self):
        """Test SQL identifier sanitization."""
        assert sanitize_sql_identifier("users") == "users"
        assert sanitize_sql_identifier("user_data") == "user_data"
        assert sanitize_sql_identifier("users-data") == "users_data"
        assert sanitize_sql_identifier("123users") == "_123users"
    
    def test_format_sql_value(self):
        """Test SQL value formatting."""
        assert format_sql_value("test") == "'test'"
        assert format_sql_value(42) == "42"
        assert format_sql_value(3.14) == "3.14"
        assert format_sql_value(True) == "TRUE"
        assert format_sql_value(False) == "FALSE"
        assert format_sql_value(None) == "NULL"


# ============================================================================
# Exception Tests
# ============================================================================

@pytest.mark.unit
class TestSDKExceptions:
    """Test SDK exceptions."""
    
    def test_base_exception(self):
        """Test base exception."""
        exc = QueryError("Test error")
        assert "Test error" in str(exc)
    
    def test_query_execution_exception(self):
        """Test query execution exception."""
        exc = QueryError("Query failed")
        assert "Query failed" in str(exc)
    
    def test_validation_exception(self):
        """Test authentication exception."""
        exc = AuthenticationError("Invalid credentials")
        assert "Invalid credentials" in str(exc)
    
    def test_exception_inheritance(self):
        """Test exception inheritance."""
        # All are Exception subclasses
        assert issubclass(QueryError, Exception)
        assert issubclass(ConnectionError, Exception)
        assert issubclass(AuthenticationError, Exception)
