"""
Tests for Query Builder.
"""
import pytest
from lakehouse_appkit.sdk.query import QueryBuilder


def test_simple_select():
    """Test simple SELECT query."""
    query = QueryBuilder().select("*").from_table("users").build()
    assert query == "SELECT * FROM users"


def test_select_with_columns():
    """Test SELECT with specific columns."""
    query = QueryBuilder().select("id", "name", "email").from_table("users").build()
    assert query == "SELECT id, name, email FROM users"


def test_select_with_where():
    """Test SELECT with WHERE clause."""
    query = (
        QueryBuilder()
        .select("*")
        .from_table("users")
        .where("age > 18")
        .where("status = 'active'")
        .build()
    )
    assert "WHERE age > 18 AND status = 'active'" in query


def test_select_with_join():
    """Test SELECT with JOIN."""
    query = (
        QueryBuilder()
        .select("u.id", "u.name", "o.total")
        .from_table("users u")
        .join("orders o", "u.id = o.user_id", "INNER")
        .build()
    )
    assert "INNER JOIN orders o ON u.id = o.user_id" in query


def test_select_with_group_by():
    """Test SELECT with GROUP BY."""
    query = (
        QueryBuilder()
        .select("category", "COUNT(*) as count")
        .from_table("products")
        .group_by("category")
        .build()
    )
    assert "GROUP BY category" in query


def test_select_with_order_by():
    """Test SELECT with ORDER BY."""
    query = (
        QueryBuilder()
        .select("*")
        .from_table("users")
        .order_by("created_at", "DESC")
        .build()
    )
    assert "ORDER BY created_at DESC" in query


def test_select_with_limit():
    """Test SELECT with LIMIT."""
    query = QueryBuilder().select("*").from_table("users").limit(10).build()
    assert "LIMIT 10" in query


def test_select_with_offset():
    """Test SELECT with OFFSET."""
    query = QueryBuilder().select("*").from_table("users").limit(10).offset(20).build()
    assert "LIMIT 10" in query
    assert "OFFSET 20" in query


def test_complex_query():
    """Test complex query with multiple clauses."""
    query = (
        QueryBuilder()
        .select("u.id", "u.name", "COUNT(o.id) as order_count")
        .from_table("users u")
        .join("orders o", "u.id = o.user_id", "LEFT")
        .where("u.status = 'active'")
        .group_by("u.id", "u.name")
        .having("COUNT(o.id) > 5")
        .order_by("order_count", "DESC")
        .limit(100)
        .build()
    )
    
    assert "SELECT u.id, u.name, COUNT(o.id) as order_count" in query
    assert "FROM users u" in query
    assert "LEFT JOIN orders o ON u.id = o.user_id" in query
    assert "WHERE u.status = 'active'" in query
    assert "GROUP BY u.id, u.name" in query
    assert "HAVING COUNT(o.id) > 5" in query
    assert "ORDER BY order_count DESC" in query
    assert "LIMIT 100" in query


def test_query_params():
    """Test query parameters."""
    qb = (
        QueryBuilder()
        .select("*")
        .from_table("users")
        .where("age > :min_age")
        .param("min_age", 18)
    )
    
    query = qb.build()
    params = qb.get_params()
    
    assert "age > :min_age" in query
    assert params["min_age"] == 18


def test_reset_builder():
    """Test resetting query builder."""
    qb = QueryBuilder().select("*").from_table("users")
    first_query = qb.build()
    
    qb.reset()
    
    with pytest.raises(ValueError):
        qb.build()  # Should fail as FROM is required


def test_missing_from_clause():
    """Test error when FROM clause is missing."""
    qb = QueryBuilder().select("*")
    
    with pytest.raises(ValueError, match="FROM clause is required"):
        qb.build()

