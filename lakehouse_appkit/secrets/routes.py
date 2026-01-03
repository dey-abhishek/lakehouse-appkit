"""FastAPI routes for Secrets."""
from fastapi import APIRouter, Depends
from typing import List, Dict, Any
from pydantic import BaseModel

router = APIRouter()

class ScopeCreate(BaseModel):
    """Model for creating a scope."""
    scope: str
    initial_manage_principal: str = "users"

class SecretPut(BaseModel):
    """Model for putting a secret."""
    scope: str
    key: str
    string_value: str

@router.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "Secrets"}

__all__ = ['router', 'ScopeCreate', 'SecretPut']
