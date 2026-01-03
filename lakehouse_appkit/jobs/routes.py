"""
FastAPI routes for Jobs.
"""
from fastapi import APIRouter, Depends
from typing import List, Dict, Any

router = APIRouter()

@router.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "Jobs"}
