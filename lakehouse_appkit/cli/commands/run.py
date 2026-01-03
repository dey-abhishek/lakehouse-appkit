"""
Application runtime.
"""
import uvicorn


def run_app(host: str, port: int, reload: bool):
    """Run the FastAPI application."""
    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        reload=reload
    )
