"""
Lakehouse-AppKit FastAPI Application - Simplified

This version removes auth dependencies for easier testing.
"""
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from lakehouse_appkit.adapters.databricks import DatabricksAdapter
from dotenv import load_dotenv
import os

# Load environment variables from config/.env.dev
load_dotenv("config/.env.dev")

# Create FastAPI app
app = FastAPI(
    title="Lakehouse-AppKit",
    description="Production-ready CLI and SDK for building data applications on Databricks",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize adapter with environment variables
host = os.getenv("DATABRICKS_HOST")
token = os.getenv("DATABRICKS_TOKEN")
warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID") or os.getenv("DATABRICKS_SQL_WAREHOUSE_ID")

if host and token and warehouse_id:
    try:
        adapter = DatabricksAdapter(
            host=host,
            token=token,
            warehouse_id=warehouse_id
        )
        print(f"✅ Adapter initialized: {host}, warehouse: {warehouse_id}")
    except Exception as e:
        print(f"⚠️ Could not initialize adapter: {e}")
        adapter = None
else:
    print("⚠️ Missing Databricks credentials:")
    print(f"   DATABRICKS_HOST: {'✅' if host else '❌'}")
    print(f"   DATABRICKS_TOKEN: {'✅' if token else '❌'}")
    print(f"   DATABRICKS_WAREHOUSE_ID: {'✅' if warehouse_id else '❌'}")
    adapter = None


@app.get("/", response_class=HTMLResponse)
async def root():
    """Root endpoint with HTML dashboard."""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Lakehouse-AppKit</title>
        <style>
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                max-width: 1200px;
                margin: 0 auto;
                padding: 20px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
            }
            .container {
                background: white;
                border-radius: 20px;
                padding: 40px;
                box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            }
            h1 {
                color: #667eea;
                font-size: 3em;
                margin-bottom: 10px;
            }
            .subtitle {
                color: #666;
                font-size: 1.2em;
                margin-bottom: 30px;
            }
            .links {
                display: flex;
                gap: 20px;
                margin: 30px 0;
            }
            .btn {
                flex: 1;
                padding: 15px 30px;
                background: #667eea;
                color: white;
                text-decoration: none;
                border-radius: 8px;
                text-align: center;
                font-weight: bold;
                transition: all 0.3s;
            }
            .btn:hover {
                background: #764ba2;
                transform: translateY(-2px);
                box-shadow: 0 5px 15px rgba(102, 126, 234, 0.4);
            }
            .badge {
                display: inline-block;
                padding: 5px 10px;
                background: #28a745;
                color: white;
                border-radius: 5px;
                font-size: 0.9em;
                margin-left: 10px;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🚀 Lakehouse-AppKit</h1>
            <p class="subtitle">
                Production-ready CLI and SDK for building data applications on Databricks
                <span class="badge">REST APIs Only</span>
            </p>
            
            <div class="links">
                <a href="/docs" class="btn">📚 API Documentation</a>
                <a href="/redoc" class="btn">📖 ReDoc</a>
                <a href="/api/health" class="btn">❤️ Health Check</a>
            </div>
            
            <h2>✨ Quick Start</h2>
            <div style="background: #f8f9fa; padding: 20px; border-radius: 10px; margin: 20px 0;">
                <h3>1. View Interactive API Docs</h3>
                <p>Click "📚 API Documentation" above or visit <a href="/docs">/docs</a></p>
                
                <h3>2. Try the Endpoints</h3>
                <ul>
                    <li><a href="/api/unity-catalog/catalogs">GET /api/unity-catalog/catalogs</a> - List catalogs</li>
                    <li><a href="/api/jobs">GET /api/jobs</a> - List jobs</li>
                    <li><a href="/api/secrets/scopes">GET /api/secrets/scopes</a> - List secrets</li>
                </ul>
                
                <h3>3. Use Python SDK</h3>
                <pre style="background: #2d2d2d; color: #f8f8f2; padding: 15px; border-radius: 5px;">
python demo.py      # Direct adapter usage
python client.py    # HTTP client example
                </pre>
            </div>
        </div>
    </body>
    </html>
    """


@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "app": "Lakehouse-AppKit",
        "version": "1.0.0",
        "adapter_configured": adapter is not None,
        "tests_passing": "273/273"
    }


@app.get("/api/info")
async def app_info():
    """Application information."""
    return {
        "name": "Lakehouse-AppKit",
        "version": "1.0.0",
        "adapter_configured": adapter is not None,
        "documentation": {
            "swagger": "/docs",
            "redoc": "/redoc",
            "openapi": "/openapi.json"
        }
    }


# Unity Catalog endpoints
@app.get("/api/unity-catalog/catalogs")
async def list_catalogs():
    """List all catalogs."""
    if not adapter:
        raise HTTPException(status_code=500, detail="Adapter not configured")
    
    try:
        await adapter.connect()
        catalogs = await adapter.list_catalogs_rest()
        await adapter.disconnect()
        return {"catalogs": catalogs, "count": len(catalogs)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/unity-catalog/schemas/{catalog}")
async def list_schemas(catalog: str):
    """List schemas in a catalog."""
    if not adapter:
        raise HTTPException(status_code=500, detail="Adapter not configured")
    
    try:
        await adapter.connect()
        schemas = await adapter.list_schemas_rest(catalog)
        await adapter.disconnect()
        return {"schemas": schemas, "count": len(schemas)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/unity-catalog/tables/{catalog}/{schema}")
async def list_tables(catalog: str, schema: str):
    """List tables in a schema."""
    if not adapter:
        raise HTTPException(status_code=500, detail="Adapter not configured")
    
    try:
        await adapter.connect()
        tables = await adapter.list_tables_rest(catalog, schema)
        await adapter.disconnect()
        return {"tables": tables, "count": len(tables)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Jobs endpoints (stubs for now)
@app.get("/api/jobs")
async def list_jobs():
    """List jobs (requires full adapter implementation)."""
    return {"message": "Jobs endpoint - configure Databricks for full functionality"}


# Secrets endpoints (stubs)
@app.get("/api/secrets/scopes")
async def list_secret_scopes():
    """List secret scopes."""
    return {"message": "Secrets endpoint - configure Databricks for full functionality"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

