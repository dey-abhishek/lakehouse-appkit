"""
Example FastAPI application with Unity Catalog integration.

This example demonstrates how to build a complete data catalog application
using Lakehouse-AppKit's Unity Catalog features.
"""
from fastapi import FastAPI, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from lakehouse_appkit.adapters.databricks import DatabricksAdapter
from lakehouse_appkit.unity_catalog import UnityCatalogManager
from lakehouse_appkit.unity_catalog.routes import router as uc_router
from lakehouse_appkit.sdk.auth import get_auth_manager, AuthContext

# Initialize FastAPI app
app = FastAPI(
    title="Unity Catalog Explorer",
    description="Explore your lakehouse data with Unity Catalog",
    version="1.0.0"
)

# Initialize auth manager
auth_manager = get_auth_manager()

# Initialize Databricks adapter
adapter = DatabricksAdapter(
    workspace_url="https://your-workspace.databricks.com",
    token="your-databricks-token"  # In production, use env vars
)

# Include Unity Catalog routes
app.include_router(
    uc_router,
    prefix="/api/unity-catalog",
    tags=["Unity Catalog"]
)


# Dependency injection for Unity Catalog Manager
def get_uc_manager() -> UnityCatalogManager:
    """Get Unity Catalog Manager instance."""
    return UnityCatalogManager(adapter)


@app.get("/", response_class=HTMLResponse)
async def homepage(request: Request):
    """
    Render Unity Catalog browser homepage.
    """
    templates = Jinja2Templates(directory="lakehouse_appkit/templates")
    return templates.TemplateResponse(
        "unity_catalog.html",
        {
            "request": request,
            "app_name": "Unity Catalog Explorer"
        }
    )


@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "unity-catalog-explorer"}


# Example: Custom endpoint for table discovery
@app.get("/api/discover/pii")
async def discover_pii_tables(
    auth_context: AuthContext = Depends(auth_manager.get_current_context),
    uc: UnityCatalogManager = Depends(get_uc_manager)
):
    """
    Discover tables that might contain PII data.
    
    Searches for columns with names like 'email', 'phone', 'ssn', etc.
    """
    pii_keywords = ['email', 'ssn', 'phone', 'address', 'name', 'dob']
    pii_tables = []
    
    # Get all catalogs
    catalogs = await uc.list_catalogs()
    
    for catalog in catalogs[:2]:  # Limit to first 2 catalogs for demo
        schemas = await uc.list_schemas(catalog.name)
        
        for schema in schemas[:5]:  # Limit to first 5 schemas
            tables = await uc.list_tables(catalog.name, schema.name)
            
            for table in tables[:10]:  # Limit to first 10 tables
                try:
                    details = await uc.get_table_details(
                        catalog.name, schema.name, table.name
                    )
                    
                    # Check for PII columns
                    pii_cols = [
                        col.name for col in details.columns
                        if any(kw in col.name.lower() for kw in pii_keywords)
                    ]
                    
                    if pii_cols:
                        pii_tables.append({
                            'table': details.full_name,
                            'catalog': catalog.name,
                            'schema': schema.name,
                            'table_name': table.name,
                            'pii_columns': pii_cols,
                            'column_count': len(details.columns),
                            'owner': details.owner
                        })
                except Exception:
                    continue
    
    return {
        'pii_tables': pii_tables,
        'count': len(pii_tables),
        'scanned_catalogs': len(catalogs[:2])
    }


# Example: Custom endpoint for stale table detection
@app.get("/api/discover/stale")
async def discover_stale_tables(
    days: int = 30,
    auth_context: AuthContext = Depends(auth_manager.get_current_context),
    uc: UnityCatalogManager = Depends(get_uc_manager)
):
    """
    Discover tables that might be stale (not updated recently).
    
    Args:
        days: Number of days to consider a table stale
    """
    from datetime import datetime, timedelta
    
    stale_tables = []
    cutoff_date = datetime.now() - timedelta(days=days)
    
    # Get all catalogs
    catalogs = await uc.list_catalogs()
    
    for catalog in catalogs[:2]:  # Limit for demo
        schemas = await uc.list_schemas(catalog.name)
        
        for schema in schemas[:5]:
            tables = await uc.list_tables(catalog.name, schema.name)
            
            for table in tables:
                try:
                    details = await uc.get_table_details(
                        catalog.name, schema.name, table.name
                    )
                    
                    # Check if updated_at is available and old
                    if details.updated_at and details.updated_at < cutoff_date:
                        stale_tables.append({
                            'table': details.full_name,
                            'last_updated': details.updated_at.isoformat(),
                            'days_stale': (datetime.now() - details.updated_at).days,
                            'owner': details.owner,
                            'table_type': details.table_type.value
                        })
                except Exception:
                    continue
    
    return {
        'stale_tables': sorted(
            stale_tables,
            key=lambda x: x['days_stale'],
            reverse=True
        ),
        'count': len(stale_tables),
        'cutoff_days': days
    }


# Example: Custom endpoint for impact analysis
@app.get("/api/analyze/impact/{catalog}/{schema}/{table}")
async def analyze_table_impact(
    catalog: str,
    schema: str,
    table: str,
    auth_context: AuthContext = Depends(auth_manager.get_current_context),
    uc: UnityCatalogManager = Depends(get_uc_manager)
):
    """
    Analyze the impact of changing a table.
    
    Returns all directly and indirectly dependent tables.
    """
    # Get lineage
    lineage = await uc.get_table_lineage(catalog, schema, table)
    
    # Recursively find all downstream dependencies
    all_downstream = set(lineage.downstream)
    to_check = list(lineage.downstream)
    depth = 1
    dependency_tree = {table: lineage.downstream.copy()}
    
    while to_check and depth < 5:  # Limit depth to prevent infinite loops
        next_level = []
        for table_name in to_check:
            parts = table_name.split('.')
            if len(parts) == 3:
                try:
                    downstream_lineage = await uc.get_table_lineage(*parts)
                    dependency_tree[table_name] = downstream_lineage.downstream
                    
                    for t in downstream_lineage.downstream:
                        if t not in all_downstream:
                            all_downstream.add(t)
                            next_level.append(t)
                except Exception:
                    continue
        
        to_check = next_level
        depth += 1
    
    return {
        'table': f"{catalog}.{schema}.{table}",
        'direct_dependencies': lineage.downstream,
        'total_dependencies': list(all_downstream),
        'dependency_count': len(all_downstream),
        'max_depth_checked': depth,
        'dependency_tree': dependency_tree,
        'impact_level': (
            'critical' if len(all_downstream) > 10 else
            'high' if len(all_downstream) > 5 else
            'medium' if len(all_downstream) > 0 else
            'low'
        )
    }


# Example: Custom endpoint for data quality checks
@app.get("/api/quality/check/{catalog}/{schema}/{table}")
async def check_data_quality(
    catalog: str,
    schema: str,
    table: str,
    auth_context: AuthContext = Depends(auth_manager.get_current_context),
    uc: UnityCatalogManager = Depends(get_uc_manager)
):
    """
    Perform basic data quality checks on a table.
    """
    # Get table details
    details = await uc.get_table_details(catalog, schema, table)
    
    # Get stats
    stats = await uc.get_table_stats(catalog, schema, table)
    
    # Get sample
    sample = await uc.get_table_sample(catalog, schema, table, limit=100)
    
    # Perform checks
    checks = {
        'table': details.full_name,
        'checks': []
    }
    
    # Check 1: Table has data
    checks['checks'].append({
        'name': 'has_data',
        'passed': stats['row_count'] > 0 if stats['row_count'] else False,
        'details': f"Row count: {stats['row_count']}"
    })
    
    # Check 2: All columns have comments
    columns_with_comments = sum(1 for col in details.columns if col.comment)
    checks['checks'].append({
        'name': 'columns_documented',
        'passed': columns_with_comments == len(details.columns),
        'details': f"{columns_with_comments}/{len(details.columns)} columns have comments"
    })
    
    # Check 3: Table has owner
    checks['checks'].append({
        'name': 'has_owner',
        'passed': details.owner is not None,
        'details': f"Owner: {details.owner or 'Not set'}"
    })
    
    # Check 4: No NULL values in sample (basic check)
    if sample:
        null_columns = []
        for col_name in sample[0].keys():
            null_count = sum(1 for row in sample if row.get(col_name) is None)
            if null_count > len(sample) * 0.5:  # More than 50% nulls
                null_columns.append(col_name)
        
        checks['checks'].append({
            'name': 'null_values',
            'passed': len(null_columns) == 0,
            'details': f"Columns with >50% nulls: {', '.join(null_columns) or 'None'}"
        })
    
    # Calculate overall score
    passed_checks = sum(1 for check in checks['checks'] if check['passed'])
    checks['score'] = (passed_checks / len(checks['checks'])) * 100
    checks['grade'] = (
        'A' if checks['score'] >= 90 else
        'B' if checks['score'] >= 75 else
        'C' if checks['score'] >= 60 else
        'D' if checks['score'] >= 50 else
        'F'
    )
    
    return checks


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        reload=True
    )

