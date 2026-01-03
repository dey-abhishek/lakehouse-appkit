# Unity Catalog Examples

This directory contains example applications demonstrating Unity Catalog integration with Lakehouse-AppKit.

## Examples

### 1. Unity Catalog App (`unity_catalog_app.py`)

A complete FastAPI application with Unity Catalog integration, featuring:

- **Catalog Browser UI**: Interactive tree view
- **Data Discovery**: Search and filter tables
- **PII Detection**: Find tables with sensitive data
- **Stale Table Detection**: Identify unused tables
- **Impact Analysis**: Understand dependencies
- **Data Quality Checks**: Basic quality validation

#### Running the Example

```bash
# Set up environment
export DATABRICKS_WORKSPACE="https://your-workspace.databricks.com"
export DATABRICKS_TOKEN="your-token"

# Run the app
python examples/unity_catalog_app.py
```

#### Endpoints

##### Core Unity Catalog API
- `GET /` - Unity Catalog browser UI
- `GET /api/unity-catalog/catalogs` - List catalogs
- `GET /api/unity-catalog/catalogs/{catalog}/schemas` - List schemas
- `GET /api/unity-catalog/catalogs/{catalog}/schemas/{schema}/tables` - List tables
- `GET /api/unity-catalog/catalogs/{catalog}/schemas/{schema}/tables/{table}` - Table details
- `GET /api/unity-catalog/catalogs/{catalog}/schemas/{schema}/tables/{table}/sample` - Sample data
- `GET /api/unity-catalog/catalogs/{catalog}/schemas/{schema}/tables/{table}/lineage` - Lineage
- `POST /api/unity-catalog/search` - Search tables

##### Custom Endpoints
- `GET /api/discover/pii` - Discover tables with PII
- `GET /api/discover/stale?days=30` - Find stale tables
- `GET /api/analyze/impact/{catalog}/{schema}/{table}` - Impact analysis
- `GET /api/quality/check/{catalog}/{schema}/{table}` - Data quality checks

#### Use Cases

**1. PII Discovery**
```bash
curl http://localhost:8000/api/discover/pii
```

Returns:
```json
{
  "pii_tables": [
    {
      "table": "main.default.customers",
      "pii_columns": ["email", "phone", "address"],
      "owner": "data_team"
    }
  ],
  "count": 1
}
```

**2. Stale Table Detection**
```bash
curl http://localhost:8000/api/discover/stale?days=90
```

Returns:
```json
{
  "stale_tables": [
    {
      "table": "main.legacy.old_reports",
      "last_updated": "2023-01-15T10:30:00",
      "days_stale": 365,
      "owner": "analytics"
    }
  ]
}
```

**3. Impact Analysis**
```bash
curl http://localhost:8000/api/analyze/impact/main/default/raw_events
```

Returns:
```json
{
  "table": "main.default.raw_events",
  "direct_dependencies": ["main.default.processed_events"],
  "total_dependencies": ["main.default.processed_events", "main.default.metrics"],
  "dependency_count": 2,
  "impact_level": "medium"
}
```

**4. Data Quality Check**
```bash
curl http://localhost:8000/api/quality/check/main/default/customers
```

Returns:
```json
{
  "table": "main.default.customers",
  "score": 75.0,
  "grade": "B",
  "checks": [
    {
      "name": "has_data",
      "passed": true,
      "details": "Row count: 1000"
    },
    {
      "name": "columns_documented",
      "passed": false,
      "details": "3/5 columns have comments"
    }
  ]
}
```

### 2. CLI Examples

#### List Catalogs
```bash
lakehouse-appkit uc list-catalogs \
  --workspace "https://your-workspace.databricks.com" \
  --token "your-token"
```

#### List Tables
```bash
lakehouse-appkit uc list-tables \
  --workspace "https://your-workspace.databricks.com" \
  --token "your-token" \
  --catalog main \
  --schema default
```

#### Describe Table
```bash
lakehouse-appkit uc describe \
  --workspace "https://your-workspace.databricks.com" \
  --token "your-token" \
  --catalog main \
  --schema default \
  --table customers
```

#### Sample Data
```bash
lakehouse-appkit uc sample \
  --workspace "https://your-workspace.databricks.com" \
  --token "your-token" \
  --catalog main \
  --schema default \
  --table customers \
  --limit 10
```

#### Search Tables
```bash
lakehouse-appkit uc search "customer" \
  --workspace "https://your-workspace.databricks.com" \
  --token "your-token" \
  --catalog main \
  --limit 20
```

#### View Lineage
```bash
lakehouse-appkit uc lineage \
  --workspace "https://your-workspace.databricks.com" \
  --token "your-token" \
  --catalog main \
  --schema default \
  --table customer_metrics
```

#### Catalog Tree
```bash
lakehouse-appkit uc tree \
  --workspace "https://your-workspace.databricks.com" \
  --token "your-token"
```

### 3. Programmatic Usage

#### Basic Usage
```python
from lakehouse_appkit.adapters.databricks import DatabricksAdapter
from lakehouse_appkit.unity_catalog import UnityCatalogManager

# Initialize
adapter = DatabricksAdapter(
    workspace_url="https://your-workspace.databricks.com",
    token="your-token"
)
uc = UnityCatalogManager(adapter)

# List catalogs
catalogs = await uc.list_catalogs()
for catalog in catalogs:
    print(f"Catalog: {catalog.name}")

# Get table details
table = await uc.get_table_details("main", "default", "customers")
print(f"Table: {table.full_name}")
print(f"Columns: {len(table.columns)}")
```

#### Data Discovery
```python
# Search for tables
results = await uc.search_tables("customer", catalogs=["main"], limit=10)

for table in results:
    print(f"Found: {table.full_name}")
    
    # Get details
    details = await uc.get_table_details(
        table.catalog_name,
        table.schema_name,
        table.name
    )
    
    # Get sample
    sample = await uc.get_table_sample(
        table.catalog_name,
        table.schema_name,
        table.name,
        limit=5
    )
    
    print(f"  Rows: {len(sample)}")
```

#### Lineage Analysis
```python
# Get lineage
lineage = await uc.get_table_lineage("main", "default", "customer_metrics")

print(f"Analyzing: {lineage.object_name}")
print("\nUpstream (sources):")
for table in lineage.upstream:
    print(f"  ← {table}")

print("\nDownstream (consumers):")
for table in lineage.downstream:
    print(f"  → {table}")
```

### 4. Integration with FastAPI

```python
from fastapi import FastAPI, Depends
from lakehouse_appkit.unity_catalog import UnityCatalogManager
from lakehouse_appkit.unity_catalog.routes import router as uc_router

app = FastAPI()

# Include Unity Catalog routes
app.include_router(uc_router, prefix="/api/uc", tags=["Unity Catalog"])

# Custom endpoint
@app.get("/tables")
async def get_tables(
    catalog: str = "main",
    schema: str = "default",
    uc: UnityCatalogManager = Depends()
):
    """Get tables in a schema."""
    tables = await uc.list_tables(catalog, schema)
    return {"tables": [t.dict() for t in tables]}
```

## Best Practices

1. **Authentication**: Use environment variables for credentials
2. **Error Handling**: Wrap calls in try/except for permission errors
3. **Performance**: Cache catalog metadata for frequently accessed data
4. **Rate Limiting**: Implement rate limiting for production apps
5. **Pagination**: Use limits when searching large catalogs

## Security Considerations

- All operations are read-only (safe)
- Respects user permissions
- No write operations to Unity Catalog
- Audit trail maintained by Databricks

## Next Steps

- Explore the [Unity Catalog Documentation](../docs/UNITY_CATALOG.md)
- Check out the [Architecture Guide](../docs/ARCHITECTURE.md)
- Try the [Quick Start Guide](../QUICKSTART.md)

---

**Unity Catalog + Lakehouse-AppKit** = 🚀 Powerful Data Applications
