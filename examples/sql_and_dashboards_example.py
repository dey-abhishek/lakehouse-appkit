"""
Databricks SQL & AI/BI Dashboards REST API Example

Shows how to use the fast REST API clients for SQL queries and dashboard management.
"""
import asyncio
import os
from lakehouse_appkit.sql import DatabricksSQLClient
from lakehouse_appkit.dashboards import DatabricksAIBIDashboardClient


async def sql_example():
    """Demonstrate SQL REST API usage."""
    print("=" * 80)
    print("Databricks SQL REST API - Execute Queries FAST! ⚡")
    print("=" * 80)
    print()
    
    host = os.getenv("DATABRICKS_HOST", "https://xxx.cloud.databricks.com")
    token = os.getenv("DATABRICKS_TOKEN", "your-token")
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "your-warehouse-id")
    
    if token == "your-token":
        print("⚠️  Set DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID")
        return
    
    sql_client = DatabricksSQLClient(host, token, warehouse_id)
    
    # Example 1: Simple query
    print("-" * 80)
    print("Example 1: Execute Simple Query")
    print("-" * 80)
    
    import time
    start = time.time()
    results = await sql_client.execute_and_fetch(
        "SELECT 1 as id, 'test' as name, current_timestamp() as ts"
    )
    elapsed = time.time() - start
    
    print(f"⚡ Query executed in {elapsed:.2f}s\n")
    for row in results:
        print(f"   {row}")
    
    # Example 2: Query with catalog/schema
    print()
    print("-" * 80)
    print("Example 2: Query Public Dataset")
    print("-" * 80)
    
    start = time.time()
    results = await sql_client.execute_and_fetch(
        "SELECT * FROM samples.nyctaxi.trips LIMIT 5",
        catalog="samples",
        schema="nyctaxi"
    )
    elapsed = time.time() - start
    
    print(f"⚡ Query executed in {elapsed:.2f}s")
    print(f"   Found {len(results)} rows\n")
    
    if results:
        print("   First row:")
        for key, value in list(results[0].items())[:5]:
            print(f"      {key}: {value}")
    
    # Example 3: List warehouses
    print()
    print("-" * 80)
    print("Example 3: List SQL Warehouses")
    print("-" * 80)
    
    start = time.time()
    warehouses = await sql_client.list_warehouses()
    elapsed = time.time() - start
    
    print(f"⚡ Listed warehouses in {elapsed:.2f}s")
    print(f"   Found {len(warehouses)} warehouses\n")
    
    for wh in warehouses[:3]:
        print(f"   📦 {wh.get('name')}")
        print(f"      ID: {wh.get('id')}")
        print(f"      State: {wh.get('state')}")
        print(f"      Cluster size: {wh.get('cluster_size', 'N/A')}")
        print()


async def dashboard_example():
    """Demonstrate AI/BI Dashboard REST API usage."""
    print("=" * 80)
    print("AI/BI Dashboards REST API - Manage Dashboards! 📊")
    print("=" * 80)
    print()
    
    host = os.getenv("DATABRICKS_HOST", "https://xxx.cloud.databricks.com")
    token = os.getenv("DATABRICKS_TOKEN", "your-token")
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "your-warehouse-id")
    
    if token == "your-token":
        print("⚠️  Set DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID")
        return
    
    dashboard_client = DatabricksAIBIDashboardClient(host, token)
    
    # Example 1: List existing dashboards
    print("-" * 80)
    print("Example 1: List Existing Dashboards")
    print("-" * 80)
    
    import time
    start = time.time()
    dashboards_response = await dashboard_client.list_dashboards()
    elapsed = time.time() - start
    
    dashboards = dashboards_response.get("dashboards", [])
    print(f"⚡ Listed dashboards in {elapsed:.2f}s")
    print(f"   Found {len(dashboards)} dashboards\n")
    
    for dash in dashboards[:5]:
        print(f"   📊 {dash.get('display_name')}")
        print(f"      ID: {dash.get('dashboard_id')}")
        print(f"      Warehouse: {dash.get('warehouse_id')}")
        print(f"      State: {dash.get('lifecycle_state')}")
        print()
    
    # Example 2: Create new dashboard
    print("-" * 80)
    print("Example 2: Create New Dashboard")
    print("-" * 80)
    
    try:
        start = time.time()
        dashboard = await dashboard_client.create_dashboard(
            display_name="Lakehouse-AppKit Test Dashboard",
            warehouse_id=warehouse_id
        )
        elapsed = time.time() - start
        
        dashboard_id = dashboard.get('dashboard_id')
        print(f"⚡ Created dashboard in {elapsed:.2f}s")
        print(f"   ✅ Dashboard ID: {dashboard_id}")
        print(f"   ✅ Name: {dashboard.get('display_name')}")
        print()
        
        # Example 3: Get dashboard details
        print("-" * 80)
        print("Example 3: Get Dashboard Details")
        print("-" * 80)
        
        start = time.time()
        details = await dashboard_client.get_dashboard(dashboard_id)
        elapsed = time.time() - start
        
        print(f"⚡ Retrieved details in {elapsed:.2f}s")
        print(f"   Name: {details.get('display_name')}")
        print(f"   Warehouse: {details.get('warehouse_id')}")
        print(f"   Path: {details.get('path', 'N/A')}")
        print(f"   State: {details.get('lifecycle_state')}")
        print()
        
        # Example 4: Publish dashboard
        print("-" * 80)
        print("Example 4: Publish Dashboard")
        print("-" * 80)
        
        start = time.time()
        published = await dashboard_client.publish_dashboard(dashboard_id)
        elapsed = time.time() - start
        
        print(f"⚡ Published in {elapsed:.2f}s")
        print(f"   ✅ Published version: {published.get('version')}")
        print(f"   ✅ Dashboard ID: {published.get('dashboard_id')}")
        print()
        
        # Example 5: Get published dashboard
        print("-" * 80)
        print("Example 5: Get Published Dashboard")
        print("-" * 80)
        
        start = time.time()
        pub_dash = await dashboard_client.get_published_dashboard(dashboard_id)
        elapsed = time.time() - start
        
        print(f"⚡ Retrieved published dashboard in {elapsed:.2f}s")
        print(f"   Version: {pub_dash.get('version')}")
        print(f"   Dashboard ID: {pub_dash.get('dashboard_id')}")
        print()
        
        # Cleanup: Unpublish and trash
        print("-" * 80)
        print("Cleanup: Unpublish and Trash Dashboard")
        print("-" * 80)
        
        await dashboard_client.unpublish_dashboard(dashboard_id)
        print("   ✅ Unpublished")
        
        await dashboard_client.trash_dashboard(dashboard_id)
        print("   ✅ Moved to trash")
        print()
        
    except Exception as e:
        print(f"   ⚠️  Error: {e}")
        print()


async def main():
    """Run all examples."""
    print("\n")
    print("╔" + "═" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print("║" + "  Databricks SQL & AI/BI Dashboards REST API Examples".center(78) + "║")
    print("║" + " " * 78 + "║")
    print("╚" + "═" * 78 + "╝")
    print()
    
    # SQL Examples
    await sql_example()
    
    print("\n")
    
    # Dashboard Examples
    await dashboard_example()
    
    print("=" * 80)
    print("🎉 All Examples Complete!")
    print("=" * 80)
    print()
    print("Benefits of REST API:")
    print("  ✅ Fast (no connection overhead)")
    print("  ✅ Simple (just HTTP calls)")
    print("  ✅ Async (perfect for FastAPI)")
    print("  ✅ No heavy dependencies")
    print()
    print("📖 Learn more: docs/SQL_AND_DASHBOARDS_REST_API.md")
    print()


if __name__ == "__main__":
    asyncio.run(main())

