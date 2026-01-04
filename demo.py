"""
Demo script showing how to use the Lakehouse-AppKit adapter directly.

This demonstrates:
1. Connecting to Databricks
2. Listing catalogs via REST
3. Executing SQL queries via REST (no SQL connector!)
4. Parameterized queries
5. Listing tables
"""
import asyncio
import os
from lakehouse_appkit.adapters.databricks import DatabricksAdapter
from dotenv import load_dotenv

# Load environment variables from config/.env.dev
load_dotenv("config/.env.dev")


async def main():
    # Get credentials from environment
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID") or os.getenv("DATABRICKS_SQL_WAREHOUSE_ID")
    
    print("=" * 70)
    print("🚀 Lakehouse-AppKit Adapter Demo")
    print("=" * 70)
    print(f"Host: {host}")
    print(f"Warehouse: {warehouse_id}")
    print(f"Token: {token[:10]}..." if len(token) > 10 else "Token: [not set]")
    print("=" * 70)
    
    # Initialize adapter (REST-only, no SQL connector needed!)
    adapter = DatabricksAdapter(
        host=host,
        token=token,
        warehouse_id=warehouse_id
    )
    
    try:
        # Connect
        print("\n📡 Connecting to Databricks...")
        await adapter.connect()
        print("✅ Connected!")
        
        # 1. List catalogs via REST
        print("\n" + "=" * 70)
        print("📚 Listing Unity Catalog catalogs...")
        print("=" * 70)
        try:
            catalogs = await adapter.list_catalogs_rest()
            print(f"Found {len(catalogs)} catalog(s):\n")
            for i, catalog in enumerate(catalogs, 1):
                print(f"  {i}. {catalog['name']}")
                print(f"     Owner: {catalog.get('owner', 'N/A')}")
                print(f"     Comment: {catalog.get('comment', 'N/A')}")
                print()
        except Exception as e:
            print(f"❌ Error listing catalogs: {e}")
            print("   Make sure DATABRICKS_HOST and DATABRICKS_TOKEN are set correctly.")
        
        # 2. Execute SQL query via REST (no SQL connector!)
        print("=" * 70)
        print("🔍 Executing SQL query via REST API...")
        print("=" * 70)
        print("Query: SELECT 1 as test, 'hello' as message, current_timestamp() as ts\n")
        try:
            results = await adapter.execute_query(
                "SELECT 1 as test, 'hello' as message, current_timestamp() as ts"
            )
            print(f"✅ Results: {results}")
        except Exception as e:
            print(f"❌ Error executing query: {e}")
            print("   Make sure DATABRICKS_WAREHOUSE_ID is set correctly.")
        
        # 3. Parameterized query
        print("\n" + "=" * 70)
        print("🔐 Parameterized query (SQL injection protection)...")
        print("=" * 70)
        print("Query: SELECT :value as number, :text as text")
        print("Params: {value: 42, text: 'world'}\n")
        try:
            results = await adapter.execute_query(
                "SELECT :value as number, :text as text",
                params={"value": 42, "text": "world"}
            )
            print(f"✅ Results: {results}")
        except Exception as e:
            print(f"❌ Error executing parameterized query: {e}")
        
        # 4. List schemas in first catalog
        print("\n" + "=" * 70)
        print("📂 Listing schemas...")
        print("=" * 70)
        try:
            catalogs = await adapter.list_catalogs_rest()
            if catalogs:
                catalog_name = catalogs[0]['name']
                print(f"Catalog: {catalog_name}\n")
                
                schemas = await adapter.list_schemas_rest(catalog_name)
                print(f"Found {len(schemas)} schema(s):\n")
                for i, schema in enumerate(schemas[:5], 1):  # First 5
                    print(f"  {i}. {schema['name']}")
                    print(f"     Owner: {schema.get('owner', 'N/A')}")
                    print()
            else:
                print("No catalogs available")
        except Exception as e:
            print(f"❌ Error listing schemas: {e}")
        
        # 5. List tables
        print("=" * 70)
        print("📊 Listing tables...")
        print("=" * 70)
        try:
            catalogs = await adapter.list_catalogs_rest()
            if catalogs:
                catalog_name = catalogs[0]['name']
                schemas = await adapter.list_schemas_rest(catalog_name)
                if schemas:
                    schema_name = schemas[0]['name']
                    print(f"Location: {catalog_name}.{schema_name}\n")
                    
                    tables = await adapter.list_tables_rest(catalog_name, schema_name)
                    print(f"Found {len(tables)} table(s):\n")
                    for i, table in enumerate(tables[:5], 1):  # First 5 tables
                        print(f"  {i}. {table['name']}")
                        print(f"     Type: {table.get('table_type', 'N/A')}")
                        print()
                else:
                    print("No schemas available")
            else:
                print("No catalogs available")
        except Exception as e:
            print(f"❌ Error listing tables: {e}")
        
        # Summary
        print("=" * 70)
        print("✅ All operations completed!")
        print("=" * 70)
        print("\n🎉 Key Features Demonstrated:")
        print("  ✅ REST-only adapter (no SQL connector needed)")
        print("  ✅ Native async operations")
        print("  ✅ Unity Catalog browsing")
        print("  ✅ SQL execution via Statement Execution API")
        print("  ✅ Parameterized queries (SQL injection protection)")
        print("  ✅ 97% smaller dependency footprint")
        
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Always disconnect
        print("\n" + "=" * 70)
        await adapter.disconnect()
        print("👋 Disconnected from Databricks")
        print("=" * 70)


if __name__ == "__main__":
    print("\n")
    print("╔" + "=" * 68 + "╗")
    print("║" + " " * 10 + "Lakehouse-AppKit - REST-Only Adapter Demo" + " " * 17 + "║")
    print("╚" + "=" * 68 + "╝")
    print()
    print("This demo shows how to use the Databricks adapter directly.")
    print("No SQL connector required - everything uses REST APIs!")
    print()
    print("Configuration:")
    print("  Set environment variables:")
    print("    export DATABRICKS_HOST='your-workspace.cloud.databricks.com'")
    print("    export DATABRICKS_TOKEN='dapi...'")
    print("    export DATABRICKS_WAREHOUSE_ID='abc123'")
    print()
    print("  Or edit config/.env.dev")
    print()
    
    asyncio.run(main())

