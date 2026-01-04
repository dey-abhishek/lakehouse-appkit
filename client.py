"""
Client script showing how to use the running FastAPI application.

This demonstrates:
1. Making HTTP requests to the API
2. Health checks
3. Getting app info
4. Calling Unity Catalog endpoints
"""
import requests
import json
import sys

BASE_URL = "http://localhost:8000"


def print_section(title):
    """Print a formatted section header."""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def print_response(response):
    """Pretty print a response."""
    if response.status_code == 200:
        print(f"✅ Status: {response.status_code}")
        try:
            data = response.json()
            print(json.dumps(data, indent=2))
        except:
            print(response.text)
    else:
        print(f"❌ Status: {response.status_code}")
        print(f"Response: {response.text}")


def main():
    print("\n")
    print("╔" + "=" * 68 + "╗")
    print("║" + " " * 15 + "Lakehouse-AppKit API Client Demo" + " " * 21 + "║")
    print("╚" + "=" * 68 + "╝")
    print()
    
    # Check if app is running
    print_section("🔌 Checking if app is running...")
    try:
        response = requests.get(f"{BASE_URL}/api/health", timeout=2)
        if response.status_code == 200:
            print("✅ App is running!")
        else:
            print(f"⚠️  App responded with status {response.status_code}")
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to app!")
        print(f"   Make sure the app is running on {BASE_URL}")
        print("   Run: python app.py")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
    
    # 1. Health Check
    print_section("🏥 Health Check")
    response = requests.get(f"{BASE_URL}/api/health")
    print_response(response)
    
    # 2. App Info
    print_section("ℹ️  App Info")
    response = requests.get(f"{BASE_URL}/api/info")
    print_response(response)
    
    # 3. List Catalogs (requires Databricks config)
    print_section("📚 Unity Catalog - List Catalogs")
    print("This requires DATABRICKS_HOST and DATABRICKS_TOKEN to be configured.\n")
    try:
        response = requests.get(f"{BASE_URL}/api/uc/catalogs")
        if response.status_code == 200:
            data = response.json()
            catalogs = data.get('catalogs', [])
            print(f"✅ Found {len(catalogs)} catalog(s):\n")
            for i, catalog in enumerate(catalogs[:3], 1):
                print(f"  {i}. {catalog['name']}")
                print(f"     Owner: {catalog.get('owner', 'N/A')}")
                print()
        else:
            print(f"❌ Status: {response.status_code}")
            print(f"Response: {response.text}")
            print("\n💡 To enable this, configure Databricks credentials:")
            print("   export DATABRICKS_HOST='your-workspace.cloud.databricks.com'")
            print("   export DATABRICKS_TOKEN='dapi...'")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # 4. List Jobs
    print_section("⚙️  Jobs - List Jobs")
    print("This requires Databricks configuration.\n")
    try:
        response = requests.get(f"{BASE_URL}/api/jobs")
        if response.status_code == 200:
            data = response.json()
            jobs = data.get('jobs', [])
            print(f"✅ Found {len(jobs)} job(s):\n")
            for i, job in enumerate(jobs[:3], 1):
                print(f"  {i}. {job.get('settings', {}).get('name', 'N/A')}")
                print(f"     ID: {job.get('job_id', 'N/A')}")
                print()
        else:
            print(f"⚠️  Status: {response.status_code}")
            print("   Requires Databricks configuration")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # 5. List Secrets Scopes
    print_section("🔐 Secrets - List Scopes")
    print("This requires Databricks configuration.\n")
    try:
        response = requests.get(f"{BASE_URL}/api/secrets/scopes")
        if response.status_code == 200:
            data = response.json()
            scopes = data.get('scopes', [])
            print(f"✅ Found {len(scopes)} scope(s):\n")
            for i, scope in enumerate(scopes[:3], 1):
                print(f"  {i}. {scope['name']}")
                print(f"     Backend: {scope.get('backend_type', 'N/A')}")
                print()
        else:
            print(f"⚠️  Status: {response.status_code}")
            print("   Requires Databricks configuration")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Summary
    print_section("✅ Demo Complete")
    print()
    print("🎯 What you can do next:")
    print()
    print("1. 📖 Interactive API Docs:")
    print(f"   Visit: {BASE_URL}/docs")
    print("   Try endpoints with 'Try it out' button")
    print()
    print("2. 🔧 Configure Databricks:")
    print("   export DATABRICKS_HOST='your-workspace.cloud.databricks.com'")
    print("   export DATABRICKS_TOKEN='dapi...'")
    print("   export DATABRICKS_WAREHOUSE_ID='abc123'")
    print()
    print("3. 🐍 Use Python SDK:")
    print("   Run: python demo.py")
    print("   See: HOW_TO_USE.md")
    print()
    print("4. 🖥️  Use CLI:")
    print("   Run: lakehouse-appkit --help")
    print()
    print("=" * 70)


if __name__ == "__main__":
    main()

