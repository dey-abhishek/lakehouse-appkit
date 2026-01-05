"""
Test script to run a YAML workflow.
"""
import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from lakehouse_appkit.workflows import WorkflowEngine
from lakehouse_appkit.adapters.databricks import DatabricksAdapter
from dotenv import load_dotenv
import os

# Load environment
load_dotenv("config/.env.dev")


async def main():
    print("=" * 70)
    print("🚀 Lakehouse-AppKit YAML Workflow Test")
    print("=" * 70)
    
    # Get credentials
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID") or os.getenv("DATABRICKS_SQL_WAREHOUSE_ID")
    
    print(f"\n📝 Configuration:")
    print(f"   Host: {host}")
    print(f"   Warehouse: {warehouse_id}")
    print(f"   Token: {'✅ Set' if token else '❌ Missing'}")
    
    if not all([host, token, warehouse_id]):
        print("\n❌ Missing required configuration!")
        return
    
    # Create adapter
    print(f"\n🔌 Creating Databricks adapter...")
    adapter = DatabricksAdapter(
        host=host,
        token=token,
        warehouse_id=warehouse_id
    )
    
    # Load workflow
    workflow_path = "examples/workflows/simple_etl.yaml"
    print(f"\n📄 Loading workflow: {workflow_path}")
    
    try:
        workflow = WorkflowEngine.load_workflow(workflow_path)
        print(f"   ✅ Workflow loaded: {workflow.name}")
        print(f"   📋 Steps: {len(workflow.steps)}")
        print(f"   📂 Catalog: {workflow.catalog}")
        print(f"   🗂️  Schema: {workflow.schema_}")
        
        # Show steps
        print(f"\n📊 Workflow Steps:")
        for i, step in enumerate(workflow.steps, 1):
            deps = f" (depends on: {', '.join(step.depends_on)})" if step.depends_on else ""
            print(f"   {i}. {step.name} ({step.type}){deps}")
        
        # Ask for confirmation
        print(f"\n⚠️  This workflow will execute SQL queries on your Databricks workspace!")
        print(f"   Workspace: {host}")
        print(f"   Catalog: {workflow.catalog}")
        print(f"   Schema: {workflow.schema_}")
        
        response = input("\n   Continue with execution? [y/N]: ")
        
        if response.lower() != 'y':
            print("\n❌ Execution cancelled by user")
            return
        
        # Create engine
        print(f"\n🎯 Creating workflow engine...")
        engine = WorkflowEngine(adapter)
        
        # Execute workflow
        print(f"\n▶️  Executing workflow...")
        print("=" * 70)
        
        context = await engine.execute_workflow(
            workflow=workflow,
            user="test_user@company.com",
            parameters={}
        )
        
        # Show results
        print("\n" + "=" * 70)
        print("📊 Execution Results")
        print("=" * 70)
        print(f"Execution ID: {context.execution_id}")
        print(f"Workflow: {context.workflow_name}")
        print(f"User: {context.user}")
        print(f"Status: {context.status}")
        
        if context.error:
            print(f"\n❌ Error: {context.error}")
        
        if context.step_results:
            print(f"\n📋 Step Results:")
            for step_name, result in context.step_results.items():
                print(f"\n   {step_name}:")
                if isinstance(result, list) and result:
                    print(f"      Rows: {len(result)}")
                    if len(result) <= 3:
                        for row in result:
                            print(f"      {row}")
                elif isinstance(result, dict):
                    for key, value in result.items():
                        print(f"      {key}: {value}")
                else:
                    print(f"      {result}")
        
        # Summary
        print(f"\n" + "=" * 70)
        if context.status == "completed":
            print(f"✅ Workflow completed successfully!")
        else:
            print(f"❌ Workflow failed with status: {context.status}")
        print("=" * 70)
        
    except FileNotFoundError:
        print(f"❌ Workflow file not found: {workflow_path}")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())

