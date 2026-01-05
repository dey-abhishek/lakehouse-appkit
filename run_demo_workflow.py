"""
Run the test demo workflow.
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from lakehouse_appkit.workflows import WorkflowEngine
from lakehouse_appkit.adapters.databricks import DatabricksAdapter
from dotenv import load_dotenv
import os

# Load environment
load_dotenv("config/.env.dev")


async def main():
    print("\n" + "=" * 70)
    print("🚀 Running YAML Workflow Demo")
    print("=" * 70)
    
    # Get credentials
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID") or os.getenv("DATABRICKS_SQL_WAREHOUSE_ID")
    
    print(f"\n📝 Configuration:")
    print(f"   Host: {host}")
    print(f"   Warehouse: {warehouse_id}")
    
    if not all([host, token, warehouse_id]):
        print("\n❌ Missing required configuration!")
        return
    
    # Load workflow
    workflow_path = "examples/workflows/test_demo.yaml"
    print(f"\n📄 Loading workflow: {workflow_path}")
    
    try:
        workflow = WorkflowEngine.load_workflow(workflow_path)
        print(f"   ✅ Loaded: {workflow.name}")
        print(f"   📋 Steps: {len(workflow.steps)}")
        
        # Create adapter
        adapter = DatabricksAdapter(
            host=host,
            token=token,
            warehouse_id=warehouse_id
        )
        
        # Create engine
        engine = WorkflowEngine(adapter)
        
        # Execute workflow
        print(f"\n▶️  Executing workflow...\n")
        print("=" * 70)
        
        context = await engine.execute_workflow(
            workflow=workflow,
            user="demo_user@company.com",
            parameters={}
        )
        
        # Show results
        print("\n" + "=" * 70)
        print("📊 Execution Results")
        print("=" * 70)
        print(f"Execution ID: {context.execution_id}")
        print(f"Status: {context.status}")
        
        if context.error:
            print(f"\n❌ Error: {context.error}")
        
        if context.step_results:
            print(f"\n📋 Step Results:")
            for step_name, result in context.step_results.items():
                print(f"\n   ✅ {step_name}:")
                if isinstance(result, list) and result:
                    for row in result[:3]:  # Show first 3 rows
                        print(f"      {row}")
        
        print(f"\n" + "=" * 70)
        if context.status == "completed":
            print(f"🎉 Workflow completed successfully!")
        else:
            print(f"❌ Workflow failed: {context.status}")
        print("=" * 70 + "\n")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())

