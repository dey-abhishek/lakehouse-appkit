"""
Validate workflow without executing.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from lakehouse_appkit.workflows import WorkflowEngine
import yaml

workflow_path = "examples/workflows/simple_etl.yaml"

print("=" * 70)
print("🔍 Validating YAML Workflow")
print("=" * 70)

try:
    # Validate
    print(f"\n📄 File: {workflow_path}")
    is_valid, error = WorkflowEngine.validate_workflow(workflow_path)
    
    if is_valid:
        print("✅ Workflow is valid!")
        
        # Load and show details
        workflow = WorkflowEngine.load_workflow(workflow_path)
        
        print(f"\n📋 Workflow Details:")
        print(f"   Name: {workflow.name}")
        print(f"   Version: {workflow.version}")
        print(f"   Description: {workflow.description}")
        print(f"   Catalog: {workflow.catalog}")
        print(f"   Schema: {workflow.schema_}")
        print(f"   Owner: {workflow.owner}")
        
        print(f"\n🔐 Governance:")
        print(f"   Approval Required: {workflow.governance.require_approval}")
        print(f"   Audit Table: {workflow.governance.audit_table}")
        
        print(f"\n📊 Steps ({len(workflow.steps)}):")
        for i, step in enumerate(workflow.steps, 1):
            print(f"\n   Step {i}: {step.name}")
            print(f"      Type: {step.type}")
            if step.description:
                print(f"      Description: {step.description}")
            if step.depends_on:
                print(f"      Depends on: {', '.join(step.depends_on)}")
            if step.query:
                query_preview = step.query.strip().replace('\n', ' ')[:80]
                print(f"      Query: {query_preview}...")
        
        if workflow.tags:
            print(f"\n🏷️  Tags: {', '.join(workflow.tags)}")
        
        print(f"\n" + "=" * 70)
        print("✅ Workflow is ready to execute!")
        print("=" * 70)
        
    else:
        print(f"❌ Workflow validation failed!")
        print(f"\nError: {error}")
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()

