"""
Configuration initialization.
"""
from pathlib import Path


def init_config():
    """Initialize Lakehouse-AppKit configuration."""
    config_content = """# Lakehouse-AppKit Configuration

# Databricks
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-token-here
DATABRICKS_WAREHOUSE_ID=your-warehouse-id

# AI Provider (Optional)
AI_ENABLED=false
ANTHROPIC_API_KEY=your-key-here
"""
    
    env_file = Path(".env")
    if env_file.exists():
        print("⚠️  .env file already exists")
        return
    
    env_file.write_text(config_content)
    print("✓ Created .env configuration file")
