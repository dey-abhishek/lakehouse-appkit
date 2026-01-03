# Databricks Secrets Configuration Guide

This guide explains how to securely manage credentials using Databricks secrets.

## 🔐 For Production: Use Databricks Secrets

Databricks secrets provide secure credential management without exposing sensitive data in code or config files.

### Step 1: Create a Secret Scope

Create a secret scope to store your credentials:

```bash
# Using Databricks CLI
databricks secrets create-scope lakehouse-prod-secrets
```

Or via the UI:
1. Go to `https://<your-workspace>#secrets/createScope`
2. Enter scope name: `lakehouse-prod-secrets`
3. Select "Creator" for Manage Principal (recommended)
4. Click "Create"

### Step 2: Add Secrets to the Scope

Add your credentials as secrets:

```bash
# Databricks token (if accessing another workspace)
databricks secrets put-secret lakehouse-prod-secrets DATABRICKS_TOKEN

# SQL Warehouse ID
databricks secrets put-secret lakehouse-prod-secrets DATABRICKS_SQL_WAREHOUSE_ID

# AI Provider keys (optional)
databricks secrets put-secret lakehouse-prod-secrets OPENAI_API_KEY
databricks secrets put-secret lakehouse-prod-secrets ANTHROPIC_API_KEY
```

You'll be prompted to enter each secret value. The values are encrypted and never displayed.

### Step 3: Use in Your Application

```python
from lakehouse_appkit.config import load_config

# Load config with secrets
config = load_config(secret_scope="lakehouse-prod-secrets")

# Use in adapter
from lakehouse_appkit.adapters.databricks import DatabricksAdapter

adapter = DatabricksAdapter(
    host=config.databricks.host,
    token=config.databricks.token,  # Automatically loaded from secrets
    warehouse_id=config.databricks.sql_warehouse_id
)
```

### Step 4: Grant Permissions (Optional)

Allow other users or service principals to access secrets:

```bash
# Grant READ permission to a user
databricks secrets put-acl lakehouse-prod-secrets user@company.com READ

# Grant READ permission to a group
databricks secrets put-acl lakehouse-prod-secrets data-engineers READ
```

## 🛠️ For Development: Use .env Files

For local development, use `.env` files in the `config/` directory:

### File: `config/.env.dev`

```bash
# Databricks Configuration
DATABRICKS_WORKSPACE_URL=https://your-workspace.databricks.com
DATABRICKS_TOKEN=dapi123456...
DATABRICKS_SQL_WAREHOUSE_ID=abc123...

# Unity Catalog
TEST_CATALOG=main
TEST_SCHEMA=default

# Environment
ENVIRONMENT=development
DEBUG=true
```

### Usage in Development

```python
from lakehouse_appkit.config import load_config

# Automatically loads from config/.env.dev
config = load_config()

# Your credentials are loaded from the .env file
print(config.databricks.workspace_url)
```

## 🔄 Hybrid Approach: Environment-Based

The config manager automatically detects the environment:

```python
from lakehouse_appkit.config import load_config
import os

# Determine environment
if os.getenv("DATABRICKS_RUNTIME_VERSION"):
    # Running in Databricks - use secrets
    config = load_config(secret_scope="lakehouse-prod-secrets")
else:
    # Running locally - use .env file
    config = load_config()
```

## 📋 Secret Naming Convention

Use consistent naming for your secrets:

| Secret Key | Description | Example Value |
|------------|-------------|---------------|
| `DATABRICKS_TOKEN` | Access token | `dapi123...` |
| `DATABRICKS_SQL_WAREHOUSE_ID` | Warehouse ID | `abc123def456` |
| `DATABRICKS_WORKSPACE_URL` | Workspace URL | `https://company.databricks.com` |
| `OPENAI_API_KEY` | OpenAI key | `sk-...` |
| `ANTHROPIC_API_KEY` | Claude key | `sk-ant-...` |
| `GOOGLE_API_KEY` | Gemini key | `AIza...` |

## 🔒 Security Best Practices

1. **Never commit `.env` files** - They're in `.gitignore`
2. **Use separate scopes for environments**:
   - `lakehouse-dev-secrets`
   - `lakehouse-staging-secrets`
   - `lakehouse-prod-secrets`
3. **Limit secret scope permissions** - Grant only READ access
4. **Rotate secrets regularly** - Update tokens periodically
5. **Use service principals** - For production workloads

## 📚 Managing Multiple Environments

### Development
```python
config = load_config()  # Uses config/.env.dev
```

### Staging
```python
config = load_config(secret_scope="lakehouse-staging-secrets")
```

### Production
```python
config = load_config(secret_scope="lakehouse-prod-secrets")
```

## 🧪 Testing with Secrets

Tests automatically use the appropriate config source:

```python
# tests/test_config.py
from lakehouse_appkit.config import load_config

def test_with_secrets():
    # Loads from config/.env.dev in local testing
    # Loads from secrets when running in Databricks
    config = load_config()
    
    assert config.databricks.token is not None
```

## 🔍 Verifying Secrets

Check which secrets are available:

```bash
# List all secret scopes
databricks secrets list-scopes

# List secrets in a scope
databricks secrets list-secrets lakehouse-prod-secrets

# Get metadata (not the value) for a secret
databricks secrets get-secret lakehouse-prod-secrets DATABRICKS_TOKEN
```

## 🎯 Migration Path

### Phase 1: Development (Current)
- Use `config/.env.dev` for local development
- Commit `config/.env.example` as template

### Phase 2: Staging
- Create `lakehouse-staging-secrets` scope
- Add secrets via CLI
- Update deployment scripts

### Phase 3: Production
- Create `lakehouse-prod-secrets` scope
- Add production secrets
- Deploy with secret scope parameter

## 📖 References

- [Databricks Secret Management](https://docs.databricks.com/aws/en/security/secrets/)
- [Secret Scopes](https://docs.databricks.com/aws/en/security/secrets/)
- [Secret ACLs](https://docs.databricks.com/aws/en/security/secrets/)

---

**Security Note**: Databricks automatically redacts secret values in notebook outputs and logs when accessed via `dbutils.secrets.get()`. Your credentials are protected! 🛡️

