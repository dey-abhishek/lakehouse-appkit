# Configuration

This directory contains configuration files for Lakehouse-AppKit.

## 📁 Files

- **`.env.example`** - Template for environment variables (commit this)
- **`.env.dev`** - Your development credentials (git-ignored, DO NOT commit)
- **`README.md`** - Complete guide to Databricks secrets and configuration

## 🚀 Quick Start

### For Development

1. Copy the example file:
   ```bash
   cp config/.env.example config/.env.dev
   ```

2. Edit `config/.env.dev` and fill in your credentials:
   ```bash
   nano config/.env.dev
   ```

3. Use in your code:
   ```python
   from lakehouse_appkit.config import load_config
   
   config = load_config()
   print(config.databricks.workspace_url)
   ```

### For Production

Use Databricks secrets for secure credential management:

```bash
# Create secret scope
databricks secrets create-scope lakehouse-prod-secrets

# Add secrets
databricks secrets put-secret lakehouse-prod-secrets DATABRICKS_TOKEN
databricks secrets put-secret lakehouse-prod-secrets DATABRICKS_SQL_WAREHOUSE_ID
```

Then in your code:
```python
from lakehouse_appkit.config import load_config

config = load_config(secret_scope="lakehouse-prod-secrets")
```

## 📚 Documentation

See **`config/README.md`** for:
- Complete Databricks secrets guide
- Security best practices
- Environment-based configuration
- Migration strategies
- Testing with secrets

## 🔒 Security

✅ **DO commit**: `.env.example`  
❌ **NEVER commit**: `.env.dev`, `.env.test`, `.env.prod`

All credential files are automatically git-ignored.

## 🔗 References

- [Databricks Secret Management](https://docs.databricks.com/aws/en/security/secrets/)
- [Databricks Authentication](https://docs.databricks.com/en/dev-tools/auth.html)

