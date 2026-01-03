# 🚀 Quick Start Guide - Running Lakehouse-AppKit

## Step 1: Verify Installation

Check if everything is set up:

```bash
cd /Users/<username>/lakehouse-appkit

# Activate virtual environment
source lakehouse-app/bin/activate

# Verify installation
lakehouse-appkit --version
```

---

## Step 2: Configure Your Environment

### Option A: Use Existing Configuration

If you already have `config/.env.dev`:

```bash
# Check your configuration
cat config/.env.dev
```

### Option B: Create New Configuration

```bash
# Initialize config (creates .env file)
lakehouse-appkit init

# Or copy from example
cp config/.env.dev.example config/.env.dev

# Edit with your Databricks credentials
nano config/.env.dev  # or use your preferred editor
```

**Required settings:**
```env
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi...
DATABRICKS_WAREHOUSE_ID=your-warehouse-id
```

---

## Step 3: Test CLI (Optional but Recommended)

Quick test to ensure everything works:

```bash
# Test CLI help
lakehouse-appkit --help

# Test Unity Catalog
lakehouse-appkit uc list-catalogs

# Test Secrets
lakehouse-appkit secrets list-scopes
```

---

## Step 4: Run the Application

### Option A: Run as FastAPI Server

Create a simple app file:

```bash
cat > app.py << 'EOF'
from fastapi import FastAPI
from lakehouse_appkit.unity_catalog.routes import router as uc_router
from lakehouse_appkit.secrets.routes import router as secrets_router
from lakehouse_appkit.jobs.routes import router as jobs_router

app = FastAPI(
    title="Lakehouse-AppKit",
    description="Production-ready data application on Databricks",
    version="1.0.0"
)

# Register routes
app.include_router(uc_router, prefix="/api/uc", tags=["Unity Catalog"])
app.include_router(secrets_router, prefix="/api/secrets", tags=["Secrets"])
app.include_router(jobs_router, prefix="/api/jobs", tags=["Jobs"])

@app.get("/")
def root():
    return {"message": "Welcome to Lakehouse-AppKit", "status": "running"}

@app.get("/health")
def health():
    return {"status": "healthy"}
EOF

# Run the server
uvicorn app:app --reload --host 0.0.0.0 --port 8000
```

### Option B: Create a New App with CLI

```bash
# Create new app
lakehouse-appkit create my-data-app --template dashboard

# Navigate to app
cd my-data-app

# Run the app
lakehouse-appkit run --reload
```

---

## Step 5: Access Your Application

Once running, access your app:

- **Main App:** http://localhost:8000
- **API Docs:** http://localhost:8000/docs (Interactive Swagger UI)
- **ReDoc:** http://localhost:8000/redoc (Alternative API docs)
- **Health Check:** http://localhost:8000/health

---

## Step 6: Test the API

### Using curl:

```bash
# Health check
curl http://localhost:8000/health

# List catalogs
curl http://localhost:8000/api/uc/catalogs

# List secret scopes
curl http://localhost:8000/api/secrets/scopes
```

### Using Browser:

1. Go to http://localhost:8000/docs
2. Click "Try it out" on any endpoint
3. Execute the request
4. See the response

---

## Common Commands Reference

### CLI Commands

```bash
# Unity Catalog
lakehouse-appkit uc list-catalogs
lakehouse-appkit uc list-schemas --catalog main
lakehouse-appkit uc list-tables --catalog main --schema default

# Secrets
lakehouse-appkit secrets list-scopes
lakehouse-appkit secrets list-secrets <scope-name>

# Jobs
lakehouse-appkit jobs list
lakehouse-appkit jobs get <job-id>

# Model Serving
lakehouse-appkit model list-endpoints

# Vector Search
lakehouse-appkit vector list-endpoints

# Delta Lake
lakehouse-appkit delta history main.default.my_table

# AI Scaffolding
lakehouse-appkit ai generate-endpoint user-api
lakehouse-appkit ai providers
```

---

## Troubleshooting

### Issue: Command not found

```bash
# Make sure virtual environment is activated
source lakehouse-app/bin/activate

# Verify installation
which lakehouse-appkit
```

### Issue: Import errors

```bash
# Reinstall dependencies
pip install -e .
```

### Issue: Connection errors

```bash
# Check your config
cat config/.env.dev

# Verify Databricks host (include https://)
# Verify token is valid
# Test connection
curl -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  https://your-workspace.cloud.databricks.com/api/2.0/preview/scim/v2/Me
```

### Issue: Port already in use

```bash
# Use a different port
uvicorn app:app --port 8001

# Or kill the process using port 8000
lsof -ti:8000 | xargs kill -9
```

---

## Production Deployment

### Deploy to Databricks:

```bash
# Create Databricks App
lakehouse-appkit deploy create-app my-production-app \
  --description "Production data application"

# Package and deploy using Databricks CLI
databricks apps deploy \
  --source-dir . \
  --app-name my-production-app
```

---

## Environment Variables Quick Reference

```env
# === Required ===
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi...
DATABRICKS_WAREHOUSE_ID=abc123

# === Optional - AI Features ===
AI_ENABLED=true
ANTHROPIC_API_KEY=sk-ant-...

# === Optional - Environment ===
APP_ENV=dev  # dev, test, or prod
```

---

## Quick Test Script

Create and run this test script:

```bash
cat > test_app.sh << 'EOF'
#!/bin/bash
echo "🚀 Testing Lakehouse-AppKit..."

# Activate environment
source lakehouse-app/bin/activate

# Test CLI
echo "✅ Testing CLI..."
lakehouse-appkit --version

# Test Unity Catalog
echo "✅ Testing Unity Catalog..."
lakehouse-appkit uc list-catalogs

# Test Secrets
echo "✅ Testing Secrets..."
lakehouse-appkit secrets list-scopes

echo "🎉 All tests passed!"
EOF

chmod +x test_app.sh
./test_app.sh
```

---

## Next Steps

1. **Explore API Docs:** Visit http://localhost:8000/docs
2. **Try CLI Commands:** Run `lakehouse-appkit --help`
3. **Build Features:** Add your own endpoints
4. **Deploy:** Push to Databricks Apps

---

## Getting Help

- **Documentation:** See README.md for complete guide
- **Examples:** Check `/tests` for code examples
- **Issues:** https://github.com/deyabhishek/lakehouse-appkit/issues
- **Contact:** abhishek.dey@databricks.com

---

## Summary

**Quickest way to run:**

```bash
cd /Users/abhishek.dey/lakehouse-appkit
source lakehouse-app/bin/activate
lakehouse-appkit uc list-catalogs  # Test CLI
# OR
uvicorn app:app --reload  # Run API server
```

**That's it!** Your Lakehouse-AppKit is now running! 🎉

