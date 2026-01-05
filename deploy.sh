#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
APP_NAME="lakehouse-appkit"
SOURCE_DIR="."
APP_DESCRIPTION="Governed data workflows platform for Databricks"

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Deploying Lakehouse-AppKit to Databricks Apps          ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# 1. Validate configuration
echo -e "${YELLOW}[1/7]${NC} Validating configuration..."
if [ ! -f "databricks.yml" ]; then
    echo -e "${RED}❌ databricks.yml not found${NC}"
    exit 1
fi

if [ ! -f "app.py" ]; then
    echo -e "${RED}❌ app.py not found${NC}"
    exit 1
fi

if [ ! -f "requirements.txt" ]; then
    echo -e "${RED}❌ requirements.txt not found${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Configuration files found${NC}"

# 2. Check Databricks CLI
echo -e "\n${YELLOW}[2/7]${NC} Checking Databricks CLI..."
if ! command -v databricks &> /dev/null; then
    echo -e "${RED}❌ Databricks CLI not found${NC}"
    echo -e "${YELLOW}Install with: pip install databricks-cli${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Databricks CLI found${NC}"

# 3. Verify Databricks connection
echo -e "\n${YELLOW}[3/7]${NC} Verifying Databricks connection..."
if ! databricks workspace ls / &> /dev/null; then
    echo -e "${RED}❌ Cannot connect to Databricks${NC}"
    echo -e "${YELLOW}Configure with: databricks configure${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Connected to Databricks${NC}"

# 4. Check if secret scope exists
echo -e "\n${YELLOW}[4/7]${NC} Checking secret scope..."
if databricks secrets list-scopes 2>/dev/null | grep -q "lakehouse-appkit-secrets"; then
    echo -e "${GREEN}✓ Secret scope exists${NC}"
else
    echo -e "${YELLOW}⚠ Secret scope not found. Creating...${NC}"
    databricks secrets create-scope lakehouse-appkit-secrets 2>/dev/null || true
    echo -e "${GREEN}✓ Secret scope created${NC}"
fi

# 5. Check if app already exists
echo -e "\n${YELLOW}[5/7]${NC} Checking existing deployment..."
if databricks apps get "$APP_NAME" &> /dev/null; then
    echo -e "${YELLOW}⚠ App already exists. Updating...${NC}"
    UPDATE_FLAG="--update"
else
    echo -e "${GREEN}✓ New deployment${NC}"
    UPDATE_FLAG=""
fi

# 6. Deploy app
echo -e "\n${YELLOW}[6/7]${NC} Deploying app..."
echo -e "${BLUE}App Name: ${APP_NAME}${NC}"
echo -e "${BLUE}Source: ${SOURCE_DIR}${NC}"
echo ""

if databricks apps deploy \
  --source-dir "$SOURCE_DIR" \
  --app-name "$APP_NAME" \
  --description "$APP_DESCRIPTION" \
  $UPDATE_FLAG; then
    echo -e "${GREEN}✓ App deployed successfully${NC}"
else
    echo -e "${RED}❌ Deployment failed${NC}"
    exit 1
fi

# 7. Wait for deployment
echo -e "\n${YELLOW}[7/7]${NC} Waiting for app to be ready..."
if databricks apps wait "$APP_NAME" --timeout 300; then
    echo -e "${GREEN}✓ App is ready${NC}"
else
    echo -e "${RED}❌ App failed to start${NC}"
    echo -e "${YELLOW}Check logs with: databricks apps logs $APP_NAME${NC}"
    exit 1
fi

# Get app details
echo ""
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Deployment Complete!                                    ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

APP_INFO=$(databricks apps get "$APP_NAME" 2>/dev/null)
APP_URL=$(echo "$APP_INFO" | grep -o 'https://[^"]*' | head -1)

if [ -n "$APP_URL" ]; then
    echo -e "${GREEN}🚀 App URL:${NC}"
    echo -e "${BLUE}   $APP_URL${NC}"
    echo ""
    echo -e "${GREEN}📋 API Endpoints:${NC}"
    echo -e "   ${BLUE}$APP_URL/docs${NC} - Interactive API documentation"
    echo -e "   ${BLUE}$APP_URL/api/health${NC} - Health check"
    echo -e "   ${BLUE}$APP_URL/api/workflows/${NC} - Workflow management"
else
    echo -e "${YELLOW}⚠ Could not retrieve app URL${NC}"
    echo -e "${YELLOW}Get app details with: databricks apps get $APP_NAME${NC}"
fi

echo ""
echo -e "${GREEN}📊 View logs:${NC}"
echo -e "   ${BLUE}databricks apps logs $APP_NAME --follow${NC}"
echo ""
echo -e "${GREEN}🔄 Update app:${NC}"
echo -e "   ${BLUE}./deploy.sh${NC}"
echo ""
echo -e "${GREEN}❌ Delete app:${NC}"
echo -e "   ${BLUE}databricks apps delete $APP_NAME${NC}"
echo ""

# Check health endpoint
if [ -n "$APP_URL" ]; then
    echo -e "${YELLOW}Testing health endpoint...${NC}"
    sleep 5
    if curl -s -f "$APP_URL/api/health" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ App is responding${NC}"
    else
        echo -e "${YELLOW}⚠ App may still be starting. Check logs.${NC}"
    fi
fi

echo ""
echo -e "${GREEN}✅ Deployment completed successfully!${NC}"

