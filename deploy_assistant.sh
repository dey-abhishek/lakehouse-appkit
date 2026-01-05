#!/bin/bash

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

clear

echo -e "${BLUE}"
cat << "EOF"
╔════════════════════════════════════════════════════════════╗
║                                                            ║
║   Lakehouse-AppKit Deployment Assistant                   ║
║   Databricks Apps Deployment                              ║
║                                                            ║
╚════════════════════════════════════════════════════════════╝
EOF
echo -e "${NC}"

echo ""
echo -e "${CYAN}This assistant will help you deploy Lakehouse-AppKit to Databricks Apps${NC}"
echo ""

# Step 1: Check Databricks CLI
echo -e "${YELLOW}[Step 1/6]${NC} Checking Databricks CLI..."
if ! command -v databricks &> /dev/null; then
    echo -e "${RED}✗ Databricks CLI not found${NC}"
    echo ""
    echo "Install with:"
    echo "  pip install databricks-cli"
    exit 1
fi

CLI_VERSION=$(databricks --version 2>&1)
echo -e "${GREEN}✓ Databricks CLI found: ${CLI_VERSION}${NC}"
echo ""

# Step 2: Check Configuration
echo -e "${YELLOW}[Step 2/6]${NC} Checking Databricks CLI configuration..."
if databricks workspace ls / &> /dev/null; then
    echo -e "${GREEN}✓ Databricks CLI is configured${NC}"
    CONFIGURED=true
else
    echo -e "${YELLOW}⚠ Databricks CLI is not configured${NC}"
    CONFIGURED=false
fi
echo ""

# Step 3: Configure if needed
if [ "$CONFIGURED" = false ]; then
    echo -e "${YELLOW}[Step 3/6]${NC} Configuration Required"
    echo ""
    echo "You need to configure the Databricks CLI with your workspace credentials."
    echo ""
    echo -e "${CYAN}To configure:${NC}"
    echo "  1. Run: databricks configure --token"
    echo "  2. Enter Host: https://e2-demo-field-eng.cloud.databricks.com"
    echo "  3. Enter Token: Your personal access token (starts with dapi)"
    echo ""
    echo -e "${CYAN}To get your token:${NC}"
    echo "  1. Log into Databricks"
    echo "  2. Click User Settings (top right)"
    echo "  3. Go to 'Access tokens'"
    echo "  4. Click 'Generate new token'"
    echo "  5. Copy the token"
    echo ""
    
    read -p "Would you like to configure now? [y/N]: " -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        databricks configure --token
        
        # Verify
        if databricks workspace ls / &> /dev/null; then
            echo -e "${GREEN}✓ Configuration successful!${NC}"
        else
            echo -e "${RED}✗ Configuration failed. Please check your credentials.${NC}"
            exit 1
        fi
    else
        echo ""
        echo -e "${YELLOW}Please configure the Databricks CLI and run this script again:${NC}"
        echo "  databricks configure --token"
        echo ""
        exit 1
    fi
else
    echo -e "${YELLOW}[Step 3/6]${NC} Configuration"
    echo -e "${GREEN}✓ Already configured${NC}"
fi
echo ""

# Step 4: Check files
echo -e "${YELLOW}[Step 4/6]${NC} Checking deployment files..."
FILES_OK=true

if [ ! -f "databricks.yml" ]; then
    echo -e "${RED}✗ databricks.yml not found${NC}"
    FILES_OK=false
fi

if [ ! -f "app.py" ]; then
    echo -e "${RED}✗ app.py not found${NC}"
    FILES_OK=false
fi

if [ ! -f "requirements.txt" ]; then
    echo -e "${RED}✗ requirements.txt not found${NC}"
    FILES_OK=false
fi

if [ "$FILES_OK" = true ]; then
    echo -e "${GREEN}✓ All deployment files present${NC}"
else
    echo -e "${RED}✗ Missing required files${NC}"
    exit 1
fi
echo ""

# Step 5: Preview deployment
echo -e "${YELLOW}[Step 5/6]${NC} Deployment Preview"
echo ""
echo -e "${CYAN}App Name:${NC} lakehouse-appkit"
echo -e "${CYAN}Source:${NC} $(pwd)"
echo -e "${CYAN}Target:${NC} Databricks Apps"
echo -e "${CYAN}Workspace:${NC} e2-demo-field-eng.cloud.databricks.com"
echo ""
echo -e "${CYAN}What will be deployed:${NC}"
echo "  • FastAPI application"
echo "  • YAML workflow engine"
echo "  • REST APIs (Unity Catalog, Workflows, etc.)"
echo "  • Databricks adapter (REST-only)"
echo ""
echo -e "${CYAN}Resources to be created:${NC}"
echo "  • SQL Warehouse (lakehouse_appkit_warehouse)"
echo "  • Secret Scope (lakehouse-appkit-secrets)"
echo "  • Unity Catalog Volume (workflow_definitions)"
echo ""

# Step 6: Confirm deployment
echo -e "${YELLOW}[Step 6/6]${NC} Ready to Deploy"
echo ""
read -p "Do you want to proceed with deployment? [y/N]: " -n 1 -r
echo

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo ""
    echo -e "${YELLOW}Deployment cancelled${NC}"
    echo ""
    echo "When you're ready, run:"
    echo "  ./deploy.sh"
    echo ""
    exit 0
fi

echo ""
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  Starting Deployment...${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo ""

# Run the actual deployment
if [ -f "./deploy.sh" ]; then
    ./deploy.sh
else
    echo -e "${RED}✗ deploy.sh script not found${NC}"
    echo ""
    echo "Running manual deployment..."
    echo ""
    
    databricks apps deploy \
      --source-dir . \
      --app-name lakehouse-appkit \
      --description "Governed data workflows platform"
fi

