#!/bin/bash

# Lakehouse-AppKit Installation Script
# Run this script to set up the development environment

set -e  # Exit on error

echo "🚀 Lakehouse-AppKit Installation"
echo "================================"
echo ""

# Check Python version
echo "📌 Checking Python version..."
python3 --version

# Activate virtual environment
echo ""
echo "📦 Activating virtual environment..."
source lakehouse-app/bin/activate

# Upgrade pip
echo ""
echo "⬆️  Upgrading pip..."
pip install --upgrade pip

# Install the package in editable mode with all dependencies
echo ""
echo "📥 Installing lakehouse-appkit with development dependencies..."
pip install -e ".[dev]"

# Install Databricks support
echo ""
echo "🔧 Installing Databricks adapter..."
pip install databricks-sdk databricks-sql-connector

# Verify installation
echo ""
echo "✅ Verifying installation..."
lakehouse-appkit --version

echo ""
echo "🎉 Installation complete!"
echo ""
echo "Next steps:"
echo "  1. Activate the venv: source lakehouse-app/bin/activate"
echo "  2. Create an app: lakehouse-appkit create my-app"
echo "  3. Configure Databricks: cd my-app && cp .env.example .env"
echo "  4. Run the app: lakehouse-appkit run --reload"
echo ""
echo "📖 See SETUP_COMPLETE.md for full documentation"
echo ""

