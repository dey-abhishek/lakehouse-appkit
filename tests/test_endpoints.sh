#!/bin/bash

echo "╔════════════════════════════════════════════════════════════╗"
echo "║   Testing Lakehouse-AppKit FastAPI Endpoints              ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

BASE_URL="http://localhost:8000"

# Test 1: Health Check
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🏥 Test 1: Health Check"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "GET $BASE_URL/api/health"
echo ""
curl -s "$BASE_URL/api/health" | python -m json.tool
echo ""

# Test 2: App Info
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "ℹ️  Test 2: App Info"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "GET $BASE_URL/api/info"
echo ""
curl -s "$BASE_URL/api/info" | python -m json.tool
echo ""

# Test 3: List Catalogs
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📚 Test 3: List Unity Catalog Catalogs"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "GET $BASE_URL/api/unity-catalog/catalogs"
echo ""
curl -s "$BASE_URL/api/unity-catalog/catalogs" | python -m json.tool | head -40
echo "... (showing first 40 lines)"
echo ""

# Test 4: List Schemas
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📂 Test 4: List Schemas in 'main' Catalog"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "GET $BASE_URL/api/unity-catalog/schemas/main"
echo ""
curl -s "$BASE_URL/api/unity-catalog/schemas/main" | python -m json.tool | head -30
echo "... (showing first 30 lines)"
echo ""

# Test 5: List Tables
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 Test 5: List Tables in 'main.default'"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "GET $BASE_URL/api/unity-catalog/tables/main/default"
echo ""
curl -s "$BASE_URL/api/unity-catalog/tables/main/default" | python -m json.tool | head -30
echo "... (showing first 30 lines)"
echo ""

# Test 6: Execute SQL Query
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔍 Test 6: Execute SQL Query"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "POST $BASE_URL/api/unity-catalog/query"
echo "Query: SELECT current_date() as today, 'FastAPI Test' as message"
echo ""
curl -s -X POST "$BASE_URL/api/unity-catalog/query?query=SELECT%20current_date()%20as%20today,%20%27FastAPI%20Test%27%20as%20message" | python -m json.tool
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ All Endpoint Tests Complete!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📖 View Interactive API Docs: http://localhost:8000/docs"
echo "🔄 View Alternative Docs: http://localhost:8000/redoc"
echo "🏠 View Home Page: http://localhost:8000"
echo ""
