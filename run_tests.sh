#!/usr/bin/env bash
#
# Test runner script for Lakehouse-AppKit
#
# Usage:
#   ./run_tests.sh              # Run all unit tests
#   ./run_tests.sh integration  # Run integration tests
#   ./run_tests.sh coverage     # Run with coverage report
#   ./run_tests.sh slow         # Include slow tests
#   ./run_tests.sh ai           # Run AI provider tests

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║        Lakehouse-AppKit Test Runner                      ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""

# Check if virtual environment is activated
if [ -z "$VIRTUAL_ENV" ]; then
    echo -e "${YELLOW}⚠️  Virtual environment not activated${NC}"
    echo -e "${YELLOW}   Attempting to activate lakehouse-app...${NC}"
    if [ -d "lakehouse-app/bin" ]; then
        source lakehouse-app/bin/activate
        echo -e "${GREEN}✓ Virtual environment activated${NC}"
    else
        echo -e "${RED}✗ Virtual environment not found${NC}"
        echo -e "${YELLOW}  Please run: python -m venv lakehouse-app && source lakehouse-app/bin/activate${NC}"
        exit 1
    fi
fi

# Check if .env.test exists
if [ ! -f ".env.test" ]; then
    echo -e "${YELLOW}⚠️  .env.test not found${NC}"
    echo -e "${YELLOW}   Creating from .env.example...${NC}"
    cp .env.example .env.test
    echo -e "${GREEN}✓ Created .env.test${NC}"
    echo -e "${YELLOW}   Please edit .env.test with your test configuration${NC}"
    echo ""
fi

# Default pytest args
PYTEST_ARGS="-v --tb=short"

# Parse command line arguments
TEST_TYPE=${1:-unit}

case "$TEST_TYPE" in
    unit)
        echo -e "${BLUE}Running unit tests (no external dependencies)...${NC}"
        PYTEST_ARGS="$PYTEST_ARGS -m unit"
        ;;
    integration)
        echo -e "${BLUE}Running integration tests (requires Databricks)...${NC}"
        PYTEST_ARGS="$PYTEST_ARGS -m integration"
        echo -e "${YELLOW}⚠️  Make sure .env.test is configured with valid Databricks credentials${NC}"
        ;;
    all)
        echo -e "${BLUE}Running all tests...${NC}"
        ;;
    coverage)
        echo -e "${BLUE}Running tests with coverage report...${NC}"
        PYTEST_ARGS="$PYTEST_ARGS --cov=lakehouse_appkit --cov-report=html --cov-report=term-missing"
        ;;
    slow)
        echo -e "${BLUE}Running all tests including slow tests...${NC}"
        PYTEST_ARGS="$PYTEST_ARGS --run-slow"
        ;;
    ai)
        echo -e "${BLUE}Running AI provider tests...${NC}"
        PYTEST_ARGS="$PYTEST_ARGS -m ai"
        echo -e "${YELLOW}⚠️  Make sure AI provider API keys are configured in .env.test${NC}"
        ;;
    specific)
        if [ -z "$2" ]; then
            echo -e "${RED}✗ Please specify a test file or pattern${NC}"
            echo -e "  Example: ./run_tests.sh specific tests/test_unity_catalog.py"
            exit 1
        fi
        echo -e "${BLUE}Running specific tests: $2${NC}"
        PYTEST_ARGS="$PYTEST_ARGS $2"
        ;;
    *)
        echo -e "${RED}✗ Unknown test type: $TEST_TYPE${NC}"
        echo ""
        echo "Usage: ./run_tests.sh [type]"
        echo ""
        echo "Types:"
        echo "  unit         - Run unit tests only (no external deps)"
        echo "  integration  - Run integration tests (requires Databricks)"
        echo "  all          - Run all tests"
        echo "  coverage     - Run with coverage report"
        echo "  slow         - Include slow tests"
        echo "  ai           - Run AI provider tests"
        echo "  specific     - Run specific test file/pattern"
        exit 1
        ;;
esac

echo ""

# Run pytest
echo -e "${BLUE}Executing: pytest $PYTEST_ARGS${NC}"
echo ""

if pytest $PYTEST_ARGS; then
    echo ""
    echo -e "${GREEN}╔══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║               ✓ All tests passed!                        ║${NC}"
    echo -e "${GREEN}╚══════════════════════════════════════════════════════════╝${NC}"
    
    # If coverage was run, show link to HTML report
    if [ "$TEST_TYPE" = "coverage" ]; then
        echo ""
        echo -e "${BLUE}📊 Coverage report generated:${NC}"
        echo -e "   Open: ${YELLOW}htmlcov/index.html${NC}"
    fi
    
    exit 0
else
    echo ""
    echo -e "${RED}╔══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${RED}║               ✗ Some tests failed                        ║${NC}"
    echo -e "${RED}╚══════════════════════════════════════════════════════════╝${NC}"
    exit 1
fi

