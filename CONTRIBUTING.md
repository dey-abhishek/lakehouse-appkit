# Contributing to Lakehouse-AppKit

First off, thank you for considering contributing to Lakehouse-AppKit! It's people like you that make this toolkit better for everyone.

## Code of Conduct

This project and everyone participating in it is governed by our Code of Conduct. By participating, you are expected to uphold this code.

## How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check the existing issues to avoid duplicates. When you create a bug report, include as many details as possible:

**Bug Report Template:**
```markdown
**Describe the bug**
A clear description of what the bug is.

**To Reproduce**
Steps to reproduce the behavior:
1. Run command '...'
2. See error

**Expected behavior**
What you expected to happen.

**Environment:**
- OS: [e.g., macOS 13.0]
- Python version: [e.g., 3.10]
- Lakehouse-AppKit version: [e.g., 1.0.0]
- Databricks Runtime: [e.g., 13.3 LTS]

**Additional context**
Any other relevant information.
```

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion, include:

- **Clear title and description**
- **Specific examples** of how the enhancement would be used
- **Current workarounds** if any exist
- **Why this enhancement would be useful** to most users

### Pull Requests

1. **Fork the repo** and create your branch from `main`
2. **Add tests** for any new functionality
3. **Update documentation** including README and docstrings
4. **Ensure tests pass** - Run `pytest` before submitting
5. **Follow code style** - We use Black for formatting
6. **Write meaningful commit messages**

## Development Setup

### 1. Fork and Clone

```bash
git clone https://github.com/YOUR_USERNAME/lakehouse-appkit.git
cd lakehouse-appkit
```

### 2. Create Virtual Environment

```bash
python -m venv lakehouse-app
source lakehouse-app/bin/activate  # On Windows: lakehouse-app\Scripts\activate
```

### 3. Install Development Dependencies

```bash
pip install -e ".[dev]"
```

### 4. Set Up Pre-commit Hooks (Optional but Recommended)

```bash
pip install pre-commit
pre-commit install
```

## Development Workflow

### Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_unity_catalog.py

# Run with coverage
pytest --cov=lakehouse_appkit --cov-report=html

# Run only fast tests (skip integration)
pytest -m "not integration"
```

### Code Formatting

We use Black for code formatting:

```bash
# Format all code
black lakehouse_appkit tests

# Check formatting without changes
black --check lakehouse_appkit tests
```

### Linting

```bash
# Run flake8
flake8 lakehouse_appkit tests

# Run mypy for type checking
mypy lakehouse_appkit
```

### Documentation

Update docstrings for any new functions/classes:

```python
def my_function(param1: str, param2: int) -> dict:
    """
    Brief description of what the function does.
    
    Args:
        param1: Description of param1
        param2: Description of param2
        
    Returns:
        Description of return value
        
    Raises:
        ValueError: When param1 is invalid
        
    Example:
        >>> result = my_function("test", 42)
        >>> print(result)
        {'status': 'success'}
    """
    pass
```

## Style Guide

### Python Style

- Follow [PEP 8](https://pep8.org/)
- Use type hints for all function signatures
- Maximum line length: 100 characters
- Use f-strings for string formatting
- Use descriptive variable names

### Example:

```python
from typing import List, Optional
from pydantic import BaseModel


class UserData(BaseModel):
    """User data model."""
    name: str
    age: int
    email: Optional[str] = None


async def get_users(
    limit: int = 10,
    offset: int = 0
) -> List[UserData]:
    """
    Retrieve paginated list of users.
    
    Args:
        limit: Maximum number of users to return
        offset: Number of users to skip
        
    Returns:
        List of UserData objects
    """
    # Implementation
    pass
```

### Commit Messages

Use conventional commit format:

```
type(scope): subject

body

footer
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

**Examples:**
```
feat(unity-catalog): add support for external tables

Implement REST API integration for external tables including
list, create, and describe operations.

Closes #123
```

```
fix(secrets): handle missing scope gracefully

Previously would crash with KeyError when scope didn't exist.
Now returns empty list with warning.

Fixes #456
```

## Testing Guidelines

### Writing Tests

1. **Test file naming:** `test_<module>.py`
2. **Test function naming:** `test_<functionality>_<scenario>`
3. **Use fixtures** for common setup
4. **Mark integration tests:** Use `@pytest.mark.integration`
5. **Use mocks** for external dependencies

### Example Test:

```python
import pytest
from unittest.mock import AsyncMock, patch
from lakehouse_appkit.unity_catalog import UnityCatalogManager


@pytest.fixture
def uc_manager():
    """Fixture for Unity Catalog manager."""
    return UnityCatalogManager(
        host="https://test.databricks.com",
        token="test-token"
    )


@pytest.mark.asyncio
async def test_list_catalogs_success(uc_manager):
    """Test successful catalog listing."""
    with patch.object(
        uc_manager.rest_client,
        'list_catalogs',
        new=AsyncMock(return_value={"catalogs": [{"name": "main"}]})
    ):
        catalogs = await uc_manager.list_catalogs()
        assert len(catalogs) == 1
        assert catalogs[0]["name"] == "main"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_catalogs_integration(uc_manager):
    """Integration test with real Databricks."""
    catalogs = await uc_manager.list_catalogs()
    assert isinstance(catalogs, list)
```

## Adding New Features

### 1. Create Feature Branch

```bash
git checkout -b feature/my-new-feature
```

### 2. Implement Feature

Follow the existing code structure:

```
lakehouse_appkit/
├── new_feature/
│   ├── __init__.py          # Public API
│   ├── rest_client.py       # REST API client
│   ├── routes.py            # FastAPI routes
│   └── models.py            # Pydantic models
```

### 3. Add CLI Command

```python
# lakehouse_appkit/cli/commands/my_feature.py
import click
from rich.console import Console

console = Console()


@click.group(name="my-feature")
def my_feature():
    """My feature commands."""
    pass


@my_feature.command("list")
def list_items():
    """List items."""
    console.print("[green]Listing items...[/green]")
```

### 4. Register Command

```python
# lakehouse_appkit/cli/main.py
from lakehouse_appkit.cli.commands.my_feature import my_feature
cli.add_command(my_feature)
```

### 5. Add Tests

```python
# tests/test_my_feature.py
import pytest


def test_my_feature():
    """Test my feature."""
    pass
```

### 6. Update Documentation

- Add feature to README.md
- Add API documentation
- Add usage examples

## Release Process

(For maintainers)

### 1. Update Version

```python
# lakehouse_appkit/__init__.py
__version__ = "1.1.0"
```

### 2. Update CHANGELOG

```markdown
## [1.1.0] - 2024-02-01

### Added
- New feature X
- Support for Y

### Fixed
- Bug in Z

### Changed
- Improved performance of W
```

### 3. Create Release

```bash
git tag -a v1.1.0 -m "Release v1.1.0"
git push origin v1.1.0
```

## Questions?

If you have questions about contributing, feel free to:
- Open a [Discussion](https://github.com/yourusername/lakehouse-appkit/discussions)
- Ask in the issue you're working on
- Reach out to maintainers

## Recognition

Contributors will be recognized in:
- README.md Contributors section
- Release notes
- Project documentation

Thank you for contributing to Lakehouse-AppKit! 🚀

