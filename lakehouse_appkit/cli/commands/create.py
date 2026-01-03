"""
App creation utilities.
"""
import os
from pathlib import Path


def create_app(app_name: str, template: str, adapter: str, path: str):
    """Create a new Lakehouse-AppKit application."""
    app_path = Path(path) / app_name
    app_path.mkdir(parents=True, exist_ok=True)
    
    # Create basic structure
    (app_path / "app").mkdir(exist_ok=True)
    (app_path / "app" / "__init__.py").touch()
    
    # Create main.py
    main_content = f'''"""
{app_name} - FastAPI application
"""
from fastapi import FastAPI

app = FastAPI(title="{app_name}")

@app.get("/")
def read_root():
    return {{"message": "Welcome to {app_name}"}}

@app.get("/health")
def health():
    return {{"status": "ok"}}
'''
    
    (app_path / "app" / "main.py").write_text(main_content)
    
    # Create requirements.txt
    requirements = """fastapi
uvicorn[standard]
lakehouse-appkit
"""
    (app_path / "requirements.txt").write_text(requirements)
    
    # Create README
    readme = f"""# {app_name}

Created with Lakehouse-AppKit

## Run
```bash
cd {app_name}
uvicorn app.main:app --reload
```
"""
    (app_path / "README.md").write_text(readme)
