"""
App packager for Databricks Apps deployment.
"""
import os
import zipfile
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Set
import fnmatch


class AppPackager:
    """Package FastAPI apps for Databricks deployment."""
    
    DEFAULT_EXCLUDES = [
        "*.pyc", "__pycache__", "*.pyo", "*.pyd",
        ".git", ".gitignore", ".env", ".env.*", "*.log",
        ".vscode", ".idea", "*.egg-info", "dist", "build",
        ".pytest_cache", ".coverage", "htmlcov",
        "node_modules", ".DS_Store"
    ]
    
    def __init__(self, source_dir: str, exclude_patterns: Optional[List[str]] = None):
        """Initialize app packager."""
        self.source_dir = Path(source_dir).resolve()
        
        if not self.source_dir.exists():
            raise FileNotFoundError(f"Source directory not found: {source_dir}")
        
        self.exclude_patterns = self.DEFAULT_EXCLUDES.copy()
        if exclude_patterns:
            self.exclude_patterns.extend(exclude_patterns)
    
    def _should_exclude(self, file_path: Path) -> bool:
        """Check if file should be excluded."""
        relative_path = str(file_path.relative_to(self.source_dir))
        for pattern in self.exclude_patterns:
            if fnmatch.fnmatch(relative_path, pattern) or fnmatch.fnmatch(file_path.name, pattern):
                return True
        return False
    
    def get_files_to_package(self) -> List[Path]:
        """Get list of files to include in package."""
        files = []
        for root, dirs, filenames in os.walk(self.source_dir):
            root_path = Path(root)
            
            # Filter directories
            dirs[:] = [d for d in dirs if not self._should_exclude(root_path / d)]
            
            # Add files
            for filename in filenames:
                file_path = root_path / filename
                if not self._should_exclude(file_path):
                    files.append(file_path)
        
        return files
    
    def create_package(self, output_dir: Optional[str] = None, package_name: Optional[str] = None) -> Path:
        """Create deployment package."""
        if output_dir is None:
            output_dir = tempfile.mkdtemp()
        
        if package_name is None:
            package_name = self.source_dir.name
        
        output_path = Path(output_dir) / f"{package_name}.zip"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        files = self.get_files_to_package()
        
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in files:
                arcname = file_path.relative_to(self.source_dir)
                zipf.write(file_path, arcname)
        
        return output_path
    
    def validate_app_structure(self) -> Dict[str, any]:
        """Validate app structure."""
        errors = []
        warnings = []
        
        # Check for main.py or app.py
        has_main = (self.source_dir / "main.py").exists()
        has_app = (self.source_dir / "app.py").exists()
        
        if not has_main and not has_app:
            errors.append("No main.py or app.py found")
            return {"valid": False, "errors": errors, "warnings": warnings}
        
        # Check for requirements.txt
        if not (self.source_dir / "requirements.txt").exists():
            warnings.append("No requirements.txt found")
        
        # Check for README
        if not any((self.source_dir / f).exists() for f in ["README.md", "README.txt", "README"]):
            warnings.append("No README found")
        
        return {"valid": True, "errors": errors, "warnings": warnings}
    
    def get_package_info(self) -> Dict[str, any]:
        """Get package information."""
        files = self.get_files_to_package()
        py_files = [f for f in files if f.suffix == '.py']
        total_size = sum(f.stat().st_size for f in files)
        
        return {
            "source_dir": str(self.source_dir),
            "file_count": len(files),
            "python_files": len(py_files),
            "total_size": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2)
        }


__all__ = ['AppPackager']
