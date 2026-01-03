"""
AI-assisted app scaffolding for Lakehouse-AppKit.
"""
from typing import Optional, Dict, Any
from lakehouse_appkit.sdk.ai_providers import AIProviderClient


class AIScaffolder:
    """AI code scaffolder."""
    
    def __init__(self, provider: Optional[AIProviderClient] = None, enable_ai: bool = True):
        """
        Initialize scaffolder.
        
        Args:
            provider: AI provider client (optional)
            enable_ai: Whether to enable AI features
        """
        self.enable_ai = enable_ai
        if provider:
            self.provider = provider
        else:
            from lakehouse_appkit.sdk.ai_providers import TemplateProvider
            self.provider = TemplateProvider()
    
    async def generate_endpoint(self, spec: Dict[str, Any]) -> str:
        """Generate FastAPI endpoint."""
        if hasattr(self.provider, 'generate_code'):
            return await self.provider.generate_code(
                prompt=f"Generate endpoint: {spec}",
                scaffold_type="endpoint"
            )
        return await self.provider.generate(f"Generate endpoint: {spec}")
    
    async def generate_model(self, spec: Dict[str, Any]) -> str:
        """Generate model."""
        if hasattr(self.provider, 'generate_code'):
            return await self.provider.generate_code(
                prompt=f"Generate model: {spec}",
                scaffold_type="model"
            )
        return await self.provider.generate(f"Generate model: {spec}")
    
    async def generate_service(self, spec: Dict[str, Any]) -> str:
        """Generate service."""
        if hasattr(self.provider, 'generate_code'):
            return await self.provider.generate_code(
                prompt=f"Generate service: {spec}",
                scaffold_type="service"
            )
        return await self.provider.generate(f"Generate service: {spec}")
    
    async def generate_template(self, spec: Dict[str, Any]) -> str:
        """Generate UI template."""
        return await self.provider.generate(f"Generate template: {spec}")
    
    async def generate_adapter_wiring(self, spec: Dict[str, Any]) -> str:
        """Generate adapter wiring code."""
        return await self.provider.generate(f"Generate adapter: {spec}")
    
    async def generate_test_stubs(self, spec: Dict[str, Any]) -> str:
        """Generate test stubs."""
        return await self.provider.generate(f"Generate tests: {spec}")
    
    def validate_code(self, code: str, max_length: int = 50000) -> bool:
        """
        Validate generated code for safety.
        
        Args:
            code: Code to validate
            max_length: Maximum allowed code length (default 50000)
            
        Returns:
            True if code is safe and valid
        """
        # Check length
        if len(code) > max_length:
            return False
        
        # Check for dangerous patterns
        dangerous = ['eval(', 'exec(', '__import__', 'os.system', 'subprocess']
        if any(d in code for d in dangerous):
            return False
        
        # Validate syntax
        try:
            compile(code, '<generated>', 'exec')
            return True
        except:
            return False
    
    def validate_generated_code(self, code: str) -> bool:
        """Validate generated code (alias)."""
        return self.validate_code(code)


class AIScaffoldGenerator(AIScaffolder):
    """AI scaffold generator (alias for backwards compatibility)."""
    pass


__all__ = ['AIScaffolder', 'AIScaffoldGenerator']
