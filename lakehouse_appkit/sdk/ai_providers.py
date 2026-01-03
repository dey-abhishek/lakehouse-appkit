"""AI provider integrations."""
from typing import Dict, Any, Optional, List
from enum import Enum
from pydantic import BaseModel
import os


class AIProvider(str, Enum):
    """AI provider types."""
    OPENAI = "openai"
    CLAUDE = "claude"
    GEMINI = "gemini"
    TEMPLATE = "template"


class AIProviderConfig(BaseModel):
    """AI provider configuration."""
    provider: AIProvider
    api_key: Optional[str] = None
    model: Optional[str] = None
    enabled: bool = True


class AIProviderClient:
    """Base AI provider client."""
    
    def __init__(self, config: AIProviderConfig):
        self.config = config
    
    async def generate(self, prompt: str, **kwargs) -> str:
        """Generate text from prompt."""
        raise NotImplementedError()
    
    async def generate_code(
        self,
        prompt: str,
        context: Optional[Dict[str, Any]] = None,
        scaffold_type: Optional[str] = None
    ) -> str:
        """Generate code from prompt."""
        return await self.generate(prompt, **kwargs)


class OpenAIProvider(AIProviderClient):
    """OpenAI provider."""
    
    async def generate(self, prompt: str, **kwargs) -> str:
        """Generate using OpenAI."""
        # Placeholder - requires openai package
        return f"OpenAI response to: {prompt}"


class ClaudeProvider(AIProviderClient):
    """Claude provider using Anthropic API."""
    
    async def generate(self, prompt: str, **kwargs) -> str:
        """Generate using Claude."""
        try:
            import anthropic
        except ImportError:
            raise ImportError(
                "anthropic package is required for Claude provider. "
                "Install it with: pip install anthropic"
            )
        
        # Get API key from config or environment
        api_key = self.config.api_key or os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY is required for Claude provider")
        
        # Get model from config or use default
        # Using Claude 3 Haiku which is faster and cheaper for testing
        model = self.config.model or "claude-3-haiku-20240307"
        
        # Initialize client
        client = anthropic.Anthropic(api_key=api_key)
        
        # Make API call
        message = client.messages.create(
            model=model,
            max_tokens=4096,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        # Extract text from response
        return message.content[0].text
    
    async def generate_code(
        self,
        prompt: str,
        context: Optional[Dict[str, Any]] = None,
        scaffold_type: Optional[str] = None
    ) -> str:
        """Generate code using Claude."""
        # Add code generation context to prompt
        code_prompt = f"""Generate Python code for the following requirement:

{prompt}

Requirements:
- Use FastAPI for endpoints
- Use Pydantic for models
- Include type hints
- Add docstrings
- Follow PEP 8 style

Return ONLY the code, no explanations or markdown formatting."""

        if scaffold_type:
            code_prompt += f"\n\nScaffold type: {scaffold_type}"
        
        if context:
            code_prompt += f"\n\nContext: {context}"
        
        return await self.generate(code_prompt)

class GeminiProvider(AIProviderClient):
    """Gemini provider."""
    
    async def generate(self, prompt: str, **kwargs) -> str:
        """Generate using Gemini."""
        return f"Gemini response to: {prompt}"

class TemplateProvider(AIProviderClient):
    """Template-based provider (no AI)."""
    
    def __init__(self, config=None):
        """Initialize template provider (config optional)."""
        self.config = config
    
    async def generate(self, prompt: str, **kwargs) -> str:
        """Generate using templates."""
        return f"Template response to: {prompt}"
    
    async def generate_code(
        self,
        prompt: str,
        context: Optional[Dict[str, Any]] = None,
        scaffold_type: Optional[str] = None
    ) -> str:
        """Generate code using templates."""
        scaffold_type = scaffold_type or (context.get("type") if context else "endpoint")
        
        if scaffold_type == "endpoint":
            return '''
@app.get("/example")
async def example_endpoint():
    """Example endpoint."""
    return {"message": "Hello World"}
'''
        elif scaffold_type == "model":
            return '''
class ExampleModel(BaseModel):
    """Example model."""
    id: int
    name: str
'''
        elif scaffold_type == "service":
            return '''
class ExampleService:
    """Example service."""
    
    async def process(self, data: Dict) -> Dict:
        """Process data."""
        return {"result": "processed"}
'''
        else:
            return f"# Generated {scaffold_type} code"


def get_available_providers() -> List[AIProvider]:
    """Get list of available providers based on API keys."""
    providers = []
    if os.getenv("OPENAI_API_KEY"):
        providers.append(AIProvider.OPENAI)
    if os.getenv("ANTHROPIC_API_KEY"):
        providers.append(AIProvider.CLAUDE)
    if os.getenv("GEMINI_API_KEY"):
        providers.append(AIProvider.GEMINI)
    providers.append(AIProvider.TEMPLATE)
    return providers

def get_provider(provider_or_config, config: Optional[AIProviderConfig] = None) -> AIProviderClient:
    """Get AI provider client.
    
    Args:
        provider_or_config: Either AIProvider enum or AIProviderConfig object
        config: Optional config (ignored if provider_or_config is AIProviderConfig)
    """
    # Handle both AIProvider enum and AIProviderConfig
    if isinstance(provider_or_config, AIProviderConfig):
        config = provider_or_config
        provider = config.provider
    else:
        provider = provider_or_config
        if config is None:
            config = AIProviderConfig(provider=provider)
    
    if provider == AIProvider.OPENAI:
        return OpenAIProvider(config)
    elif provider == AIProvider.CLAUDE:
        return ClaudeProvider(config)
    elif provider == AIProvider.GEMINI:
        return GeminiProvider(config)
    else:
        return TemplateProvider(config)

def get_ai_provider(provider_name: str = None) -> AIProviderClient:
    """Get AI provider by name."""
    if provider_name:
        provider = AIProvider(provider_name)
    else:
        available = get_available_providers()
        provider = available[0] if available else AIProvider.TEMPLATE
    return get_provider(provider)

__all__ = [
    'AIProvider',
    'AIProviderConfig',
    'AIProviderClient',
    'OpenAIProvider',
    'ClaudeProvider',
    'GeminiProvider',
    'TemplateProvider',
    'get_provider',
    'get_available_providers',
    'get_ai_provider'
]
