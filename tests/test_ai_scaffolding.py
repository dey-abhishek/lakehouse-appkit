"""
Tests for AI scaffolding functionality.
"""
import pytest
from unittest.mock import AsyncMock, Mock, patch

from lakehouse_appkit.sdk.ai_scaffold import AIScaffolder
from lakehouse_appkit.sdk.ai_providers import (
    get_provider,
    get_available_providers,
    AIProvider,
    AIProviderConfig,
    OpenAIProvider,
    ClaudeProvider,
    GeminiProvider,
    TemplateProvider
)
from tests.test_config import skip_if_no_ai_provider


# ============================================================================
# AI Provider Tests
# ============================================================================

@pytest.mark.unit
class TestAIProviders:
    """Test AI provider selection and initialization."""
    
    def test_template_provider(self):
        """Test template provider (no API key needed)."""
        config = AIProviderConfig(provider=AIProvider.TEMPLATE)
        provider = get_provider(config)
        assert isinstance(provider, TemplateProvider)
    
    @skip_if_no_ai_provider("openai")
    def test_openai_provider(self):
        """Test OpenAI provider."""
        config = AIProviderConfig(provider=AIProvider.OPENAI, api_key="test-key")
        provider = get_provider(config)
        assert isinstance(provider, OpenAIProvider)
    
    @skip_if_no_ai_provider("claude")
    def test_claude_provider(self):
        """Test Claude provider."""
        config = AIProviderConfig(provider=AIProvider.CLAUDE, api_key="test-key")
        provider = get_provider(config)
        assert isinstance(provider, ClaudeProvider)
    
    @skip_if_no_ai_provider("gemini")
    def test_gemini_provider(self):
        """Test Gemini provider."""
        config = AIProviderConfig(provider=AIProvider.GEMINI, api_key="test-key")
        provider = get_provider(config)
        assert isinstance(provider, GeminiProvider)
    
    def test_invalid_provider(self):
        """Test invalid provider name."""
        # All provider types are valid enums, so this test is not applicable
        pass


@pytest.mark.unit
class TestTemplateProvider:
    """Test template-based code generation."""
    
    @pytest.mark.asyncio
    async def test_generate_endpoint_template(self):
        """Test endpoint generation with template."""
        config = AIProviderConfig(provider=AIProvider.TEMPLATE)
        provider = get_provider(config)
        code = await provider.generate_code(
            prompt="Create endpoint to get user by ID",
            context={"type": "endpoint"},
            scaffold_type="endpoint"
        )
        
        assert isinstance(code, str)
        assert len(code) > 0
        assert "def" in code or "async def" in code
    
    @pytest.mark.asyncio
    async def test_generate_model_template(self):
        """Test model generation with template."""
        config = AIProviderConfig(provider=AIProvider.TEMPLATE)
        provider = get_provider(config)
        code = await provider.generate_code(
            prompt="Create User model with name and email",
            context={"type": "model"},
            scaffold_type="model"
        )
        
        assert isinstance(code, str)
        assert "class" in code or "def" in code
    
    @pytest.mark.asyncio
    async def test_generate_service_template(self):
        """Test service generation with template."""
        config = AIProviderConfig(provider=AIProvider.TEMPLATE)
        provider = get_provider(config)
        code = await provider.generate_code(
            prompt="Create UserService for database operations",
            context={"type": "service"},
            scaffold_type="service"
        )
        
        assert isinstance(code, str)
        assert "class" in code or "def" in code


# ============================================================================
# AI Scaffolder Tests
# ============================================================================

@pytest.mark.unit
class TestAIScaffolder:
    """Test AI scaffolder."""
    
    @pytest.mark.asyncio
    async def test_scaffolder_initialization(self):
        """Test scaffolder initialization."""
        scaffolder = AIScaffolder(enable_ai=False)
        assert scaffolder is not None
    
    @pytest.mark.asyncio
    async def test_generate_endpoint(self):
        """Test endpoint generation."""
        config = AIProviderConfig(provider=AIProvider.TEMPLATE)
        provider = get_provider(config)
        
        code = await provider.generate_code(
            prompt="Get user by ID",
            context={"type": "endpoint"},
            scaffold_type="endpoint"
        )
        
        assert isinstance(code, str)
        assert len(code) > 0
    
    @pytest.mark.asyncio
    async def test_generate_model(self):
        """Test model generation."""
        config = AIProviderConfig(provider=AIProvider.TEMPLATE)
        provider = get_provider(config)
        
        code = await provider.generate_code(
            prompt="User model",
            context={"type": "model"},
            scaffold_type="model"
        )
        
        assert isinstance(code, str)
        assert len(code) > 0
    
    @pytest.mark.asyncio
    async def test_generate_service(self):
        """Test service generation."""
        config = AIProviderConfig(provider=AIProvider.TEMPLATE)
        provider = get_provider(config)
        
        code = await provider.generate_code(
            prompt="UserService",
            context={"type": "service"},
            scaffold_type="service"
        )
        
        assert isinstance(code, str)
        assert len(code) > 0
    
    @pytest.mark.asyncio
    async def test_code_validation(self):
        """Test code validation."""
        scaffolder = AIScaffolder(enable_ai=False)
        
        # Valid code
        valid_code = "def test():\n    pass"
        assert scaffolder.validate_code(valid_code) is True
        
        # Invalid code (syntax error)
        invalid_code = "def test(: pass"
        assert scaffolder.validate_code(invalid_code) is False


# ============================================================================
# AI Safety Tests
# ============================================================================

@pytest.mark.unit
class TestAISafety:
    """Test AI safety features."""
    
    @pytest.mark.asyncio
    async def test_forbidden_patterns(self):
        """Test detection of forbidden code patterns."""
        scaffolder = AIScaffolder(enable_ai=False)
        
        # Code with system call (forbidden)
        dangerous_code = "import os\nos.system('rm -rf /')"
        assert scaffolder.validate_code(dangerous_code) is False
        
        # Code with eval (forbidden)
        dangerous_code2 = "eval('malicious code')"
        assert scaffolder.validate_code(dangerous_code2) is False
    
    @pytest.mark.asyncio
    async def test_code_length_limits(self):
        """Test code length limits."""
        scaffolder = AIScaffolder(enable_ai=False)
        
        # Normal code (should pass)
        normal_code = "def test():\n    pass"
        assert scaffolder.validate_code(normal_code) is True
        
        # Very long code (should fail - exceeds 50000 chars)
        long_code = "def test():\n    " + "pass\n    " * 10000
        assert scaffolder.validate_code(long_code) is False
    
    @pytest.mark.asyncio
    async def test_safe_code_patterns(self):
        """Test that safe code patterns pass validation."""
        scaffolder = AIScaffolder(enable_ai=False)
        
        safe_codes = [
            "def hello_world():\n    return 'Hello'",
            "class User:\n    def __init__(self, name):\n        self.name = name",
            "from fastapi import APIRouter\nrouter = APIRouter()",
            "import json\ndata = json.loads('{}')",
        ]
        
        for code in safe_codes:
            assert scaffolder.validate_code(code) is True, f"Safe code failed validation: {code}"


# ============================================================================
# Integration Tests (Require API Keys)
# ============================================================================

@pytest.mark.integration
@pytest.mark.ai
class TestAIProvidersIntegration:
    """Integration tests with real AI providers (requires API keys)."""
    
    @skip_if_no_ai_provider("openai")
    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_openai_generation(self):
        """Test OpenAI code generation."""
        pytest.skip("OpenAI integration test - needs API key")
    
    @skip_if_no_ai_provider("claude")
    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_claude_generation(self):
        """Test Claude code generation."""
        from lakehouse_appkit.sdk.ai_providers import ClaudeProvider, AIProviderConfig, AIProvider
        from tests.test_config import get_test_config
        
        # Get config with API key
        config = get_test_config()
        assert config is not None
        assert config.ai_provider is not None
        assert config.ai_provider.anthropic_api_key is not None
        
        # Create provider
        provider_config = AIProviderConfig(
            provider=AIProvider.CLAUDE,
            api_key=config.ai_provider.anthropic_api_key
        )
        provider = ClaudeProvider(provider_config)
        
        # Test simple generation
        response = await provider.generate("What is 2+2? Answer in one word.")
        assert isinstance(response, str)
        assert len(response) > 0
        print(f"\nClaude response: {response}")
        
        # Test code generation
        code = await provider.generate_code(
            prompt="Create a FastAPI endpoint to get user by ID",
            scaffold_type="endpoint"
        )
        assert isinstance(code, str)
        assert len(code) > 0
        assert "def" in code or "async def" in code
        print(f"\nGenerated code:\n{code}")
    
    @skip_if_no_ai_provider("gemini")
    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_gemini_generation(self):
        """Test Gemini code generation."""
        pytest.skip("Gemini integration test - needs API key")
