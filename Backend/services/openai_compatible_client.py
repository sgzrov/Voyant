import os
from typing import Dict, Optional

from openai import AsyncOpenAI


_PROVIDER_CFG: Dict[str, Dict[str, Optional[str]]] = {
    "openai": {"env": "OPENAI_API_KEY", "base_url": None},
    "grok": {"env": "GROK_API_KEY", "base_url": "https://api.x.ai/v1"},
    "gemini": {"env": "GEMINI_API_KEY", "base_url": "https://generativelanguage.googleapis.com/v1beta/openai"},
    "anthropic": {"env": "ANTHROPIC_API_KEY", "base_url": "https://api.anthropic.com/v1"},
}

# Create an async OpenAI-compatible client for multiple model providers
def get_async_openai_compatible_client(provider: Optional[str], *, default_openai_api_key: Optional[str] = None) -> AsyncOpenAI:
    provider_l = (provider or "openai").strip().lower()
    cfg = _PROVIDER_CFG.get(provider_l)
    if cfg is None:
        raise ValueError(f"Unsupported provider: {provider_l}")

    env_var = cfg["env"]
    if not env_var:
        raise ValueError(f"Missing env var mapping for provider '{provider_l}'.")

    api_key = os.getenv(env_var)
    if not api_key and provider_l == "openai":
        api_key = default_openai_api_key
    if not api_key:
        raise ValueError(f"Missing API key for provider '{provider_l}'. Set {env_var}.")

    kwargs = {"api_key": api_key}
    if cfg["base_url"]:
        kwargs["base_url"] = cfg["base_url"]
    return AsyncOpenAI(**kwargs)


