import os
from typing import Dict, Optional

from openai import OpenAI


_PROVIDER_CFG: Dict[str, Dict[str, Optional[str]]] = {
    "openai": {"env": "OPENAI_API_KEY", "base_url": None},
    "grok": {"env": "GROK_API_KEY", "base_url": "https://api.x.ai/v1"},
    "gemini": {"env": "GEMINI_API_KEY", "base_url": "https://generativelanguage.googleapis.com/v1beta/openai"},
    "anthropic": {"env": "ANTHROPIC_API_KEY", "base_url": "https://api.anthropic.com/v1"},
}

# Create an OpenAI-compatible client for multiple providers
def get_openai_compatible_client(provider: Optional[str], *, default_openai_api_key: Optional[str] = None) -> OpenAI:
    provider_l = (provider or "openai").lower()
    cfg = _PROVIDER_CFG.get(provider_l)
    if cfg is None:
        raise ValueError(f"Unsupported provider: {provider_l}")

    api_key = os.getenv(cfg["env"])
    if not api_key and provider_l == "openai":
        api_key = default_openai_api_key
    if not api_key:
        raise ValueError(f"Missing API key for provider '{provider_l}'. Set {cfg['env']}.")

    kwargs = {"api_key": api_key}
    if cfg["base_url"]:
        kwargs["base_url"] = cfg["base_url"]
    return OpenAI(**kwargs)


