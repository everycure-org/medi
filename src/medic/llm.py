"""Centralized LLM configuration for MeDIC.

All LLM calls in the pipeline go through this module, which provides:
- Configurable model selection via environment variables or config file
- Unified interface via litellm (supports Anthropic, OpenAI, and others)
- Consistent API key handling from .env
- Default models for different task categories

Configuration priority:
1. Function argument (explicit model override)
2. Environment variable (MEDIC_LLM_MODEL, MEDIC_LLM_MODEL_FAST)
3. Config file (conf/llm_config.yaml)
4. Built-in defaults

Environment variables:
    MEDIC_LLM_MODEL: Default model for reasoning tasks (grounding, reranking)
    MEDIC_LLM_MODEL_FAST: Model for high-volume tasks (extraction, classification)
    ANTHROPIC_API_KEY: Required for Anthropic models
    OPENAI_API_KEY: Required for OpenAI models

Usage:
    from medic.llm import llm_call, get_model

    # Simple call with default model
    response = llm_call("Extract the drug name from: ...", task="extraction")

    # Explicit model override
    response = llm_call("Rerank these candidates: ...", model="claude-sonnet-4-20250514")
"""

import logging
import os
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

# Built-in defaults — override via env vars or config
_DEFAULTS = {
    "model": "anthropic/claude-sonnet-4-20250514",
    "model_fast": "anthropic/claude-haiku-4-5-20251001",
}

# Task -> model category mapping
_TASK_MODELS = {
    # Reasoning tasks (need higher quality) -> model
    "grounding_preprocess": "model",
    "grounding_rerank": "model",
    # High-volume tasks (need speed/cost efficiency) -> model_fast
    "extraction": "model_fast",
    "classification": "model_fast",
    "snippet_curation": "model_fast",
}

_config_loaded = False
_config: dict = {}


def _load_config() -> dict:
    """Load LLM config from conf/llm_config.yaml if it exists."""
    global _config_loaded, _config
    if _config_loaded:
        return _config

    config_path = Path("conf/llm_config.yaml")
    if config_path.exists():
        try:
            with open(config_path) as f:
                _config = yaml.safe_load(f) or {}
        except Exception:
            _config = {}
    _config_loaded = True
    return _config


def _load_env_keys() -> None:
    """Load API keys from .env file into environment if not already set."""
    env_path = Path(".env")
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            if key and not os.environ.get(key):
                os.environ[key] = value


def get_model(task: str | None = None, model: str | None = None) -> str:
    """Get the model name for a given task.

    Args:
        task: Task category (e.g., "extraction", "grounding_rerank").
              Determines whether to use the standard or fast model.
        model: Explicit model override. If provided, returned as-is.

    Returns:
        Model name in litellm format (e.g., "anthropic/claude-sonnet-4-20250514").
    """
    if model:
        # Ensure litellm provider prefix
        if "/" not in model:
            model = f"anthropic/{model}"
        return model

    config = _load_config()

    # Determine which model category this task needs
    model_key = _TASK_MODELS.get(task, "model") if task else "model"

    # Priority: env var > config file > defaults
    env_var = "MEDIC_LLM_MODEL" if model_key == "model" else "MEDIC_LLM_MODEL_FAST"
    env_value = os.environ.get(env_var, "")
    if env_value:
        if "/" not in env_value:
            env_value = f"anthropic/{env_value}"
        return env_value

    config_value = config.get(model_key, "")
    if config_value:
        if "/" not in config_value:
            config_value = f"anthropic/{config_value}"
        return config_value

    return _DEFAULTS[model_key]


def llm_call(
    prompt: str,
    *,
    task: str | None = None,
    model: str | None = None,
    max_tokens: int = 500,
    system: str | None = None,
) -> str:
    """Make an LLM call via litellm.

    Args:
        prompt: The user message.
        task: Task category for model selection.
        model: Explicit model override.
        max_tokens: Maximum tokens in response.
        system: Optional system message.

    Returns:
        The LLM response text.
    """
    import litellm

    _load_env_keys()

    resolved_model = get_model(task=task, model=model)

    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    logger.debug("LLM call: model=%s, task=%s, tokens=%d", resolved_model, task, max_tokens)

    response = litellm.completion(
        model=resolved_model,
        messages=messages,
        max_tokens=max_tokens,
        num_retries=4,          # survive transient network/rate-limit/5xx blips over long runs
        timeout=60,
    )

    return response.choices[0].message.content.strip()
