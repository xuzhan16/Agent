"""
config.py

统一大模型调用接口层配置。

读取优先级：
1. llm_interface_layer/local_llm_config.py 中的本地调试配置；
2. 环境变量；
3. 代码内默认 Base URL / 默认模型。
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

try:
    from .local_llm_config import (
        LOCAL_LLM_API_BASE_URL,
        LOCAL_LLM_API_KEY,
        LOCAL_LLM_MODEL,
    )
except ModuleNotFoundError:
    LOCAL_LLM_API_BASE_URL = ""
    LOCAL_LLM_API_KEY = ""
    LOCAL_LLM_MODEL = ""


DEFAULT_API_BASE_URL = "https://aihubmix.com/v1"
DEFAULT_MODEL_NAME = "coding-glm-5-free"


def _clean_config_text(value: Optional[str]) -> str:
    """统一清理配置字符串。"""
    return str(value or "").strip()


def _resolve_api_base_url() -> str:
    """读取真实大模型 API Base URL。"""
    return (
        _clean_config_text(LOCAL_LLM_API_BASE_URL)
        or _clean_config_text(os.getenv("LLM_API_BASE_URL"))
        or _clean_config_text(os.getenv("LLM_BASE_URL"))
        or DEFAULT_API_BASE_URL
    )


def _resolve_model_name() -> str:
    """读取真实大模型名称。"""
    return (
        _clean_config_text(LOCAL_LLM_MODEL)
        or _clean_config_text(os.getenv("LLM_MODEL_NAME"))
        or _clean_config_text(os.getenv("LLM_MODEL"))
        or DEFAULT_MODEL_NAME
    )


def _resolve_api_key() -> str:
    """读取真实大模型 API Key。"""
    return (
        _clean_config_text(LOCAL_LLM_API_KEY)
        or _clean_config_text(os.getenv("LLM_API_KEY"))
    )


@dataclass(frozen=True)
class LLMConfig:
    """真实大模型客户端配置。"""

    model_name: str = _resolve_model_name()
    temperature: float = float(os.getenv("LLM_TEMPERATURE", "0.2"))
    timeout_seconds: int = int(os.getenv("LLM_TIMEOUT_SECONDS", "60"))
    retry_times: int = int(os.getenv("LLM_RETRY_TIMES", "5"))
    api_base_url: str = _resolve_api_base_url()
    api_key: str = _resolve_api_key()
    api_key_env_name: str = os.getenv("LLM_API_KEY_ENV_NAME", "LLM_API_KEY")


@dataclass(frozen=True)
class StateConfig:
    """学生主状态文件配置。"""

    default_state_path: Path = Path(
        os.getenv("STUDENT_STATE_PATH", "outputs/state/student.json")
    )
    encoding: str = "utf-8"
    indent: int = 2


DEFAULT_LLM_CONFIG = LLMConfig()
DEFAULT_STATE_CONFIG = StateConfig()
