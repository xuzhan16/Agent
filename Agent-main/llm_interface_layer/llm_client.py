"""
llm_client.py

统一的大模型底层调用客户端。
只保留真实 OpenAI 兼容接口调用，不再走 mock 分支。
"""

from __future__ import annotations

import json
import os
import time
from urllib import error, request

from .config import DEFAULT_LLM_CONFIG, LLMConfig
from .schemas import TaskType


class LLMClient:
    """底层模型调用客户端。输入 prompt，输出模型原始文本。"""

    def __init__(self, config: LLMConfig = DEFAULT_LLM_CONFIG) -> None:
        self.config = config

    def generate(
        self,
        task_type: "TaskType | str",
        system_prompt: str,
        user_prompt: str,
    ) -> str:
        """统一生成入口，只调用真实大模型接口。"""
        TaskType.normalize(task_type)

        last_error: Exception | None = None
        for _ in range(self.config.retry_times + 1):
            try:
                return self._real_generate(system_prompt, user_prompt)
            except Exception as exc:
                last_error = exc
                import time
                print(f"LLM请求受限 (429/并发超限等)，等待 10 秒后重试... ({exc})")
                time.sleep(10)

        raise RuntimeError(f"LLM request failed after retries: {last_error}") from last_error

    def _real_generate(self, system_prompt: str, user_prompt: str) -> str:
        """调用真实 OpenAI 兼容 /chat/completions 接口。"""
        api_key = self.config.api_key or os.getenv(self.config.api_key_env_name, "").strip()
        if not self.config.api_base_url:
            raise RuntimeError("Real LLM call requires LLM_API_BASE_URL or LLM_BASE_URL.")
        if not api_key:
            raise RuntimeError(
                "Real LLM call requires API key. Please set LOCAL_LLM_API_KEY in "
                "llm_interface_layer/local_llm_config.py or env var LLM_API_KEY."
            )

        payload = {
            "model": self.config.model_name,
            "temperature": self.config.temperature,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }

        req = request.Request(
            self.config.api_base_url.rstrip("/") + "/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            method="POST",
        )

        try:
            with request.urlopen(req, timeout=self.config.timeout_seconds) as resp:
                response_json = json.loads(resp.read().decode("utf-8"))
        except error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(
                f"LLM HTTPError: status={exc.code}, reason={exc.reason}, body={error_body}"
            ) from exc

        try:
            return response_json["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise RuntimeError(f"Unexpected LLM response schema: {response_json}") from exc
