"""
llm_client.py

统一的大模型底层调用客户端。
只保留真实 OpenAI 兼容接口调用，不再走 mock 分支。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from pathlib import Path
from urllib import error, request

from .config import DEFAULT_LLM_CONFIG, LLMConfig
from .schemas import TaskType


_TASK_MAX_TOKENS = {
    TaskType.RESUME_PARSE: 1200,
    TaskType.JOB_EXTRACT: 1000,
    TaskType.JOB_DEDUP: 900,
    TaskType.JOB_PROFILE: 900,
    TaskType.STUDENT_PROFILE: 1100,
    TaskType.JOB_MATCH: 1000,
    TaskType.CAREER_PATH_PLAN: 1400,
    TaskType.CAREER_REPORT: 2600,
}


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
        normalized_task = TaskType.normalize(task_type)
        cache_key = self._build_cache_key(
            task_type=normalized_task,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
        )
        cached_text = self._load_cached_response(cache_key)
        if cached_text is not None:
            return cached_text

        last_error: Exception | None = None
        for _ in range(self.config.retry_times + 1):
            try:
                raw_text = self._real_generate(normalized_task, system_prompt, user_prompt)
                self._save_cached_response(cache_key, raw_text)
                return raw_text
            except Exception as exc:
                last_error = exc
                if not self._should_retry(exc):
                    raise

                print(f"LLM请求受限或服务暂不可用，等待 10 秒后重试... ({exc})")
                time.sleep(10)

        raise RuntimeError(f"LLM request failed after retries: {last_error}") from last_error

    def _real_generate(
        self,
        task_type: TaskType,
        system_prompt: str,
        user_prompt: str,
    ) -> str:
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
            "max_tokens": self._resolve_max_tokens(task_type),
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

    def _resolve_cache_path(self, cache_key: str) -> Path:
        return Path(self.config.cache_dir) / f"{cache_key}.json"

    def _build_cache_key(
        self,
        task_type: TaskType,
        system_prompt: str,
        user_prompt: str,
    ) -> str:
        key_payload = {
            "cache_version": 1,
            "task_type": task_type.value,
            "model_name": self.config.model_name,
            "temperature": self.config.temperature,
            "api_base_url": self.config.api_base_url,
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
        }
        return hashlib.sha256(
            json.dumps(key_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()

    def _load_cached_response(self, cache_key: str) -> str | None:
        if not self.config.cache_enabled:
            return None

        cache_path = self._resolve_cache_path(cache_key)
        if not cache_path.exists():
            return None

        try:
            cache_payload = json.loads(cache_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

        cached_text = cache_payload.get("response_text")
        if isinstance(cached_text, str) and cached_text.strip():
            return cached_text
        return None

    def _save_cached_response(self, cache_key: str, response_text: str) -> None:
        if not self.config.cache_enabled or not isinstance(response_text, str):
            return

        cache_path = self._resolve_cache_path(cache_key)
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_payload = {
                "cache_key": cache_key,
                "model_name": self.config.model_name,
                "response_text": response_text,
            }
            cache_path.write_text(
                json.dumps(cache_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except OSError:
            return

    def _resolve_max_tokens(self, task_type: TaskType) -> int:
        return _TASK_MAX_TOKENS.get(task_type, 1000)

    def _extract_http_status(self, exc: Exception) -> int | None:
        match = re.search(r"status=(\d{3})", str(exc))
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None
        return None

    def _should_retry(self, exc: Exception) -> bool:
        status_code = self._extract_http_status(exc)
        if status_code is not None:
            if status_code in {400, 401, 403, 404}:
                return False
            if status_code in {408, 409, 425, 429}:
                return True
            if 500 <= status_code < 600:
                return True

        if isinstance(exc, error.URLError):
            return True

        error_text = str(exc).lower()
        transient_markers = (
            "timed out",
            "timeout",
            "temporarily unavailable",
            "connection reset",
            "connection aborted",
            "remote end hung up unexpectedly",
        )
        return any(marker in error_text for marker in transient_markers)
