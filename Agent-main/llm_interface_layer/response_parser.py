"""
response_parser.py

统一解析模型原始输出，并按 task_type 做字段补全与类型容错。
"""

from __future__ import annotations

import json
import re
from copy import deepcopy
from typing import Any, Dict

from .schemas import TaskType, get_default_output_dict


class ResponseParser:
    """将模型输出文本解析成结构化 dict。"""

    def parse(self, task_type: "TaskType | str", raw_text: str) -> Dict[str, Any]:
        normalized_task = TaskType.normalize(task_type)
        default_result = get_default_output_dict(normalized_task)
        parsed_data = self._parse_json_text(raw_text)
        if not isinstance(parsed_data, dict):
            parsed_data = {}

        if normalized_task == TaskType.CAREER_REPORT and not parsed_data:
            fallback = deepcopy(default_result)
            fallback["report_text"] = str(raw_text or "").strip()
            return fallback

        return self._coerce_with_default(parsed_data, default_result)

    def _parse_json_text(self, raw_text: str) -> Dict[str, Any]:
        """兼容标准 JSON、```json 代码块、夹杂解释文本。"""
        text = str(raw_text or "").strip()
        if not text:
            return {}

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        code_block_match = re.search(r"```json\s*([\s\S]*?)\s*```", text, flags=re.IGNORECASE)
        if code_block_match:
            try:
                return json.loads(code_block_match.group(1).strip())
            except json.JSONDecodeError:
                pass

        object_match = re.search(r"\{[\s\S]*\}", text)
        if object_match:
            try:
                return json.loads(object_match.group(0))
            except json.JSONDecodeError:
                pass

        return {}

    def _coerce_with_default(self, value: Any, default_value: Any) -> Any:
        """按默认结构递归补字段和修正类型。"""
        if isinstance(default_value, dict):
            if not default_value:
                return value if isinstance(value, dict) else {}
            source = value if isinstance(value, dict) else {}
            fixed = {}
            for key, sub_default in default_value.items():
                fixed[key] = self._coerce_with_default(source.get(key), sub_default)
            return fixed

        if isinstance(default_value, list):
            if isinstance(value, list):
                return value
            if value is None or value == "":
                return []
            if isinstance(value, str):
                text = value.strip()
                if text.startswith("[") and text.endswith("]"):
                    try:
                        loaded = json.loads(text)
                        return loaded if isinstance(loaded, list) else []
                    except json.JSONDecodeError:
                        return [text]
                parts = re.split(r"[、,，;/；|]+", text)
                return [item.strip() for item in parts if item.strip()]
            return [value]

        if isinstance(default_value, bool):
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.strip().lower() in {"1", "true", "yes", "y"}
            return bool(value) if value is not None else default_value

        if isinstance(default_value, (int, float)):
            if value is None or value == "":
                return default_value
            try:
                return type(default_value)(value)
            except (TypeError, ValueError):
                return default_value

        if value is None:
            return default_value
        return str(value).strip()
