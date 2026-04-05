"""
context_builder.py

统一负责上下文拼装，不做真实 Neo4j / SQL 查询。
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Optional

from .schemas import TaskType


class ContextBuilder:
    """将 input_data、context_data、student_state、graph_context、sql_context 等统一拼装。"""

    def build_context(
        self,
        task_type: "TaskType | str",
        input_data: Dict[str, Any],
        context_data: Optional[Dict[str, Any]] = None,
        student_state: Optional[Dict[str, Any]] = None,
        extra_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        normalized_task = TaskType.normalize(task_type)
        safe_context_data = deepcopy(context_data or {})
        graph_context = safe_context_data.get("graph_context", {})
        sql_context = safe_context_data.get("sql_context", {})

        return {
            "task_type": normalized_task.value,
            "input_data": deepcopy(input_data or {}),
            "context_data": safe_context_data,
            "student_state": deepcopy(student_state or {}),
            "graph_context": deepcopy(graph_context),
            "sql_context": deepcopy(sql_context),
            "extra_context": deepcopy(extra_context or {}),
        }
