"""
state_manager.py

负责 student_api_state.json 的初始化、读取、保存、更新。
"""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Optional

from .config import DEFAULT_STATE_CONFIG, StateConfig
from .schemas import TASK_RESULT_FIELD_MAP, TaskType, build_empty_student_state


class StateManager:
    """student_api_state.json 状态管理器。"""

    def __init__(self, config: StateConfig = DEFAULT_STATE_CONFIG) -> None:
        self.config = config

    def resolve_state_path(self, state_path: Optional[str | Path] = None) -> Path:
        """返回实际 state 文件路径。"""
        return Path(state_path) if state_path else self.config.default_state_path

    def init_state(self, state_path: Optional[str | Path] = None, overwrite: bool = False) -> Dict[str, Any]:
        """初始化 student_api_state.json。"""
        path = self.resolve_state_path(state_path)
        if path.exists() and not overwrite:
            return self.load_state(path)

        state = build_empty_student_state()
        self.save_state(state, path)
        return state

    def load_state(self, state_path: Optional[str | Path] = None) -> Dict[str, Any]:
        """读取 student_api_state.json；若不存在则自动初始化。"""
        path = self.resolve_state_path(state_path)
        if not path.exists():
            return self.init_state(path, overwrite=True)

        with path.open("r", encoding=self.config.encoding) as f:
            data = json.load(f)

        state = build_empty_student_state()
        if isinstance(data, dict):
            state.update(data)
        return state

    def save_state(self, student_state: Dict[str, Any], state_path: Optional[str | Path] = None) -> None:
        """保存 student_api_state.json。"""
        path = self.resolve_state_path(state_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding=self.config.encoding) as f:
            json.dump(student_state, f, ensure_ascii=False, indent=self.config.indent)

    def update_state(
        self,
        task_type: "TaskType | str",
        task_result: Dict[str, Any],
        state_path: Optional[str | Path] = None,
        student_state: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """根据 task_type 自动写回 student_api_state.json 对应字段。"""
        normalized_task = TaskType.normalize(task_type)
        target_field = TASK_RESULT_FIELD_MAP[normalized_task]

        current_state = deepcopy(student_state) if student_state is not None else self.load_state(state_path)
        base_state = build_empty_student_state()
        base_state.update(current_state if isinstance(current_state, dict) else {})
        base_state[target_field] = deepcopy(task_result or {})

        self.save_state(base_state, state_path)
        return base_state


