"""
llm_service.py

统一大模型调用服务层。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from .config import DEFAULT_LLM_CONFIG, DEFAULT_STATE_CONFIG, LLMConfig, StateConfig
from .context_builder import ContextBuilder
from .llm_client import LLMClient
from .prompt_manager import PromptManager
from .response_parser import ResponseParser
from .schemas import TaskType
from .state_manager import StateManager


class LLMService:
    """
    统一服务入口，负责：
    - task_type 校验
    - prompt 获取
    - context 组装
    - 模型调用
    - 响应解析
    - 状态写回
    """

    def __init__(
        self,
        llm_config: LLMConfig = DEFAULT_LLM_CONFIG,
        state_config: StateConfig = DEFAULT_STATE_CONFIG,
        prompt_manager: Optional[PromptManager] = None,
        context_builder: Optional[ContextBuilder] = None,
        llm_client: Optional[LLMClient] = None,
        response_parser: Optional[ResponseParser] = None,
        state_manager: Optional[StateManager] = None,
    ) -> None:
        self.prompt_manager = prompt_manager or PromptManager()
        self.context_builder = context_builder or ContextBuilder()
        self.llm_client = llm_client or LLMClient(config=llm_config)
        self.response_parser = response_parser or ResponseParser()
        self.state_manager = state_manager or StateManager(config=state_config)

    def call_llm(
        self,
        task_type: "TaskType | str",
        input_data: Dict[str, Any],
        context_data: Optional[Dict[str, Any]] = None,
        student_state: Optional[Dict[str, Any]] = None,
        extra_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """统一大模型入口：只返回解析后的任务结果，不自动写 state。"""
        normalized_task = TaskType.normalize(task_type)

        context_payload = self.context_builder.build_context(
            task_type=normalized_task,
            input_data=input_data,
            context_data=context_data,
            student_state=student_state,
            extra_context=extra_context,
        )

        system_prompt, user_prompt = self.prompt_manager.get_prompts(
            task_type=normalized_task,
            context_payload=context_payload,
        )

        raw_text = self.llm_client.generate(
            task_type=normalized_task,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
        )

        return self.response_parser.parse(normalized_task, raw_text)

    def run_task_and_update_state(
        self,
        task_type: "TaskType | str",
        input_data: Dict[str, Any],
        context_data: Optional[Dict[str, Any]] = None,
        state_path: Optional[str | Path] = None,
        extra_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        执行任务并把结果写回 student_api_state.json。

        返回结构：
        {
          "task_type": "...",
          "result": {...},
          "student_state": {...}
        }
        """
        normalized_task = TaskType.normalize(task_type)
        current_state = self.state_manager.load_state(state_path)

        result = self.call_llm(
            task_type=normalized_task,
            input_data=input_data,
            context_data=context_data,
            student_state=current_state,
            extra_context=extra_context,
        )

        updated_state = self.state_manager.update_state(
            task_type=normalized_task,
            task_result=result,
            state_path=state_path,
            student_state=current_state,
        )

        return {
            "task_type": normalized_task.value,
            "result": result,
            "student_state": updated_state,
        }


_DEFAULT_SERVICE = LLMService()


def call_llm(
    task_type: "TaskType | str",
    input_data: Dict[str, Any],
    context_data: Optional[Dict[str, Any]] = None,
    student_state: Optional[Dict[str, Any]] = None,
    extra_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """模块级统一入口，方便业务层直接调用。"""
    return _DEFAULT_SERVICE.call_llm(
        task_type=task_type,
        input_data=input_data,
        context_data=context_data,
        student_state=student_state,
        extra_context=extra_context,
    )


def run_task_and_update_state(
    task_type: "TaskType | str",
    input_data: Dict[str, Any],
    context_data: Optional[Dict[str, Any]] = None,
    state_path: Optional[str | Path] = None,
    extra_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """模块级“执行并写回 state”入口。"""
    return _DEFAULT_SERVICE.run_task_and_update_state(
        task_type=task_type,
        input_data=input_data,
        context_data=context_data,
        state_path=state_path,
        extra_context=extra_context,
    )


