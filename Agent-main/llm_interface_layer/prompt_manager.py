"""
prompt_manager.py

统一管理各任务类型的 system_prompt 和 user_prompt。
"""

from __future__ import annotations

import json
from typing import Dict, Tuple

from .schemas import TaskType, get_default_output_dict


class PromptManager:
    """根据 task_type 返回对应 prompt。"""

    def __init__(self) -> None:
        self._system_prompts = {
            TaskType.RESUME_PARSE: (
                "你是一名简历结构化解析助手。"
                "请仅返回合法 JSON，不要输出 Markdown 或额外解释。"
            ),
            TaskType.JOB_EXTRACT: (
                "你是一名招聘岗位要求结构化抽取助手。"
                "根据岗位名称、JD、公司信息抽取学历、经验、技能、证书、软技能等字段，仅返回合法 JSON。"
            ),
            TaskType.JOB_DEDUP: (
                "你是一名招聘岗位名称标准化与重复判断助手。"
                "根据成对岗位与其样本描述，判断是否同一标准岗位；可输出 mappings 或 duplicate_groups，仅返回合法 JSON。"
            ),
            TaskType.JOB_PROFILE: (
                "你是一名岗位画像生成助手。"
                "请根据岗位输入和上下文生成结构化岗位画像，仅返回合法 JSON。"
            ),
            TaskType.STUDENT_PROFILE: (
                "你是一名学生就业能力画像生成助手。"
                "请根据学生状态和上下文生成结构化画像，仅返回合法 JSON。"
            ),
            TaskType.JOB_MATCH: (
                "你是一名人岗匹配分析助手。"
                "请根据岗位画像和学生画像输出结构化匹配结果，仅返回合法 JSON。"
            ),
            TaskType.CAREER_PATH_PLAN: (
                "你是一名职业路径规划助手。"
                "请根据画像、匹配结果、图谱上下文和 SQL 上下文输出结构化规划结果，仅返回合法 JSON。"
            ),
            TaskType.CAREER_REPORT: (
                "你是一名职业生涯发展报告生成助手。"
                "允许 report_text 较长，但整体输出仍必须是合法 JSON。"
            ),
        }

        self._task_instructions = {
            TaskType.RESUME_PARSE: "从简历文本抽取 basic_info、技能、证书、项目、实习等结构化信息。",
            TaskType.JOB_EXTRACT: (
                "按输出模板填写：标准岗位名、岗位大类、学历/专业/经验要求、硬技能与工具栈、证书、软技能、"
                "实习实践要求、职级、适合候选人画像、需求摘要；不确定的字段填空字符串或空列表。"
            ),
            TaskType.JOB_DEDUP: (
                "优先输出 is_same_standard_job、standard_job_name、confidence、merge_reason；"
                "或输出 mappings（含 raw_title、normalized_title、confidence），"
                "或输出 duplicate_groups（master_record_id、duplicate_record_ids、reason）。"
            ),
            TaskType.JOB_PROFILE: "生成岗位画像、技能/学历/专业要求、垂直路径、换岗路径等字段。",
            TaskType.STUDENT_PROFILE: "生成学生能力画像、完整度评分、竞争力评分、优势和短板。",
            TaskType.JOB_MATCH: "输出多维度匹配分数、优势项、缺失项和提升建议。",
            TaskType.CAREER_PATH_PLAN: "输出首选目标岗位、备选岗位、直接路径、过渡路径、短期/中期计划。",
            TaskType.CAREER_REPORT: "基于前序结果生成最终职业规划报告，同时保留部分结构化摘要字段。",
        }

    def get_prompts(
        self,
        task_type: "TaskType | str",
        context_payload: Dict,
    ) -> Tuple[str, str]:
        """返回 system_prompt, user_prompt。"""
        normalized_task = TaskType.normalize(task_type)
        system_prompt = self._system_prompts[normalized_task]
        output_schema = get_default_output_dict(normalized_task)

        user_prompt = (
            f"任务类型：{normalized_task.value}\n"
            f"任务要求：{self._task_instructions[normalized_task]}\n"
            "请严格按下面的输出 JSON 模板返回，字段缺失时补空字符串、空数组、空对象或 0。\n"
            "输出 JSON 模板：\n"
            f"{json.dumps(output_schema, ensure_ascii=False, indent=2)}\n\n"
            "输入上下文 JSON：\n"
            f"{json.dumps(context_payload, ensure_ascii=False, indent=2)}"
        )
        return system_prompt, user_prompt
