"""
context_builder.py

统一负责上下文拼装，不做真实 Neo4j / SQL 查询。
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Optional

from .schemas import TaskType


_MAX_TEXT_LENGTH = 320
_MAX_LIST_ITEMS = 8
_STATE_FIELDS_BY_TASK = {
    TaskType.RESUME_PARSE: ("basic_info",),
    TaskType.JOB_EXTRACT: tuple(),
    TaskType.JOB_DEDUP: tuple(),
    TaskType.STUDENT_PROFILE: ("basic_info", "resume_parse_result"),
    TaskType.JOB_PROFILE: ("basic_info", "resume_parse_result"),
    TaskType.JOB_MATCH: ("student_profile_result", "job_profile_result"),
    TaskType.CAREER_PATH_PLAN: (
        "student_profile_result",
        "job_profile_result",
        "job_match_result",
    ),
    TaskType.CAREER_REPORT: (
        "student_profile_result",
        "job_profile_result",
        "job_match_result",
        "career_path_plan_result",
    ),
}
_STATE_RESULT_KEY_ALLOWLIST = {
    "basic_info": (
        "name",
        "gender",
        "school",
        "major",
        "degree",
        "graduation_year",
        "target_job",
    ),
    "resume_parse_result": (
        "basic_info",
        "skills",
        "certificates",
        "awards",
        "project_experience",
        "internship_experience",
        "parse_warnings",
        "raw_summary",
    ),
    "student_profile_result": (
        "skill_profile",
        "certificate_profile",
        "soft_skills",
        "employment_ability_profile",
        "potential_profile",
        "complete_score",
        "competitiveness_score",
        "score_level",
        "strengths",
        "weaknesses",
        "missing_dimensions",
        "summary",
        "ability_evidence",
    ),
    "job_profile_result": (
        "standard_job_name",
        "job_category",
        "job_level",
        "degree_requirement",
        "major_requirement",
        "experience_requirement",
        "hard_skills",
        "tools_or_tech_stack",
        "certificate_requirement",
        "practice_requirement",
        "soft_skills",
        "summary",
        "vertical_paths",
        "transfer_paths",
        "salary_stats",
    ),
    "job_match_result": (
        "basic_requirement_score",
        "vocational_skill_score",
        "professional_quality_score",
        "development_potential_score",
        "overall_match_score",
        "score_level",
        "matched_items",
        "missing_items",
        "strengths",
        "weaknesses",
        "improvement_suggestions",
        "recommendation",
        "analysis_summary",
    ),
    "career_path_plan_result": (
        "primary_target_job",
        "secondary_target_jobs",
        "goal_positioning",
        "goal_reason",
        "direct_path",
        "transition_path",
        "long_term_path",
        "path_strategy",
        "short_term_plan",
        "mid_term_plan",
        "decision_summary",
        "risk_and_gap",
        "fallback_strategy",
    ),
    "career_report_result": (
        "report_title",
        "report_summary",
        "report_sections",
    ),
}
_HEAVY_INTERNAL_KEYS = {
    "profile_input_payload",
    "match_input_payload",
    "career_plan_input_payload",
    "report_input_payload",
    "report_sections_draft",
    "report_text_draft",
    "report_text_markdown",
    "rule_score_result",
    "selector_result",
    "llm_profile_result",
    "llm_match_result",
    "llm_plan_result",
    "llm_report_result",
    "explicit_requirements",
    "normalized_requirements",
    "representative_samples",
    "dimension_details",
    "score_weights",
    "raw_resume_text",
    "raw_llm_result",
}


def _truncate_text(value: Any, max_length: int = _MAX_TEXT_LENGTH) -> str:
    text = str(value or "").strip()
    if len(text) <= max_length:
        return text
    return f"{text[: max_length - 1]}…"


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if value == "":
        return True
    if isinstance(value, (list, dict, tuple, set)) and len(value) == 0:
        return True
    return False


def _compact_value(value: Any) -> Any:
    if isinstance(value, str):
        return _truncate_text(value)

    if isinstance(value, dict):
        compacted: Dict[str, Any] = {}
        for key, item in value.items():
            if key in _HEAVY_INTERNAL_KEYS:
                continue
            normalized_key = str(key)
            compact_item = _compact_value(item)
            if not _is_empty(compact_item):
                compacted[normalized_key] = compact_item
        return compacted

    if isinstance(value, list):
        compacted_items = []
        for item in value[:_MAX_LIST_ITEMS]:
            compact_item = _compact_value(item)
            if not _is_empty(compact_item):
                compacted_items.append(compact_item)
        return compacted_items

    if isinstance(value, tuple):
        return _compact_value(list(value))

    return value


def _compact_state_field(field_name: str, value: Any) -> Any:
    if field_name in _STATE_RESULT_KEY_ALLOWLIST and isinstance(value, dict):
        source = {
            key: deepcopy(value.get(key))
            for key in _STATE_RESULT_KEY_ALLOWLIST[field_name]
            if key in value
        }
    else:
        source = deepcopy(value)
    return _compact_value(source)


def _build_task_state_snapshot(
    task_type: TaskType,
    student_state: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    if not isinstance(student_state, dict):
        return {}

    snapshot: Dict[str, Any] = {}
    for field_name in _STATE_FIELDS_BY_TASK.get(task_type, tuple()):
        compacted = _compact_state_field(field_name, student_state.get(field_name))
        if not _is_empty(compacted):
            snapshot[field_name] = compacted
    return snapshot


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
        graph_context = safe_context_data.pop("graph_context", {})
        sql_context = safe_context_data.pop("sql_context", {})

        payload = {
            "task_type": normalized_task.value,
        }

        compact_input_data = _compact_value(deepcopy(input_data or {}))
        compact_context_data = _compact_value(safe_context_data)
        compact_student_state = _build_task_state_snapshot(
            normalized_task,
            student_state=student_state,
        )
        compact_graph_context = _compact_value(graph_context)
        compact_sql_context = _compact_value(sql_context)
        compact_extra_context = _compact_value(deepcopy(extra_context or {}))

        if not _is_empty(compact_input_data):
            payload["input_data"] = compact_input_data
        if not _is_empty(compact_context_data):
            payload["context_data"] = compact_context_data
        if not _is_empty(compact_student_state):
            payload["student_state"] = compact_student_state
        if not _is_empty(compact_graph_context):
            payload["graph_context"] = compact_graph_context
        if not _is_empty(compact_sql_context):
            payload["sql_context"] = compact_sql_context
        if not _is_empty(compact_extra_context):
            payload["extra_context"] = compact_extra_context

        return payload
