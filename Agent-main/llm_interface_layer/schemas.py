"""
schemas.py

定义任务类型、输入输出数据结构、student.json 推荐结构、
以及 task_type 到 state 字段的映射关系（含 job_extract / job_dedup）。
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Type


class TaskType(str, Enum):
    """系统支持的大模型任务类型（含岗位数据流水线）。"""

    RESUME_PARSE = "resume_parse"
    NON_CS_FILTER = "non_cs_filter"
    JOB_EXTRACT = "job_extract"
    JOB_DEDUP = "job_dedup"
    JOB_PROFILE = "job_profile"
    STUDENT_PROFILE = "student_profile"
    JOB_MATCH = "job_match"
    CAREER_PATH_PLAN = "career_path_plan"
    CAREER_REPORT = "career_report"

    @classmethod
    def normalize(cls, task_type: "TaskType | str") -> "TaskType":
        """将字符串或枚举统一转换为 TaskType。"""
        if isinstance(task_type, TaskType):
            return task_type
        try:
            return TaskType(str(task_type).strip())
        except ValueError as exc:
            supported = ", ".join(item.value for item in TaskType)
            raise ValueError(
                f"Unsupported task_type: {task_type}. Supported task types: {supported}"
            ) from exc


TASK_RESULT_FIELD_MAP: Dict[TaskType, str] = {
    TaskType.RESUME_PARSE: "resume_parse_result",
    TaskType.NON_CS_FILTER: "non_cs_filter_result",
    TaskType.JOB_EXTRACT: "job_extract_result",
    TaskType.JOB_DEDUP: "job_dedup_result",
    TaskType.JOB_PROFILE: "job_profile_result",
    TaskType.STUDENT_PROFILE: "student_profile_result",
    TaskType.JOB_MATCH: "job_match_result",
    TaskType.CAREER_PATH_PLAN: "career_path_plan_result",
    TaskType.CAREER_REPORT: "career_report_result",
}


@dataclass
class ResumeParseInput:
    resume_text: str = ""


@dataclass
class NonCSFilterInput:
    job_title: str = ""
    industry: str = ""
    job_description: str = ""


@dataclass
class NonCSFilterOutput:
    is_cs_related: bool = False
    confidence: float = 0.0
    reason: str = ""


@dataclass
class JobExtractInput:
    """岗位画像抽取输入（与 job_data.job_extract.build_extraction_input 字段对齐）。"""

    job_name: str = ""
    standard_job_name: str = ""
    industry: str = ""
    company_name: str = ""
    company_type: str = ""
    company_size: str = ""
    city: str = ""
    salary_raw: str = ""
    job_desc: str = ""
    company_desc: str = ""


@dataclass
class JobExtractOutput:
    """岗位画像抽取输出模板（与 DEFAULT_JOB_PROFILE 一致，供 prompt / mock 使用）。"""

    standard_job_name: str = ""
    job_category: str = ""
    degree_requirement: str = ""
    major_requirement: str = ""
    experience_requirement: str = ""
    hard_skills: List[str] = field(default_factory=list)
    tools_or_tech_stack: List[str] = field(default_factory=list)
    certificate_requirement: List[str] = field(default_factory=list)
    soft_skills: List[str] = field(default_factory=list)
    practice_requirement: str = ""
    job_level: str = ""
    suitable_student_profile: str = ""
    raw_requirement_summary: str = ""
    vertical_paths: List[str] = field(default_factory=list)
    transfer_paths: List[str] = field(default_factory=list)
    path_relation_details: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class JobDedupInput:
    """成对岗位去重/标准化 LLM 输入占位；实际载荷由业务在 input_data 中传入。"""

    titles: List[str] = field(default_factory=list)
    records: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class JobDedupOutput:
    """去重判断：可直接给 bool，或通过 mappings / duplicate_groups 推断。"""

    is_same_standard_job: bool = False
    standard_job_name: str = ""
    confidence: float = 0.0
    merge_reason: str = ""
    mappings: List[Dict[str, Any]] = field(default_factory=list)
    duplicate_groups: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ResumeParseOutput:
    name: str = ""
    gender: str = ""
    phone: str = ""
    email: str = ""
    school: str = ""
    major: str = ""
    degree: str = ""
    graduation_year: str = ""
    skills: List[str] = field(default_factory=list)
    certificates: List[str] = field(default_factory=list)
    project_experience: List[Dict[str, Any]] = field(default_factory=list)
    internship_experience: List[Dict[str, Any]] = field(default_factory=list)
    raw_summary: str = ""


@dataclass
class JobProfileInput:
    target_job_name: str = ""
    job_context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class JobProfileOutput:
    standard_job_name: str = ""
    job_category: str = ""
    required_degree: str = ""
    preferred_majors: List[str] = field(default_factory=list)
    required_skills: List[str] = field(default_factory=list)
    required_certificates: List[str] = field(default_factory=list)
    soft_skills: List[str] = field(default_factory=list)
    vertical_paths: List[str] = field(default_factory=list)
    transfer_paths: List[str] = field(default_factory=list)
    job_summary: str = ""


@dataclass
class StudentProfileInput:
    student_json: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StudentProfileOutput:
    skill_profile: Dict[str, Any] = field(default_factory=dict)
    certificate_profile: List[str] = field(default_factory=list)
    soft_skill_profile: Dict[str, Any] = field(default_factory=dict)
    complete_score: float = 0.0
    competitiveness_score: float = 0.0
    strengths: List[str] = field(default_factory=list)
    weaknesses: List[str] = field(default_factory=list)
    summary: str = ""


@dataclass
class JobMatchInput:
    job_profile: Dict[str, Any] = field(default_factory=dict)
    student_profile: Dict[str, Any] = field(default_factory=dict)


@dataclass
class JobMatchOutput:
    overall_score: float = 0.0
    basic_requirement_score: float = 0.0
    skill_score: float = 0.0
    professional_quality_score: float = 0.0
    growth_potential_score: float = 0.0
    strengths: List[str] = field(default_factory=list)
    gaps: List[str] = field(default_factory=list)
    improvement_suggestions: List[str] = field(default_factory=list)
    summary: str = ""


@dataclass
class CareerPathPlanInput:
    student_profile_result: Dict[str, Any] = field(default_factory=dict)
    job_profile_result: Dict[str, Any] = field(default_factory=dict)
    job_match_result: Dict[str, Any] = field(default_factory=dict)
    graph_context: Dict[str, Any] = field(default_factory=dict)
    sql_context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CareerPathPlanOutput:
    primary_target_job: str = ""
    backup_target_jobs: List[str] = field(default_factory=list)
    direct_path: List[str] = field(default_factory=list)
    transition_path: List[str] = field(default_factory=list)
    short_term_plan: List[str] = field(default_factory=list)
    mid_term_plan: List[str] = field(default_factory=list)
    risk_notes: List[str] = field(default_factory=list)
    summary: str = ""


@dataclass
class CareerReportInput:
    all_task_results: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CareerReportOutput:
    report_title: str = "大学生职业规划报告"
    target_job: str = ""
    match_summary: str = ""
    path_summary: str = ""
    action_summary: str = ""
    report_text: str = ""
    report_sections: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StudentState:
    """student.json 推荐结构。"""

    basic_info: Dict[str, Any] = field(default_factory=dict)
    resume_parse_result: Dict[str, Any] = field(default_factory=dict)
    job_extract_result: Dict[str, Any] = field(default_factory=dict)
    job_dedup_result: Dict[str, Any] = field(default_factory=dict)
    job_profile_result: Dict[str, Any] = field(default_factory=dict)
    student_profile_result: Dict[str, Any] = field(default_factory=dict)
    job_match_result: Dict[str, Any] = field(default_factory=dict)
    career_path_plan_result: Dict[str, Any] = field(default_factory=dict)
    career_report_result: Dict[str, Any] = field(default_factory=dict)


TASK_INPUT_SCHEMA_MAP: Dict[TaskType, Type[Any]] = {
    TaskType.RESUME_PARSE: ResumeParseInput,
    TaskType.NON_CS_FILTER: NonCSFilterInput,
    TaskType.JOB_EXTRACT: JobExtractInput,
    TaskType.JOB_DEDUP: JobDedupInput,
    TaskType.JOB_PROFILE: JobProfileInput,
    TaskType.STUDENT_PROFILE: StudentProfileInput,
    TaskType.JOB_MATCH: JobMatchInput,
    TaskType.CAREER_PATH_PLAN: CareerPathPlanInput,
    TaskType.CAREER_REPORT: CareerReportInput,
}

TASK_OUTPUT_SCHEMA_MAP: Dict[TaskType, Type[Any]] = {
    TaskType.RESUME_PARSE: ResumeParseOutput,
    TaskType.NON_CS_FILTER: NonCSFilterOutput,
    TaskType.JOB_EXTRACT: JobExtractOutput,
    TaskType.JOB_DEDUP: JobDedupOutput,
    TaskType.JOB_PROFILE: JobProfileOutput,
    TaskType.STUDENT_PROFILE: StudentProfileOutput,
    TaskType.JOB_MATCH: JobMatchOutput,
    TaskType.CAREER_PATH_PLAN: CareerPathPlanOutput,
    TaskType.CAREER_REPORT: CareerReportOutput,
}


def dataclass_to_dict(data_obj: Any) -> Dict[str, Any]:
    """dataclass -> dict，若已是 dict 则直接返回。"""
    if is_dataclass(data_obj):
        return asdict(data_obj)
    if isinstance(data_obj, dict):
        return data_obj
    return {}


def get_default_output_dict(task_type: "TaskType | str") -> Dict[str, Any]:
    """获取某个任务的默认输出结构。"""
    normalized_task = TaskType.normalize(task_type)
    schema_cls = TASK_OUTPUT_SCHEMA_MAP[normalized_task]
    return asdict(schema_cls())


def build_empty_student_state() -> Dict[str, Any]:
    """生成空 student.json 状态。"""
    return asdict(StudentState())

