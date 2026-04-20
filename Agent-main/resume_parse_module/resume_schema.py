"""
resume_schema.py — 简历解析结果的数据契约

本模块用 dataclass 描述「模型应返回 / 下游应消费」的结构，并通过
default_resume_parse_result_dict() 提供与 JSON 互通的空模板。

与 resume_parser.validate_resume_parse_result 配合：校验层按同名字段补全缺省，
保证流水线输出键稳定，便于入库与岗位匹配等后续步骤。
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List


@dataclass
class ResumeBasicInfo:
    """简历头部常见个人信息；字段均为字符串便于兼容「2026 届」等非纯数字表达。"""

    name: str = ""
    gender: str = ""
    phone: str = ""
    email: str = ""
    school: str = ""
    school_level: str = ""
    major: str = ""
    degree: str = ""
    graduation_year: str = ""


@dataclass
class EducationExperienceItem:
    """单条教育经历；日期字段保留原始字符串以兼容「至今」「在读」等写法。"""

    school: str = ""
    school_level: str = ""
    major: str = ""
    degree: str = ""
    start_date: str = ""
    end_date: str = ""
    description: str = ""


@dataclass
class InternshipExperienceItem:
    """实习/工作条目；与项目经历分列，便于匹配不同 JD 权重。"""

    company_name: str = ""
    position: str = ""
    start_date: str = ""
    end_date: str = ""
    description: str = ""


@dataclass
class ProjectExperienceItem:
    """项目经历条目；role 对应职责角色，description 可存 bullet 合并文本。"""

    project_name: str = ""
    role: str = ""
    start_date: str = ""
    end_date: str = ""
    description: str = ""


@dataclass
class ResumeParseResult:
    """
    简历解析模块的标准输出结构（与 call_llm resume_parse 任务对齐）。

    raw_resume_text：清洗后全文留存，便于人工复核；parse_warnings：解析质量与异常说明。
    """

    basic_info: ResumeBasicInfo = field(default_factory=ResumeBasicInfo)
    education_experience: List[EducationExperienceItem] = field(default_factory=list)
    internship_experience: List[InternshipExperienceItem] = field(default_factory=list)
    project_experience: List[ProjectExperienceItem] = field(default_factory=list)
    skills: List[str] = field(default_factory=list)
    certificates: List[str] = field(default_factory=list)
    awards: List[str] = field(default_factory=list)
    self_evaluation: str = ""
    target_job_intention: str = ""
    raw_resume_text: str = ""
    parse_warnings: List[str] = field(default_factory=list)


def default_resume_parse_result_dict() -> Dict[str, Any]:
    """
    返回标准简历解析结果的默认 dict（嵌套 dict/list，非 dataclass 实例）。

    用途：LLM 失败时的 fallback、extra_context["schema_hint"]、单测基线。
    """
    return asdict(ResumeParseResult())
