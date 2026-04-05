"""
job_match_builder.py

人岗匹配模块的 builder 层。

职责边界：
1. 读取 student_profile_result 和 job_profile_result；
2. 构造统一的可比字段结构；
3. 对缺失字段做默认值补齐；
4. 对列表/文本/分数字段做统一格式标准化；
5. 输出适合传给大模型或后续规则匹配模块的 match_input_payload。

说明：
- 本文件不重写 student_profile 和 job_profile 的业务逻辑；
- 只消费它们已经生成的结构化结果；
- 输出重点是“人”和“岗”两侧字段对齐，方便后续 job_match 服务层直接使用。
"""

from __future__ import annotations

import argparse
import json
import re
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_STATE_PATH = Path("outputs/state/student.json")
DEFAULT_OUTPUT_PATH = Path("outputs/state/job_match_input_payload.json")


DEGREE_RANK_MAP: Dict[str, int] = {
    "": 0,
    "学历不限": 0,
    "高中/中专": 1,
    "高中": 1,
    "中专": 1,
    "大专": 2,
    "专科": 2,
    "本科": 3,
    "硕士": 4,
    "研究生": 4,
    "博士": 5,
}


DEGREE_ALIAS_PATTERNS: List[Tuple[str, str]] = [
    (r"(博士|phd)", "博士"),
    (r"(硕士|研究生|master|mba)", "硕士"),
    (r"(本科|学士|bachelor)", "本科"),
    (r"(大专|专科|高职|college)", "大专"),
    (r"(高中|中专)", "高中/中专"),
    (r"(学历不限|不限学历|无学历要求)", "学历不限"),
]


@dataclass
class StudentComparableProfile:
    """学生侧可比字段结构。"""

    degree: str = ""
    degree_rank: int = 0
    school: str = ""
    major: str = ""
    hard_skills: List[str] = field(default_factory=list)
    tool_skills: List[str] = field(default_factory=list)
    soft_skills: List[str] = field(default_factory=list)
    certificates: List[str] = field(default_factory=list)
    experience_tags: List[str] = field(default_factory=list)
    project_count: int = 0
    internship_count: int = 0
    occupation_hints: List[str] = field(default_factory=list)
    domain_tags: List[str] = field(default_factory=list)
    complete_score: float = 0.0
    competitiveness_score: float = 0.0
    score_level: str = ""
    strengths: List[str] = field(default_factory=list)
    weaknesses: List[str] = field(default_factory=list)
    missing_dimensions: List[str] = field(default_factory=list)
    summary: str = ""
    raw_student_profile_result: Dict[str, Any] = field(default_factory=dict)


@dataclass
class JobComparableProfile:
    """岗位侧可比字段结构。"""

    standard_job_name: str = ""
    job_category: str = ""
    job_level: str = ""
    degree_requirement: str = ""
    degree_rank_requirement: int = 0
    major_requirement: List[str] = field(default_factory=list)
    experience_requirement: List[str] = field(default_factory=list)
    hard_skills: List[str] = field(default_factory=list)
    tool_skills: List[str] = field(default_factory=list)
    certificate_requirement: List[str] = field(default_factory=list)
    practice_requirement: List[str] = field(default_factory=list)
    soft_skills: List[str] = field(default_factory=list)
    suitable_student_profile: str = ""
    summary: str = ""
    vertical_paths: List[str] = field(default_factory=list)
    transfer_paths: List[str] = field(default_factory=list)
    skill_frequency: List[Dict[str, Any]] = field(default_factory=list)
    industry_distribution: List[Dict[str, Any]] = field(default_factory=list)
    city_distribution: List[Dict[str, Any]] = field(default_factory=list)
    salary_stats: Dict[str, Any] = field(default_factory=dict)
    raw_job_profile_result: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MatchComparableSchema:
    """统一可比维度结构，便于后续 job_match 模块直接消费。"""

    education: Dict[str, Any] = field(default_factory=dict)
    major: Dict[str, Any] = field(default_factory=dict)
    hard_skills: Dict[str, Any] = field(default_factory=dict)
    tool_skills: Dict[str, Any] = field(default_factory=dict)
    soft_skills: Dict[str, Any] = field(default_factory=dict)
    certificates: Dict[str, Any] = field(default_factory=dict)
    practice_experience: Dict[str, Any] = field(default_factory=dict)
    career_direction: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MatchInputPayload:
    """最终输出给 job_match 模块的输入 payload。"""

    student_profile: Dict[str, Any] = field(default_factory=dict)
    job_profile: Dict[str, Any] = field(default_factory=dict)
    comparable_schema: Dict[str, Any] = field(default_factory=dict)
    matching_guidance: Dict[str, Any] = field(default_factory=dict)
    build_warnings: List[str] = field(default_factory=list)


def clean_text(value: Any) -> str:
    """基础文本清洗。"""
    if value is None:
        return ""
    text = str(value).replace("\u00a0", " ").replace("\u3000", " ").strip()
    text = re.sub(r"\s+", " ", text)
    if text.lower() in {"", "nan", "none", "null", "n/a", "na", "-"}:
        return ""
    return text


def safe_dict(value: Any) -> Dict[str, Any]:
    """安全转 dict。"""
    return value if isinstance(value, dict) else {}


def safe_float(value: Any, default: float = 0.0) -> float:
    """安全转 float。"""
    text = clean_text(value)
    if not text:
        return default
    try:
        return float(text)
    except (TypeError, ValueError):
        return default


def dedup_keep_order(values: Iterable[Any]) -> List[Any]:
    """对标量或可 JSON 序列化对象做稳定去重。"""
    seen = set()
    result = []
    for value in values:
        if value is None or value == "":
            continue
        key = json.dumps(value, ensure_ascii=False, sort_keys=True) if isinstance(value, (dict, list)) else str(value)
        if key not in seen:
            seen.add(key)
            result.append(value)
    return result


def parse_list_like(value: Any) -> List[Any]:
    """
    将 list / JSON 字符串 / 普通分隔符字符串 统一转成 list。

    示例：
    - ["Python", "SQL"]
    - '["Python", "SQL"]'
    - "Python、SQL、Excel"
    """
    if isinstance(value, list):
        return dedup_keep_order(value)

    text = clean_text(value)
    if not text:
        return []

    if text.startswith("[") and text.endswith("]"):
        try:
            loaded = json.loads(text)
            if isinstance(loaded, list):
                return dedup_keep_order(loaded)
        except json.JSONDecodeError:
            pass

    parts = [clean_text(part) for part in re.split(r"[、,，;；/|｜\n]+", text) if clean_text(part)]
    return dedup_keep_order(parts)


def normalize_tag_list(value: Any) -> List[str]:
    """统一标签列表格式，并去掉空值。"""
    return dedup_keep_order(clean_text(item) for item in parse_list_like(value) if clean_text(item))


def normalize_degree_text(value: Any) -> str:
    """将学历文本归一到标准层级标签。"""
    text = clean_text(value)
    if not text:
        return ""
    for pattern, normalized in DEGREE_ALIAS_PATTERNS:
        if re.search(pattern, text, flags=re.IGNORECASE):
            return normalized
    return text


def degree_to_rank(value: Any) -> int:
    """将学历文本映射为可比较等级。"""
    degree = normalize_degree_text(value)
    return int(DEGREE_RANK_MAP.get(degree, 0))


def extract_skill_names_from_skill_profile(skill_profile: Dict[str, Any]) -> List[str]:
    """
    从 student_profile_result.skill_profile 中提取技能名称。

    支持两种常见格式：
    1. {"Python": "熟悉", "SQL": "掌握"}
    2. {"hard_skills": [...], "tool_skills": [...]}
    """
    if not isinstance(skill_profile, dict):
        return []

    if "hard_skills" in skill_profile or "tool_skills" in skill_profile:
        return normalize_tag_list(skill_profile.get("hard_skills")) + normalize_tag_list(
            skill_profile.get("tool_skills")
        )

    return normalize_tag_list(skill_profile.keys())


def infer_project_and_internship_count(student_profile_result: Dict[str, Any]) -> Tuple[int, int]:
    """从 profile_input_payload 或 ability_evidence 中推断项目/实习数量。"""
    payload = safe_dict(student_profile_result.get("profile_input_payload"))
    explicit_profile = safe_dict(payload.get("explicit_profile"))
    ability_evidence = safe_dict(student_profile_result.get("ability_evidence"))

    project_count = len(parse_list_like(explicit_profile.get("project_experience")))
    if project_count <= 0:
        project_count = len(parse_list_like(ability_evidence.get("project_examples")))

    internship_count = len(parse_list_like(explicit_profile.get("internship_experience")))
    if internship_count <= 0:
        internship_count = len(parse_list_like(ability_evidence.get("internship_examples")))

    return project_count, internship_count


def normalize_student_profile_result(student_profile_result: Dict[str, Any]) -> Dict[str, Any]:
    """将 student_profile_result 标准化为学生侧可比结构。"""
    source = safe_dict(student_profile_result)
    payload = safe_dict(source.get("profile_input_payload"))
    basic_info = safe_dict(payload.get("basic_info"))
    normalized_education = safe_dict(payload.get("normalized_education"))
    normalized_profile = safe_dict(payload.get("normalized_profile"))

    hard_skills = normalize_tag_list(normalized_profile.get("hard_skills"))
    if not hard_skills:
        hard_skills = extract_skill_names_from_skill_profile(safe_dict(source.get("skill_profile")))

    tool_skills = normalize_tag_list(normalized_profile.get("tool_skills"))
    if not tool_skills:
        skill_profile = safe_dict(source.get("skill_profile"))
        if "tool_skills" in skill_profile:
            tool_skills = normalize_tag_list(skill_profile.get("tool_skills"))

    certificates = normalize_tag_list(normalized_profile.get("qualification_tags"))
    if not certificates:
        certificates = normalize_tag_list(source.get("certificate_profile"))

    soft_skills = normalize_tag_list(source.get("soft_skills"))
    if not soft_skills:
        soft_skills = normalize_tag_list(safe_dict(source.get("soft_skill_profile")).keys())

    degree = normalize_degree_text(
        normalized_education.get("degree")
        or basic_info.get("degree")
        or source.get("degree")
    )
    major = clean_text(
        normalized_education.get("major_std")
        or normalized_education.get("major")
        or basic_info.get("major")
        or source.get("major")
    )
    school = clean_text(
        normalized_education.get("school")
        or basic_info.get("school")
        or source.get("school")
    )

    project_count, internship_count = infer_project_and_internship_count(source)

    student_profile = StudentComparableProfile(
        degree=degree,
        degree_rank=degree_to_rank(degree),
        school=school,
        major=major,
        hard_skills=hard_skills,
        tool_skills=tool_skills,
        soft_skills=soft_skills,
        certificates=certificates,
        experience_tags=normalize_tag_list(normalized_profile.get("experience_tags")),
        project_count=project_count,
        internship_count=internship_count,
        occupation_hints=normalize_tag_list(normalized_profile.get("occupation_hints")),
        domain_tags=normalize_tag_list(normalized_profile.get("domain_tags")),
        complete_score=safe_float(source.get("complete_score"), default=0.0),
        competitiveness_score=safe_float(source.get("competitiveness_score"), default=0.0),
        score_level=clean_text(source.get("score_level")),
        strengths=normalize_tag_list(source.get("strengths")),
        weaknesses=normalize_tag_list(source.get("weaknesses")),
        missing_dimensions=normalize_tag_list(source.get("missing_dimensions")),
        summary=clean_text(source.get("summary")),
        raw_student_profile_result=deepcopy(source),
    )
    return asdict(student_profile)


def infer_required_degree_from_distribution(job_profile_result: Dict[str, Any]) -> str:
    """当 degree_requirement 缺失时，从聚合分布中兜底推断。"""
    degree_dist = parse_list_like(job_profile_result.get("degree_requirement_distribution"))
    for item in degree_dist:
        item_dict = safe_dict(item)
        degree_name = clean_text(item_dict.get("name"))
        if degree_name and degree_name != "未明确":
            return normalize_degree_text(degree_name)
    return ""


def infer_hard_skills_from_skill_frequency(job_profile_result: Dict[str, Any], top_n: int = 15) -> List[str]:
    """当 hard_skills 缺失时，从 skill_frequency 中兜底提取技能名。"""
    skills = []
    for item in parse_list_like(job_profile_result.get("skill_frequency"))[:top_n]:
        item_dict = safe_dict(item)
        skill_name = clean_text(item_dict.get("name"))
        if skill_name:
            skills.append(skill_name)
    return dedup_keep_order(skills)


def normalize_job_profile_result(job_profile_result: Dict[str, Any]) -> Dict[str, Any]:
    """将 job_profile_result 标准化为岗位侧可比结构。"""
    source = safe_dict(job_profile_result)
    normalized_requirements = safe_dict(source.get("normalized_requirements"))

    hard_skills = normalize_tag_list(source.get("hard_skills"))
    if not hard_skills:
        hard_skills = normalize_tag_list(normalized_requirements.get("hard_skill_tags"))
    if not hard_skills:
        hard_skills = infer_hard_skills_from_skill_frequency(source)

    tool_skills = normalize_tag_list(source.get("tools_or_tech_stack"))
    if not tool_skills:
        tool_skills = normalize_tag_list(normalized_requirements.get("tool_skill_tags"))

    certificate_requirement = normalize_tag_list(source.get("certificate_requirement"))
    if not certificate_requirement:
        certificate_requirement = normalize_tag_list(normalized_requirements.get("certificate_tags"))

    major_requirement = normalize_tag_list(source.get("major_requirement"))
    if not major_requirement:
        major_requirement = normalize_tag_list(normalized_requirements.get("major_tags"))

    experience_requirement = normalize_tag_list(source.get("experience_requirement"))
    if not experience_requirement:
        experience_requirement = normalize_tag_list(normalized_requirements.get("experience_tags"))

    practice_requirement = normalize_tag_list(source.get("practice_requirement"))
    if not practice_requirement:
        practice_requirement = normalize_tag_list(normalized_requirements.get("practice_tags"))

    soft_skills = normalize_tag_list(source.get("soft_skills"))
    if not soft_skills:
        soft_skills = normalize_tag_list(normalized_requirements.get("soft_skill_tags"))

    degree_requirement = normalize_degree_text(source.get("degree_requirement"))
    if not degree_requirement:
        degree_requirement = normalize_degree_text(
            " / ".join(normalize_tag_list(normalized_requirements.get("degree_tags")))
        )
    if not degree_requirement:
        degree_requirement = infer_required_degree_from_distribution(source)

    job_profile = JobComparableProfile(
        standard_job_name=clean_text(source.get("standard_job_name")),
        job_category=clean_text(source.get("job_category")),
        job_level=clean_text(source.get("job_level")),
        degree_requirement=degree_requirement,
        degree_rank_requirement=degree_to_rank(degree_requirement),
        major_requirement=major_requirement,
        experience_requirement=experience_requirement,
        hard_skills=hard_skills,
        tool_skills=tool_skills,
        certificate_requirement=certificate_requirement,
        practice_requirement=practice_requirement,
        soft_skills=soft_skills,
        suitable_student_profile=clean_text(source.get("suitable_student_profile")),
        summary=clean_text(source.get("summary")),
        vertical_paths=normalize_tag_list(source.get("vertical_paths")),
        transfer_paths=normalize_tag_list(source.get("transfer_paths")),
        skill_frequency=deepcopy(parse_list_like(source.get("skill_frequency"))),
        industry_distribution=deepcopy(parse_list_like(source.get("industry_distribution"))),
        city_distribution=deepcopy(parse_list_like(source.get("city_distribution"))),
        salary_stats=deepcopy(safe_dict(source.get("salary_stats"))),
        raw_job_profile_result=deepcopy(source),
    )
    return asdict(job_profile)


def build_comparable_schema(
    student_profile: Dict[str, Any],
    job_profile: Dict[str, Any],
) -> Dict[str, Any]:
    """构造“学生-岗位”统一可比维度结构。"""
    comparable = MatchComparableSchema(
        education={
            "student_degree": clean_text(student_profile.get("degree")),
            "student_degree_rank": int(student_profile.get("degree_rank") or 0),
            "job_degree_requirement": clean_text(job_profile.get("degree_requirement")),
            "job_degree_rank_requirement": int(job_profile.get("degree_rank_requirement") or 0),
        },
        major={
            "student_major": clean_text(student_profile.get("major")),
            "job_major_requirement": normalize_tag_list(job_profile.get("major_requirement")),
        },
        hard_skills={
            "student_hard_skills": normalize_tag_list(student_profile.get("hard_skills")),
            "job_hard_skills": normalize_tag_list(job_profile.get("hard_skills")),
        },
        tool_skills={
            "student_tool_skills": normalize_tag_list(student_profile.get("tool_skills")),
            "job_tool_skills": normalize_tag_list(job_profile.get("tool_skills")),
        },
        soft_skills={
            "student_soft_skills": normalize_tag_list(student_profile.get("soft_skills")),
            "job_soft_skills": normalize_tag_list(job_profile.get("soft_skills")),
        },
        certificates={
            "student_certificates": normalize_tag_list(student_profile.get("certificates")),
            "job_certificate_requirement": normalize_tag_list(job_profile.get("certificate_requirement")),
        },
        practice_experience={
            "student_experience_tags": normalize_tag_list(student_profile.get("experience_tags")),
            "student_project_count": int(student_profile.get("project_count") or 0),
            "student_internship_count": int(student_profile.get("internship_count") or 0),
            "job_experience_requirement": normalize_tag_list(job_profile.get("experience_requirement")),
            "job_practice_requirement": normalize_tag_list(job_profile.get("practice_requirement")),
        },
        career_direction={
            "student_occupation_hints": normalize_tag_list(student_profile.get("occupation_hints")),
            "student_domain_tags": normalize_tag_list(student_profile.get("domain_tags")),
            "job_name": clean_text(job_profile.get("standard_job_name")),
            "job_category": clean_text(job_profile.get("job_category")),
        },
    )
    return asdict(comparable)


def build_matching_guidance(
    student_profile: Dict[str, Any],
    job_profile: Dict[str, Any],
) -> Dict[str, Any]:
    """补充后续 job_match 任务可直接使用的解释性上下文。"""
    return {
        "student_summary": clean_text(student_profile.get("summary")),
        "student_strengths": normalize_tag_list(student_profile.get("strengths")),
        "student_weaknesses": normalize_tag_list(student_profile.get("weaknesses")),
        "student_missing_dimensions": normalize_tag_list(student_profile.get("missing_dimensions")),
        "student_complete_score": safe_float(student_profile.get("complete_score"), default=0.0),
        "student_competitiveness_score": safe_float(student_profile.get("competitiveness_score"), default=0.0),
        "student_score_level": clean_text(student_profile.get("score_level")),
        "job_summary": clean_text(job_profile.get("summary")),
        "suitable_student_profile": clean_text(job_profile.get("suitable_student_profile")),
        "job_level": clean_text(job_profile.get("job_level")),
        "vertical_paths": normalize_tag_list(job_profile.get("vertical_paths")),
        "transfer_paths": normalize_tag_list(job_profile.get("transfer_paths")),
        "salary_stats": deepcopy(safe_dict(job_profile.get("salary_stats"))),
        "industry_distribution": deepcopy(parse_list_like(job_profile.get("industry_distribution"))),
        "city_distribution": deepcopy(parse_list_like(job_profile.get("city_distribution"))),
        "dimension_weights_hint": {
            "education": 0.15,
            "major": 0.10,
            "hard_skills": 0.30,
            "tool_skills": 0.15,
            "soft_skills": 0.10,
            "certificates": 0.05,
            "practice_experience": 0.10,
            "career_direction": 0.05,
        },
    }


def build_match_warnings(
    student_profile: Dict[str, Any],
    job_profile: Dict[str, Any],
) -> List[str]:
    """生成 builder 层质量提示。"""
    warnings = []

    if not clean_text(student_profile.get("degree")):
        warnings.append("student_profile_result 缺少学历字段")
    if not clean_text(student_profile.get("major")):
        warnings.append("student_profile_result 缺少专业字段")
    if not normalize_tag_list(student_profile.get("hard_skills")) and not normalize_tag_list(
        student_profile.get("tool_skills")
    ):
        warnings.append("student_profile_result 缺少可用于匹配的技能/工具标签")
    if not normalize_tag_list(student_profile.get("occupation_hints")) and not normalize_tag_list(
        student_profile.get("domain_tags")
    ):
        warnings.append("student_profile_result 缺少职业方向或领域标签")

    if not clean_text(job_profile.get("standard_job_name")):
        warnings.append("job_profile_result 缺少 standard_job_name")
    if not clean_text(job_profile.get("degree_requirement")):
        warnings.append("job_profile_result 缺少学历要求字段")
    if not normalize_tag_list(job_profile.get("hard_skills")) and not normalize_tag_list(
        job_profile.get("tool_skills")
    ):
        warnings.append("job_profile_result 缺少岗位技能/工具要求")
    if not normalize_tag_list(job_profile.get("major_requirement")):
        warnings.append("job_profile_result 缺少专业要求标签")

    return dedup_keep_order(clean_text(item) for item in warnings if clean_text(item))


def build_match_input_payload(
    student_profile_result: Dict[str, Any],
    job_profile_result: Dict[str, Any],
    output_path: Optional[str | Path] = None,
) -> Dict[str, Any]:
    """
    主入口：从 student_profile_result 和 job_profile_result 构造 match_input_payload。
    """
    student_profile = normalize_student_profile_result(student_profile_result)
    job_profile = normalize_job_profile_result(job_profile_result)
    comparable_schema = build_comparable_schema(student_profile, job_profile)
    matching_guidance = build_matching_guidance(student_profile, job_profile)
    build_warnings = build_match_warnings(student_profile, job_profile)

    payload = MatchInputPayload(
        student_profile=student_profile,
        job_profile=job_profile,
        comparable_schema=comparable_schema,
        matching_guidance=matching_guidance,
        build_warnings=build_warnings,
    )
    payload_dict = asdict(payload)

    if output_path:
        save_json(payload_dict, output_path)
    return payload_dict


def load_student_and_job_profile_from_state(
    state_path: str | Path = DEFAULT_STATE_PATH,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """从 student.json 中读取 student_profile_result 和 job_profile_result。"""
    path = Path(state_path)
    if not path.exists():
        raise FileNotFoundError(f"state file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        state_data = json.load(f)

    if not isinstance(state_data, dict):
        raise ValueError("state file content must be a JSON object")

    return (
        safe_dict(state_data.get("student_profile_result")),
        safe_dict(state_data.get("job_profile_result")),
    )


def build_match_input_payload_from_state(
    state_path: str | Path = DEFAULT_STATE_PATH,
    output_path: Optional[str | Path] = DEFAULT_OUTPUT_PATH,
) -> Dict[str, Any]:
    """从 student.json 读取两个画像结果，并构造 match_input_payload。"""
    student_profile_result, job_profile_result = load_student_and_job_profile_from_state(state_path)
    return build_match_input_payload(
        student_profile_result=student_profile_result,
        job_profile_result=job_profile_result,
        output_path=output_path,
    )


def save_json(data: Dict[str, Any], output_path: str | Path) -> None:
    """保存 JSON 输出。"""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def build_demo_student_profile_result() -> Dict[str, Any]:
    """构造可直接运行的 mock student_profile_result。"""
    return {
        "skill_profile": {
            "Python": "熟悉",
            "SQL": "熟悉",
            "机器学习": "入门",
        },
        "certificate_profile": ["CET-6"],
        "soft_skills": ["沟通能力", "学习能力", "责任心"],
        "complete_score": 82.0,
        "competitiveness_score": 76.0,
        "score_level": "B-具备一定竞争力",
        "strengths": ["具备 Python/SQL 基础", "有项目和实习经历"],
        "weaknesses": ["业务分析经验仍需补强"],
        "missing_dimensions": ["高阶分析方法", "行业项目深度"],
        "summary": "候选人具备数据分析方向基础技能和一定实践经历，适合投递初级数据分析岗位。",
        "profile_input_payload": {
            "basic_info": {
                "name": "张三",
                "school": "某某大学",
                "major": "计算机科学与技术",
                "degree": "本科",
            },
            "normalized_education": {
                "degree": "本科",
                "school": "某某大学",
                "major_std": "计算机科学与技术",
            },
            "normalized_profile": {
                "hard_skills": ["Python", "SQL", "机器学习"],
                "tool_skills": ["Excel", "Tableau"],
                "qualification_tags": ["CET-6"],
                "experience_tags": ["项目:数据分析", "实习:报表分析", "有项目经历", "有实习经历"],
                "occupation_hints": ["数据分析"],
                "domain_tags": ["数据智能", "商业分析", "互联网"],
            },
            "explicit_profile": {
                "project_experience": [{"project_name": "用户行为分析项目"}],
                "internship_experience": [{"company_name": "某科技公司", "position": "数据分析实习生"}],
            },
        },
    }


def build_demo_job_profile_result() -> Dict[str, Any]:
    """构造可直接运行的 mock job_profile_result。"""
    return {
        "standard_job_name": "数据分析师",
        "job_category": "数据类",
        "job_level": "初级",
        "degree_requirement": "本科",
        "major_requirement": ["统计学", "计算机科学与技术", "数据科学与大数据技术"],
        "experience_requirement": ["经验不限/应届可投", "有项目经验", "有实习/实践经验"],
        "hard_skills": ["SQL", "Python", "数据分析", "A/B测试"],
        "tools_or_tech_stack": ["Excel", "Tableau", "Power BI"],
        "certificate_requirement": ["CET-6"],
        "practice_requirement": ["项目要求", "实习要求", "跨部门协作要求"],
        "soft_skills": ["沟通协作", "逻辑分析", "学习能力"],
        "suitable_student_profile": "适合统计、计算机、数据科学相关专业，掌握 SQL/Python/Excel 并有数据项目或实习经历的学生。",
        "summary": "该岗位关注数据提取分析、指标体系建设、可视化表达和业务协作能力。",
        "vertical_paths": ["数据分析师 -> 高级数据分析师", "高级数据分析师 -> 数据分析负责人"],
        "transfer_paths": ["数据分析师 -> 商业分析师", "数据分析师 -> 数据产品经理"],
        "skill_frequency": [
            {"name": "SQL", "count": 18, "ratio": 0.9},
            {"name": "Python", "count": 15, "ratio": 0.75},
            {"name": "Excel", "count": 14, "ratio": 0.7},
        ],
        "industry_distribution": [
            {"name": "互联网", "count": 10, "ratio": 0.5},
            {"name": "电子商务", "count": 6, "ratio": 0.3},
        ],
        "city_distribution": [
            {"name": "杭州", "count": 8, "ratio": 0.4},
            {"name": "上海", "count": 7, "ratio": 0.35},
        ],
        "salary_stats": {
            "salary_min_month_avg": 12000,
            "salary_max_month_avg": 18000,
            "salary_mid_month_avg": 15000,
            "valid_salary_count": 20,
        },
    }


def parse_args() -> argparse.Namespace:
    """命令行参数解析。"""
    parser = argparse.ArgumentParser(description="Build job-match input payload from student/job profiles")
    parser.add_argument(
        "--state-path",
        default="",
        help="可选：包含 student_profile_result 和 job_profile_result 的 student.json 路径",
    )
    parser.add_argument(
        "--student-profile-json",
        default="",
        help="可选：单独的 student_profile_result JSON 文件路径",
    )
    parser.add_argument(
        "--job-profile-json",
        default="",
        help="可选：单独的 job_profile_result JSON 文件路径",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_PATH),
        help="match_input_payload 输出路径",
    )
    return parser.parse_args()


def load_json_file(file_path: str | Path) -> Dict[str, Any]:
    """读取 JSON 文件并校验对象结构。"""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"json file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"json file content must be object: {path}")
    return data


if __name__ == "__main__":
    args = parse_args()

    if args.state_path:
        result = build_match_input_payload_from_state(
            state_path=args.state_path,
            output_path=args.output,
        )
    elif args.student_profile_json and args.job_profile_json:
        result = build_match_input_payload(
            student_profile_result=load_json_file(args.student_profile_json),
            job_profile_result=load_json_file(args.job_profile_json),
            output_path=args.output,
        )
    else:
        result = build_match_input_payload(
            student_profile_result=build_demo_student_profile_result(),
            job_profile_result=build_demo_job_profile_result(),
            output_path=args.output,
        )

    print(json.dumps(result, ensure_ascii=False, indent=2))
