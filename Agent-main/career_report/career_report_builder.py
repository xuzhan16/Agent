"""
career_report_builder.py

career_report 模块的 builder 层。

职责：
1. 读取 student_profile_result、job_profile_result、job_match_result、career_path_plan_result；
2. 整理为统一的 report_input_payload；
3. 对缺失字段做默认值补齐；
4. 统一列表、字典、文本、分数字段格式；
5. 输出给 formatter 和 career_report 大模型服务层使用。
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
DEFAULT_OUTPUT_PATH = Path("outputs/state/career_report_input_payload.json")


@dataclass
class StudentReportSnapshot:
    """学生画像报告快照。"""

    name: str = ""
    school: str = ""
    major: str = ""
    degree: str = ""
    graduation_year: str = ""
    hard_skills: List[str] = field(default_factory=list)
    tool_skills: List[str] = field(default_factory=list)
    certificates: List[str] = field(default_factory=list)
    soft_skills: List[str] = field(default_factory=list)
    practice_profile: Dict[str, Any] = field(default_factory=dict)
    potential_profile: Dict[str, Any] = field(default_factory=dict)
    profile_completeness_score: float = 0.0
    competitiveness_score: float = 0.0
    strengths: List[str] = field(default_factory=list)
    weaknesses: List[str] = field(default_factory=list)
    summary: str = ""


@dataclass
class JobReportSnapshot:
    """岗位画像报告快照。"""

    standard_job_name: str = ""
    job_category: str = ""
    job_level: str = ""
    degree_requirement: str = ""
    major_requirement: List[str] = field(default_factory=list)
    hard_skills: List[str] = field(default_factory=list)
    tool_skills: List[str] = field(default_factory=list)
    soft_skills: List[str] = field(default_factory=list)
    practice_requirement: List[str] = field(default_factory=list)
    salary_summary: Dict[str, Any] = field(default_factory=dict)
    city_distribution: List[Dict[str, Any]] = field(default_factory=list)
    industry_distribution: List[Dict[str, Any]] = field(default_factory=list)
    summary: str = ""


@dataclass
class JobMatchReportSnapshot:
    """人岗匹配报告快照。"""

    target_job_name: str = ""
    overall_match_score: float = 0.0
    match_level: str = ""
    dimension_scores: Dict[str, Any] = field(default_factory=dict)
    matched_items: List[Dict[str, Any]] = field(default_factory=list)
    missing_items: List[Dict[str, Any]] = field(default_factory=list)
    strengths: List[str] = field(default_factory=list)
    weaknesses: List[str] = field(default_factory=list)
    improvement_suggestions: List[str] = field(default_factory=list)
    recommendation: str = ""
    analysis_summary: str = ""


@dataclass
class CareerPathPlanReportSnapshot:
    """职业路径规划报告快照。"""

    career_goal: Dict[str, Any] = field(default_factory=dict)
    career_path: Dict[str, Any] = field(default_factory=dict)
    phase_plan: Dict[str, Any] = field(default_factory=dict)
    risk_and_gap: List[str] = field(default_factory=list)
    decision_summary: str = ""


@dataclass
class CareerReportInputPayload:
    """最终传给 formatter / LLM 的统一报告输入 payload。"""

    student_snapshot: Dict[str, Any] = field(default_factory=dict)
    job_snapshot: Dict[str, Any] = field(default_factory=dict)
    job_match_snapshot: Dict[str, Any] = field(default_factory=dict)
    career_path_plan_snapshot: Dict[str, Any] = field(default_factory=dict)
    report_meta: Dict[str, Any] = field(default_factory=dict)
    report_generation_context: Dict[str, Any] = field(default_factory=dict)
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
    """稳定去重。"""
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
    """将 list / JSON 字符串 / 分隔符字符串统一转 list。"""
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


def normalize_text_list(value: Any) -> List[str]:
    """统一文本列表格式。"""
    return dedup_keep_order(clean_text(item) for item in parse_list_like(value) if clean_text(item))


def normalize_dict_list(value: Any) -> List[Dict[str, Any]]:
    """统一 dict 列表格式。"""
    result = []
    for item in parse_list_like(value):
        if isinstance(item, dict):
            result.append(deepcopy(item))
    return dedup_keep_order(result)


def load_json_file(file_path: str | Path) -> Dict[str, Any]:
    """读取 JSON 对象文件。"""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"json file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"json file content must be object: {path}")
    return data


def save_json(data: Dict[str, Any], output_path: str | Path) -> None:
    """保存 JSON 输出。"""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def normalize_student_profile_result(student_profile_result: Dict[str, Any]) -> Dict[str, Any]:
    """将 student_profile_result 归一成报告侧学生快照。"""
    source = safe_dict(student_profile_result)
    profile_payload = safe_dict(source.get("profile_input_payload"))
    basic_info = safe_dict(profile_payload.get("basic_info"))
    normalized_education = safe_dict(profile_payload.get("normalized_education"))
    normalized_profile = safe_dict(profile_payload.get("normalized_profile"))
    explicit_profile = safe_dict(profile_payload.get("explicit_profile"))
    ability_evidence = safe_dict(source.get("ability_evidence"))

    hard_skills = normalize_text_list(source.get("hard_skills") or normalized_profile.get("hard_skills"))
    if not hard_skills:
        hard_skills = normalize_text_list(safe_dict(source.get("skill_profile")).keys())

    snapshot = StudentReportSnapshot(
        name=clean_text(basic_info.get("name")),
        school=clean_text(normalized_education.get("school") or basic_info.get("school")),
        major=clean_text(normalized_education.get("major_std") or normalized_education.get("major") or basic_info.get("major")),
        degree=clean_text(normalized_education.get("degree") or basic_info.get("degree")),
        graduation_year=clean_text(normalized_education.get("graduation_year") or basic_info.get("graduation_year")),
        hard_skills=hard_skills,
        tool_skills=normalize_text_list(source.get("tool_skills") or normalized_profile.get("tool_skills")),
        certificates=normalize_text_list(
            source.get("certificate_list")
            or source.get("certificate_profile")
            or explicit_profile.get("certificates")
            or ability_evidence.get("certificate_tags")
        ),
        soft_skills=normalize_text_list(source.get("soft_skills") or safe_dict(source.get("soft_skill_profile")).keys()),
        practice_profile={
            "project_experience": normalize_dict_list(
                explicit_profile.get("project_experience") or ability_evidence.get("project_examples")
            ),
            "internship_experience": normalize_dict_list(
                explicit_profile.get("internship_experience") or ability_evidence.get("internship_examples")
            ),
            "awards": normalize_text_list(explicit_profile.get("awards")),
            "experience_tags": normalize_text_list(normalized_profile.get("experience_tags")),
        },
        potential_profile=deepcopy(safe_dict(source.get("potential_profile"))),
        profile_completeness_score=safe_float(
            source.get("profile_completeness_score") or source.get("complete_score"),
            default=0.0,
        ),
        competitiveness_score=safe_float(source.get("competitiveness_score"), default=0.0),
        strengths=normalize_text_list(source.get("strengths")),
        weaknesses=normalize_text_list(source.get("weaknesses")),
        summary=clean_text(source.get("summary")),
    )
    return asdict(snapshot)


def normalize_job_profile_result(job_profile_result: Dict[str, Any]) -> Dict[str, Any]:
    """将 job_profile_result 归一成报告侧岗位快照。"""
    source = safe_dict(job_profile_result)
    normalized_requirements = safe_dict(source.get("normalized_requirements"))
    salary_summary = safe_dict(source.get("salary_summary") or source.get("salary_stats"))

    snapshot = JobReportSnapshot(
        standard_job_name=clean_text(source.get("standard_job_name")),
        job_category=clean_text(source.get("job_category")),
        job_level=clean_text(source.get("job_level")),
        degree_requirement=clean_text(source.get("degree_requirement")),
        major_requirement=normalize_text_list(
            source.get("major_requirement") or normalized_requirements.get("major_tags")
        ),
        hard_skills=normalize_text_list(
            source.get("hard_skills") or normalized_requirements.get("hard_skill_tags")
        ),
        tool_skills=normalize_text_list(
            source.get("tool_skills")
            or source.get("tools_or_tech_stack")
            or normalized_requirements.get("tool_skill_tags")
        ),
        soft_skills=normalize_text_list(
            source.get("soft_skills") or normalized_requirements.get("soft_skill_tags")
        ),
        practice_requirement=normalize_text_list(
            source.get("practice_requirement") or normalized_requirements.get("practice_tags")
        ),
        salary_summary=deepcopy(salary_summary),
        city_distribution=normalize_dict_list(source.get("city_distribution")),
        industry_distribution=normalize_dict_list(source.get("industry_distribution")),
        summary=clean_text(source.get("summary")),
    )
    return asdict(snapshot)


def normalize_job_match_result(job_match_result: Dict[str, Any], target_job_name: str = "") -> Dict[str, Any]:
    """将 job_match_result 归一成报告侧人岗匹配快照。"""
    source = safe_dict(job_match_result)
    rule_result = safe_dict(source.get("rule_score_result"))
    raw_payload = safe_dict(source.get("match_input_payload"))
    job_profile = safe_dict(raw_payload.get("job_profile"))
    dimension_details = safe_dict(source.get("dimension_details") or rule_result.get("dimension_details"))

    snapshot = JobMatchReportSnapshot(
        target_job_name=clean_text(
            source.get("target_job_name")
            or job_profile.get("standard_job_name")
            or target_job_name
        ),
        overall_match_score=safe_float(
            source.get("overall_match_score") or rule_result.get("overall_match_score"),
            default=0.0,
        ),
        match_level=clean_text(source.get("match_level") or source.get("score_level") or rule_result.get("score_level")),
        dimension_scores={
            "basic_requirement_score": safe_float(
                source.get("basic_requirement_score") or rule_result.get("basic_requirement_score"),
                default=0.0,
            ),
            "vocational_skill_score": safe_float(
                source.get("vocational_skill_score") or rule_result.get("vocational_skill_score"),
                default=0.0,
            ),
            "professional_quality_score": safe_float(
                source.get("professional_quality_score") or rule_result.get("professional_quality_score"),
                default=0.0,
            ),
            "development_potential_score": safe_float(
                source.get("development_potential_score") or rule_result.get("development_potential_score"),
                default=0.0,
            ),
            "dimension_details": deepcopy(dimension_details),
        },
        matched_items=normalize_dict_list(source.get("matched_items") or rule_result.get("matched_items")),
        missing_items=normalize_dict_list(source.get("missing_items") or rule_result.get("missing_items")),
        strengths=normalize_text_list(source.get("strengths")),
        weaknesses=normalize_text_list(source.get("weaknesses")),
        improvement_suggestions=normalize_text_list(source.get("improvement_suggestions")),
        recommendation=clean_text(source.get("recommendation")),
        analysis_summary=clean_text(source.get("analysis_summary") or source.get("summary")),
    )
    return asdict(snapshot)


def normalize_career_path_plan_result(career_path_plan_result: Dict[str, Any]) -> Dict[str, Any]:
    """将 career_path_plan_result 归一成报告侧路径规划快照。"""
    source = safe_dict(career_path_plan_result)
    selector_result = safe_dict(source.get("selector_result"))

    career_goal = safe_dict(source.get("career_goal"))
    if not career_goal:
        career_goal = {
            "primary_target_job": clean_text(source.get("primary_target_job") or selector_result.get("primary_target_job")),
            "secondary_target_jobs": normalize_text_list(
                source.get("secondary_target_jobs") or selector_result.get("secondary_target_jobs")
            ),
            "goal_positioning": clean_text(source.get("goal_positioning") or selector_result.get("goal_positioning")),
            "goal_reason": clean_text(source.get("goal_reason")),
        }

    career_path = safe_dict(source.get("career_path"))
    if not career_path:
        career_path = {
            "direct_path": normalize_text_list(source.get("direct_path") or selector_result.get("direct_path")),
            "transition_path": normalize_text_list(source.get("transition_path") or selector_result.get("transition_path")),
            "long_term_path": normalize_text_list(source.get("long_term_path") or selector_result.get("long_term_path")),
            "path_strategy": clean_text(source.get("path_strategy") or selector_result.get("path_strategy")),
        }

    phase_plan = safe_dict(source.get("phase_plan"))
    if not phase_plan:
        phase_plan = {
            "short_term_plan": normalize_text_list(source.get("short_term_plan")),
            "mid_term_plan": normalize_text_list(source.get("mid_term_plan")),
        }

    snapshot = CareerPathPlanReportSnapshot(
        career_goal=career_goal,
        career_path=career_path,
        phase_plan=phase_plan,
        risk_and_gap=normalize_text_list(source.get("risk_and_gap") or source.get("risk_notes")),
        decision_summary=clean_text(source.get("decision_summary") or source.get("summary")),
    )
    return asdict(snapshot)


def build_report_meta(
    student_snapshot: Dict[str, Any],
    job_snapshot: Dict[str, Any],
    career_plan_snapshot: Dict[str, Any],
) -> Dict[str, Any]:
    """构造报告元信息。"""
    name = clean_text(student_snapshot.get("name")) or "学生"
    target_job = clean_text(
        safe_dict(career_plan_snapshot.get("career_goal")).get("primary_target_job")
        or job_snapshot.get("standard_job_name")
    ) or "目标岗位"
    return {
        "report_title": f"{name}的{target_job}职业生涯发展报告",
        "student_name": name,
        "target_job_name": target_job,
    }


def build_report_generation_context(
    student_snapshot: Dict[str, Any],
    job_snapshot: Dict[str, Any],
    job_match_snapshot: Dict[str, Any],
    career_plan_snapshot: Dict[str, Any],
) -> Dict[str, Any]:
    """构造报告生成辅助上下文。"""
    return {
        "student_summary": clean_text(student_snapshot.get("summary")),
        "job_summary": clean_text(job_snapshot.get("summary")),
        "match_summary": clean_text(job_match_snapshot.get("analysis_summary")),
        "decision_summary": clean_text(career_plan_snapshot.get("decision_summary")),
        "goal_reason": clean_text(safe_dict(career_plan_snapshot.get("career_goal")).get("goal_reason")),
        "recommendation": clean_text(job_match_snapshot.get("recommendation")),
        "expected_sections": [
            "学生基本情况与能力画像",
            "目标岗位画像与职业探索",
            "人岗匹配分析",
            "职业目标设定与职业路径规划",
            "分阶段行动计划",
            "风险分析与动态调整建议",
            "总结与建议",
        ],
    }


def build_report_warnings(
    student_snapshot: Dict[str, Any],
    job_snapshot: Dict[str, Any],
    job_match_snapshot: Dict[str, Any],
    career_plan_snapshot: Dict[str, Any],
) -> List[str]:
    """生成 builder 质量提示。"""
    warnings = []
    if not clean_text(student_snapshot.get("name")):
        warnings.append("student_profile_result 缺少学生姓名字段")
    if not clean_text(student_snapshot.get("major")):
        warnings.append("student_profile_result 缺少专业字段")
    if not normalize_text_list(student_snapshot.get("hard_skills")):
        warnings.append("student_profile_result 缺少硬技能标签，报告能力画像章节可能不够具体")
    if not clean_text(job_snapshot.get("standard_job_name")):
        warnings.append("job_profile_result 缺少标准岗位名称")
    if safe_float(job_match_snapshot.get("overall_match_score"), default=0.0) <= 0:
        warnings.append("job_match_result 缺少有效 overall_match_score")
    career_goal = safe_dict(career_plan_snapshot.get("career_goal"))
    if not clean_text(career_goal.get("primary_target_job")):
        warnings.append("career_path_plan_result 缺少 primary_target_job")
    phase_plan = safe_dict(career_plan_snapshot.get("phase_plan"))
    if not normalize_text_list(phase_plan.get("short_term_plan")) and not normalize_text_list(phase_plan.get("mid_term_plan")):
        warnings.append("career_path_plan_result 缺少阶段计划，报告行动计划章节可能偏空")
    return dedup_keep_order(clean_text(item) for item in warnings if clean_text(item))


def build_report_input_payload(
    student_profile_result: Dict[str, Any],
    job_profile_result: Dict[str, Any],
    job_match_result: Dict[str, Any],
    career_path_plan_result: Dict[str, Any],
    output_path: Optional[str | Path] = None,
) -> Dict[str, Any]:
    """主入口：构造 report_input_payload。"""
    student_snapshot = normalize_student_profile_result(student_profile_result)
    job_snapshot = normalize_job_profile_result(job_profile_result)
    career_plan_snapshot = normalize_career_path_plan_result(career_path_plan_result)
    job_match_snapshot = normalize_job_match_result(
        job_match_result,
        target_job_name=clean_text(safe_dict(career_plan_snapshot.get("career_goal")).get("primary_target_job")),
    )

    payload = CareerReportInputPayload(
        student_snapshot=student_snapshot,
        job_snapshot=job_snapshot,
        job_match_snapshot=job_match_snapshot,
        career_path_plan_snapshot=career_plan_snapshot,
        report_meta=build_report_meta(student_snapshot, job_snapshot, career_plan_snapshot),
        report_generation_context=build_report_generation_context(
            student_snapshot,
            job_snapshot,
            job_match_snapshot,
            career_plan_snapshot,
        ),
        build_warnings=build_report_warnings(
            student_snapshot,
            job_snapshot,
            job_match_snapshot,
            career_plan_snapshot,
        ),
    )
    result = asdict(payload)

    if output_path:
        save_json(result, output_path)
    return result


def load_upstream_results_from_state(
    state_path: str | Path = DEFAULT_STATE_PATH,
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """从 student.json 读取 career_report 上游四个模块结果。"""
    state_data = load_json_file(state_path)
    return (
        safe_dict(state_data.get("student_profile_result")),
        safe_dict(state_data.get("job_profile_result")),
        safe_dict(state_data.get("job_match_result")),
        safe_dict(state_data.get("career_path_plan_result")),
    )


def build_report_input_payload_from_state(
    state_path: str | Path = DEFAULT_STATE_PATH,
    output_path: Optional[str | Path] = DEFAULT_OUTPUT_PATH,
) -> Dict[str, Any]:
    """从 student.json 读取四个上游结果并构造 report_input_payload。"""
    student_profile_result, job_profile_result, job_match_result, career_path_plan_result = load_upstream_results_from_state(
        state_path
    )
    return build_report_input_payload(
        student_profile_result=student_profile_result,
        job_profile_result=job_profile_result,
        job_match_result=job_match_result,
        career_path_plan_result=career_path_plan_result,
        output_path=output_path,
    )


def build_demo_student_profile_result() -> Dict[str, Any]:
    """构造 demo student_profile_result。"""
    return {
        "soft_skills": ["沟通能力", "学习能力", "责任心"],
        "potential_profile": {
            "growth_level": "中等偏上",
            "preferred_directions": ["数据分析"],
            "domain_tags": ["数据智能", "商业分析"],
            "basis_score": 76.0,
        },
        "complete_score": 82.0,
        "competitiveness_score": 76.0,
        "strengths": ["具备 Python/SQL 基础", "有项目和实习经历"],
        "weaknesses": ["业务分析经验仍需补强", "BI 工具实战证明不足"],
        "summary": "候选人具备数据分析方向基础技能和一定实践经历。",
        "profile_input_payload": {
            "basic_info": {
                "name": "张三",
                "school": "某某大学",
                "major": "计算机科学与技术",
                "degree": "本科",
                "graduation_year": "2026",
            },
            "normalized_education": {
                "school": "某某大学",
                "major_std": "计算机科学与技术",
                "degree": "本科",
                "graduation_year": "2026",
            },
            "explicit_profile": {
                "certificates": ["CET-6"],
                "project_experience": [{"project_name": "用户行为分析项目", "description": "完成指标分析与可视化"}],
                "internship_experience": [{"company_name": "某科技公司", "position": "数据分析实习生"}],
                "awards": ["校级数据竞赛二等奖"],
            },
            "normalized_profile": {
                "hard_skills": ["Python", "SQL", "机器学习"],
                "tool_skills": ["Excel", "Tableau"],
                "experience_tags": ["项目:数据分析", "实习:报表分析"],
            },
        },
    }


def build_demo_job_profile_result() -> Dict[str, Any]:
    """构造 demo job_profile_result。"""
    return {
        "standard_job_name": "数据分析师",
        "job_category": "数据类",
        "job_level": "初级",
        "degree_requirement": "本科",
        "major_requirement": ["统计学", "计算机科学与技术", "数据科学与大数据技术"],
        "hard_skills": ["SQL", "Python", "数据分析", "A/B测试"],
        "tool_skills": ["Excel", "Tableau", "Power BI"],
        "soft_skills": ["沟通协作", "逻辑分析", "学习能力"],
        "practice_requirement": ["项目要求", "实习要求", "跨部门协作要求"],
        "salary_summary": {"salary_min_month_avg": 12000, "salary_max_month_avg": 18000},
        "city_distribution": [{"name": "杭州", "count": 8, "ratio": 0.4}],
        "industry_distribution": [{"name": "互联网", "count": 10, "ratio": 0.5}],
        "summary": "该岗位关注数据提取分析、指标体系建设、可视化表达和业务协作能力。",
    }


def build_demo_job_match_result() -> Dict[str, Any]:
    """构造 demo job_match_result。"""
    return {
        "target_job_name": "数据分析师",
        "overall_match_score": 78.57,
        "score_level": "C-中等匹配",
        "basic_requirement_score": 100.0,
        "vocational_skill_score": 55.0,
        "professional_quality_score": 86.67,
        "development_potential_score": 92.33,
        "matched_items": [
            {"dimension": "education", "student_item": "本科", "required_item": "本科"},
            {"dimension": "major", "student_item": "计算机科学与技术", "required_item": "计算机科学与技术"},
        ],
        "missing_items": [
            {"dimension": "tool_skills", "required_item": "Power BI", "reason": "学生侧暂未匹配到岗位要求项：Power BI"},
            {"dimension": "hard_skills", "required_item": "A/B测试", "reason": "学生侧暂未匹配到岗位要求项：A/B测试"},
        ],
        "strengths": ["学历满足要求", "Python/SQL 技能匹配度较高"],
        "weaknesses": ["行业项目经验不足", "缺少 BI 工具实战证明"],
        "improvement_suggestions": ["补充 1 个真实业务分析项目", "学习 PowerBI/Tableau 并产出作品集"],
        "recommendation": "可以投递该岗位，但建议优先补齐关键技能或项目短板后再重点冲刺。",
        "analysis_summary": "候选人与数据分析师岗位整体达到中等匹配，短板集中在 BI 工具和业务分析项目深度。",
    }


def build_demo_career_path_plan_result() -> Dict[str, Any]:
    """构造 demo career_path_plan_result。"""
    return {
        "primary_target_job": "数据分析师",
        "secondary_target_jobs": ["BI分析师", "数据运营"],
        "goal_positioning": "以数据分析师作为主目标岗位，但定位为短期补强后冲刺。",
        "goal_reason": "学生具备本科计算机背景与 Python/SQL 基础，适合继续向数据分析师方向推进。",
        "direct_path": ["数据分析师", "高级数据分析师"],
        "transition_path": ["数据运营", "BI分析师", "数据分析师"],
        "long_term_path": ["数据运营", "BI分析师", "数据分析师", "高级数据分析师"],
        "path_strategy": "direct_with_transition_backup",
        "short_term_plan": ["3个月内补齐 SQL/BI 项目作品集", "完善一版面向数据岗位的简历"],
        "mid_term_plan": ["6-12个月争取数据分析实习", "沉淀行业分析方法论"],
        "decision_summary": "优先以数据分析师为主目标，同时保留 BI 分析师和数据运营作为备选路径。",
        "risk_and_gap": ["避免只学工具不做项目", "定期根据招聘要求调整技能栈"],
        "fallback_strategy": "若直接冲刺数据分析师受阻，可先进入 BI 或数据运营岗位积累经验，再回到主目标路径。",
    }


def parse_args() -> argparse.Namespace:
    """命令行参数解析。"""
    parser = argparse.ArgumentParser(description="Build career_report input payload")
    parser.add_argument("--state-path", default="", help="可选：包含四个上游结果的 student.json 路径")
    parser.add_argument("--student-profile-json", default="", help="可选：单独的 student_profile_result JSON")
    parser.add_argument("--job-profile-json", default="", help="可选：单独的 job_profile_result JSON")
    parser.add_argument("--job-match-json", default="", help="可选：单独的 job_match_result JSON")
    parser.add_argument("--career-path-plan-json", default="", help="可选：单独的 career_path_plan_result JSON")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="report_input_payload 输出路径")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.state_path:
        payload_result = build_report_input_payload_from_state(
            state_path=args.state_path,
            output_path=args.output,
        )
    elif args.student_profile_json and args.job_profile_json and args.job_match_json and args.career_path_plan_json:
        payload_result = build_report_input_payload(
            student_profile_result=load_json_file(args.student_profile_json),
            job_profile_result=load_json_file(args.job_profile_json),
            job_match_result=load_json_file(args.job_match_json),
            career_path_plan_result=load_json_file(args.career_path_plan_json),
            output_path=args.output,
        )
    else:
        payload_result = build_report_input_payload(
            student_profile_result=build_demo_student_profile_result(),
            job_profile_result=build_demo_job_profile_result(),
            job_match_result=build_demo_job_match_result(),
            career_path_plan_result=build_demo_career_path_plan_result(),
            output_path=args.output,
        )

    print(json.dumps(payload_result, ensure_ascii=False, indent=2))
