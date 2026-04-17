"""
career_path_plan_builder.py

career_path_plan 模块的 builder 层。

职责边界：
1. 读取 student_profile_result、job_profile_result、job_match_result；
2. 构造统一的 career_plan_input_payload；
3. 对缺失字段做默认值补齐；
4. 统一列表、文本、分数字段格式；
5. 输出给后续 selector 和大模型职业路径规划模块使用。

说明：
- 本文件不重写 student_profile / job_profile / job_match 的业务逻辑；
- 只消费它们已经生成的结构化结果；
- 输出重点是把“学生现状、目标岗位画像、人岗匹配结论、候选路径、优先补强项”整理成统一规划上下文。
"""

from __future__ import annotations

import argparse
import json
import re
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_STATE_PATH = Path("student_api_state.json")
DEFAULT_OUTPUT_PATH = Path("outputs/state/career_path_plan_input_payload.json")


@dataclass
class StudentPlanSnapshot:
    """学生侧职业规划输入快照。"""

    name: str = ""
    school: str = ""
    major: str = ""
    degree: str = ""
    graduation_year: str = ""
    hard_skills: List[str] = field(default_factory=list)
    tool_skills: List[str] = field(default_factory=list)
    soft_skills: List[str] = field(default_factory=list)
    certificates: List[str] = field(default_factory=list)
    project_experience: List[Dict[str, Any]] = field(default_factory=list)
    internship_experience: List[Dict[str, Any]] = field(default_factory=list)
    occupation_hints: List[str] = field(default_factory=list)
    domain_tags: List[str] = field(default_factory=list)
    complete_score: float = 0.0
    competitiveness_score: float = 0.0
    score_level: str = ""
    strengths: List[str] = field(default_factory=list)
    weaknesses: List[str] = field(default_factory=list)
    missing_dimensions: List[str] = field(default_factory=list)
    summary: str = ""
    potential_profile: Dict[str, Any] = field(default_factory=dict)
    ability_evidence: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TargetJobPlanSnapshot:
    """目标岗位侧职业规划输入快照。"""

    standard_job_name: str = ""
    job_category: str = ""
    job_level: str = ""
    degree_requirement: str = ""
    major_requirement: List[str] = field(default_factory=list)
    hard_skills: List[str] = field(default_factory=list)
    tools_or_tech_stack: List[str] = field(default_factory=list)
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


@dataclass
class MatchPlanSnapshot:
    """人岗匹配结论快照。"""

    user_target_job: str = ""
    system_recommended_job: str = ""
    basic_requirement_score: float = 0.0
    vocational_skill_score: float = 0.0
    professional_quality_score: float = 0.0
    development_potential_score: float = 0.0
    overall_match_score: float = 0.0
    score_level: str = ""
    matched_items: List[Dict[str, Any]] = field(default_factory=list)
    missing_items: List[Dict[str, Any]] = field(default_factory=list)
    strengths: List[str] = field(default_factory=list)
    weaknesses: List[str] = field(default_factory=list)
    improvement_suggestions: List[str] = field(default_factory=list)
    recommendation: str = ""
    analysis_summary: str = ""
    dimension_details: Dict[str, Any] = field(default_factory=dict)
    target_display_match_score: float = 0.0
    recommended_display_match_score: float = 0.0
    target_asset_match_score: float = 0.0
    recommended_asset_match_score: float = 0.0
    target_knowledge_point_accuracy: float = 0.0
    recommended_knowledge_point_accuracy: float = 0.0
    target_contest_match_success: bool = False
    recommended_contest_match_success: bool = False
    target_hard_info_pass: bool = False
    recommended_hard_info_pass: bool = False
    target_job_match: Dict[str, Any] = field(default_factory=dict)
    recommended_job_match: Dict[str, Any] = field(default_factory=dict)
    recommendation_ranking: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class CareerPlanNeedGap:
    """职业路径规划中的待补强缺口项。"""

    gap_type: str = ""
    gap_item: str = ""
    current_status: str = ""
    action_hint: str = ""
    priority: str = "medium"
    source: str = ""


@dataclass
class CareerPlanPathOption:
    """可选职业路径节点。"""

    path_type: str = ""
    from_job: str = ""
    to_job: str = ""
    path_text: str = ""
    source: str = ""


@dataclass
class CareerPlanInputPayload:
    """最终输出给 selector / LLM 的统一职业规划输入。"""

    target_job_name: str = ""
    student_snapshot: Dict[str, Any] = field(default_factory=dict)
    target_job_snapshot: Dict[str, Any] = field(default_factory=dict)
    match_snapshot: Dict[str, Any] = field(default_factory=dict)
    candidate_goal_jobs: List[str] = field(default_factory=list)
    direct_path_options: List[Dict[str, Any]] = field(default_factory=list)
    transition_path_options: List[Dict[str, Any]] = field(default_factory=list)
    gap_analysis: List[Dict[str, Any]] = field(default_factory=list)
    planning_constraints: Dict[str, Any] = field(default_factory=dict)
    planner_context: Dict[str, Any] = field(default_factory=dict)
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


def safe_bool(value: Any) -> bool:
    """安全转 bool，兼容字符串/数字形式。"""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = clean_text(value).lower()
    if text in {"true", "yes", "y", "1", "通过", "是"}:
        return True
    if text in {"false", "no", "n", "0", "未通过", "否"}:
        return False
    return False


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
    """将 list / JSON 字符串 / 普通分隔符字符串统一转成 list。"""
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
    """统一字符串列表格式。"""
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
    """保存 JSON 输出文件。"""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def extract_skill_names_from_skill_profile(skill_profile: Dict[str, Any]) -> List[str]:
    """从 skill_profile 字典兜底提取技能名称。"""
    if not isinstance(skill_profile, dict):
        return []
    if "hard_skills" in skill_profile or "tool_skills" in skill_profile:
        return normalize_text_list(skill_profile.get("hard_skills")) + normalize_text_list(
            skill_profile.get("tool_skills")
        )
    return normalize_text_list(skill_profile.keys())


def normalize_student_profile_result(student_profile_result: Dict[str, Any]) -> Dict[str, Any]:
    """将 student_profile_result 归一成职业规划可用的学生快照。"""
    source = safe_dict(student_profile_result)
    profile_payload = safe_dict(source.get("profile_input_payload"))
    basic_info = safe_dict(profile_payload.get("basic_info"))
    normalized_education = safe_dict(profile_payload.get("normalized_education"))
    explicit_profile = safe_dict(profile_payload.get("explicit_profile"))
    normalized_profile = safe_dict(profile_payload.get("normalized_profile"))
    ability_evidence = safe_dict(source.get("ability_evidence"))
    potential_profile = safe_dict(source.get("potential_profile"))

    hard_skills = normalize_text_list(normalized_profile.get("hard_skills"))
    if not hard_skills:
        hard_skills = extract_skill_names_from_skill_profile(safe_dict(source.get("skill_profile")))

    tool_skills = normalize_text_list(normalized_profile.get("tool_skills"))
    soft_skills = normalize_text_list(source.get("soft_skills"))
    if not soft_skills:
        soft_skills = normalize_text_list(safe_dict(source.get("soft_skill_profile")).keys())

    certificates = normalize_text_list(explicit_profile.get("certificates"))
    if not certificates:
        certificates = normalize_text_list(source.get("certificate_profile"))
    if not certificates:
        certificates = normalize_text_list(ability_evidence.get("certificate_tags"))

    project_experience = normalize_dict_list(explicit_profile.get("project_experience"))
    if not project_experience:
        project_experience = normalize_dict_list(ability_evidence.get("project_examples"))

    internship_experience = normalize_dict_list(explicit_profile.get("internship_experience"))
    if not internship_experience:
        internship_experience = normalize_dict_list(ability_evidence.get("internship_examples"))

    snapshot = StudentPlanSnapshot(
        name=clean_text(basic_info.get("name")),
        school=clean_text(normalized_education.get("school") or basic_info.get("school")),
        major=clean_text(normalized_education.get("major_std") or basic_info.get("major")),
        degree=clean_text(normalized_education.get("degree") or basic_info.get("degree")),
        graduation_year=clean_text(normalized_education.get("graduation_year") or basic_info.get("graduation_year")),
        hard_skills=hard_skills,
        tool_skills=tool_skills,
        soft_skills=soft_skills,
        certificates=certificates,
        project_experience=project_experience,
        internship_experience=internship_experience,
        occupation_hints=normalize_text_list(
            normalized_profile.get("occupation_hints") or potential_profile.get("preferred_directions")
        ),
        domain_tags=normalize_text_list(
            normalized_profile.get("domain_tags") or potential_profile.get("domain_tags")
        ),
        complete_score=safe_float(source.get("complete_score"), default=0.0),
        competitiveness_score=safe_float(source.get("competitiveness_score"), default=0.0),
        score_level=clean_text(source.get("score_level")),
        strengths=normalize_text_list(source.get("strengths")),
        weaknesses=normalize_text_list(source.get("weaknesses")),
        missing_dimensions=normalize_text_list(source.get("missing_dimensions")),
        summary=clean_text(source.get("summary")),
        potential_profile=deepcopy(potential_profile),
        ability_evidence=deepcopy(ability_evidence),
    )
    return asdict(snapshot)


def normalize_job_profile_result(job_profile_result: Dict[str, Any]) -> Dict[str, Any]:
    """将 job_profile_result 归一成职业规划可用的目标岗位快照。"""
    source = safe_dict(job_profile_result)
    normalized_requirements = safe_dict(source.get("normalized_requirements"))
    target_assets = safe_dict(source.get("target_job_profile_assets"))

    hard_skills = normalize_text_list(source.get("hard_skills"))
    if not hard_skills:
        hard_skills = normalize_text_list(normalized_requirements.get("hard_skill_tags"))

    tool_skills = normalize_text_list(source.get("tools_or_tech_stack"))
    if not tool_skills:
        tool_skills = normalize_text_list(normalized_requirements.get("tool_skill_tags"))

    asset_required_knowledge = normalize_text_list(target_assets.get("required_knowledge_points"))
    asset_summary = ""
    if safe_bool(target_assets.get("asset_found")):
        asset_job_name = clean_text(
            target_assets.get("resolved_standard_job_name")
            or target_assets.get("standard_job_name")
            or source.get("standard_job_name")
        )
        asset_degree = clean_text(target_assets.get("degree_gate") or target_assets.get("mainstream_degree"))
        asset_majors = normalize_text_list(target_assets.get("major_gate_set") or target_assets.get("mainstream_majors"))[:5]
        summary_parts = [f"{asset_job_name}岗位画像来自后处理资产"]
        if asset_degree:
            summary_parts.append(f"主流学历门槛为{asset_degree}")
        if asset_majors:
            summary_parts.append(f"高频专业方向包括{'、'.join(asset_majors)}")
        if asset_required_knowledge:
            summary_parts.append(f"核心知识点包括{'、'.join(asset_required_knowledge[:8])}")
        asset_summary = "；".join(part for part in summary_parts if clean_text(part)) + "。"

    snapshot = TargetJobPlanSnapshot(
        standard_job_name=clean_text(source.get("standard_job_name")),
        job_category=clean_text(source.get("job_category")),
        job_level=clean_text(source.get("job_level")),
        degree_requirement=clean_text(source.get("degree_requirement")),
        major_requirement=normalize_text_list(
            source.get("major_requirement") or normalized_requirements.get("major_tags")
        ),
        hard_skills=hard_skills,
        tools_or_tech_stack=tool_skills,
        certificate_requirement=normalize_text_list(
            source.get("certificate_requirement") or normalized_requirements.get("certificate_tags")
        ),
        practice_requirement=normalize_text_list(
            source.get("practice_requirement") or normalized_requirements.get("practice_tags")
        ),
        soft_skills=normalize_text_list(
            source.get("soft_skills") or normalized_requirements.get("soft_skill_tags")
        ),
        suitable_student_profile=clean_text(source.get("suitable_student_profile")),
        summary=asset_summary or clean_text(source.get("summary")),
        vertical_paths=normalize_text_list(source.get("vertical_paths")),
        transfer_paths=normalize_text_list(source.get("transfer_paths")),
        skill_frequency=normalize_dict_list(source.get("skill_frequency")),
        industry_distribution=normalize_dict_list(source.get("industry_distribution")),
        city_distribution=normalize_dict_list(source.get("city_distribution")),
        salary_stats=deepcopy(safe_dict(source.get("salary_stats"))),
    )
    return asdict(snapshot)


def pick_job_match_job_name(match_detail: Dict[str, Any]) -> str:
    """从 target/recommended match 结构中取展示岗位名。"""
    detail = safe_dict(match_detail)
    return clean_text(
        detail.get("job_name")
        or detail.get("asset_job_name")
        or detail.get("resolved_standard_job_name")
    )


def pick_job_match_display_score(match_detail: Dict[str, Any]) -> float:
    """优先使用赛题资产主展示分，避免旧规则分把规划目标拉偏。"""
    detail = safe_dict(match_detail)
    return safe_float(
        detail.get("display_match_score")
        or detail.get("asset_match_score")
        or detail.get("overall_match_score"),
        default=0.0,
    )


def pick_job_match_asset_score(match_detail: Dict[str, Any]) -> float:
    """读取岗位资产分，缺失时回退到主展示分。"""
    detail = safe_dict(match_detail)
    return safe_float(
        detail.get("asset_match_score")
        or detail.get("display_match_score")
        or detail.get("overall_match_score"),
        default=0.0,
    )


def pick_job_match_knowledge_accuracy(match_detail: Dict[str, Any]) -> float:
    """读取技能知识点覆盖率。"""
    detail = safe_dict(match_detail)
    return safe_float(
        safe_dict(detail.get("skill_knowledge_match")).get("knowledge_point_accuracy"),
        default=0.0,
    )


def pick_job_match_hard_pass(match_detail: Dict[str, Any]) -> bool:
    """读取学历/专业/证书硬门槛是否通过。"""
    detail = safe_dict(match_detail)
    hard_info = safe_dict(detail.get("hard_info_evaluation"))
    contest = safe_dict(detail.get("contest_evaluation"))
    if "all_pass" in hard_info:
        return safe_bool(hard_info.get("all_pass"))
    return safe_bool(contest.get("hard_info_pass"))


def pick_job_match_contest_success(match_detail: Dict[str, Any]) -> bool:
    """读取赛题综合评测是否通过。"""
    detail = safe_dict(match_detail)
    return safe_bool(safe_dict(detail.get("contest_evaluation")).get("contest_match_success"))


def normalize_job_match_result(job_match_result: Dict[str, Any]) -> Dict[str, Any]:
    """将 job_match_result 归一成职业规划可用的人岗匹配快照。"""
    source = safe_dict(job_match_result)
    rule_score_result = safe_dict(source.get("rule_score_result"))
    target_job_match = deepcopy(safe_dict(source.get("target_job_match")))
    recommended_job_match = deepcopy(safe_dict(source.get("recommended_job_match")))
    recommendation_ranking = deepcopy(normalize_dict_list(source.get("recommendation_ranking")))
    user_target_job = clean_text(
        source.get("target_job_name")
        or pick_job_match_job_name(target_job_match)
        or safe_dict(safe_dict(source.get("match_input_payload")).get("job_profile")).get("standard_job_name")
    )
    ranking_top_job = clean_text(safe_dict(recommendation_ranking[0]).get("job_name")) if recommendation_ranking else ""
    system_recommended_job = clean_text(pick_job_match_job_name(recommended_job_match) or ranking_top_job)

    snapshot = MatchPlanSnapshot(
        user_target_job=user_target_job,
        system_recommended_job=system_recommended_job,
        basic_requirement_score=safe_float(
            source.get("basic_requirement_score") or rule_score_result.get("basic_requirement_score"),
            default=0.0,
        ),
        vocational_skill_score=safe_float(
            source.get("vocational_skill_score") or rule_score_result.get("vocational_skill_score"),
            default=0.0,
        ),
        professional_quality_score=safe_float(
            source.get("professional_quality_score") or rule_score_result.get("professional_quality_score"),
            default=0.0,
        ),
        development_potential_score=safe_float(
            source.get("development_potential_score") or rule_score_result.get("development_potential_score"),
            default=0.0,
        ),
        overall_match_score=safe_float(
            source.get("overall_match_score") or rule_score_result.get("overall_match_score"),
            default=0.0,
        ),
        score_level=clean_text(source.get("score_level") or rule_score_result.get("score_level")),
        matched_items=normalize_dict_list(source.get("matched_items") or rule_score_result.get("matched_items")),
        missing_items=normalize_dict_list(source.get("missing_items") or rule_score_result.get("missing_items")),
        strengths=normalize_text_list(source.get("strengths")),
        weaknesses=normalize_text_list(source.get("weaknesses")),
        improvement_suggestions=normalize_text_list(source.get("improvement_suggestions")),
        recommendation=clean_text(source.get("recommendation")),
        analysis_summary=clean_text(source.get("analysis_summary") or source.get("summary")),
        dimension_details=deepcopy(safe_dict(source.get("dimension_details"))),
        target_display_match_score=pick_job_match_display_score(target_job_match),
        recommended_display_match_score=pick_job_match_display_score(recommended_job_match),
        target_asset_match_score=pick_job_match_asset_score(target_job_match),
        recommended_asset_match_score=pick_job_match_asset_score(recommended_job_match),
        target_knowledge_point_accuracy=pick_job_match_knowledge_accuracy(target_job_match),
        recommended_knowledge_point_accuracy=pick_job_match_knowledge_accuracy(recommended_job_match),
        target_contest_match_success=pick_job_match_contest_success(target_job_match),
        recommended_contest_match_success=pick_job_match_contest_success(recommended_job_match),
        target_hard_info_pass=pick_job_match_hard_pass(target_job_match),
        recommended_hard_info_pass=pick_job_match_hard_pass(recommended_job_match),
        target_job_match=target_job_match,
        recommended_job_match=recommended_job_match,
        recommendation_ranking=recommendation_ranking,
    )
    return asdict(snapshot)


def parse_path_text(path_text: Any, path_type: str, source: str) -> Dict[str, Any]:
    """把“岗位A -> 岗位B”格式路径解析成结构化 path option。"""
    text = clean_text(path_text)
    from_job = ""
    to_job = ""
    if text and "->" in text:
        parts = [clean_text(part) for part in text.split("->") if clean_text(part)]
        if len(parts) >= 2:
            from_job = parts[0]
            to_job = parts[-1]
    option = CareerPlanPathOption(
        path_type=path_type,
        from_job=from_job,
        to_job=to_job,
        path_text=text,
        source=source,
    )
    return asdict(option)


def extract_graph_path_context(context_data: Optional[Dict[str, Any]]) -> Dict[str, List[str]]:
    """从上游 context_data 中提取图谱/离线路径候选。"""
    graph_context = safe_dict(safe_dict(context_data).get("graph_context"))
    return {
        "promote_paths": normalize_text_list(
            graph_context.get("promote_paths") or graph_context.get("vertical_paths")
        ),
        "transfer_paths": normalize_text_list(
            graph_context.get("transfer_paths") or graph_context.get("lateral_paths")
        ),
        "related_jobs": normalize_text_list(graph_context.get("related_jobs")),
    }


def extract_sql_market_context(context_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """从上游 context_data 中提取 SQL 市场事实补充。"""
    sql_context = safe_dict(safe_dict(context_data).get("sql_context"))
    return {
        "job_count": sql_context.get("job_count", 0),
        "salary_stats": deepcopy(safe_dict(sql_context.get("salary_stats"))),
        "top_cities": normalize_text_list(sql_context.get("top_cities")),
        "top_industries": normalize_text_list(sql_context.get("top_industries")),
        "top_company_types": normalize_text_list(sql_context.get("top_company_types")),
        "top_company_sizes": normalize_text_list(sql_context.get("top_company_sizes")),
        "company_samples": deepcopy(parse_list_like(sql_context.get("company_samples"))[:6]),
        "representative_samples": deepcopy(normalize_dict_list(sql_context.get("representative_samples"))[:5]),
    }


def extract_semantic_context(context_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """从上游 context_data 中提取语义检索结果。"""
    semantic_context = safe_dict(safe_dict(context_data).get("semantic_context"))
    hits = []
    for item in parse_list_like(semantic_context.get("hits"))[:5]:
        item_dict = safe_dict(item)
        if not item_dict:
            continue
        hits.append(
            {
                "standard_job_name": clean_text(item_dict.get("standard_job_name")),
                "job_category": clean_text(item_dict.get("job_category")),
                "job_level": clean_text(item_dict.get("job_level")),
                "score": item_dict.get("score"),
                "doc_text_excerpt": clean_text(item_dict.get("doc_text_excerpt")),
                "hard_skills": normalize_text_list(item_dict.get("hard_skills")),
                "vertical_paths": normalize_text_list(item_dict.get("vertical_paths")),
                "transfer_paths": normalize_text_list(item_dict.get("transfer_paths")),
            }
        )
    return {
        "query": clean_text(semantic_context.get("query")),
        "top_k": int(semantic_context.get("top_k") or len(hits) or 0),
        "hits": hits,
    }


def normalize_path_strings(values: List[str], source_job_name: str) -> List[str]:
    """把仅包含目标岗位名的路径候选补成标准 'A -> B' 字符串。"""
    source_job = clean_text(source_job_name)
    normalized: List[str] = []
    for item in values:
        text = clean_text(item)
        if not text:
            continue
        if "->" in text:
            normalized.append(text)
        elif source_job and text != source_job:
            normalized.append(f"{source_job} -> {text}")
        else:
            normalized.append(text)
    return dedup_keep_order(normalized)


def annotate_path_option(
    option: Dict[str, Any],
    match_score: float,
    score_delta: float = 0.0,
    priority_hint: str = "",
    source_tier: str = "",
    is_fallback: bool = False,
) -> Dict[str, Any]:
    """统一补充路径候选的评分与来源元信息。"""
    option = deepcopy(safe_dict(option))
    option["path_score_hint"] = round(max(0.0, match_score + score_delta), 2)
    option["priority_hint"] = priority_hint or ("high" if option["path_score_hint"] >= 75 else "medium")
    option["source_tier"] = source_tier or ("fallback" if is_fallback else "knowledge")
    option["is_fallback"] = bool(is_fallback)
    return option


def build_candidate_goal_jobs(
    student_snapshot: Dict[str, Any],
    target_job_snapshot: Dict[str, Any],
    match_snapshot: Dict[str, Any],
    context_data: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """构造候选目标岗位列表，供后续 selector 做主目标/备选目标选择。"""
    candidates = []
    target_job = clean_text(target_job_snapshot.get("standard_job_name"))
    if target_job:
        candidates.append(target_job)

    for job_name in [
        match_snapshot.get("user_target_job"),
        match_snapshot.get("system_recommended_job"),
        safe_dict(match_snapshot.get("target_job_match")).get("asset_job_name"),
        safe_dict(match_snapshot.get("recommended_job_match")).get("asset_job_name"),
    ]:
        if clean_text(job_name):
            candidates.append(clean_text(job_name))

    for ranking_item in normalize_dict_list(match_snapshot.get("recommendation_ranking"))[:10]:
        ranking_job = clean_text(ranking_item.get("job_name") or ranking_item.get("asset_job_name"))
        if ranking_job:
            candidates.append(ranking_job)

    graph_paths = extract_graph_path_context(context_data)
    for related_job in normalize_text_list(graph_paths.get("related_jobs")):
        candidates.append(related_job)

    for path_text in normalize_path_strings(graph_paths.get("transfer_paths", []), target_job):
        path_option = parse_path_text(path_text, path_type="transfer", source="graph_context.transfer_paths")
        to_job = clean_text(path_option.get("to_job"))
        if to_job:
            candidates.append(to_job)

    for path_text in normalize_path_strings(graph_paths.get("promote_paths", []), target_job):
        path_option = parse_path_text(path_text, path_type="direct_vertical", source="graph_context.promote_paths")
        to_job = clean_text(path_option.get("to_job"))
        if to_job:
            candidates.append(to_job)

    for path_text in normalize_text_list(target_job_snapshot.get("transfer_paths")):
        path_option = parse_path_text(path_text, path_type="transfer", source="job_profile.transfer_paths")
        to_job = clean_text(path_option.get("to_job"))
        if to_job:
            candidates.append(to_job)

    for direction in normalize_text_list(student_snapshot.get("occupation_hints")):
        candidates.append(direction)

    for matched_item in normalize_dict_list(match_snapshot.get("matched_items")):
        if clean_text(matched_item.get("dimension")) == "career_direction":
            candidates.append(clean_text(matched_item.get("required_item")))

    return dedup_keep_order(clean_text(item) for item in candidates if clean_text(item))


def build_direct_path_options(
    target_job_snapshot: Dict[str, Any],
    match_snapshot: Dict[str, Any],
    context_data: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """构造直接发展路径候选，只使用图谱或离线岗位画像中的真实路径。"""
    options = []
    score = safe_float(match_snapshot.get("overall_match_score"), default=0.0)
    graph_paths = extract_graph_path_context(context_data)
    target_job = clean_text(target_job_snapshot.get("standard_job_name"))

    for path_text in normalize_path_strings(graph_paths.get("promote_paths", []), target_job):
        options.append(
            annotate_path_option(
                parse_path_text(
                    path_text=path_text,
                    path_type="direct_vertical",
                    source="graph_context.promote_paths",
                ),
                match_score=score,
                score_delta=8.0,
                priority_hint="high",
                source_tier="graph",
            )
        )

    for path_text in normalize_text_list(target_job_snapshot.get("vertical_paths")):
        options.append(
            annotate_path_option(
                parse_path_text(
                    path_text=path_text,
                    path_type="direct_vertical",
                    source="job_profile.vertical_paths",
                ),
                match_score=score,
                score_delta=4.0,
                priority_hint="high" if score >= 75 else "medium",
                source_tier="offline_profile",
            )
        )

    return dedup_keep_order(options)


def build_transition_path_options(
    student_snapshot: Dict[str, Any],
    target_job_snapshot: Dict[str, Any],
    match_snapshot: Dict[str, Any],
    context_data: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """构造过渡路径候选，只使用图谱或离线岗位画像中的真实转岗路径。"""
    options = []
    target_job = clean_text(target_job_snapshot.get("standard_job_name"))
    score = safe_float(match_snapshot.get("overall_match_score"), default=0.0)
    graph_paths = extract_graph_path_context(context_data)

    for path_text in normalize_path_strings(graph_paths.get("transfer_paths", []), target_job):
        options.append(
            annotate_path_option(
                parse_path_text(
                    path_text=path_text,
                    path_type="transfer",
                    source="graph_context.transfer_paths",
                ),
                match_score=max(50.0, score - 2.0),
                score_delta=6.0,
                priority_hint="high" if score < 80 else "medium",
                source_tier="graph",
            )
        )

    for path_text in normalize_text_list(target_job_snapshot.get("transfer_paths")):
        options.append(
            annotate_path_option(
                parse_path_text(
                    path_text=path_text,
                    path_type="transfer",
                    source="job_profile.transfer_paths",
                ),
                match_score=max(50.0, score - 5.0),
                score_delta=3.0,
                priority_hint="high" if score < 75 else "medium",
                source_tier="offline_profile",
            )
        )

    return dedup_keep_order(options)


def summarize_path_sources(
    direct_path_options: List[Dict[str, Any]],
    transition_path_options: List[Dict[str, Any]],
) -> Dict[str, int]:
    """统计路径候选来源，便于后续解释和调试。"""
    summary: Dict[str, int] = {}
    for option in direct_path_options + transition_path_options:
        source = clean_text(safe_dict(option).get("source")) or "unknown"
        summary[source] = summary.get(source, 0) + 1
    return summary


def build_direct_and_transition_paths(
    student_snapshot: Dict[str, Any],
    target_job_snapshot: Dict[str, Any],
    match_snapshot: Dict[str, Any],
    context_data: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """统一构造直接/过渡路径候选，集中管理路径来源优先级。"""
    direct_path_options = build_direct_path_options(
        target_job_snapshot=target_job_snapshot,
        match_snapshot=match_snapshot,
        context_data=context_data,
    )
    transition_path_options = build_transition_path_options(
        student_snapshot=student_snapshot,
        target_job_snapshot=target_job_snapshot,
        match_snapshot=match_snapshot,
        context_data=context_data,
    )
    return direct_path_options, transition_path_options


def infer_gap_priority(gap_type: str, match_snapshot: Dict[str, Any]) -> str:
    """根据缺口类型和匹配分数给出优先级标签。"""
    overall_score = safe_float(match_snapshot.get("overall_match_score"), default=0.0)
    if gap_type in {"hard_skills", "tool_skills", "practice_experience"}:
        return "high"
    if gap_type in {"education", "major", "certificates"}:
        return "high" if overall_score < 75 else "medium"
    if gap_type in {"soft_skills", "career_direction"}:
        return "medium"
    return "low"


def build_action_hint_for_gap(
    gap_type: str,
    gap_item: str,
    match_snapshot: Dict[str, Any],
) -> str:
    """基于缺口类型生成可执行补强建议。"""
    suggestions = normalize_text_list(match_snapshot.get("improvement_suggestions"))
    if suggestions:
        for suggestion in suggestions:
            if gap_item and gap_item in suggestion:
                return suggestion

    if gap_type == "hard_skills":
        return f"围绕 {gap_item or '岗位核心硬技能'} 补充系统学习和项目实践，并在简历中补充应用场景与成果。"
    if gap_type == "tool_skills":
        return f"补充 {gap_item or '岗位工具栈'} 的实操案例，例如分析看板、数据处理脚本或作品集。"
    if gap_type == "practice_experience":
        return f"围绕“{gap_item or '岗位实践要求'}”补充课程项目、实习任务或业务场景复盘。"
    if gap_type == "soft_skills":
        return f"在项目和实习经历中补充 {gap_item or '关键软技能'} 的行为证据与协作成果。"
    if gap_type == "certificates":
        return f"如岗位确实重视 {gap_item or '相关证书'}，可规划短期备考或补充替代性证明材料。"
    if gap_type == "education" or gap_type == "major":
        return "若学历/专业背景不完全匹配，建议通过相关课程、项目、证书或竞赛提升岗位方向背书。"
    return "结合岗位要求补充可量化的学习成果、项目经验和简历表达证据。"


def build_gap_analysis(
    student_snapshot: Dict[str, Any],
    target_job_snapshot: Dict[str, Any],
    match_snapshot: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """融合 job_match 缺口项、学生短板、岗位技能要求，生成职业规划缺口清单。"""
    gaps = []
    existing_gap_keys = set()

    for item in normalize_dict_list(match_snapshot.get("missing_items")):
        gap_type = clean_text(item.get("dimension")) or "general"
        gap_item = clean_text(item.get("required_item"))
        current_status = clean_text(item.get("student_item")) or clean_text(item.get("reason"))
        existing_gap_keys.add((gap_type, gap_item))
        gaps.append(
            asdict(
                CareerPlanNeedGap(
                    gap_type=gap_type,
                    gap_item=gap_item,
                    current_status=current_status,
                    action_hint=build_action_hint_for_gap(gap_type, gap_item, match_snapshot),
                    priority=infer_gap_priority(gap_type, match_snapshot),
                    source="job_match.missing_items",
                )
            )
        )

    matched_hard_skills = {
        clean_text(item.get("required_item"))
        for item in normalize_dict_list(match_snapshot.get("matched_items"))
        if clean_text(item.get("dimension")) == "hard_skills"
    }
    matched_hard_skills.update(normalize_text_list(student_snapshot.get("hard_skills")))

    for skill in normalize_text_list(target_job_snapshot.get("hard_skills")):
        if not skill:
            continue
        if skill in matched_hard_skills:
            continue
        if ("hard_skills", skill) in existing_gap_keys:
            continue
        existing_gap_keys.add(("hard_skills", skill))
        gaps.append(
            asdict(
                CareerPlanNeedGap(
                    gap_type="hard_skills",
                    gap_item=skill,
                    current_status="学生侧尚未形成稳定技能证据",
                    action_hint=build_action_hint_for_gap("hard_skills", skill, match_snapshot),
                    priority="high",
                    source="job_profile.hard_skills",
                )
            )
        )

    matched_tool_skills = {
        clean_text(item.get("required_item"))
        for item in normalize_dict_list(match_snapshot.get("matched_items"))
        if clean_text(item.get("dimension")) == "tool_skills"
    }
    matched_tool_skills.update(normalize_text_list(student_snapshot.get("tool_skills")))
    for tool_name in normalize_text_list(target_job_snapshot.get("tools_or_tech_stack")):
        if not tool_name:
            continue
        if tool_name in matched_tool_skills:
            continue
        if ("tool_skills", tool_name) in existing_gap_keys:
            continue
        existing_gap_keys.add(("tool_skills", tool_name))
        gaps.append(
            asdict(
                CareerPlanNeedGap(
                    gap_type="tool_skills",
                    gap_item=tool_name,
                    current_status="学生侧尚未形成稳定工具使用证据",
                    action_hint=build_action_hint_for_gap("tool_skills", tool_name, match_snapshot),
                    priority="high",
                    source="job_profile.tools_or_tech_stack",
                )
            )
        )

    for weakness in normalize_text_list(student_snapshot.get("weaknesses")):
        gaps.append(
            asdict(
                CareerPlanNeedGap(
                    gap_type="student_weakness",
                    gap_item=weakness,
                    current_status=weakness,
                    action_hint="将该短板拆成可执行训练任务，并补充可量化成果或项目证明。",
                    priority="medium",
                    source="student_profile.weaknesses",
                )
            )
        )

    for dimension in normalize_text_list(student_snapshot.get("missing_dimensions")):
        gaps.append(
            asdict(
                CareerPlanNeedGap(
                    gap_type="missing_dimension",
                    gap_item=dimension,
                    current_status=f"学生画像仍缺少{dimension}相关证据",
                    action_hint="优先补充该维度的信息、经历或作品材料，以便提升后续匹配和报告质量。",
                    priority="medium",
                    source="student_profile.missing_dimensions",
                )
            )
        )

    return dedup_keep_order(gaps)


def build_planning_constraints(
    student_snapshot: Dict[str, Any],
    target_job_snapshot: Dict[str, Any],
    match_snapshot: Dict[str, Any],
) -> Dict[str, Any]:
    """构造路径规划约束与默认策略提示。"""
    overall_score = safe_float(match_snapshot.get("overall_match_score"), default=0.0)
    target_display_score = safe_float(
        match_snapshot.get("target_display_match_score"),
        default=overall_score,
    )
    recommended_display_score = safe_float(match_snapshot.get("recommended_display_match_score"), default=0.0)
    planning_score = max(target_display_score, recommended_display_score, overall_score)
    direct_path_feasible = planning_score >= 75
    transition_first_recommended = planning_score < 70

    return {
        "current_degree": clean_text(student_snapshot.get("degree")),
        "current_major": clean_text(student_snapshot.get("major")),
        "graduation_year": clean_text(student_snapshot.get("graduation_year")),
        "target_job_name": clean_text(target_job_snapshot.get("standard_job_name")),
        "target_job_level": clean_text(target_job_snapshot.get("job_level")),
        "target_degree_requirement": clean_text(target_job_snapshot.get("degree_requirement")),
        "overall_match_score": round(overall_score, 2),
        "target_display_match_score": round(target_display_score, 2),
        "recommended_display_match_score": round(recommended_display_score, 2),
        "target_knowledge_point_accuracy": round(
            safe_float(match_snapshot.get("target_knowledge_point_accuracy"), default=0.0),
            4,
        ),
        "recommended_knowledge_point_accuracy": round(
            safe_float(match_snapshot.get("recommended_knowledge_point_accuracy"), default=0.0),
            4,
        ),
        "user_target_job": clean_text(match_snapshot.get("user_target_job")),
        "system_recommended_job": clean_text(match_snapshot.get("system_recommended_job")),
        "direct_path_feasible": direct_path_feasible,
        "transition_first_recommended": transition_first_recommended,
        "short_term_focus": "优先补齐高优先级技能/工具/项目缺口，并准备 1-2 个能直接支撑目标岗位的作品或案例。",
        "mid_term_focus": "围绕主目标岗位和备选目标岗位持续补充真实项目、实习经历和投递反馈证据。",
    }


def build_planner_context(
    student_snapshot: Dict[str, Any],
    target_job_snapshot: Dict[str, Any],
    match_snapshot: Dict[str, Any],
    direct_path_options: List[Dict[str, Any]],
    transition_path_options: List[Dict[str, Any]],
    gap_analysis: List[Dict[str, Any]],
    context_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """组装给后续 LLM 的规划上下文说明。"""
    graph_paths = extract_graph_path_context(context_data)
    sql_market_context = extract_sql_market_context(context_data)
    semantic_context = extract_semantic_context(context_data)
    return {
        "student_summary": clean_text(student_snapshot.get("summary")),
        "job_summary": clean_text(target_job_snapshot.get("summary")),
        "match_summary": clean_text(match_snapshot.get("analysis_summary")),
        "recommendation": clean_text(match_snapshot.get("recommendation")),
        "student_strengths": normalize_text_list(student_snapshot.get("strengths")),
        "student_weaknesses": normalize_text_list(student_snapshot.get("weaknesses")),
        "match_strengths": normalize_text_list(match_snapshot.get("strengths")),
        "match_weaknesses": normalize_text_list(match_snapshot.get("weaknesses")),
        "improvement_suggestions": normalize_text_list(match_snapshot.get("improvement_suggestions")),
        "direct_path_count": len(direct_path_options),
        "transition_path_count": len(transition_path_options),
        "high_priority_gap_count": sum(1 for gap in gap_analysis if safe_dict(gap).get("priority") == "high"),
        "path_source_summary": summarize_path_sources(direct_path_options, transition_path_options),
        "graph_fact_snapshot": {
            "related_jobs": deepcopy(graph_paths.get("related_jobs", [])[:6]),
            "promote_path_count": len(graph_paths.get("promote_paths", [])),
            "transfer_path_count": len(graph_paths.get("transfer_paths", [])),
        },
        "market_fact_snapshot": {
            "job_count": sql_market_context.get("job_count", 0),
            "salary_stats": deepcopy(sql_market_context.get("salary_stats")),
            "top_cities": deepcopy(sql_market_context.get("top_cities", [])[:5]),
            "top_industries": deepcopy(sql_market_context.get("top_industries", [])[:5]),
            "top_company_types": deepcopy(sql_market_context.get("top_company_types", [])[:5]),
            "top_company_sizes": deepcopy(sql_market_context.get("top_company_sizes", [])[:5]),
            "company_samples": deepcopy(sql_market_context.get("company_samples", [])[:5]),
        },
        "semantic_fact_snapshot": {
            "query": clean_text(semantic_context.get("query")),
            "top_k": semantic_context.get("top_k", 0),
            "hits": deepcopy(semantic_context.get("hits", [])[:3]),
        },
        "goal_decision_seed": {
            "user_target_job": clean_text(match_snapshot.get("user_target_job")),
            "system_recommended_job": clean_text(match_snapshot.get("system_recommended_job")),
            "target_display_match_score": safe_float(match_snapshot.get("target_display_match_score"), default=0.0),
            "recommended_display_match_score": safe_float(
                match_snapshot.get("recommended_display_match_score"),
                default=0.0,
            ),
            "target_knowledge_point_accuracy": safe_float(
                match_snapshot.get("target_knowledge_point_accuracy"),
                default=0.0,
            ),
            "recommended_knowledge_point_accuracy": safe_float(
                match_snapshot.get("recommended_knowledge_point_accuracy"),
                default=0.0,
            ),
            "target_contest_match_success": bool(match_snapshot.get("target_contest_match_success")),
            "recommended_contest_match_success": bool(match_snapshot.get("recommended_contest_match_success")),
            "target_hard_info_pass": bool(match_snapshot.get("target_hard_info_pass")),
            "recommended_hard_info_pass": bool(match_snapshot.get("recommended_hard_info_pass")),
        },
        "expected_output_hint": {
            "primary_target_job": "首选目标岗位",
            "alternative_target_jobs": "备选岗位列表",
            "direct_path": "直接达成路径",
            "transition_path": "过渡路径",
            "short_term_plan": "短期3-6个月计划",
            "mid_term_plan": "中期6-18个月计划",
            "risk_notes": "路径风险和注意事项",
        },
    }


def build_warnings(
    student_snapshot: Dict[str, Any],
    target_job_snapshot: Dict[str, Any],
    match_snapshot: Dict[str, Any],
    direct_path_options: List[Dict[str, Any]],
    transition_path_options: List[Dict[str, Any]],
) -> List[str]:
    """生成 builder 阶段质量提示。"""
    warnings = []
    if not clean_text(student_snapshot.get("major")):
        warnings.append("student_profile_result 缺少学生专业字段")
    if not clean_text(student_snapshot.get("degree")):
        warnings.append("student_profile_result 缺少学生学历字段")
    if not normalize_text_list(student_snapshot.get("hard_skills")) and not normalize_text_list(student_snapshot.get("tool_skills")):
        warnings.append("student_profile_result 缺少技能/工具标签，职业路径规划可信度可能下降")
    if not clean_text(target_job_snapshot.get("standard_job_name")):
        warnings.append("job_profile_result 缺少目标岗位名称")
    if not normalize_text_list(target_job_snapshot.get("hard_skills")):
        warnings.append("job_profile_result 缺少岗位核心技能要求")
    if safe_float(match_snapshot.get("overall_match_score"), default=0.0) <= 0:
        warnings.append("job_match_result 缺少有效 overall_match_score")
    if not direct_path_options and not transition_path_options:
        warnings.append("当前未解析到可用岗位路径选项，后续 selector 将返回空路径并提示暂无目标岗位路径数据")
    if direct_path_options and all(bool(safe_dict(item).get("is_fallback")) for item in direct_path_options):
        warnings.append("当前直接路径缺少图谱或离线画像中的真实晋升关系")
    if transition_path_options and all(bool(safe_dict(item).get("is_fallback")) for item in transition_path_options):
        warnings.append("当前过渡路径缺少图谱或离线画像中的真实转岗关系")
    return dedup_keep_order(clean_text(item) for item in warnings if clean_text(item))


def build_career_plan_input_payload(
    student_profile_result: Dict[str, Any],
    job_profile_result: Dict[str, Any],
    job_match_result: Dict[str, Any],
    context_data: Optional[Dict[str, Any]] = None,
    output_path: Optional[str | Path] = None,
) -> Dict[str, Any]:
    """主入口：构造 career_plan_input_payload。"""
    student_snapshot = normalize_student_profile_result(student_profile_result)
    target_job_snapshot = normalize_job_profile_result(job_profile_result)
    match_snapshot = normalize_job_match_result(job_match_result)

    candidate_goal_jobs = build_candidate_goal_jobs(
        student_snapshot=student_snapshot,
        target_job_snapshot=target_job_snapshot,
        match_snapshot=match_snapshot,
        context_data=context_data,
    )
    direct_path_options, transition_path_options = build_direct_and_transition_paths(
        student_snapshot=student_snapshot,
        target_job_snapshot=target_job_snapshot,
        match_snapshot=match_snapshot,
        context_data=context_data,
    )
    gap_analysis = build_gap_analysis(
        student_snapshot=student_snapshot,
        target_job_snapshot=target_job_snapshot,
        match_snapshot=match_snapshot,
    )
    planning_constraints = build_planning_constraints(
        student_snapshot=student_snapshot,
        target_job_snapshot=target_job_snapshot,
        match_snapshot=match_snapshot,
    )
    planner_context = build_planner_context(
        student_snapshot=student_snapshot,
        target_job_snapshot=target_job_snapshot,
        match_snapshot=match_snapshot,
        direct_path_options=direct_path_options,
        transition_path_options=transition_path_options,
        gap_analysis=gap_analysis,
        context_data=context_data,
    )
    build_warning_list = build_warnings(
        student_snapshot=student_snapshot,
        target_job_snapshot=target_job_snapshot,
        match_snapshot=match_snapshot,
        direct_path_options=direct_path_options,
        transition_path_options=transition_path_options,
    )

    payload = CareerPlanInputPayload(
        target_job_name=clean_text(target_job_snapshot.get("standard_job_name")),
        student_snapshot=student_snapshot,
        target_job_snapshot=target_job_snapshot,
        match_snapshot=match_snapshot,
        candidate_goal_jobs=candidate_goal_jobs,
        direct_path_options=direct_path_options,
        transition_path_options=transition_path_options,
        gap_analysis=gap_analysis,
        planning_constraints=planning_constraints,
        planner_context=planner_context,
        build_warnings=build_warning_list,
    )
    payload_dict = asdict(payload)

    if output_path:
        save_json(payload_dict, output_path)
    return payload_dict


def load_upstream_results_from_state(
    state_path: str | Path = DEFAULT_STATE_PATH,
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """从 student_api_state.json 读取三个上游模块结果。"""
    state_data = load_json_file(state_path)
    return (
        safe_dict(state_data.get("student_profile_result")),
        safe_dict(state_data.get("job_profile_result")),
        safe_dict(state_data.get("job_match_result")),
    )


def build_career_plan_input_payload_from_state(
    state_path: str | Path = DEFAULT_STATE_PATH,
    context_data: Optional[Dict[str, Any]] = None,
    output_path: Optional[str | Path] = DEFAULT_OUTPUT_PATH,
) -> Dict[str, Any]:
    """从 student_api_state.json 读取上游结果并构造 career_plan_input_payload。"""
    student_profile_result, job_profile_result, job_match_result = load_upstream_results_from_state(state_path)
    return build_career_plan_input_payload(
        student_profile_result=student_profile_result,
        job_profile_result=job_profile_result,
        job_match_result=job_match_result,
        context_data=context_data,
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
        "score_level": "B-具备一定竞争力",
        "strengths": ["具备 Python/SQL 基础", "有项目和实习经历"],
        "weaknesses": ["业务分析经验仍需补强"],
        "missing_dimensions": ["高阶分析方法", "行业项目深度"],
        "summary": "候选人具备数据分析方向基础技能和一定实践经历。",
        "ability_evidence": {
            "project_examples": [{"project_name": "用户行为分析项目", "description": "完成指标分析与可视化"}],
            "internship_examples": [{"company_name": "某科技公司", "position": "数据分析实习生"}],
            "certificate_tags": ["CET-6"],
        },
        "profile_input_payload": {
            "basic_info": {
                "name": "张三",
                "school": "某某大学",
                "major": "计算机科学与技术",
                "degree": "本科",
                "graduation_year": "2026",
            },
            "normalized_education": {
                "degree": "本科",
                "school": "某某大学",
                "major_std": "计算机科学与技术",
                "graduation_year": "2026",
            },
            "explicit_profile": {
                "certificates": ["CET-6"],
                "project_experience": [{"project_name": "用户行为分析项目", "description": "完成指标分析与可视化"}],
                "internship_experience": [{"company_name": "某科技公司", "position": "数据分析实习生"}],
            },
            "normalized_profile": {
                "hard_skills": ["Python", "SQL", "机器学习"],
                "tool_skills": ["Excel", "Tableau"],
                "occupation_hints": ["数据分析"],
                "domain_tags": ["数据智能", "商业分析", "互联网"],
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
        ],
        "industry_distribution": [{"name": "互联网", "count": 10, "ratio": 0.5}],
        "city_distribution": [{"name": "杭州", "count": 8, "ratio": 0.4}],
        "salary_stats": {
            "salary_min_month_avg": 12000,
            "salary_max_month_avg": 18000,
            "salary_mid_month_avg": 15000,
            "valid_salary_count": 20,
        },
    }


def build_demo_job_match_result() -> Dict[str, Any]:
    """构造 demo job_match_result。"""
    return {
        "basic_requirement_score": 100.0,
        "vocational_skill_score": 55.0,
        "professional_quality_score": 86.67,
        "development_potential_score": 92.33,
        "overall_match_score": 78.57,
        "score_level": "C-中等匹配",
        "matched_items": [
            {"dimension": "education", "student_item": "本科", "required_item": "本科"},
            {"dimension": "major", "student_item": "计算机科学与技术", "required_item": "计算机科学与技术"},
            {"dimension": "career_direction", "student_item": "数据分析", "required_item": "数据分析师"},
        ],
        "missing_items": [
            {"dimension": "hard_skills", "required_item": "A/B测试", "reason": "学生侧暂未匹配到岗位要求项：A/B测试"},
            {"dimension": "tool_skills", "required_item": "Power BI", "reason": "学生侧暂未匹配到岗位要求项：Power BI"},
            {"dimension": "soft_skills", "required_item": "逻辑分析", "reason": "学生侧暂未匹配到岗位要求项：逻辑分析"},
        ],
        "strengths": ["学历满足要求", "Python/SQL 技能匹配度较高"],
        "weaknesses": ["行业项目经验不足", "缺少 BI 工具实战证明"],
        "improvement_suggestions": ["补充 1 个真实业务分析项目", "学习 PowerBI/Tableau 并产出作品集"],
        "recommendation": "可以投递该岗位，但建议优先补齐关键技能或项目短板后再重点冲刺。",
        "analysis_summary": "mock 人岗匹配结果",
        "dimension_details": {
            "basic_requirement": {"score": 100.0},
            "vocational_skill": {"score": 55.0},
            "professional_quality": {"score": 86.67},
            "development_potential": {"score": 92.33},
        },
    }


def parse_args() -> argparse.Namespace:
    """命令行参数解析。"""
    parser = argparse.ArgumentParser(description="Build career_path_plan input payload")
    parser.add_argument("--state-path", default="", help="可选：包含三个上游结果的 student_api_state.json 路径")
    parser.add_argument("--student-profile-json", default="", help="可选：单独的 student_profile_result JSON 文件")
    parser.add_argument("--job-profile-json", default="", help="可选：单独的 job_profile_result JSON 文件")
    parser.add_argument("--job-match-json", default="", help="可选：单独的 job_match_result JSON 文件")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="career_plan_input_payload 输出路径")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.state_path:
        payload_result = build_career_plan_input_payload_from_state(
            state_path=args.state_path,
            output_path=args.output,
        )
    elif args.student_profile_json and args.job_profile_json and args.job_match_json:
        payload_result = build_career_plan_input_payload(
            student_profile_result=load_json_file(args.student_profile_json),
            job_profile_result=load_json_file(args.job_profile_json),
            job_match_result=load_json_file(args.job_match_json),
            output_path=args.output,
        )
    else:
        payload_result = build_career_plan_input_payload(
            student_profile_result=build_demo_student_profile_result(),
            job_profile_result=build_demo_job_profile_result(),
            job_match_result=build_demo_job_match_result(),
            output_path=args.output,
        )

    print(json.dumps(payload_result, ensure_ascii=False, indent=2))
