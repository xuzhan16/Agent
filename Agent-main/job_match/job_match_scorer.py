"""
job_match_scorer.py

人岗匹配模块的规则评分层。

职责边界：
1. 对 student_profile_result 和 job_profile_result 做显式匹配；
2. 计算四个维度分数：
   - basic_requirement_score
   - vocational_skill_score
   - professional_quality_score
   - development_potential_score
3. 计算 overall_match_score；
4. 输出 matched_items 和 missing_items；
5. 不依赖复杂机器学习框架，优先使用可解释的规则评分。
"""

from __future__ import annotations

import argparse
import json
import re
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .job_match_builder import (
    build_demo_job_profile_result,
    build_demo_student_profile_result,
    build_match_input_payload,
    build_match_input_payload_from_state,
)


DEFAULT_OUTPUT_PATH = Path("outputs/state/job_match_score_result.json")


SOFT_SKILL_ALIAS_MAP: Dict[str, List[str]] = {
    "沟通协作": ["沟通能力", "沟通协作", "协作能力", "团队协作", "跨部门沟通"],
    "学习能力": ["学习能力", "快速学习", "自驱学习", "主动学习"],
    "责任心": ["责任心", "认真负责", "owner意识", "主人翁"],
    "逻辑分析": ["逻辑分析", "逻辑思维", "结构化思考", "分析能力", "问题拆解"],
    "执行抗压": ["执行力", "抗压能力", "结果导向", "推动落地"],
    "团队合作": ["团队合作", "团队协作", "合作意识"],
    "创新意识": ["创新意识", "创新", "探索精神", "好奇心"],
}


JOB_LEVEL_RANK_MAP: Dict[str, int] = {
    "": 0,
    "实习": 1,
    "初级": 2,
    "中级": 3,
    "高级": 4,
    "专家": 5,
    "负责人": 5,
}


@dataclass
class DimensionScoreResult:
    """单个匹配维度的评分结果。"""

    score: float = 0.0
    matched_items: List[Dict[str, Any]] = field(default_factory=list)
    missing_items: List[Dict[str, Any]] = field(default_factory=list)
    score_reasons: List[str] = field(default_factory=list)
    detail: Dict[str, Any] = field(default_factory=dict)


@dataclass
class JobMatchScoreResult:
    """最终人岗匹配规则评分结果。"""

    basic_requirement_score: float = 0.0
    vocational_skill_score: float = 0.0
    professional_quality_score: float = 0.0
    development_potential_score: float = 0.0
    overall_match_score: float = 0.0
    score_level: str = ""
    matched_items: List[Dict[str, Any]] = field(default_factory=list)
    missing_items: List[Dict[str, Any]] = field(default_factory=list)
    dimension_details: Dict[str, Any] = field(default_factory=dict)
    score_weights: Dict[str, float] = field(default_factory=dict)
    rule_summary: str = ""
    match_input_payload: Dict[str, Any] = field(default_factory=dict)


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
    """将 list / JSON 字符串 / 分隔符字符串统一转成 list。"""
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


def normalize_tag_token(value: Any) -> str:
    """压缩标签文本，便于做轻量模糊匹配。"""
    text = clean_text(value).lower()
    text = re.sub(r"^(证书|学历|学校|专业|项目|实习|技能|工具)[:：]", "", text)
    return re.sub(r"[()（）\[\]【】<>《》\-_/\\|·,，;；:：+.#\s]", "", text)


def normalize_tag_list(value: Any) -> List[str]:
    """统一标签列表格式。"""
    return dedup_keep_order(
        clean_text(item)
        for item in parse_list_like(value)
        if clean_text(item)
    )


def normalize_soft_skill_tag(value: Any) -> str:
    """对软技能标签做轻量别名归一。"""
    raw = clean_text(value)
    compact = normalize_tag_token(raw)
    if not compact:
        return ""

    for standard_name, aliases in SOFT_SKILL_ALIAS_MAP.items():
        if compact == normalize_tag_token(standard_name):
            return standard_name
        for alias in aliases:
            alias_compact = normalize_tag_token(alias)
            if compact == alias_compact:
                return standard_name
            if alias_compact and alias_compact in compact:
                return standard_name
            if compact and compact in alias_compact:
                return standard_name
    return raw


def normalize_soft_skill_list(value: Any) -> List[str]:
    """归一化软技能列表。"""
    return dedup_keep_order(
        normalize_soft_skill_tag(item)
        for item in parse_list_like(value)
        if clean_text(item)
    )


def token_similarity(left: Any, right: Any) -> float:
    """轻量标签相似度：完全相等 > 子串包含 > 字符 Dice 系数。"""
    left_token = normalize_tag_token(left)
    right_token = normalize_tag_token(right)
    if not left_token or not right_token:
        return 0.0
    if left_token == right_token:
        return 1.0
    if left_token in right_token or right_token in left_token:
        return 0.9

    left_chars = set(left_token)
    right_chars = set(right_token)
    if not left_chars or not right_chars:
        return 0.0
    return round(2.0 * len(left_chars & right_chars) / (len(left_chars) + len(right_chars)), 4)


def match_required_tags(
    student_tags: Sequence[Any],
    job_tags: Sequence[Any],
    dimension_name: str,
    min_similarity: float = 0.75,
) -> Tuple[float, List[Dict[str, Any]], List[Dict[str, Any]]]:
    """对岗位要求标签和学生标签做可解释匹配。"""
    normalized_student_tags = dedup_keep_order(clean_text(item) for item in student_tags if clean_text(item))
    normalized_job_tags = dedup_keep_order(clean_text(item) for item in job_tags if clean_text(item))

    if not normalized_job_tags:
        return 100.0, [], []
    if not normalized_student_tags:
        missing_items = [
            {
                "dimension": dimension_name,
                "required_item": job_tag,
                "reason": f"学生侧缺少可与岗位 {dimension_name} 要求比较的标签",
            }
            for job_tag in normalized_job_tags
        ]
        return 0.0, [], missing_items

    candidate_pairs = []
    for job_tag in normalized_job_tags:
        for student_tag in normalized_student_tags:
            similarity = token_similarity(student_tag, job_tag)
            if similarity >= min_similarity:
                candidate_pairs.append((similarity, student_tag, job_tag))
    candidate_pairs.sort(key=lambda item: item[0], reverse=True)

    used_student_tags = set()
    matched_job_tags = set()
    matched_items = []
    for similarity, student_tag, job_tag in candidate_pairs:
        if student_tag in used_student_tags or job_tag in matched_job_tags:
            continue
        used_student_tags.add(student_tag)
        matched_job_tags.add(job_tag)
        matched_items.append(
            {
                "dimension": dimension_name,
                "student_item": student_tag,
                "required_item": job_tag,
                "similarity": round(float(similarity), 4),
            }
        )

    missing_items = [
        {
            "dimension": dimension_name,
            "required_item": job_tag,
            "reason": f"学生侧暂未匹配到岗位要求项：{job_tag}",
        }
        for job_tag in normalized_job_tags
        if job_tag not in matched_job_tags
    ]
    return round(len(matched_job_tags) / len(normalized_job_tags) * 100.0, 2), matched_items, missing_items


def match_major(
    student_major: Any,
    job_major_requirement: Any,
) -> Tuple[float, List[Dict[str, Any]], List[Dict[str, Any]]]:
    """专业匹配评分。"""
    student_major_text = clean_text(student_major)
    job_majors = normalize_tag_list(job_major_requirement)

    if not job_majors:
        return 100.0, [], []
    if not student_major_text:
        return 0.0, [], [{
            "dimension": "major",
            "required_item": " / ".join(job_majors),
            "reason": "学生侧缺少专业字段",
        }]

    best_job_major = ""
    best_similarity = 0.0
    for job_major in job_majors:
        similarity = token_similarity(student_major_text, job_major)
        if similarity > best_similarity:
            best_similarity = similarity
            best_job_major = job_major

    if best_similarity >= 0.75:
        score = 100.0
    elif best_similarity >= 0.5:
        score = 75.0
    elif best_similarity >= 0.3:
        score = 45.0
    else:
        score = 20.0

    if best_similarity >= 0.5:
        matched_items = [{
            "dimension": "major",
            "student_item": student_major_text,
            "required_item": best_job_major,
            "similarity": round(best_similarity, 4),
        }]
        missing_items = []
    else:
        matched_items = []
        missing_items = [{
            "dimension": "major",
            "student_item": student_major_text,
            "required_item": " / ".join(job_majors),
            "reason": "学生专业与岗位偏好专业重合度较低",
            "similarity": round(best_similarity, 4),
        }]
    return round(score, 2), matched_items, missing_items


def score_degree_requirement(
    education_schema: Dict[str, Any],
) -> Tuple[float, List[Dict[str, Any]], List[Dict[str, Any]], List[str], Dict[str, Any]]:
    """学历要求匹配评分。"""
    student_degree = clean_text(education_schema.get("student_degree"))
    job_degree_requirement = clean_text(education_schema.get("job_degree_requirement"))
    student_degree_rank = int(education_schema.get("student_degree_rank") or 0)
    job_degree_rank = int(education_schema.get("job_degree_rank_requirement") or 0)

    detail = {
        "student_degree": student_degree,
        "job_degree_requirement": job_degree_requirement,
        "student_degree_rank": student_degree_rank,
        "job_degree_rank_requirement": job_degree_rank,
    }

    if job_degree_rank <= 0:
        return 100.0, [], [], ["岗位未设置明确学历门槛，学历项默认满分。"], detail

    if student_degree_rank <= 0:
        return 0.0, [], [{
            "dimension": "education",
            "required_item": job_degree_requirement,
            "reason": "学生侧缺少学历字段，无法满足岗位学历门槛判断",
        }], ["学生侧缺少学历字段，学历要求项记为 0 分。"], detail

    if student_degree_rank >= job_degree_rank:
        return 100.0, [{
            "dimension": "education",
            "student_item": student_degree,
            "required_item": job_degree_requirement,
            "match_type": "学历门槛满足",
        }], [], [f"学生学历 {student_degree} 满足岗位学历要求 {job_degree_requirement}。"], detail

    if student_degree_rank == job_degree_rank - 1:
        return 60.0, [], [{
            "dimension": "education",
            "student_item": student_degree,
            "required_item": job_degree_requirement,
            "reason": "学历略低于岗位要求",
        }], [f"学生学历 {student_degree} 略低于岗位学历要求 {job_degree_requirement}。"], detail

    return 20.0, [], [{
        "dimension": "education",
        "student_item": student_degree,
        "required_item": job_degree_requirement,
        "reason": "学历与岗位要求存在较明显差距",
    }], [f"学生学历 {student_degree} 明显低于岗位学历要求 {job_degree_requirement}。"], detail


def score_basic_requirement(match_input_payload: Dict[str, Any]) -> Dict[str, Any]:
    """计算 basic_requirement_score：学历、专业、证书。"""
    comparable_schema = safe_dict(match_input_payload.get("comparable_schema"))
    education_schema = safe_dict(comparable_schema.get("education"))
    major_schema = safe_dict(comparable_schema.get("major"))
    certificate_schema = safe_dict(comparable_schema.get("certificates"))

    degree_score, degree_matched, degree_missing, degree_reasons, degree_detail = score_degree_requirement(
        education_schema
    )
    major_score, major_matched, major_missing = match_major(
        major_schema.get("student_major"),
        major_schema.get("job_major_requirement"),
    )
    cert_score, cert_matched, cert_missing = match_required_tags(
        student_tags=normalize_tag_list(certificate_schema.get("student_certificates")),
        job_tags=normalize_tag_list(certificate_schema.get("job_certificate_requirement")),
        dimension_name="certificates",
        min_similarity=0.8,
    )

    result = DimensionScoreResult(
        score=round(degree_score * 0.45 + major_score * 0.35 + cert_score * 0.20, 2),
        matched_items=dedup_keep_order(degree_matched + major_matched + cert_matched),
        missing_items=dedup_keep_order(degree_missing + major_missing + cert_missing),
        score_reasons=dedup_keep_order(
            degree_reasons + [f"专业匹配得分 {major_score:.2f}，证书匹配得分 {cert_score:.2f}。"]
        ),
        detail={
            "degree_score": degree_score,
            "major_score": major_score,
            "certificate_score": cert_score,
            "degree_detail": degree_detail,
        },
    )
    return asdict(result)


def score_vocational_skill(match_input_payload: Dict[str, Any]) -> Dict[str, Any]:
    """计算 vocational_skill_score：硬技能、工具技能。"""
    comparable_schema = safe_dict(match_input_payload.get("comparable_schema"))
    hard_skill_schema = safe_dict(comparable_schema.get("hard_skills"))
    tool_skill_schema = safe_dict(comparable_schema.get("tool_skills"))

    hard_score, hard_matched, hard_missing = match_required_tags(
        student_tags=normalize_tag_list(hard_skill_schema.get("student_hard_skills")),
        job_tags=normalize_tag_list(hard_skill_schema.get("job_hard_skills")),
        dimension_name="hard_skills",
        min_similarity=0.75,
    )
    tool_score, tool_matched, tool_missing = match_required_tags(
        student_tags=normalize_tag_list(tool_skill_schema.get("student_tool_skills")),
        job_tags=normalize_tag_list(tool_skill_schema.get("job_tool_skills")),
        dimension_name="tool_skills",
        min_similarity=0.8,
    )

    reasons = [f"硬技能覆盖得分 {hard_score:.2f}，工具技能覆盖得分 {tool_score:.2f}。"]
    if hard_score >= 80:
        reasons.append("学生硬技能与岗位核心技能要求匹配度较高。")
    elif hard_score >= 50:
        reasons.append("学生已覆盖部分核心硬技能，但仍存在岗位技能缺口。")
    else:
        reasons.append("学生硬技能与岗位要求重合度偏低，需要优先补齐核心技能。")

    return asdict(
        DimensionScoreResult(
            score=round(hard_score * 0.70 + tool_score * 0.30, 2),
            matched_items=dedup_keep_order(hard_matched + tool_matched),
            missing_items=dedup_keep_order(hard_missing + tool_missing),
            score_reasons=dedup_keep_order(reasons),
            detail={"hard_skill_score": hard_score, "tool_skill_score": tool_score},
        )
    )


def is_practice_requirement_matched(
    requirement_tag: str,
    student_experience_tags: Sequence[str],
    project_count: int,
    internship_count: int,
    student_soft_skills: Sequence[str],
) -> Tuple[bool, str]:
    """判断单个实践/经验要求是否被学生侧经历满足。"""
    req = normalize_tag_token(requirement_tag)
    exp_tags = [clean_text(item) for item in student_experience_tags if clean_text(item)]
    soft_tags = normalize_soft_skill_list(student_soft_skills)

    if not req:
        return False, ""
    if any(keyword in req for keyword in ["经验不限", "应届可投", "可实习"]):
        return True, "岗位接受应届/经验不限，默认认为实践门槛满足。"
    if "项目" in req:
        if project_count > 0 or any("项目" in normalize_tag_token(item) for item in exp_tags):
            return True, "学生已有项目经历或项目标签，满足项目相关要求。"
        return False, "岗位要求项目经验，但学生侧项目经历不足。"
    if "实习" in req or "实践" in req:
        if internship_count > 0 or any("实习" in normalize_tag_token(item) for item in exp_tags):
            return True, "学生已有实习/实践经历，满足实践相关要求。"
        return False, "岗位要求实习/实践经历，但学生侧暂未体现。"
    if "出差" in req or "驻场" in req:
        if any("出差" in normalize_tag_token(item) or "驻场" in normalize_tag_token(item) for item in exp_tags):
            return True, "学生经历标签中已有出差/驻场相关信号。"
        return False, "岗位要求可出差/驻场，但学生侧暂无对应经历或意向信号。"
    if "跨部门" in req or "协作" in req or "沟通" in req:
        if any(skill in {"沟通协作", "团队合作"} for skill in soft_tags):
            return True, "学生软技能中已有沟通协作类标签，可支撑跨部门协作要求。"
        return False, "岗位强调沟通/协作，但学生侧相关软技能标签不足。"

    for exp_tag in exp_tags:
        if token_similarity(requirement_tag, exp_tag) >= 0.65:
            return True, f"学生经历标签“{exp_tag}”与岗位要求“{requirement_tag}”存在较高重合。"
    return False, f"学生侧暂未匹配到经验/实践要求：{requirement_tag}"


def score_practice_experience(
    student_experience_tags: Sequence[str],
    project_count: int,
    internship_count: int,
    job_experience_requirement: Sequence[str],
    job_practice_requirement: Sequence[str],
    student_soft_skills: Sequence[str],
) -> Tuple[float, List[Dict[str, Any]], List[Dict[str, Any]], List[str], Dict[str, Any]]:
    """计算实践经历/经验要求匹配得分。"""
    requirements = dedup_keep_order(
        [clean_text(item) for item in job_experience_requirement if clean_text(item)]
        + [clean_text(item) for item in job_practice_requirement if clean_text(item)]
    )

    if not requirements:
        return 100.0, [], [], ["岗位未提供明确实践/经验要求，该项默认满分。"], {
            "requirement_count": 0,
            "project_count": project_count,
            "internship_count": internship_count,
        }

    matched_items = []
    missing_items = []
    reasons = []
    for requirement in requirements:
        matched, reason = is_practice_requirement_matched(
            requirement_tag=requirement,
            student_experience_tags=student_experience_tags,
            project_count=project_count,
            internship_count=internship_count,
            student_soft_skills=student_soft_skills,
        )
        if matched:
            matched_items.append({
                "dimension": "practice_experience",
                "student_item": " / ".join(normalize_tag_list(student_experience_tags)) or "项目/实习经历",
                "required_item": requirement,
                "reason": reason,
            })
        else:
            missing_items.append({
                "dimension": "practice_experience",
                "required_item": requirement,
                "reason": reason,
            })
        if reason:
            reasons.append(reason)

    return (
        round(len(matched_items) / len(requirements) * 100.0, 2),
        dedup_keep_order(matched_items),
        dedup_keep_order(missing_items),
        dedup_keep_order(reasons),
        {
            "requirement_count": len(requirements),
            "matched_requirement_count": len(matched_items),
            "project_count": int(project_count),
            "internship_count": int(internship_count),
        },
    )


def score_professional_quality(match_input_payload: Dict[str, Any]) -> Dict[str, Any]:
    """计算 professional_quality_score：实践经历、经验要求、软技能。"""
    comparable_schema = safe_dict(match_input_payload.get("comparable_schema"))
    practice_schema = safe_dict(comparable_schema.get("practice_experience"))
    soft_skill_schema = safe_dict(comparable_schema.get("soft_skills"))

    student_soft_skills = normalize_soft_skill_list(soft_skill_schema.get("student_soft_skills"))
    job_soft_skills = normalize_soft_skill_list(soft_skill_schema.get("job_soft_skills"))
    soft_score, soft_matched, soft_missing = match_required_tags(
        student_tags=student_soft_skills,
        job_tags=job_soft_skills,
        dimension_name="soft_skills",
        min_similarity=0.7,
    )
    practice_score, practice_matched, practice_missing, practice_reasons, practice_detail = score_practice_experience(
        student_experience_tags=normalize_tag_list(practice_schema.get("student_experience_tags")),
        project_count=int(practice_schema.get("student_project_count") or 0),
        internship_count=int(practice_schema.get("student_internship_count") or 0),
        job_experience_requirement=normalize_tag_list(practice_schema.get("job_experience_requirement")),
        job_practice_requirement=normalize_tag_list(practice_schema.get("job_practice_requirement")),
        student_soft_skills=student_soft_skills,
    )

    return asdict(
        DimensionScoreResult(
            score=round(practice_score * 0.60 + soft_score * 0.40, 2),
            matched_items=dedup_keep_order(practice_matched + soft_matched),
            missing_items=dedup_keep_order(practice_missing + soft_missing),
            score_reasons=dedup_keep_order(
                practice_reasons + [f"软技能覆盖得分 {soft_score:.2f}，实践经历/经验要求得分 {practice_score:.2f}。"]
            ),
            detail={
                "practice_score": practice_score,
                "soft_skill_score": soft_score,
                "practice_detail": practice_detail,
            },
        )
    )


def infer_student_level_rank_from_score(competitiveness_score: float) -> int:
    """根据学生竞争力分数粗略推断当前可承接岗位层级。"""
    if competitiveness_score >= 90:
        return 4
    if competitiveness_score >= 80:
        return 3
    if competitiveness_score >= 65:
        return 2
    if competitiveness_score >= 50:
        return 1
    return 0


def normalize_job_level_rank(job_level: Any) -> int:
    """将岗位层级文本映射为等级。"""
    text = clean_text(job_level)
    if not text:
        return 0
    for keyword, rank in JOB_LEVEL_RANK_MAP.items():
        if keyword and keyword in text:
            return rank
    return 0


def score_career_direction_alignment(
    student_occupation_hints: Sequence[str],
    student_domain_tags: Sequence[str],
    job_name: str,
    job_category: str,
) -> Tuple[float, List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    """计算职业方向和岗位方向一致性。"""
    student_tags = dedup_keep_order(
        [clean_text(item) for item in student_occupation_hints if clean_text(item)]
        + [clean_text(item) for item in student_domain_tags if clean_text(item)]
    )
    job_tags = dedup_keep_order([clean_text(job_name), clean_text(job_category)])

    score, matched_items, missing_items = match_required_tags(
        student_tags=student_tags,
        job_tags=job_tags,
        dimension_name="career_direction",
        min_similarity=0.55,
    )
    return score, matched_items, missing_items, {
        "student_direction_tags": student_tags,
        "job_direction_tags": [tag for tag in job_tags if clean_text(tag)],
    }


def score_development_potential(match_input_payload: Dict[str, Any]) -> Dict[str, Any]:
    """计算 development_potential_score：潜力标签、职业方向、成长基础。"""
    student_profile = safe_dict(match_input_payload.get("student_profile"))
    job_profile = safe_dict(match_input_payload.get("job_profile"))
    career_direction_schema = safe_dict(
        safe_dict(match_input_payload.get("comparable_schema")).get("career_direction")
    )

    direction_score, direction_matched, direction_missing, direction_detail = score_career_direction_alignment(
        student_occupation_hints=normalize_tag_list(career_direction_schema.get("student_occupation_hints")),
        student_domain_tags=normalize_tag_list(career_direction_schema.get("student_domain_tags")),
        job_name=clean_text(career_direction_schema.get("job_name")),
        job_category=clean_text(career_direction_schema.get("job_category")),
    )

    complete_score = max(0.0, min(100.0, safe_float(student_profile.get("complete_score"), default=0.0)))
    competitiveness_score = max(0.0, min(100.0, safe_float(student_profile.get("competitiveness_score"), default=0.0)))
    readiness_score = round(complete_score * 0.35 + competitiveness_score * 0.65, 2)

    student_level_rank = infer_student_level_rank_from_score(competitiveness_score)
    job_level_rank = normalize_job_level_rank(job_profile.get("job_level"))
    if job_level_rank <= 0:
        level_fit_score = 100.0
        level_reason = "岗位层级未明确，层级适配项默认满分。"
        level_matched = []
        level_missing = []
    elif student_level_rank >= job_level_rank:
        level_fit_score = 100.0
        level_reason = f"学生当前竞争力可承接岗位层级 {clean_text(job_profile.get('job_level'))}。"
        level_matched = [{
            "dimension": "development_potential",
            "student_item": f"竞争力水平:{competitiveness_score:.1f}",
            "required_item": clean_text(job_profile.get("job_level")),
            "reason": level_reason,
        }]
        level_missing = []
    elif student_level_rank == job_level_rank - 1:
        level_fit_score = 70.0
        level_reason = f"学生发展潜力接近岗位层级 {clean_text(job_profile.get('job_level'))}，但仍需补强关键能力。"
        level_matched = []
        level_missing = [{
            "dimension": "development_potential",
            "required_item": clean_text(job_profile.get("job_level")),
            "reason": level_reason,
        }]
    else:
        level_fit_score = 40.0
        level_reason = f"学生当前竞争力与岗位层级 {clean_text(job_profile.get('job_level'))} 仍有明显差距。"
        level_matched = []
        level_missing = [{
            "dimension": "development_potential",
            "required_item": clean_text(job_profile.get("job_level")),
            "reason": level_reason,
        }]

    return asdict(
        DimensionScoreResult(
            score=round(direction_score * 0.45 + readiness_score * 0.35 + level_fit_score * 0.20, 2),
            matched_items=dedup_keep_order(direction_matched + level_matched),
            missing_items=dedup_keep_order(direction_missing + level_missing),
            score_reasons=dedup_keep_order([
                f"职业方向一致性得分 {direction_score:.2f}。",
                f"学生完整度/竞争力形成的发展基础得分 {readiness_score:.2f}。",
                f"岗位层级适配得分 {level_fit_score:.2f}。{level_reason}",
            ]),
            detail={
                "direction_alignment_score": direction_score,
                "readiness_score": readiness_score,
                "level_fit_score": level_fit_score,
                "complete_score": complete_score,
                "competitiveness_score": competitiveness_score,
                "student_level_rank": student_level_rank,
                "job_level_rank": job_level_rank,
                "direction_detail": direction_detail,
            },
        )
    )


def infer_score_level(score: float) -> str:
    """根据 overall_match_score 输出匹配等级。"""
    if score >= 90:
        return "A-高度匹配"
    if score >= 80:
        return "B-较高匹配"
    if score >= 70:
        return "C-中等匹配"
    if score >= 60:
        return "D-勉强匹配"
    return "E-匹配度较低"


def build_rule_summary(result: Dict[str, Any]) -> str:
    """生成简要规则评分摘要。"""
    return (
        f"规则侧人岗匹配总分为 {safe_float(result.get('overall_match_score')):.2f}，"
        f"等级为{clean_text(result.get('score_level'))}。"
        f"其中基础要求 {safe_float(result.get('basic_requirement_score')):.2f}，"
        f"职业技能 {safe_float(result.get('vocational_skill_score')):.2f}，"
        f"职业素质 {safe_float(result.get('professional_quality_score')):.2f}，"
        f"发展潜力 {safe_float(result.get('development_potential_score')):.2f}。"
        f"当前仍有 {len(parse_list_like(result.get('missing_items')))} 项岗位要求或能力维度存在缺口。"
    )


def score_match_input_payload(
    match_input_payload: Dict[str, Any],
    output_path: Optional[str | Path] = None,
    score_weights: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    """主评分入口：输入 match_input_payload，输出结构化规则评分结果。"""
    payload = safe_dict(match_input_payload)
    weights = {
        "basic_requirement_score": 0.30,
        "vocational_skill_score": 0.40,
        "professional_quality_score": 0.20,
        "development_potential_score": 0.10,
    }
    if isinstance(score_weights, dict):
        for key, value in score_weights.items():
            if key in weights:
                weights[key] = safe_float(value, default=weights[key])
    total_weight = sum(weights.values()) or 1.0
    weights = {key: value / total_weight for key, value in weights.items()}

    basic_result = score_basic_requirement(payload)
    skill_result = score_vocational_skill(payload)
    quality_result = score_professional_quality(payload)
    potential_result = score_development_potential(payload)

    overall_score = round(
        safe_float(basic_result.get("score")) * weights["basic_requirement_score"]
        + safe_float(skill_result.get("score")) * weights["vocational_skill_score"]
        + safe_float(quality_result.get("score")) * weights["professional_quality_score"]
        + safe_float(potential_result.get("score")) * weights["development_potential_score"],
        2,
    )

    result = JobMatchScoreResult(
        basic_requirement_score=safe_float(basic_result.get("score")),
        vocational_skill_score=safe_float(skill_result.get("score")),
        professional_quality_score=safe_float(quality_result.get("score")),
        development_potential_score=safe_float(potential_result.get("score")),
        overall_match_score=overall_score,
        score_level=infer_score_level(overall_score),
        matched_items=dedup_keep_order(
            parse_list_like(basic_result.get("matched_items"))
            + parse_list_like(skill_result.get("matched_items"))
            + parse_list_like(quality_result.get("matched_items"))
            + parse_list_like(potential_result.get("matched_items"))
        ),
        missing_items=dedup_keep_order(
            parse_list_like(basic_result.get("missing_items"))
            + parse_list_like(skill_result.get("missing_items"))
            + parse_list_like(quality_result.get("missing_items"))
            + parse_list_like(potential_result.get("missing_items"))
        ),
        dimension_details={
            "basic_requirement": basic_result,
            "vocational_skill": skill_result,
            "professional_quality": quality_result,
            "development_potential": potential_result,
        },
        score_weights=weights,
        rule_summary="",
        match_input_payload=deepcopy(payload),
    )
    result_dict = asdict(result)
    result_dict["rule_summary"] = build_rule_summary(result_dict)
    if output_path:
        save_json(result_dict, output_path)
    return result_dict


def score_student_job_profile_result(
    student_profile_result: Dict[str, Any],
    job_profile_result: Dict[str, Any],
    output_path: Optional[str | Path] = None,
) -> Dict[str, Any]:
    """直接输入两个画像结果，内部先构造 match_input_payload 再评分。"""
    payload = build_match_input_payload(
        student_profile_result=student_profile_result,
        job_profile_result=job_profile_result,
        output_path=None,
    )
    return score_match_input_payload(match_input_payload=payload, output_path=output_path)


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


def save_json(data: Dict[str, Any], output_path: str | Path) -> None:
    """保存 JSON 输出。"""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def parse_args() -> argparse.Namespace:
    """命令行参数解析。"""
    parser = argparse.ArgumentParser(description="Rule-based scorer for job_match module")
    parser.add_argument("--input", default="", help="可选：match_input_payload JSON 路径")
    parser.add_argument("--state-path", default="", help="可选：包含 student_profile_result 和 job_profile_result 的 student_api_state.json 路径")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="评分结果输出路径")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.input:
        score_result = score_match_input_payload(
            match_input_payload=load_json_file(args.input),
            output_path=args.output,
        )
    elif args.state_path:
        payload_data = build_match_input_payload_from_state(
            state_path=args.state_path,
            output_path=None,
        )
        score_result = score_match_input_payload(
            match_input_payload=payload_data,
            output_path=args.output,
        )
    else:
        score_result = score_student_job_profile_result(
            student_profile_result=build_demo_student_profile_result(),
            job_profile_result=build_demo_job_profile_result(),
            output_path=args.output,
        )

    print(json.dumps(score_result, ensure_ascii=False, indent=2))
