"""
contest_match_evaluator.py

Frontend display and contest-oriented matching layer.
"""

from __future__ import annotations

import json
import re
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .job_match_builder import degree_to_rank, normalize_degree_text
from .match_asset_loader import MatchAssetLoader, clean_text, safe_dict, safe_list


RISK_HIGH_MATCH = "high_match"
RISK_RISK = "risk"
RISK_NO_MATCH = "no_match"


def safe_float(value: Any, default: float = 0.0) -> float:
    text = clean_text(value)
    if not text:
        return default
    try:
        return float(text)
    except (TypeError, ValueError):
        return default


def dedup_keep_order(values: Iterable[Any]) -> List[Any]:
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
    return dedup_keep_order(clean_text(part) for part in re.split(r"[、,，;；/|｜\n]+", text) if clean_text(part))


def normalize_tag_list(value: Any) -> List[str]:
    return dedup_keep_order(clean_text(item) for item in parse_list_like(value) if clean_text(item))


def normalize_tag_token(value: Any) -> str:
    text = clean_text(value).lower()
    text = re.sub(r"^(证书|学历|学校|专业|项目|实习|技能|工具|知识点)[:：]", "", text)
    return re.sub(r"[()（）\[\]【】<>《》\-_/\\|·,，;；:：+.#\s]", "", text)


def token_similarity(left: Any, right: Any) -> float:
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


def match_best_similarity(candidate: Any, requirements: Sequence[Any]) -> Tuple[float, str]:
    best_score = 0.0
    best_item = ""
    for item in requirements:
        score = token_similarity(candidate, item)
        if score > best_score:
            best_score = score
            best_item = clean_text(item)
    return best_score, best_item


def list_contains_match(values: Sequence[Any], target: Any, min_similarity: float = 0.75) -> bool:
    return any(token_similarity(value, target) >= min_similarity for value in values)


def build_requirement_distributions(stats: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "degree_distribution": deepcopy(safe_list(stats.get("degree_distribution"))),
        "major_distribution": deepcopy(safe_list(stats.get("major_distribution"))),
        "certificate_distribution": deepcopy(safe_list(stats.get("certificate_distribution"))),
        "no_certificate_requirement_ratio": safe_float(stats.get("no_certificate_requirement_ratio"), default=0.0),
    }


def evaluate_degree(student_profile: Dict[str, Any], stats: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    student_degree = clean_text(student_profile.get("degree"))
    student_rank = int(student_profile.get("degree_rank") or degree_to_rank(student_degree))
    degree_gate = normalize_degree_text(stats.get("degree_gate"))
    gate_rank = degree_to_rank(degree_gate)
    mainstream_requirement = clean_text(stats.get("mainstream_degree") or degree_gate)
    mainstream_ratio = safe_float(stats.get("mainstream_degree_ratio"), default=0.0)

    qualified_ratio = 0.0
    higher_requirement_ratio = 0.0
    for item in safe_list(stats.get("degree_distribution")):
        item_dict = safe_dict(item)
        name = clean_text(item_dict.get("name"))
        ratio = safe_float(item_dict.get("ratio"), default=0.0)
        rank = degree_to_rank(name)
        if rank <= 0 or student_rank >= rank:
            qualified_ratio += ratio
        else:
            higher_requirement_ratio += ratio

    if gate_rank <= 0:
        passed = True
        reason = "岗位未形成明确学历硬门槛。"
    elif student_rank <= 0:
        passed = False
        reason = "学生侧缺少学历信息，无法满足岗位学历硬门槛。"
    elif student_rank >= gate_rank:
        passed = True
        reason = f"学生学历达到岗位主流门槛：{degree_gate}。"
    else:
        passed = False
        reason = f"学生学历为{student_degree or '未填写'}，低于岗位主流门槛：{degree_gate}。"

    if passed and higher_requirement_ratio <= 0.2:
        risk_level = RISK_HIGH_MATCH
        message = "学历达到岗位主流要求，学历风险较低。"
    elif qualified_ratio > 0:
        risk_level = RISK_RISK
        message = "学历可满足部分企业要求，但仍存在更高学历样本，建议用项目和技能增强竞争力。"
    else:
        risk_level = RISK_NO_MATCH
        message = "学历暂未覆盖岗位样本中的主流要求，存在明显风险。"

    display = {
        "student_value": student_degree,
        "mainstream_requirement": mainstream_requirement,
        "mainstream_ratio": round(mainstream_ratio, 4),
        "qualified_ratio": round(min(qualified_ratio, 1.0), 4),
        "higher_requirement_ratio": round(min(higher_requirement_ratio, 1.0), 4),
        "risk_level": risk_level,
        "message": message,
    }
    evaluation = {
        "student_value": student_degree,
        "job_gate": degree_gate,
        "pass": bool(passed),
        "reason": reason,
    }
    return display, evaluation


def evaluate_major(student_profile: Dict[str, Any], stats: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    student_major = clean_text(student_profile.get("major"))
    gate_set = normalize_tag_list(stats.get("major_gate_set"))
    mainstream_majors = normalize_tag_list(stats.get("mainstream_majors")) or gate_set

    matched_ratio = 0.0
    for item in safe_list(stats.get("major_distribution")):
        item_dict = safe_dict(item)
        name = clean_text(item_dict.get("name"))
        if name and name != "未明确" and token_similarity(student_major, name) >= 0.75:
            matched_ratio += safe_float(item_dict.get("ratio"), default=0.0)

    best_similarity, best_major = match_best_similarity(student_major, gate_set or mainstream_majors)
    if not gate_set:
        passed = True
        reason = "岗位未形成明确专业硬门槛。"
    elif not student_major:
        passed = False
        reason = "学生侧缺少专业信息，无法满足岗位专业要求。"
    elif best_similarity >= 0.75:
        passed = True
        reason = f"学生专业与岗位要求专业“{best_major}”匹配。"
    else:
        passed = False
        reason = "学生专业与岗位主流专业集合重合度较低。"

    if passed and (matched_ratio >= 0.3 or not gate_set):
        risk_level = RISK_HIGH_MATCH
        message = "专业属于岗位主流相关方向，专业匹配风险较低。"
    elif matched_ratio > 0 or best_similarity >= 0.5:
        risk_level = RISK_RISK
        message = "专业与岗位存在一定相关性，但不是最主流要求，建议用项目经历强化方向证明。"
    else:
        risk_level = RISK_NO_MATCH
        message = "专业与岗位主流要求重合较弱，存在明显专业匹配风险。"

    display = {
        "student_value": student_major,
        "mainstream_majors": deepcopy(mainstream_majors),
        "matched_ratio": round(min(matched_ratio, 1.0), 4),
        "risk_level": risk_level,
        "message": message,
    }
    evaluation = {
        "student_value": student_major,
        "job_gate_set": deepcopy(gate_set),
        "pass": bool(passed),
        "reason": reason,
    }
    return display, evaluation


def evaluate_certificate(student_profile: Dict[str, Any], stats: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    student_values = normalize_tag_list(student_profile.get("certificates"))
    must_have = normalize_tag_list(stats.get("must_have_certificates"))
    preferred = normalize_tag_list(stats.get("preferred_certificates"))
    no_cert_ratio = safe_float(stats.get("no_certificate_requirement_ratio"), default=0.0)

    matched_must = [cert for cert in must_have if list_contains_match(student_values, cert)]
    matched_preferred = [cert for cert in preferred if list_contains_match(student_values, cert)]
    total_requirements = len(must_have) + len(preferred)
    matched_ratio = 1.0 if total_requirements <= 0 else (len(matched_must) + len(matched_preferred)) / total_requirements

    if not must_have:
        passed = True
        reason = "岗位未形成强制证书硬门槛。"
    elif len(matched_must) == len(must_have):
        passed = True
        reason = "学生已满足岗位强制证书要求。"
    else:
        missing = [cert for cert in must_have if cert not in matched_must]
        passed = False
        reason = f"学生缺少岗位强制证书：{'、'.join(missing)}。"

    if passed and (matched_ratio >= 0.6 or no_cert_ratio >= 0.5):
        risk_level = RISK_HIGH_MATCH
        message = "证书项满足岗位要求或岗位证书硬性要求较弱。"
    elif passed:
        risk_level = RISK_RISK
        message = "没有触发强制证书风险，但建议补充岗位高频偏好证书提升竞争力。"
    else:
        risk_level = RISK_NO_MATCH
        message = "缺少岗位高频强制证书，证书项存在明显风险。"

    display = {
        "student_values": deepcopy(student_values),
        "must_have_certificates": deepcopy(must_have),
        "preferred_certificates": deepcopy(preferred),
        "matched_ratio": round(float(matched_ratio), 4),
        "risk_level": risk_level,
        "message": message,
    }
    evaluation = {
        "student_values": deepcopy(student_values),
        "must_have_certificates": deepcopy(must_have),
        "preferred_certificates": deepcopy(preferred),
        "pass": bool(passed),
        "reason": reason,
    }
    return display, evaluation


def build_hard_info(student_profile: Dict[str, Any], stats: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    degree_display, degree_eval = evaluate_degree(student_profile, stats)
    major_display, major_eval = evaluate_major(student_profile, stats)
    certificate_display, certificate_eval = evaluate_certificate(student_profile, stats)

    display = {
        "degree": degree_display,
        "major": major_display,
        "certificate": certificate_display,
    }
    evaluation = {
        "degree": degree_eval,
        "major": major_eval,
        "certificate": certificate_eval,
        "all_pass": bool(degree_eval.get("pass") and major_eval.get("pass") and certificate_eval.get("pass")),
    }
    return display, evaluation


def extract_student_terms(student_profile: Dict[str, Any]) -> List[str]:
    raw = safe_dict(student_profile.get("raw_student_profile_result"))
    profile_input_payload = safe_dict(raw.get("profile_input_payload"))
    normalized_profile = safe_dict(profile_input_payload.get("normalized_profile"))
    ability_evidence = safe_dict(raw.get("ability_evidence"))

    terms: List[str] = []
    for key in [
        "hard_skills",
        "tool_skills",
        "certificates",
        "experience_tags",
        "occupation_hints",
        "domain_tags",
        "strengths",
    ]:
        terms.extend(normalize_tag_list(student_profile.get(key)))
    for key in ["hard_skills", "tool_skills", "experience_tags", "qualification_tags", "domain_tags"]:
        terms.extend(normalize_tag_list(normalized_profile.get(key)))
    for key in ["project_examples", "internship_examples"]:
        terms.extend(normalize_tag_list(ability_evidence.get(key)))
    summary = clean_text(student_profile.get("summary") or raw.get("summary"))
    if summary:
        terms.append(summary)
    return dedup_keep_order(terms)


def match_knowledge_points(
    student_profile: Dict[str, Any],
    skill_assets: Dict[str, Any],
    min_similarity: float = 0.72,
) -> Dict[str, Any]:
    required_points = normalize_tag_list(skill_assets.get("required_knowledge_points"))
    preferred_points = normalize_tag_list(skill_assets.get("preferred_knowledge_points"))
    student_terms = extract_student_terms(student_profile)

    matched_required = [
        point
        for point in required_points
        if any(token_similarity(term, point) >= min_similarity for term in student_terms)
    ]
    matched_preferred = [
        point
        for point in preferred_points
        if any(token_similarity(term, point) >= min_similarity for term in student_terms)
    ]
    missing_points = [point for point in required_points if point not in matched_required]
    accuracy = len(matched_required) / len(required_points) if required_points else 1.0

    if accuracy >= 0.8:
        risk_level = RISK_HIGH_MATCH
    elif accuracy > 0:
        risk_level = RISK_RISK
    else:
        risk_level = RISK_NO_MATCH if required_points else RISK_HIGH_MATCH

    return {
        "required_knowledge_points": deepcopy(required_points),
        "preferred_knowledge_points": deepcopy(preferred_points),
        "student_knowledge_points": dedup_keep_order(matched_required + matched_preferred + student_terms[:20]),
        "matched_knowledge_points": deepcopy(matched_required),
        "missing_knowledge_points": deepcopy(missing_points),
        "knowledge_point_accuracy": round(float(accuracy), 4),
        "pass": bool(accuracy >= 0.8),
        "risk_level": risk_level,
    }


def build_contest_evaluation(
    hard_info_evaluation: Dict[str, Any],
    skill_knowledge_match: Dict[str, Any],
) -> Dict[str, Any]:
    hard_info_pass = bool(hard_info_evaluation.get("all_pass"))
    skill_accuracy_pass = bool(skill_knowledge_match.get("pass"))
    return {
        "hard_info_pass": hard_info_pass,
        "skill_accuracy_pass": skill_accuracy_pass,
        "contest_match_success": bool(hard_info_pass and skill_accuracy_pass),
    }


def aggregate_risk_level(
    hard_info_display: Dict[str, Any],
    skill_knowledge_match: Dict[str, Any],
    contest_evaluation: Dict[str, Any],
) -> str:
    if contest_evaluation.get("contest_match_success"):
        return RISK_HIGH_MATCH
    risk_values = [
        safe_dict(hard_info_display.get("degree")).get("risk_level"),
        safe_dict(hard_info_display.get("major")).get("risk_level"),
        safe_dict(hard_info_display.get("certificate")).get("risk_level"),
        skill_knowledge_match.get("risk_level"),
    ]
    if RISK_NO_MATCH in risk_values:
        return RISK_NO_MATCH
    return RISK_RISK


def calculate_asset_match_score(
    stats: Dict[str, Any],
    hard_info_evaluation: Dict[str, Any],
    skill_knowledge_match: Dict[str, Any],
) -> float:
    pass_count = sum(
        1
        for key in ["degree", "major", "certificate"]
        if safe_dict(hard_info_evaluation.get(key)).get("pass")
    )
    hard_ratio = pass_count / 3.0
    knowledge_accuracy = safe_float(skill_knowledge_match.get("knowledge_point_accuracy"), default=0.0)
    sample_count = int(stats.get("sample_count") or 0)
    sample_reliability = min(sample_count / 100.0, 1.0)

    score = hard_ratio * 45.0 + knowledge_accuracy * 45.0 + sample_reliability * 10.0
    if not hard_info_evaluation.get("all_pass"):
        score -= (3 - pass_count) * 8.0
    if knowledge_accuracy < 0.4:
        score -= 8.0
    return round(max(0.0, min(score, 100.0)), 2)


def build_recommendation_reason(
    job_name: str,
    hard_info_evaluation: Dict[str, Any],
    skill_knowledge_match: Dict[str, Any],
) -> str:
    accuracy = safe_float(skill_knowledge_match.get("knowledge_point_accuracy"), default=0.0)
    if hard_info_evaluation.get("all_pass") and accuracy >= 0.8:
        return f"{job_name} 的学历、专业、证书硬门槛全部通过，技能知识点覆盖率达到 {accuracy:.0%}。"
    if hard_info_evaluation.get("all_pass"):
        return f"{job_name} 的硬门槛通过，但技能知识点覆盖率为 {accuracy:.0%}，建议作为可冲刺岗位。"
    return f"{job_name} 存在学历、专业或证书硬门槛风险，技能知识点覆盖率为 {accuracy:.0%}。"


def evaluate_single_job(
    job_name: Any,
    student_profile: Dict[str, Any],
    loader: MatchAssetLoader,
    match_type: str,
    overall_match_score: Optional[float] = None,
) -> Dict[str, Any]:
    """Evaluate one job against the student using post-processing assets."""
    standard_job_name = clean_text(job_name)
    stats = loader.get_requirement_stats(standard_job_name)
    skill_assets = loader.get_skill_assets(standard_job_name)
    asset_found = bool(stats or skill_assets)

    hard_info_display, hard_info_evaluation = build_hard_info(student_profile, stats)
    skill_knowledge_match = match_knowledge_points(student_profile, skill_assets)
    contest_evaluation = build_contest_evaluation(hard_info_evaluation, skill_knowledge_match)
    asset_score = calculate_asset_match_score(stats, hard_info_evaluation, skill_knowledge_match)
    final_score = safe_float(overall_match_score, default=0.0) if overall_match_score is not None else asset_score
    if final_score <= 0:
        final_score = asset_score

    result = {
        "job_name": standard_job_name,
        "match_type": match_type,
        "asset_found": asset_found,
        "sample_count": int(stats.get("sample_count") or 0),
        "overall_match_score": round(float(final_score), 2),
        "asset_match_score": asset_score,
        "requirement_distributions": build_requirement_distributions(stats),
        "hard_info_display": hard_info_display,
        "hard_info_evaluation": hard_info_evaluation,
        "skill_knowledge_match": skill_knowledge_match,
        "contest_evaluation": contest_evaluation,
        "risk_level": aggregate_risk_level(hard_info_display, skill_knowledge_match, contest_evaluation),
    }
    if match_type == "recommended_job":
        result["recommendation_reason"] = build_recommendation_reason(
            standard_job_name,
            hard_info_evaluation,
            skill_knowledge_match,
        )
    return result


def build_recommendation_ranking(
    student_profile: Dict[str, Any],
    loader: MatchAssetLoader,
    top_n: int = 5,
) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    for job_name in loader.all_standard_job_names():
        evaluated = evaluate_single_job(
            job_name=job_name,
            student_profile=student_profile,
            loader=loader,
            match_type="ranking_candidate",
        )
        candidates.append(
            {
                "job_name": job_name,
                "overall_match_score": safe_float(evaluated.get("asset_match_score"), default=0.0),
                "hard_info_pass": bool(safe_dict(evaluated.get("contest_evaluation")).get("hard_info_pass")),
                "knowledge_point_accuracy": safe_float(
                    safe_dict(evaluated.get("skill_knowledge_match")).get("knowledge_point_accuracy"),
                    default=0.0,
                ),
                "risk_level": clean_text(evaluated.get("risk_level")),
                "recommendation_reason": build_recommendation_reason(
                    job_name,
                    safe_dict(evaluated.get("hard_info_evaluation")),
                    safe_dict(evaluated.get("skill_knowledge_match")),
                ),
            }
        )

    candidates.sort(key=lambda item: item.get("overall_match_score", 0.0), reverse=True)
    return [{"rank": index, **item} for index, item in enumerate(candidates[:top_n], start=1)]


def build_match_asset_evaluation(
    match_input_payload: Dict[str, Any],
    rule_score_result: Optional[Dict[str, Any]] = None,
    final_overall_match_score: Optional[float] = None,
    project_root: Optional[str | Path] = None,
    top_n: int = 5,
) -> Dict[str, Any]:
    """Build target job match, recommended job match, and recommendation ranking."""
    payload = safe_dict(match_input_payload)
    student_profile = safe_dict(payload.get("student_profile"))
    job_profile = safe_dict(payload.get("job_profile"))
    target_job_name = clean_text(job_profile.get("standard_job_name"))
    loader = MatchAssetLoader(project_root=project_root)

    target_overall = final_overall_match_score
    if target_overall is None and rule_score_result:
        target_overall = safe_float(safe_dict(rule_score_result).get("overall_match_score"), default=0.0)

    target_job_match = (
        evaluate_single_job(
            job_name=target_job_name,
            student_profile=student_profile,
            loader=loader,
            match_type="target_job",
            overall_match_score=target_overall,
        )
        if target_job_name
        else {}
    )

    ranking = build_recommendation_ranking(student_profile=student_profile, loader=loader, top_n=top_n)
    recommended_name = clean_text(safe_dict(ranking[0] if ranking else {}).get("job_name"))
    recommended_job_match = (
        evaluate_single_job(
            job_name=recommended_name,
            student_profile=student_profile,
            loader=loader,
            match_type="recommended_job",
        )
        if recommended_name
        else {}
    )

    return {
        "target_job_match": target_job_match,
        "recommended_job_match": recommended_job_match,
        "recommendation_ranking": ranking,
        "asset_warnings": deepcopy(loader.warnings),
    }
