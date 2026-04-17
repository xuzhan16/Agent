"""
career_path_plan_selector.py

career_path_plan 模块中的规则决策层。

职责：
1. 基于 job_match_result / match_snapshot 做目标岗位选择；
2. 生成 primary_target_job、secondary_target_jobs、goal_positioning；
3. 基于 job_profile_result 中的 vertical_paths / transfer_paths 生成
   direct_path、transition_path、long_term_path；
4. 输出初步职业目标与路径结构，供后续 LLM 规划服务层继续扩写。

说明：
- 本文件只做纯 Python 规则决策，不调用大模型；
- 推荐输入为 career_path_plan_builder.py 生成的 career_plan_input_payload；
- 也支持从 student_api_state.json 或内置 demo 上游结果构造 payload 后再决策。
"""

from __future__ import annotations

import argparse
import json
import re
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .career_path_plan_builder import (
    build_career_plan_input_payload,
    build_career_plan_input_payload_from_state,
    build_demo_job_match_result,
    build_demo_job_profile_result,
    build_demo_student_profile_result,
)


DEFAULT_OUTPUT_PATH = Path("outputs/state/career_path_plan_selection_result.json")
TARGET_PATH_STATUS_AVAILABLE = "available"
TARGET_PATH_STATUS_MISSING = "missing"
TARGET_PATH_MISSING_MESSAGE = "当前目标岗位暂无可用晋升/转岗路径数据，系统不会强行生成路径。"


@dataclass
class CareerGoalSelectionResult:
    """职业目标岗位选择结果。"""

    primary_target_job: str = ""
    primary_plan_job: str = ""
    user_target_job: str = ""
    system_recommended_job: str = ""
    target_job_role: str = ""
    recommended_job_role: str = ""
    goal_decision_source: str = ""
    goal_decision_confidence: str = ""
    goal_decision_reason: List[str] = field(default_factory=list)
    goal_decision_context: Dict[str, Any] = field(default_factory=dict)
    secondary_target_jobs: List[str] = field(default_factory=list)
    goal_positioning: str = ""
    target_selection_reason: List[str] = field(default_factory=list)


@dataclass
class CareerPathSelectionResult:
    """初步职业路径结构。"""

    primary_target_job: str = ""
    primary_plan_job: str = ""
    user_target_job: str = ""
    system_recommended_job: str = ""
    target_job_role: str = ""
    recommended_job_role: str = ""
    goal_decision_source: str = ""
    goal_decision_confidence: str = ""
    goal_decision_reason: List[str] = field(default_factory=list)
    goal_decision_context: Dict[str, Any] = field(default_factory=dict)
    secondary_target_jobs: List[str] = field(default_factory=list)
    goal_positioning: str = ""
    direct_path: List[str] = field(default_factory=list)
    transition_path: List[str] = field(default_factory=list)
    long_term_path: List[str] = field(default_factory=list)
    path_strategy: str = ""
    target_path_data_status: str = TARGET_PATH_STATUS_MISSING
    target_path_data_message: str = TARGET_PATH_MISSING_MESSAGE
    target_selection_reason: List[str] = field(default_factory=list)
    path_selection_reason: List[str] = field(default_factory=list)
    risk_notes: List[str] = field(default_factory=list)
    selector_metrics: Dict[str, Any] = field(default_factory=dict)
    career_plan_input_payload: Dict[str, Any] = field(default_factory=dict)


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
    """将 list / JSON 字符串 / 分隔字符串统一转 list。"""
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


def normalize_path_option_list(value: Any) -> List[Dict[str, Any]]:
    """统一路径候选列表格式。"""
    result = []
    for item in parse_list_like(value):
        if isinstance(item, dict):
            result.append(deepcopy(item))
    return dedup_keep_order(result)


def drop_fallback_path_options(path_options: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """过滤历史缓存中可能残留的伪造兜底路径。"""
    result = []
    for option in path_options:
        option_dict = safe_dict(option)
        if bool(option_dict.get("is_fallback")):
            continue
        if clean_text(option_dict.get("source_tier")) == "fallback":
            continue
        result.append(option_dict)
    return result


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


def normalize_match_level(value: Any, overall_match_score: float) -> str:
    """统一匹配等级字段，缺失时根据分数兜底。"""
    text = clean_text(value)
    if text:
        return text
    if overall_match_score >= 90:
        return "A-高度匹配"
    if overall_match_score >= 80:
        return "B-较高匹配"
    if overall_match_score >= 70:
        return "C-中等匹配"
    if overall_match_score >= 60:
        return "D-勉强匹配"
    return "E-匹配度较低"


def parse_path_text(path_text: Any, default_from_job: str = "", default_to_job: str = "") -> List[str]:
    """把路径文本标准化为路径节点列表。"""
    text = clean_text(path_text)
    if text and "->" in text:
        parts = [clean_text(part) for part in text.split("->") if clean_text(part)]
        if parts:
            return parts
    if default_from_job or default_to_job:
        return [clean_text(default_from_job), clean_text(default_to_job)]
    return [text] if text else []


def count_high_priority_gaps(gap_analysis: List[Dict[str, Any]]) -> int:
    """统计高优先级缺口数量。"""
    return sum(
        1
        for item in gap_analysis
        if clean_text(safe_dict(item).get("priority")) == "high"
    )


def has_blocking_skill_gap(gap_analysis: List[Dict[str, Any]]) -> bool:
    """判断是否存在影响当前岗位可达性的关键技能/实践缺口。"""
    for item in gap_analysis:
        item_dict = safe_dict(item)
        gap_type = clean_text(item_dict.get("gap_type"))
        priority = clean_text(item_dict.get("priority"))
        if gap_type in {"hard_skills", "tool_skills", "practice_experience"} and priority == "high":
            return True
    return False


def score_candidate_goal_job(
    candidate_job: str,
    payload: Dict[str, Any],
) -> Tuple[float, List[str]]:
    """对候选目标岗位做轻量规则打分。"""
    target_job = clean_text(payload.get("target_job_name"))
    student_snapshot = safe_dict(payload.get("student_snapshot"))
    match_snapshot = safe_dict(payload.get("match_snapshot"))
    direct_paths = drop_fallback_path_options(
        normalize_path_option_list(payload.get("direct_path_options"))
    )
    transition_paths = drop_fallback_path_options(
        normalize_path_option_list(payload.get("transition_path_options"))
    )

    candidate = clean_text(candidate_job)
    score = 50.0
    reasons = []

    overall_match_score = safe_float(match_snapshot.get("overall_match_score"), default=0.0)
    if candidate == target_job:
        score += overall_match_score * 0.45
        reasons.append(f"{candidate} 是当前目标岗位，继承人岗匹配总分 {overall_match_score:.2f} 的主权重。")
    else:
        score += overall_match_score * 0.20

    for path_option in direct_paths:
        option = safe_dict(path_option)
        if candidate in {clean_text(option.get("from_job")), clean_text(option.get("to_job"))}:
            source_tier = clean_text(option.get("source_tier"))
            if source_tier == "graph":
                score += 14.0
                reasons.append(f"{candidate} 出现在图谱直接路径候选中，可作为优先主目标或上升节点。")
            elif source_tier == "offline_profile":
                score += 10.0
                reasons.append(f"{candidate} 出现在离线路径候选中，可作为主目标或上升节点。")
            else:
                score += 4.0
                reasons.append(f"{candidate} 出现在 fallback 直接路径候选中，但可信度低于图谱路径。")
            break

    for path_option in transition_paths:
        option = safe_dict(path_option)
        if candidate in {clean_text(option.get("from_job")), clean_text(option.get("to_job"))}:
            source_tier = clean_text(option.get("source_tier"))
            if source_tier == "graph":
                score += 10.0
                reasons.append(f"{candidate} 出现在图谱过渡/转岗路径候选中，适合作为高可信备选目标。")
            elif source_tier == "offline_profile":
                score += 7.0
                reasons.append(f"{candidate} 出现在离线过渡路径候选中，适合作为备选目标。")
            else:
                score += 2.0
                reasons.append(f"{candidate} 出现在 fallback 过渡路径候选中，可作为临时备选方向。")
            break

    for hint in normalize_text_list(student_snapshot.get("occupation_hints")):
        if candidate == hint or candidate in hint or hint in candidate:
            score += 10.0
            reasons.append(f"{candidate} 与学生职业方向信号“{hint}”一致。")
            break

    growth_level = clean_text(safe_dict(student_snapshot.get("potential_profile")).get("growth_level"))
    if growth_level in {"较强", "中等偏上"} and candidate == target_job:
        score += 5.0
        reasons.append(f"学生潜力画像为“{growth_level}”，支持继续以 {candidate} 作为冲刺目标。")

    score = round(min(100.0, max(0.0, score)), 2)
    if not reasons:
        reasons.append(f"{candidate} 作为候选岗位，但当前缺少额外路径或偏好信号加成。")
    return score, dedup_keep_order(reasons)


def pick_match_job_name(match_detail: Dict[str, Any]) -> str:
    """从匹配详情中读取岗位展示名，优先保留资产标准岗位名。"""
    detail = safe_dict(match_detail)
    return clean_text(
        detail.get("asset_job_name")
        or detail.get("resolved_standard_job_name")
        or detail.get("job_name")
    )


def pick_match_display_score(match_detail: Dict[str, Any]) -> float:
    """读取用于主目标决策的主展示分。"""
    detail = safe_dict(match_detail)
    return safe_float(
        detail.get("display_match_score")
        or detail.get("asset_match_score")
        or detail.get("overall_match_score"),
        default=0.0,
    )


def pick_match_knowledge_accuracy(match_detail: Dict[str, Any]) -> float:
    """读取岗位知识点覆盖率，兼容 0-1 与 0-100。"""
    detail = safe_dict(match_detail)
    accuracy = safe_float(
        safe_dict(detail.get("skill_knowledge_match")).get("knowledge_point_accuracy"),
        default=0.0,
    )
    return accuracy / 100.0 if accuracy > 1 else accuracy


def pick_match_hard_pass(match_detail: Dict[str, Any]) -> bool:
    """读取学历/专业/证书硬门槛是否通过。"""
    detail = safe_dict(match_detail)
    hard_info = safe_dict(detail.get("hard_info_evaluation"))
    contest = safe_dict(detail.get("contest_evaluation"))
    if "all_pass" in hard_info:
        return safe_bool(hard_info.get("all_pass"))
    return safe_bool(contest.get("hard_info_pass"))


def pick_match_contest_success(match_detail: Dict[str, Any]) -> bool:
    """读取赛题综合评测是否通过。"""
    detail = safe_dict(match_detail)
    return safe_bool(safe_dict(detail.get("contest_evaluation")).get("contest_match_success"))


def pick_match_asset_found(match_detail: Dict[str, Any]) -> bool:
    """判断岗位匹配详情是否命中后处理资产。"""
    detail = safe_dict(match_detail)
    if not detail:
        return False
    if "asset_found" in detail:
        return safe_bool(detail.get("asset_found"))
    return bool(
        clean_text(detail.get("asset_job_name"))
        or safe_dict(detail.get("requirement_distributions"))
        or safe_dict(detail.get("hard_info_evaluation"))
        or safe_dict(detail.get("skill_knowledge_match"))
    )


def summarize_match_risks(match_detail: Dict[str, Any]) -> List[str]:
    """抽取可解释的岗位风险点。"""
    detail = safe_dict(match_detail)
    risks: List[str] = []
    risk_level = clean_text(detail.get("risk_level"))
    if risk_level and risk_level not in {"high_match", "match"}:
        risks.append(f"风险等级为{risk_level}")

    skill_match = safe_dict(detail.get("skill_knowledge_match"))
    missing_points = normalize_text_list(skill_match.get("missing_knowledge_points"))[:6]
    if missing_points:
        risks.append(f"缺失知识点：{'、'.join(missing_points)}")
    if not safe_bool(skill_match.get("pass")) and skill_match:
        accuracy = pick_match_knowledge_accuracy(detail)
        risks.append(f"知识点覆盖率为{accuracy * 100:.0f}%，尚未达到80%")

    hard_display = safe_dict(detail.get("hard_info_display"))
    for key in ("degree", "major", "certificate"):
        message = clean_text(safe_dict(hard_display.get(key)).get("message"))
        risk = clean_text(safe_dict(hard_display.get(key)).get("risk_level"))
        if message and risk in {"risk", "no_match"}:
            risks.append(message)

    if not risks and not pick_match_contest_success(detail):
        risks.append("赛题综合评测尚未完全通过")
    return dedup_keep_order(risks)[:6]


def summarize_match_advantages(match_detail: Dict[str, Any]) -> List[str]:
    """抽取可解释的岗位优势点。"""
    detail = safe_dict(match_detail)
    advantages: List[str] = []
    if pick_match_hard_pass(detail):
        advantages.append("学历、专业、证书硬门槛通过")

    skill_match = safe_dict(detail.get("skill_knowledge_match"))
    matched_points = normalize_text_list(skill_match.get("matched_knowledge_points"))[:6]
    if matched_points:
        advantages.append(f"已命中知识点：{'、'.join(matched_points)}")

    reason = clean_text(detail.get("recommendation_reason") or detail.get("score_explanation"))
    if reason:
        advantages.append(reason)

    score = pick_match_display_score(detail)
    if score > 0:
        advantages.append(f"主展示分为{score:.2f}")
    return dedup_keep_order(advantages)[:6]


def build_match_evidence(match_detail: Dict[str, Any], fallback_name: str = "") -> Dict[str, Any]:
    """构造主目标决策对比证据。"""
    detail = safe_dict(match_detail)
    return {
        "job_name": pick_match_job_name(detail) or clean_text(fallback_name),
        "display_match_score": round(pick_match_display_score(detail), 2),
        "rule_match_score": safe_float(detail.get("rule_match_score") or detail.get("overall_match_score"), default=0.0),
        "asset_match_score": safe_float(detail.get("asset_match_score") or detail.get("display_match_score"), default=0.0),
        "knowledge_point_accuracy": round(pick_match_knowledge_accuracy(detail), 4),
        "hard_info_pass": pick_match_hard_pass(detail),
        "contest_match_success": pick_match_contest_success(detail),
        "asset_found": pick_match_asset_found(detail),
        "risk_level": clean_text(detail.get("risk_level")),
        "main_risks": summarize_match_risks(detail),
        "main_advantages": summarize_match_advantages(detail),
    }


def select_primary_plan_job_from_match_assets(
    match_snapshot: Dict[str, Any],
    fallback_target_job: str = "",
) -> Dict[str, Any]:
    """基于 job_match 的赛题资产结果决定职业规划主目标岗位。"""
    match_snapshot = safe_dict(match_snapshot)
    target_match = safe_dict(match_snapshot.get("target_job_match"))
    recommended_match = safe_dict(match_snapshot.get("recommended_job_match"))

    user_target_job = clean_text(match_snapshot.get("user_target_job") or fallback_target_job)
    target_job_name = pick_match_job_name(target_match) or user_target_job or clean_text(fallback_target_job)
    system_recommended_job = clean_text(
        match_snapshot.get("system_recommended_job") or pick_match_job_name(recommended_match)
    )

    target_score = safe_float(
        match_snapshot.get("target_display_match_score") or pick_match_display_score(target_match),
        default=0.0,
    )
    recommended_score = safe_float(
        match_snapshot.get("recommended_display_match_score") or pick_match_display_score(recommended_match),
        default=0.0,
    )
    target_accuracy = safe_float(
        match_snapshot.get("target_knowledge_point_accuracy")
        if match_snapshot.get("target_knowledge_point_accuracy") not in {None, ""}
        else pick_match_knowledge_accuracy(target_match),
        default=0.0,
    )
    recommended_accuracy = safe_float(
        match_snapshot.get("recommended_knowledge_point_accuracy")
        if match_snapshot.get("recommended_knowledge_point_accuracy") not in {None, ""}
        else pick_match_knowledge_accuracy(recommended_match),
        default=0.0,
    )
    if target_accuracy > 1:
        target_accuracy /= 100.0
    if recommended_accuracy > 1:
        recommended_accuracy /= 100.0

    target_asset_found = pick_match_asset_found(target_match)
    recommended_asset_found = pick_match_asset_found(recommended_match)
    target_hard_pass = (
        safe_bool(match_snapshot.get("target_hard_info_pass"))
        if "target_hard_info_pass" in match_snapshot
        else pick_match_hard_pass(target_match)
    )
    recommended_hard_pass = (
        safe_bool(match_snapshot.get("recommended_hard_info_pass"))
        if "recommended_hard_info_pass" in match_snapshot
        else pick_match_hard_pass(recommended_match)
    )
    target_contest_success = (
        safe_bool(match_snapshot.get("target_contest_match_success"))
        if "target_contest_match_success" in match_snapshot
        else pick_match_contest_success(target_match)
    )
    recommended_contest_success = (
        safe_bool(match_snapshot.get("recommended_contest_match_success"))
        if "recommended_contest_match_success" in match_snapshot
        else pick_match_contest_success(recommended_match)
    )

    primary_plan_job = user_target_job or target_job_name
    target_job_role = "original_goal"
    recommended_job_role = "reference_only"
    decision_source = "fallback_user_target"
    decision_confidence = "low"
    reasons: List[str] = []

    same_job = bool(
        system_recommended_job
        and target_job_name
        and system_recommended_job == target_job_name
    )
    score_delta = recommended_score - target_score
    accuracy_delta = recommended_accuracy - target_accuracy

    if not system_recommended_job or same_job or not recommended_asset_found:
        primary_plan_job = user_target_job or target_job_name or system_recommended_job
        target_job_role = "original_goal"
        recommended_job_role = "reference_only"
        decision_source = "fallback_user_target" if not recommended_asset_found else "respect_user_target"
        decision_confidence = "medium" if target_asset_found else "low"
        if not recommended_asset_found:
            reasons.append("系统推荐岗位缺少有效后处理资产，主目标保留用户原始目标岗位。")
        else:
            reasons.append("系统推荐岗位与用户目标岗位一致或差异不足，主目标保留用户原始目标岗位。")
    elif not target_asset_found and recommended_asset_found:
        primary_plan_job = system_recommended_job
        target_job_role = "stretch_goal"
        recommended_job_role = "primary_goal_candidate" if not recommended_contest_success else "primary_goal"
        decision_source = "match_asset_score"
        decision_confidence = "medium_high"
        reasons.append("用户目标岗位未命中完整评测资产，系统优先采用资产完整的推荐岗位作为短期主路径。")
    elif target_contest_success and not (
        recommended_hard_pass and score_delta >= 20 and accuracy_delta >= 0.25
    ):
        primary_plan_job = user_target_job or target_job_name
        target_job_role = "original_goal"
        recommended_job_role = "secondary_goal"
        decision_source = "respect_user_target"
        decision_confidence = "high"
        reasons.append("用户目标岗位已通过赛题综合评测，除非推荐岗位优势特别明显，否则优先尊重用户原目标。")
    elif recommended_hard_pass and score_delta >= 10 and accuracy_delta > 0:
        primary_plan_job = system_recommended_job
        target_job_role = "stretch_goal"
        recommended_job_role = "primary_goal_candidate" if not recommended_contest_success else "primary_goal"
        decision_source = "match_asset_score"
        decision_confidence = "medium_high" if score_delta >= 20 or recommended_contest_success else "medium"
        reasons.append(
            f"{system_recommended_job}主展示分{recommended_score:.2f}，高于{target_job_name or user_target_job}{target_score:.2f}，优势为{score_delta:.2f}分。"
        )
        reasons.append(
            f"{system_recommended_job}知识点覆盖率{recommended_accuracy * 100:.0f}%，高于目标岗位{target_accuracy * 100:.0f}%。"
        )
        if recommended_contest_success:
            reasons.append(f"{system_recommended_job}已通过赛题综合评测，更适合作为当前主路径。")
        else:
            reasons.append(f"{system_recommended_job}虽未完全通过赛题综合评测，但更接近达标，适合作为短期主路径候选。")
    else:
        primary_plan_job = user_target_job or target_job_name
        target_job_role = "original_goal"
        recommended_job_role = "secondary_goal"
        decision_source = "respect_user_target"
        decision_confidence = "medium"
        reasons.append("系统推荐岗位优势未达到切换阈值，主目标继续保留用户原始目标岗位。")

    if user_target_job and primary_plan_job and user_target_job != primary_plan_job:
        reasons.append(f"用户原始目标{user_target_job}不被否定，建议作为中期补强后的冲刺目标或备选目标保留。")

    context = {
        "user_target_job": user_target_job,
        "system_recommended_job": system_recommended_job,
        "primary_plan_job": primary_plan_job,
        "target_job_role": target_job_role,
        "recommended_job_role": recommended_job_role,
        "decision_source": decision_source,
        "decision_confidence": decision_confidence,
        "comparison_evidence": {
            "target_job": build_match_evidence(target_match, fallback_name=target_job_name or user_target_job),
            "recommended_job": build_match_evidence(recommended_match, fallback_name=system_recommended_job),
        },
        "rule_decision_summary": "；".join(reasons[:4]),
    }

    return {
        "primary_plan_job": primary_plan_job,
        "primary_target_job": primary_plan_job,
        "user_target_job": user_target_job,
        "system_recommended_job": system_recommended_job,
        "target_job_role": target_job_role,
        "recommended_job_role": recommended_job_role,
        "goal_decision_source": decision_source,
        "goal_decision_confidence": decision_confidence,
        "goal_decision_reason": dedup_keep_order(reasons),
        "goal_decision_context": context,
    }


def build_goal_positioning_from_decision(decision: Dict[str, Any]) -> str:
    """根据主目标决策生成稳定的目标定位话术。"""
    primary_job = clean_text(decision.get("primary_plan_job") or decision.get("primary_target_job"))
    user_job = clean_text(decision.get("user_target_job"))
    recommended_job = clean_text(decision.get("system_recommended_job"))
    recommended_role = clean_text(decision.get("recommended_job_role"))

    if primary_job and user_job and primary_job != user_job:
        role_text = "短期主路径"
        if recommended_role == "primary_goal_candidate":
            role_text = "短期主路径候选"
        return f"系统建议以{primary_job}作为{role_text}，同时保留{user_job}作为中期补强后的冲刺目标。"
    if primary_job:
        if recommended_job and recommended_job != primary_job:
            return f"以{primary_job}作为当前主目标岗位，同时将{recommended_job}作为备选参考方向。"
        return f"以{primary_job}作为当前主目标岗位，结合匹配结果和能力缺口分阶段推进。"
    return "当前主目标岗位尚不明确，建议先补齐学生画像和岗位匹配资产后再决策。"


def select_target_jobs(payload: Dict[str, Any]) -> Dict[str, Any]:
    """基于 match_snapshot 和 candidate_goal_jobs 选择主目标/备选目标。"""
    payload = safe_dict(payload)
    match_snapshot = safe_dict(payload.get("match_snapshot"))
    student_snapshot = safe_dict(payload.get("student_snapshot"))
    planner_context = safe_dict(payload.get("planner_context"))

    target_job = clean_text(payload.get("target_job_name"))
    candidate_goal_jobs = dedup_keep_order(
        [target_job]
        + normalize_text_list(payload.get("candidate_goal_jobs"))
        + normalize_text_list(student_snapshot.get("occupation_hints"))
    )
    candidate_goal_jobs = [item for item in candidate_goal_jobs if clean_text(item)]

    scored_candidates = []
    reason_map = {}
    for candidate in candidate_goal_jobs:
        score, reasons = score_candidate_goal_job(candidate, payload)
        scored_candidates.append((score, candidate))
        reason_map[candidate] = reasons
    scored_candidates.sort(key=lambda item: item[0], reverse=True)

    overall_score = safe_float(match_snapshot.get("overall_match_score"), default=0.0)
    match_level = normalize_match_level(match_snapshot.get("score_level"), overall_score)
    gap_analysis = normalize_path_option_list(payload.get("gap_analysis"))
    blocking_gap = has_blocking_skill_gap(gap_analysis)

    decision = select_primary_plan_job_from_match_assets(
        match_snapshot=match_snapshot,
        fallback_target_job=target_job or (scored_candidates[0][1] if scored_candidates else ""),
    )
    primary_target_job = clean_text(decision.get("primary_target_job"))
    goal_positioning = build_goal_positioning_from_decision(decision)
    target_reasons = normalize_text_list(decision.get("goal_decision_reason"))

    secondary_target_jobs = [
        candidate
        for _, candidate in scored_candidates
        if clean_text(candidate) and clean_text(candidate) != primary_target_job
    ][:5]
    user_target_job = clean_text(decision.get("user_target_job"))
    system_recommended_job = clean_text(decision.get("system_recommended_job"))
    for job_name in [user_target_job, system_recommended_job]:
        if job_name and job_name != primary_target_job and job_name not in secondary_target_jobs:
            secondary_target_jobs.insert(0, job_name)
    secondary_target_jobs = dedup_keep_order(secondary_target_jobs)[:5]

    if not primary_target_job and scored_candidates:
        primary_target_job = scored_candidates[0][1]
    if primary_target_job in reason_map:
        target_reasons.extend(reason_map[primary_target_job])
    for candidate in secondary_target_jobs[:2]:
        target_reasons.extend(reason_map.get(candidate, []))

    recommendation_text = clean_text(match_snapshot.get("recommendation")) or clean_text(
        planner_context.get("recommendation")
    )
    if recommendation_text:
        target_reasons.append(f"人岗匹配模块建议：{recommendation_text}")

    result = CareerGoalSelectionResult(
        primary_target_job=clean_text(primary_target_job),
        primary_plan_job=clean_text(primary_target_job),
        user_target_job=user_target_job,
        system_recommended_job=system_recommended_job,
        target_job_role=clean_text(decision.get("target_job_role")),
        recommended_job_role=clean_text(decision.get("recommended_job_role")),
        goal_decision_source=clean_text(decision.get("goal_decision_source")),
        goal_decision_confidence=clean_text(decision.get("goal_decision_confidence")),
        goal_decision_reason=normalize_text_list(decision.get("goal_decision_reason")),
        goal_decision_context=deepcopy(safe_dict(decision.get("goal_decision_context"))),
        secondary_target_jobs=dedup_keep_order(secondary_target_jobs),
        goal_positioning=goal_positioning,
        target_selection_reason=dedup_keep_order(target_reasons),
    )
    return asdict(result) | {
        "candidate_job_scores": [
            {"job_name": candidate, "score": score, "reasons": reason_map.get(candidate, [])}
            for score, candidate in scored_candidates
        ],
        "match_level": match_level,
        "blocking_gap": blocking_gap,
    }


def path_option_to_nodes(path_option: Dict[str, Any], fallback_from_job: str = "") -> List[str]:
    """将 path_option 标准化为路径节点列表。"""
    option = safe_dict(path_option)
    if not option:
        return []
    path_nodes = parse_path_text(
        option.get("path_text"),
        default_from_job=option.get("from_job"),
        default_to_job=option.get("to_job"),
    )
    return [clean_text(node) for node in path_nodes if clean_text(node)]


def select_best_path_option(
    path_options: List[Dict[str, Any]],
    preferred_target_job: str = "",
) -> Tuple[Dict[str, Any], List[str]]:
    """从候选路径中选出最优 path option。"""
    if not path_options:
        return {}, ["当前没有可用路径候选，路径保持为空。"]

    scored = []
    reason_map = {}
    for option in path_options:
        option_dict = safe_dict(option)
        score = safe_float(option_dict.get("path_score_hint"), default=60.0)
        path_type = clean_text(option_dict.get("path_type"))
        to_job = clean_text(option_dict.get("to_job"))
        reasons = [f"路径基础分 {score:.2f}。"]
        source_tier = clean_text(option_dict.get("source_tier"))
        if source_tier == "graph":
            score += 10.0
            reasons.append("该路径来自图谱岗位关系，稳定性更高，加成 +10。")
        elif source_tier == "offline_profile":
            score += 4.0
            reasons.append("该路径来自离线岗位画像，可信度较高，加成 +4。")
        elif source_tier == "fallback":
            score -= 12.0
            reasons.append("该路径属于 fallback 兜底，仅在知识不足时使用，扣分 -12。")

        if clean_text(option_dict.get("priority_hint")) == "high":
            score += 5.0
            reasons.append("该路径 priority_hint=high，优先级加成 +5。")
        if preferred_target_job and to_job == preferred_target_job:
            score += 6.0
            reasons.append(f"路径终点与目标岗位 {preferred_target_job} 一致，加成 +6。")
        if path_type == "direct_vertical":
            score += 4.0
            reasons.append("纵向直接路径更适合作为长期成长主线，加成 +4。")
        elif path_type in {"bridge", "transfer"}:
            score += 2.0
            reasons.append("过渡/转岗路径有助于降低起步难度，加成 +2。")

        scored.append((round(score, 2), option_dict))
        reason_map[json.dumps(option_dict, ensure_ascii=False, sort_keys=True)] = reasons

    scored.sort(key=lambda item: item[0], reverse=True)
    best_option = scored[0][1]
    best_key = json.dumps(best_option, ensure_ascii=False, sort_keys=True)
    return best_option, reason_map.get(best_key, [])


def filter_path_options_for_primary_job(
    path_options: List[Dict[str, Any]],
    primary_target_job: str,
) -> List[Dict[str, Any]]:
    """主目标发生切换时，只保留与当前主目标直接相关的真实路径。"""
    primary = clean_text(primary_target_job)
    if not primary:
        return path_options

    filtered = []
    for option in path_options:
        option_dict = safe_dict(option)
        nodes = path_option_to_nodes(option_dict)
        related_names = set(nodes)
        related_names.add(clean_text(option_dict.get("from_job")))
        related_names.add(clean_text(option_dict.get("to_job")))
        if primary in related_names:
            filtered.append(option_dict)
    return filtered


def build_long_term_path(
    primary_target_job: str,
    direct_path: List[str],
    transition_path: List[str],
    target_job_snapshot: Dict[str, Any],
) -> List[str]:
    """生成长期职业路径，优先把过渡路径和纵向晋升路径拼接起来。"""
    primary_target = clean_text(primary_target_job)
    if transition_path and direct_path:
        if clean_text(transition_path[-1]) == clean_text(direct_path[0]):
            return dedup_keep_order(transition_path + direct_path[1:])
        return dedup_keep_order(transition_path + direct_path)

    if direct_path:
        return dedup_keep_order(direct_path)

    if transition_path:
        return dedup_keep_order(transition_path)

    return []


def build_risk_notes(
    payload: Dict[str, Any],
    selection_result: Dict[str, Any],
    direct_path: List[str],
    transition_path: List[str],
) -> List[str]:
    """结合匹配分、缺口和路径结构生成风险提示。"""
    match_snapshot = safe_dict(payload.get("match_snapshot"))
    planner_context = safe_dict(payload.get("planner_context"))
    gap_analysis = normalize_path_option_list(payload.get("gap_analysis"))
    high_gap_count = count_high_priority_gaps(gap_analysis)
    notes = []

    overall_score = safe_float(match_snapshot.get("overall_match_score"), default=0.0)
    if overall_score < 70:
        notes.append("当前整体匹配度偏低，直接投递主目标岗位可能面临较高筛选风险，建议先走过渡路径。")
    elif overall_score < 80:
        notes.append("当前整体匹配度中等，虽然可以冲刺主目标岗位，但应同步准备备选岗位并优先补齐关键短板。")

    if high_gap_count >= 3:
        notes.append(f"当前存在 {high_gap_count} 个高优先级能力缺口，短期计划应优先聚焦技能/工具/项目补强。")

    for weakness in normalize_text_list(match_snapshot.get("weaknesses"))[:3]:
        notes.append(f"人岗匹配短板提示：{weakness}")

    for suggestion in normalize_text_list(planner_context.get("improvement_suggestions"))[:3]:
        notes.append(f"建议转化为近期行动项：{suggestion}")

    if not direct_path and not transition_path:
        notes.append("当前目标岗位暂无可用晋升/转岗路径数据，系统不会强行生成路径。")

    selector_metrics = safe_dict(selection_result.get("selector_metrics"))
    if clean_text(selector_metrics.get("best_direct_source_tier")) == "fallback":
        notes.append("当前直接路径来自 fallback 兜底，不代表稳定的岗位晋升知识，建议优先补齐离线路径或图谱边数据。")
    if clean_text(selector_metrics.get("best_transition_source_tier")) == "fallback":
        notes.append("当前过渡路径来自 fallback 兜底，更适合作为临时建议，不宜替代真实岗位转岗知识。")

    primary_target_job = clean_text(selection_result.get("primary_target_job"))
    if primary_target_job and primary_target_job != clean_text(payload.get("target_job_name")):
        notes.append(f"主目标岗位已从原始目标切换为 {primary_target_job}，需确认该方向是否符合学生真实求职偏好。")

    return dedup_keep_order(notes)


def select_career_path_plan(
    career_plan_input_payload: Dict[str, Any],
    output_path: Optional[str | Path] = None,
) -> Dict[str, Any]:
    """主入口：基于 builder payload 生成初步职业目标和路径结构。"""
    payload = safe_dict(career_plan_input_payload)
    target_job_snapshot = safe_dict(payload.get("target_job_snapshot"))
    match_snapshot = safe_dict(payload.get("match_snapshot"))
    direct_path_options = drop_fallback_path_options(
        normalize_path_option_list(payload.get("direct_path_options"))
    )
    transition_path_options = drop_fallback_path_options(
        normalize_path_option_list(payload.get("transition_path_options"))
    )

    goal_selection = select_target_jobs(payload)
    primary_target_job = clean_text(goal_selection.get("primary_target_job"))
    direct_path_options = filter_path_options_for_primary_job(direct_path_options, primary_target_job)
    transition_path_options = filter_path_options_for_primary_job(transition_path_options, primary_target_job)

    best_direct_option, direct_reason = select_best_path_option(
        path_options=direct_path_options,
        preferred_target_job=primary_target_job,
    )
    best_transition_option, transition_reason = select_best_path_option(
        path_options=transition_path_options,
        preferred_target_job=primary_target_job,
    )

    direct_path = path_option_to_nodes(
        best_direct_option,
        fallback_from_job=primary_target_job,
    )
    transition_path = path_option_to_nodes(
        best_transition_option,
        fallback_from_job=primary_target_job,
    )
    long_term_path = build_long_term_path(
        primary_target_job=primary_target_job,
        direct_path=direct_path,
        transition_path=transition_path,
        target_job_snapshot=target_job_snapshot,
    )

    overall_score = safe_float(match_snapshot.get("overall_match_score"), default=0.0)
    has_target_path_data = bool(direct_path or transition_path)
    if not has_target_path_data:
        path_strategy = "no_target_path_data"
    elif overall_score >= 80:
        path_strategy = "direct_first"
    elif overall_score >= 70:
        path_strategy = "direct_with_transition_backup"
    else:
        path_strategy = "transition_first"

    target_path_data_status = (
        TARGET_PATH_STATUS_AVAILABLE if has_target_path_data else TARGET_PATH_STATUS_MISSING
    )
    target_path_data_message = (
        "当前目标岗位已命中真实晋升/转岗路径数据。"
        if has_target_path_data
        else TARGET_PATH_MISSING_MESSAGE
    )

    path_selection_reason = dedup_keep_order(
        [f"根据 overall_match_score={overall_score:.2f}，当前路径策略设为 {path_strategy}。"]
        + direct_reason
        + transition_reason
    )
    if not has_target_path_data:
        path_selection_reason = dedup_keep_order(
            [TARGET_PATH_MISSING_MESSAGE]
            + [f"根据 overall_match_score={overall_score:.2f}，当前路径策略设为 {path_strategy}。"]
            + direct_reason
            + transition_reason
        )

    final_result = CareerPathSelectionResult(
        primary_target_job=primary_target_job,
        primary_plan_job=clean_text(goal_selection.get("primary_plan_job") or primary_target_job),
        user_target_job=clean_text(goal_selection.get("user_target_job")),
        system_recommended_job=clean_text(goal_selection.get("system_recommended_job")),
        target_job_role=clean_text(goal_selection.get("target_job_role")),
        recommended_job_role=clean_text(goal_selection.get("recommended_job_role")),
        goal_decision_source=clean_text(goal_selection.get("goal_decision_source")),
        goal_decision_confidence=clean_text(goal_selection.get("goal_decision_confidence")),
        goal_decision_reason=normalize_text_list(goal_selection.get("goal_decision_reason")),
        goal_decision_context=deepcopy(safe_dict(goal_selection.get("goal_decision_context"))),
        secondary_target_jobs=normalize_text_list(goal_selection.get("secondary_target_jobs")),
        goal_positioning=clean_text(goal_selection.get("goal_positioning")),
        direct_path=direct_path,
        transition_path=transition_path,
        long_term_path=long_term_path,
        path_strategy=path_strategy,
        target_path_data_status=target_path_data_status,
        target_path_data_message=target_path_data_message,
        target_selection_reason=normalize_text_list(goal_selection.get("target_selection_reason")),
        path_selection_reason=path_selection_reason,
        risk_notes=[],
        selector_metrics={
            "overall_match_score": round(overall_score, 2),
            "match_level": clean_text(goal_selection.get("match_level")),
            "blocking_gap": bool(goal_selection.get("blocking_gap")),
            "candidate_job_scores": deepcopy(normalize_path_option_list(goal_selection.get("candidate_job_scores"))),
            "direct_path_option_count": len(direct_path_options),
            "transition_path_option_count": len(transition_path_options),
            "best_direct_source": clean_text(best_direct_option.get("source")),
            "best_transition_source": clean_text(best_transition_option.get("source")),
            "best_direct_source_tier": clean_text(best_direct_option.get("source_tier")),
            "best_transition_source_tier": clean_text(best_transition_option.get("source_tier")),
            "high_priority_gap_count": count_high_priority_gaps(
                normalize_path_option_list(payload.get("gap_analysis"))
            ),
        },
        career_plan_input_payload=deepcopy(payload),
    )
    result_dict = asdict(final_result)
    result_dict["risk_notes"] = build_risk_notes(
        payload=payload,
        selection_result=result_dict,
        direct_path=direct_path,
        transition_path=transition_path,
    )

    if output_path:
        save_json(result_dict, output_path)
    return result_dict


def build_selection_result_from_state(
    state_path: str | Path,
    output_path: Optional[str | Path] = DEFAULT_OUTPUT_PATH,
) -> Dict[str, Any]:
    """从 student_api_state.json 读取上游结果，经 builder 构造 payload 后再执行 selector。"""
    payload = build_career_plan_input_payload_from_state(
        state_path=state_path,
        output_path=None,
    )
    return select_career_path_plan(
        career_plan_input_payload=payload,
        output_path=output_path,
    )


def build_demo_career_plan_input_payload() -> Dict[str, Any]:
    """构造内置 demo payload。"""
    return build_career_plan_input_payload(
        student_profile_result=build_demo_student_profile_result(),
        job_profile_result=build_demo_job_profile_result(),
        job_match_result=build_demo_job_match_result(),
        output_path=None,
    )


def parse_args() -> argparse.Namespace:
    """命令行参数解析。"""
    parser = argparse.ArgumentParser(description="Select preliminary career target and paths")
    parser.add_argument(
        "--input",
        default="",
        help="可选：career_plan_input_payload JSON 路径",
    )
    parser.add_argument(
        "--state-path",
        default="",
        help="可选：包含 student_profile/job_profile/job_match 结果的 student_api_state.json 路径",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_PATH),
        help="selector 输出 JSON 路径",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.input:
        selector_result = select_career_path_plan(
            career_plan_input_payload=load_json_file(args.input),
            output_path=args.output,
        )
    elif args.state_path:
        selector_result = build_selection_result_from_state(
            state_path=args.state_path,
            output_path=args.output,
        )
    else:
        selector_result = select_career_path_plan(
            career_plan_input_payload=build_demo_career_plan_input_payload(),
            output_path=args.output,
        )

    print(json.dumps(selector_result, ensure_ascii=False, indent=2))
