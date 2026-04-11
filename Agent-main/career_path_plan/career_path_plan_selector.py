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
- 也支持从 student.json 或内置 demo 上游结果构造 payload 后再决策。
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


@dataclass
class CareerGoalSelectionResult:
    """职业目标岗位选择结果。"""

    primary_target_job: str = ""
    secondary_target_jobs: List[str] = field(default_factory=list)
    goal_positioning: str = ""
    target_selection_reason: List[str] = field(default_factory=list)


@dataclass
class CareerPathSelectionResult:
    """初步职业路径结构。"""

    primary_target_job: str = ""
    secondary_target_jobs: List[str] = field(default_factory=list)
    goal_positioning: str = ""
    direct_path: List[str] = field(default_factory=list)
    transition_path: List[str] = field(default_factory=list)
    long_term_path: List[str] = field(default_factory=list)
    path_strategy: str = ""
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
    direct_paths = normalize_path_option_list(payload.get("direct_path_options"))
    transition_paths = normalize_path_option_list(payload.get("transition_path_options"))

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
            score += 12.0
            reasons.append(f"{candidate} 出现在直接路径候选中，可作为主目标或上升节点。")
            break

    for path_option in transition_paths:
        option = safe_dict(path_option)
        if candidate in {clean_text(option.get("from_job")), clean_text(option.get("to_job"))}:
            score += 8.0
            reasons.append(f"{candidate} 出现在过渡/转岗路径候选中，适合作为备选目标。")
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


def select_target_jobs(payload: Dict[str, Any]) -> Dict[str, Any]:
    """基于 match_snapshot 和 candidate_goal_jobs 选择主目标/备选目标。"""
    payload = safe_dict(payload)
    match_snapshot = safe_dict(payload.get("match_snapshot"))
    student_snapshot = safe_dict(payload.get("student_snapshot"))
    planner_context = safe_dict(payload.get("planner_context"))
    planning_constraints = safe_dict(payload.get("planning_constraints"))
    gap_analysis = normalize_path_option_list(payload.get("gap_analysis"))

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
    blocking_gap = has_blocking_skill_gap(gap_analysis)
    direct_path_feasible = bool(planning_constraints.get("direct_path_feasible", overall_score >= 75))

    primary_target_job = target_job
    target_reasons = []
    if overall_score >= 80 and direct_path_feasible and not blocking_gap:
        primary_target_job = target_job or (scored_candidates[0][1] if scored_candidates else "")
        goal_positioning = f"以{primary_target_job}作为优先冲刺目标，当前匹配等级{match_level}，建议走直接达成路径。"
        target_reasons.append(f"overall_match_score={overall_score:.2f}，且直接路径可行，主目标保持为当前目标岗位。")
    elif overall_score >= 70:
        primary_target_job = target_job or (scored_candidates[0][1] if scored_candidates else "")
        goal_positioning = f"以{primary_target_job}作为主目标岗位，但定位为“短期补强后冲刺”，同步保留 1-2 个过渡岗位。"
        if blocking_gap:
            target_reasons.append("虽然总体匹配度达到中等水平，但存在高优先级技能/工具/实践缺口，建议先补强再重点投递。")
        else:
            target_reasons.append("总体匹配度达到中等水平，可以保留当前目标岗位为主目标，同时搭配备选路径降低风险。")
    elif overall_score >= 60:
        transfer_candidates = [
            candidate
            for _, candidate in scored_candidates
            if candidate != target_job
        ]
        primary_target_job = transfer_candidates[0] if transfer_candidates else target_job
        goal_positioning = f"优先以{primary_target_job}作为过渡型目标岗位，同时将{target_job}保留为中期冲刺目标。"
        target_reasons.append("当前匹配度偏临界，建议先选择更易进入的过渡岗位积累经验，再向目标岗位迁移。")
    else:
        transfer_candidates = [
            candidate
            for _, candidate in scored_candidates
            if candidate != target_job
        ]
        primary_target_job = transfer_candidates[0] if transfer_candidates else target_job
        goal_positioning = f"当前与{target_job}仍有明显差距，建议先以{primary_target_job}作为保守起步目标，分阶段补强后再转向主目标。"
        target_reasons.append("overall_match_score 较低，直接冲刺当前目标岗位风险较高，优先选择更稳妥的过渡岗位。")

    secondary_target_jobs = [
        candidate
        for _, candidate in scored_candidates
        if clean_text(candidate) and clean_text(candidate) != primary_target_job
    ][:5]

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
    path_nodes = parse_path_text(
        option.get("path_text"),
        default_from_job=option.get("from_job") or fallback_from_job,
        default_to_job=option.get("to_job"),
    )
    return [clean_text(node) for node in path_nodes if clean_text(node)]


def select_best_path_option(
    path_options: List[Dict[str, Any]],
    preferred_target_job: str = "",
) -> Tuple[Dict[str, Any], List[str]]:
    """从候选路径中选出最优 path option。"""
    if not path_options:
        return {}, ["当前没有可用路径候选，使用空路径兜底。"]

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
            score += 8.0
            reasons.append("该路径来自图谱/离线岗位关系，稳定性更高，加成 +8。")
        elif source_tier == "offline_profile":
            score += 4.0
            reasons.append("该路径来自离线岗位画像，可信度较高，加成 +4。")
        elif source_tier == "fallback":
            score -= 10.0
            reasons.append("该路径属于 fallback 兜底，仅在知识不足时使用，扣分 -10。")

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
        if primary_target and clean_text(transition_path[-1]) != primary_target:
            return dedup_keep_order(transition_path + [primary_target] + direct_path[1:])
        return dedup_keep_order(transition_path + direct_path)

    if direct_path:
        return dedup_keep_order(direct_path)

    if transition_path:
        if primary_target and clean_text(transition_path[-1]) != primary_target:
            return dedup_keep_order(transition_path + [primary_target])
        return dedup_keep_order(transition_path)

    vertical_paths = normalize_text_list(target_job_snapshot.get("vertical_paths"))
    if vertical_paths:
        return parse_path_text(vertical_paths[0])

    return [primary_target] if primary_target else []


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
        notes.append("当前未形成明确直接路径或过渡路径，建议补充岗位知识图谱路径数据后再做更稳定规划。")

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
    direct_path_options = normalize_path_option_list(payload.get("direct_path_options"))
    transition_path_options = normalize_path_option_list(payload.get("transition_path_options"))

    goal_selection = select_target_jobs(payload)
    primary_target_job = clean_text(goal_selection.get("primary_target_job"))

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
    if overall_score >= 80:
        path_strategy = "direct_first"
    elif overall_score >= 70:
        path_strategy = "direct_with_transition_backup"
    else:
        path_strategy = "transition_first"

    path_selection_reason = dedup_keep_order(
        [f"根据 overall_match_score={overall_score:.2f}，当前路径策略设为 {path_strategy}。"]
        + direct_reason
        + transition_reason
    )

    final_result = CareerPathSelectionResult(
        primary_target_job=primary_target_job,
        secondary_target_jobs=normalize_text_list(goal_selection.get("secondary_target_jobs")),
        goal_positioning=clean_text(goal_selection.get("goal_positioning")),
        direct_path=direct_path,
        transition_path=transition_path,
        long_term_path=long_term_path,
        path_strategy=path_strategy,
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
    """从 student.json 读取上游结果，经 builder 构造 payload 后再执行 selector。"""
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
        help="可选：包含 student_profile/job_profile/job_match 结果的 student.json 路径",
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
