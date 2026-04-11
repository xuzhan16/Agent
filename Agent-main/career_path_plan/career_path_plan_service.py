"""
career_path_plan_service.py

career_path_plan 模块业务服务层。

职责：
1. 调用 career_path_plan_builder 构造 career_plan_input_payload；
2. 调用 career_path_plan_selector 生成规则层目标岗位与路径结果；
3. 通过统一大模型接口 call_llm(task_type="career_path_plan", ...) 补充解释性规划内容；
4. 合并规则结果与模型结果；
5. 写回 student.json 的 career_path_plan_result 字段。

边界约束：
- 不重写 llm_service 和 state_manager；
- 不重写 student_profile / job_profile / job_match；
- 服务层只负责流程编排、异常兜底、字段补齐和状态写回。
"""

from __future__ import annotations

import argparse
import json
import logging
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from .career_path_plan_builder import (
    build_career_plan_input_payload,
    build_career_plan_input_payload_from_state,
    build_demo_job_match_result,
    build_demo_job_profile_result,
    build_demo_student_profile_result,
)
from .career_path_plan_selector import select_career_path_plan
from llm_interface_layer.llm_service import call_llm
from llm_interface_layer.state_manager import StateManager


LOGGER = logging.getLogger(__name__)
DEFAULT_STATE_PATH = Path("outputs/state/student.json")
DEFAULT_BUILDER_OUTPUT_PATH = Path("outputs/state/career_path_plan_input_payload.json")
DEFAULT_SELECTOR_OUTPUT_PATH = Path("outputs/state/career_path_plan_selection_result.json")
DEFAULT_SERVICE_OUTPUT_PATH = Path("outputs/state/career_path_plan_service_result.json")


@dataclass
class CareerPathPlanLLMSupplement:
    """LLM 补充的职业路径规划解释字段。"""

    goal_reason: str = ""
    decision_summary: str = ""
    risk_and_gap: List[str] = field(default_factory=list)
    fallback_strategy: str = ""
    short_term_plan: List[str] = field(default_factory=list)
    mid_term_plan: List[str] = field(default_factory=list)


@dataclass
class CareerPathPlanServiceResult:
    """最终写回 student.json 的 career_path_plan_result 结构。"""

    primary_target_job: str = ""
    secondary_target_jobs: List[str] = field(default_factory=list)
    goal_positioning: str = ""
    goal_reason: str = ""
    direct_path: List[str] = field(default_factory=list)
    transition_path: List[str] = field(default_factory=list)
    long_term_path: List[str] = field(default_factory=list)
    path_strategy: str = ""
    short_term_plan: List[str] = field(default_factory=list)
    mid_term_plan: List[str] = field(default_factory=list)
    decision_summary: str = ""
    risk_and_gap: List[str] = field(default_factory=list)
    fallback_strategy: str = ""
    target_selection_reason: List[str] = field(default_factory=list)
    path_selection_reason: List[str] = field(default_factory=list)
    selector_metrics: Dict[str, Any] = field(default_factory=dict)
    selector_result: Dict[str, Any] = field(default_factory=dict)
    career_plan_input_payload: Dict[str, Any] = field(default_factory=dict)
    llm_plan_result: Dict[str, Any] = field(default_factory=dict)
    build_warnings: List[str] = field(default_factory=list)


def setup_logging() -> None:
    """初始化日志配置。"""
    if LOGGER.handlers:
        return
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
    )


def clean_text(value: Any) -> str:
    """基础文本清洗。"""
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null", "n/a", "na", "-"}:
        return ""
    return text


def safe_dict(value: Any) -> Dict[str, Any]:
    """安全转 dict。"""
    return value if isinstance(value, dict) else {}


def safe_list(value: Any) -> List[Any]:
    """安全转 list。"""
    if isinstance(value, list):
        return value
    if value is None or value == "":
        return []
    return [value]


def dedup_keep_order(values: List[Any]) -> List[Any]:
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


def save_json(data: Dict[str, Any], output_path: Optional[str | Path]) -> None:
    """按需保存 JSON 文件。"""
    if not output_path:
        return
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


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


def build_default_goal_reason(
    selector_result: Dict[str, Any],
    career_plan_input_payload: Dict[str, Any],
) -> str:
    """当 LLM 未返回 goal_reason 时，使用规则决策和匹配结论兜底生成。"""
    primary_target_job = clean_text(selector_result.get("primary_target_job"))
    match_snapshot = safe_dict(career_plan_input_payload.get("match_snapshot"))
    student_snapshot = safe_dict(career_plan_input_payload.get("student_snapshot"))
    overall_match_score = match_snapshot.get("overall_match_score", 0.0)
    score_level = clean_text(match_snapshot.get("score_level"))
    student_major = clean_text(student_snapshot.get("major")) or "当前专业"
    student_degree = clean_text(student_snapshot.get("degree")) or "当前学历"
    strengths = [
        clean_text(item)
        for item in safe_list(match_snapshot.get("strengths") or student_snapshot.get("strengths"))
        if clean_text(item)
    ]
    strength_text = "、".join(strengths[:3]) if strengths else "已有一定岗位基础"
    return (
        f"将{primary_target_job or '目标岗位'}作为当前主目标，主要依据是学生具备{student_degree}、{student_major}背景，"
        f"且{strength_text}；当前人岗匹配总分为{overall_match_score}，等级为{score_level or '未明确'}，"
        "适合在补齐关键短板后继续推进该方向。"
    )


def build_default_short_term_plan(
    selector_result: Dict[str, Any],
    career_plan_input_payload: Dict[str, Any],
) -> List[str]:
    """当 LLM 未返回 short_term_plan 时，根据缺口和路径策略生成兜底计划。"""
    plans = []
    planner_context = safe_dict(career_plan_input_payload.get("planner_context"))
    constraints = safe_dict(career_plan_input_payload.get("planning_constraints"))
    gap_analysis = [
        safe_dict(item)
        for item in safe_list(career_plan_input_payload.get("gap_analysis"))
    ]

    for suggestion in safe_list(planner_context.get("improvement_suggestions"))[:3]:
        if clean_text(suggestion):
            plans.append(clean_text(suggestion))

    for gap in gap_analysis:
        if clean_text(gap.get("priority")) == "high" and clean_text(gap.get("action_hint")):
            plans.append(clean_text(gap.get("action_hint")))
        if len(plans) >= 5:
            break

    if clean_text(constraints.get("short_term_focus")):
        plans.insert(0, clean_text(constraints.get("short_term_focus")))

    if not plans:
        plans = [
            "在未来 3-6 个月内，优先补齐目标岗位所需核心技能，并产出 1-2 个可展示项目。",
            "同步优化简历和面试表达，将已有项目、实习经历和岗位要求建立更清晰对应关系。",
        ]
    return dedup_keep_order(plans)[:6]


def build_default_mid_term_plan(
    selector_result: Dict[str, Any],
    career_plan_input_payload: Dict[str, Any],
) -> List[str]:
    """当 LLM 未返回 mid_term_plan 时，根据路径结构生成兜底中期计划。"""
    primary_target_job = clean_text(selector_result.get("primary_target_job"))
    secondary_jobs = [clean_text(item) for item in safe_list(selector_result.get("secondary_target_jobs")) if clean_text(item)]
    direct_path = [clean_text(item) for item in safe_list(selector_result.get("direct_path")) if clean_text(item)]
    long_term_path = [clean_text(item) for item in safe_list(selector_result.get("long_term_path")) if clean_text(item)]
    constraints = safe_dict(career_plan_input_payload.get("planning_constraints"))

    plans = []
    if clean_text(constraints.get("mid_term_focus")):
        plans.append(clean_text(constraints.get("mid_term_focus")))
    if primary_target_job:
        plans.append(f"围绕{primary_target_job}持续补充真实业务项目、实习经历和行业认知，争取形成可复用的方法论和作品集。")
    if secondary_jobs:
        plans.append(f"同步关注备选岗位：{'、'.join(secondary_jobs[:3])}，根据投递反馈动态调整主目标和过渡路径。")
    if direct_path:
        plans.append(f"若直接路径进展顺利，可按“{' -> '.join(direct_path)}”推进岗位晋升和能力升级。")
    if long_term_path and long_term_path != direct_path:
        plans.append(f"中长期可参考“{' -> '.join(long_term_path)}”形成阶段性成长路线。")

    if not plans:
        plans = [
            "在 6-18 个月内争取进入与目标岗位相关的实习或初级岗位，通过真实业务项目积累行业经验。",
            "持续跟踪招聘要求变化，定期复盘技能栈、项目质量和目标岗位匹配度，动态修正职业路径。",
        ]
    return dedup_keep_order(plans)[:6]


def build_default_decision_summary(
    selector_result: Dict[str, Any],
    career_plan_input_payload: Dict[str, Any],
) -> str:
    """生成兜底版决策摘要。"""
    primary_target_job = clean_text(selector_result.get("primary_target_job"))
    goal_positioning = clean_text(selector_result.get("goal_positioning"))
    path_strategy = clean_text(selector_result.get("path_strategy"))
    match_snapshot = safe_dict(career_plan_input_payload.get("match_snapshot"))
    overall_match_score = match_snapshot.get("overall_match_score", 0.0)
    return (
        f"当前建议以{primary_target_job or '目标岗位'}为主目标，路径策略为{path_strategy or '未明确'}。"
        f"{goal_positioning or ''} 综合规则匹配得分为{overall_match_score}，短期优先补齐关键能力缺口，"
        "中期通过项目/实习积累逐步提升岗位可达性。"
    )


def build_default_risk_and_gap(
    selector_result: Dict[str, Any],
    career_plan_input_payload: Dict[str, Any],
) -> List[str]:
    """生成兜底版风险与缺口说明。"""
    notes = [
        clean_text(item)
        for item in safe_list(selector_result.get("risk_notes"))
        if clean_text(item)
    ]
    for gap in safe_list(career_plan_input_payload.get("gap_analysis")):
        gap_dict = safe_dict(gap)
        if clean_text(gap_dict.get("priority")) != "high":
            continue
        gap_item = clean_text(gap_dict.get("gap_item"))
        action_hint = clean_text(gap_dict.get("action_hint"))
        if gap_item and action_hint:
            notes.append(f"高优先级缺口：{gap_item}。建议：{action_hint}")
    if not notes:
        notes = ["当前职业路径规划暂无明显结构性风险，但仍建议持续跟踪岗位要求变化并定期复盘能力缺口。"]
    return dedup_keep_order(notes)[:10]


def build_default_fallback_strategy(
    selector_result: Dict[str, Any],
    career_plan_input_payload: Dict[str, Any],
) -> str:
    """生成兜底版备选策略。"""
    primary_target_job = clean_text(selector_result.get("primary_target_job"))
    secondary_jobs = [
        clean_text(item)
        for item in safe_list(selector_result.get("secondary_target_jobs"))
        if clean_text(item)
    ]
    transition_path = [
        clean_text(item)
        for item in safe_list(selector_result.get("transition_path"))
        if clean_text(item)
    ]
    match_snapshot = safe_dict(career_plan_input_payload.get("match_snapshot"))
    overall_match_score = float(match_snapshot.get("overall_match_score") or 0.0)

    if overall_match_score >= 80 and secondary_jobs:
        return f"若{primary_target_job}短期竞争激烈或投递反馈不理想，可平行投递{'、'.join(secondary_jobs[:3])}作为备选岗位，并保持同一技能主线。"
    if transition_path:
        return f"若直接冲刺{primary_target_job}受阻，可先按“{' -> '.join(transition_path)}”进入邻近岗位积累经验，再择机回到主目标路径。"
    if secondary_jobs:
        return f"若主目标岗位短期不可达，优先选择{'、'.join(secondary_jobs[:3])}作为过渡岗位，同时继续补齐主目标所需技能和项目经验。"
    return "若当前主目标岗位短期不可达，建议先选择要求更贴近现有能力的同方向初级岗位作为过渡，并持续补齐核心短板。"


def normalize_llm_career_path_plan_result(
    llm_result: Dict[str, Any],
    selector_result: Dict[str, Any],
    career_plan_input_payload: Dict[str, Any],
) -> Dict[str, Any]:
    """对 call_llm('career_path_plan', ...) 返回做字段兼容和默认值补齐。"""
    source = safe_dict(llm_result)
    normalized = CareerPathPlanLLMSupplement(
        goal_reason=clean_text(source.get("goal_reason"))
        or build_default_goal_reason(selector_result, career_plan_input_payload),
        decision_summary=clean_text(source.get("decision_summary") or source.get("summary"))
        or build_default_decision_summary(selector_result, career_plan_input_payload),
        risk_and_gap=dedup_keep_order(
            [clean_text(item) for item in safe_list(source.get("risk_and_gap") or source.get("risk_notes")) if clean_text(item)]
        )
        or build_default_risk_and_gap(selector_result, career_plan_input_payload),
        fallback_strategy=clean_text(source.get("fallback_strategy"))
        or build_default_fallback_strategy(selector_result, career_plan_input_payload),
        short_term_plan=dedup_keep_order(
            [clean_text(item) for item in safe_list(source.get("short_term_plan")) if clean_text(item)]
        )
        or build_default_short_term_plan(selector_result, career_plan_input_payload),
        mid_term_plan=dedup_keep_order(
            [clean_text(item) for item in safe_list(source.get("mid_term_plan")) if clean_text(item)]
        )
        or build_default_mid_term_plan(selector_result, career_plan_input_payload),
    )
    return asdict(normalized) | {"raw_llm_result": source}


def build_career_path_plan_llm_input(
    career_plan_input_payload: Dict[str, Any],
    selector_result: Dict[str, Any],
) -> Dict[str, Any]:
    """组装 career_path_plan 大模型输入。"""
    planner_context = safe_dict(career_plan_input_payload.get("planner_context"))

    gap_snapshot = []
    for item in safe_list(career_plan_input_payload.get("gap_analysis"))[:6]:
        item_dict = safe_dict(item)
        gap_snapshot.append(
            {
                "gap_item": clean_text(item_dict.get("gap_item")),
                "priority": clean_text(item_dict.get("priority")),
                "action_hint": clean_text(item_dict.get("action_hint")),
            }
        )

    return {
        "student_snapshot": deepcopy(safe_dict(career_plan_input_payload.get("student_snapshot"))),
        "target_job_snapshot": deepcopy(safe_dict(career_plan_input_payload.get("target_job_snapshot"))),
        "match_snapshot": deepcopy(safe_dict(career_plan_input_payload.get("match_snapshot"))),
        "goal_options_snapshot": {
            "candidate_goal_jobs": deepcopy(safe_list(career_plan_input_payload.get("candidate_goal_jobs"))[:5]),
            "direct_path_options": deepcopy(safe_list(career_plan_input_payload.get("direct_path_options"))[:5]),
            "transition_path_options": deepcopy(
                safe_list(career_plan_input_payload.get("transition_path_options"))[:5]
            ),
            "gap_analysis": gap_snapshot,
            "planning_constraints": deepcopy(
                safe_dict(career_plan_input_payload.get("planning_constraints"))
            ),
            "planner_context": {
                "strengths": deepcopy(safe_list(planner_context.get("strengths"))[:6]),
                "weaknesses": deepcopy(safe_list(planner_context.get("weaknesses"))[:6]),
                "improvement_suggestions": deepcopy(
                    safe_list(planner_context.get("improvement_suggestions"))[:6]
                ),
            },
        },
        "selector_snapshot": {
            "primary_target_job": clean_text(selector_result.get("primary_target_job")),
            "secondary_target_jobs": deepcopy(safe_list(selector_result.get("secondary_target_jobs"))[:5]),
            "goal_positioning": clean_text(selector_result.get("goal_positioning")),
            "direct_path": deepcopy(safe_list(selector_result.get("direct_path"))[:5]),
            "transition_path": deepcopy(safe_list(selector_result.get("transition_path"))[:5]),
            "long_term_path": deepcopy(safe_list(selector_result.get("long_term_path"))[:5]),
            "path_strategy": clean_text(selector_result.get("path_strategy")),
            "target_selection_reason": deepcopy(safe_list(selector_result.get("target_selection_reason"))[:5]),
            "path_selection_reason": deepcopy(safe_list(selector_result.get("path_selection_reason"))[:5]),
            "risk_notes": deepcopy(safe_list(selector_result.get("risk_notes"))[:6]),
        },
        "generation_requirements": {
            "goal_reason": "解释为什么选择当前主目标岗位和备选岗位，要求结合学生画像、人岗匹配结果和路径可达性。",
            "decision_summary": "输出一段更自然、可直接用于报告模块的职业目标与路径决策摘要。",
            "risk_and_gap": "结合 risk_notes、gap_analysis、weaknesses 和 improvement_suggestions，总结风险与能力缺口。",
            "fallback_strategy": "给出当主目标岗位短期不可达时的备选路径和切换策略。",
            "short_term_plan": "将短期计划改写成自然、可执行、适合3-6个月推进的行动列表。",
            "mid_term_plan": "将中期计划改写成自然、可执行、适合6-18个月推进的行动列表。",
        },
        "output_schema_hint": asdict(CareerPathPlanLLMSupplement()),
    }


def merge_career_path_plan_results(
    career_plan_input_payload: Dict[str, Any],
    selector_result: Dict[str, Any],
    llm_result: Dict[str, Any],
    service_warnings: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """合并规则 selector 结果和 LLM 补充结果。"""
    payload = safe_dict(career_plan_input_payload)
    rule_result = safe_dict(selector_result)
    normalized_llm = normalize_llm_career_path_plan_result(
        llm_result=llm_result,
        selector_result=rule_result,
        career_plan_input_payload=payload,
    )

    merged_warnings = dedup_keep_order(
        [clean_text(item) for item in safe_list(payload.get("build_warnings")) if clean_text(item)]
        + [clean_text(item) for item in safe_list(service_warnings) if clean_text(item)]
    )

    primary_target_job = clean_text(rule_result.get("primary_target_job")) or clean_text(
        safe_dict(llm_result).get("primary_target_job")
    )
    secondary_target_jobs = dedup_keep_order(
        [clean_text(item) for item in safe_list(rule_result.get("secondary_target_jobs")) if clean_text(item)]
        + [clean_text(item) for item in safe_list(safe_dict(llm_result).get("backup_target_jobs")) if clean_text(item)]
    )

    direct_path = dedup_keep_order(
        [clean_text(item) for item in safe_list(rule_result.get("direct_path")) if clean_text(item)]
        or [clean_text(item) for item in safe_list(safe_dict(llm_result).get("direct_path")) if clean_text(item)]
    )
    transition_path = dedup_keep_order(
        [clean_text(item) for item in safe_list(rule_result.get("transition_path")) if clean_text(item)]
        or [clean_text(item) for item in safe_list(safe_dict(llm_result).get("transition_path")) if clean_text(item)]
    )
    long_term_path = dedup_keep_order(
        [clean_text(item) for item in safe_list(rule_result.get("long_term_path")) if clean_text(item)]
        or direct_path
        or transition_path
    )

    result = CareerPathPlanServiceResult(
        primary_target_job=primary_target_job,
        secondary_target_jobs=secondary_target_jobs,
        goal_positioning=clean_text(rule_result.get("goal_positioning")),
        goal_reason=clean_text(normalized_llm.get("goal_reason")),
        direct_path=direct_path,
        transition_path=transition_path,
        long_term_path=long_term_path,
        path_strategy=clean_text(rule_result.get("path_strategy")),
        short_term_plan=dedup_keep_order(
            [clean_text(item) for item in safe_list(normalized_llm.get("short_term_plan")) if clean_text(item)]
        ),
        mid_term_plan=dedup_keep_order(
            [clean_text(item) for item in safe_list(normalized_llm.get("mid_term_plan")) if clean_text(item)]
        ),
        decision_summary=clean_text(normalized_llm.get("decision_summary")),
        risk_and_gap=dedup_keep_order(
            [clean_text(item) for item in safe_list(normalized_llm.get("risk_and_gap")) if clean_text(item)]
        ),
        fallback_strategy=clean_text(normalized_llm.get("fallback_strategy")),
        target_selection_reason=dedup_keep_order(
            [clean_text(item) for item in safe_list(rule_result.get("target_selection_reason")) if clean_text(item)]
        ),
        path_selection_reason=dedup_keep_order(
            [clean_text(item) for item in safe_list(rule_result.get("path_selection_reason")) if clean_text(item)]
        ),
        selector_metrics=deepcopy(safe_dict(rule_result.get("selector_metrics"))),
        selector_result=deepcopy(rule_result),
        career_plan_input_payload=deepcopy(payload),
        llm_plan_result=deepcopy(safe_dict(normalized_llm.get("raw_llm_result"))),
        build_warnings=merged_warnings,
    )
    return asdict(result)


class CareerPathPlanService:
    """career_path_plan 业务服务编排器。"""

    def __init__(self, state_manager: Optional[StateManager] = None) -> None:
        self.state_manager = state_manager or StateManager()

    def build_payload(
        self,
        student_profile_result: Dict[str, Any],
        job_profile_result: Dict[str, Any],
        job_match_result: Dict[str, Any],
        context_data: Optional[Dict[str, Any]] = None,
        output_path: Optional[str | Path] = DEFAULT_BUILDER_OUTPUT_PATH,
    ) -> Dict[str, Any]:
        """调用 builder 构造 career_plan_input_payload。"""
        return build_career_plan_input_payload(
            student_profile_result=student_profile_result,
            job_profile_result=job_profile_result,
            job_match_result=job_match_result,
            context_data=context_data,
            output_path=output_path,
        )

    def select_rules(
        self,
        career_plan_input_payload: Dict[str, Any],
        output_path: Optional[str | Path] = DEFAULT_SELECTOR_OUTPUT_PATH,
    ) -> Dict[str, Any]:
        """调用 selector 生成规则层目标与路径结果。"""
        return select_career_path_plan(
            career_plan_input_payload=career_plan_input_payload,
            output_path=output_path,
        )

    def call_career_path_plan_llm(
        self,
        career_plan_input_payload: Dict[str, Any],
        selector_result: Dict[str, Any],
        student_state: Optional[Dict[str, Any]] = None,
        context_data: Optional[Dict[str, Any]] = None,
        extra_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """通过统一大模型接口补充规划解释和自然语言短中期计划。"""
        input_data = build_career_path_plan_llm_input(
            career_plan_input_payload=career_plan_input_payload,
            selector_result=selector_result,
        )
        merged_extra_context = {
            "service_name": "career_path_plan_service",
            "expected_fields": [
                "goal_reason",
                "decision_summary",
                "risk_and_gap",
                "fallback_strategy",
                "short_term_plan",
                "mid_term_plan",
            ],
        }
        if extra_context:
            merged_extra_context.update(deepcopy(extra_context))

        return call_llm(
            task_type="career_path_plan",
            input_data=input_data,
            context_data=context_data,
            student_state=student_state,
            extra_context=merged_extra_context,
        )

    def run(
        self,
        student_profile_result: Dict[str, Any],
        job_profile_result: Dict[str, Any],
        job_match_result: Dict[str, Any],
        student_state: Optional[Dict[str, Any]] = None,
        context_data: Optional[Dict[str, Any]] = None,
        state_path: Optional[str | Path] = DEFAULT_STATE_PATH,
        builder_output_path: Optional[str | Path] = DEFAULT_BUILDER_OUTPUT_PATH,
        selector_output_path: Optional[str | Path] = DEFAULT_SELECTOR_OUTPUT_PATH,
        service_output_path: Optional[str | Path] = DEFAULT_SERVICE_OUTPUT_PATH,
        extra_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """执行完整 career_path_plan 服务流程并写回 student.json。"""
        setup_logging()
        service_warnings = []

        if student_state is None:
            student_state = self.state_manager.load_state(state_path)

        LOGGER.info("Step 1/4: build career_plan_input_payload")
        try:
            career_plan_input_payload = self.build_payload(
                student_profile_result=student_profile_result,
                job_profile_result=job_profile_result,
                job_match_result=job_match_result,
                context_data=context_data,
                output_path=builder_output_path,
            )
        except Exception as exc:
            LOGGER.exception("career_path_plan_builder failed")
            career_plan_input_payload = {
                "target_job_name": "",
                "student_snapshot": {},
                "target_job_snapshot": {},
                "match_snapshot": {},
                "candidate_goal_jobs": [],
                "direct_path_options": [],
                "transition_path_options": [],
                "gap_analysis": [],
                "planning_constraints": {},
                "planner_context": {},
                "build_warnings": [f"builder 执行失败: {exc}"],
            }
            service_warnings.append(f"builder 执行失败: {exc}")

        LOGGER.info("Step 2/4: select career target and paths")
        try:
            selector_result = self.select_rules(
                career_plan_input_payload=career_plan_input_payload,
                output_path=selector_output_path,
            )
        except Exception as exc:
            LOGGER.exception("career_path_plan_selector failed")
            target_job_name = clean_text(career_plan_input_payload.get("target_job_name"))
            selector_result = {
                "primary_target_job": target_job_name,
                "secondary_target_jobs": [],
                "goal_positioning": f"以{target_job_name or '目标岗位'}作为默认主目标岗位。",
                "direct_path": [target_job_name] if target_job_name else [],
                "transition_path": [],
                "long_term_path": [target_job_name] if target_job_name else [],
                "path_strategy": "fallback_default",
                "target_selection_reason": [f"selector 执行失败，使用默认主目标兜底：{exc}"],
                "path_selection_reason": ["selector 执行失败，路径结构使用默认兜底。"],
                "risk_notes": [f"selector 执行失败: {exc}"],
                "selector_metrics": {},
                "career_plan_input_payload": deepcopy(career_plan_input_payload),
            }
            service_warnings.append(f"selector 执行失败: {exc}")

        LOGGER.info("Step 3/4: call LLM career_path_plan")
        try:
            llm_result = self.call_career_path_plan_llm(
                career_plan_input_payload=career_plan_input_payload,
                selector_result=selector_result,
                student_state=student_state,
                context_data=context_data,
                extra_context=extra_context,
            )
        except Exception as exc:
            LOGGER.exception("call_llm('career_path_plan', ...) failed")
            llm_result = {}
            service_warnings.append(f"LLM 调用失败: {exc}")

        LOGGER.info("Step 4/4: merge results and update state")
        final_result = merge_career_path_plan_results(
            career_plan_input_payload=career_plan_input_payload,
            selector_result=selector_result,
            llm_result=llm_result,
            service_warnings=service_warnings,
        )
        self.state_manager.update_state(
            task_type="career_path_plan",
            task_result=final_result,
            state_path=state_path,
            student_state=student_state,
        )
        save_json(final_result, service_output_path)

        LOGGER.info(
            "career_path_plan service finished. primary_target_job=%s, path_strategy=%s",
            final_result.get("primary_target_job"),
            final_result.get("path_strategy"),
        )
        return final_result

    def run_from_state(
        self,
        state_path: str | Path = DEFAULT_STATE_PATH,
        context_data: Optional[Dict[str, Any]] = None,
        builder_output_path: Optional[str | Path] = DEFAULT_BUILDER_OUTPUT_PATH,
        selector_output_path: Optional[str | Path] = DEFAULT_SELECTOR_OUTPUT_PATH,
        service_output_path: Optional[str | Path] = DEFAULT_SERVICE_OUTPUT_PATH,
        extra_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """从 student.json 读取三个上游结果，执行职业路径规划并写回 state。"""
        student_state = self.state_manager.load_state(state_path)
        return self.run(
            student_profile_result=safe_dict(student_state.get("student_profile_result")),
            job_profile_result=safe_dict(student_state.get("job_profile_result")),
            job_match_result=safe_dict(student_state.get("job_match_result")),
            student_state=student_state,
            context_data=context_data,
            state_path=state_path,
            builder_output_path=builder_output_path,
            selector_output_path=selector_output_path,
            service_output_path=service_output_path,
            extra_context=extra_context,
        )


def run_career_path_plan_service(
    student_profile_result: Dict[str, Any],
    job_profile_result: Dict[str, Any],
    job_match_result: Dict[str, Any],
    student_state: Optional[Dict[str, Any]] = None,
    context_data: Optional[Dict[str, Any]] = None,
    state_path: Optional[str | Path] = DEFAULT_STATE_PATH,
    builder_output_path: Optional[str | Path] = DEFAULT_BUILDER_OUTPUT_PATH,
    selector_output_path: Optional[str | Path] = DEFAULT_SELECTOR_OUTPUT_PATH,
    service_output_path: Optional[str | Path] = DEFAULT_SERVICE_OUTPUT_PATH,
    extra_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """函数式服务入口。"""
    return CareerPathPlanService().run(
        student_profile_result=student_profile_result,
        job_profile_result=job_profile_result,
        job_match_result=job_match_result,
        student_state=student_state,
        context_data=context_data,
        state_path=state_path,
        builder_output_path=builder_output_path,
        selector_output_path=selector_output_path,
        service_output_path=service_output_path,
        extra_context=extra_context,
    )


def run_career_path_plan_service_from_state(
    state_path: str | Path = DEFAULT_STATE_PATH,
    context_data: Optional[Dict[str, Any]] = None,
    builder_output_path: Optional[str | Path] = DEFAULT_BUILDER_OUTPUT_PATH,
    selector_output_path: Optional[str | Path] = DEFAULT_SELECTOR_OUTPUT_PATH,
    service_output_path: Optional[str | Path] = DEFAULT_SERVICE_OUTPUT_PATH,
    extra_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """函数式 state 驱动入口。"""
    return CareerPathPlanService().run_from_state(
        state_path=state_path,
        context_data=context_data,
        builder_output_path=builder_output_path,
        selector_output_path=selector_output_path,
        service_output_path=service_output_path,
        extra_context=extra_context,
    )


def parse_args() -> argparse.Namespace:
    """命令行参数解析。"""
    parser = argparse.ArgumentParser(description="Run career_path_plan service")
    parser.add_argument("--state-path", default=str(DEFAULT_STATE_PATH), help="student.json 路径")
    parser.add_argument("--student-profile-json", default="", help="可选：单独的 student_profile_result JSON 文件")
    parser.add_argument("--job-profile-json", default="", help="可选：单独的 job_profile_result JSON 文件")
    parser.add_argument("--job-match-json", default="", help="可选：单独的 job_match_result JSON 文件")
    parser.add_argument("--builder-output", default=str(DEFAULT_BUILDER_OUTPUT_PATH), help="builder 输出 JSON 路径")
    parser.add_argument("--selector-output", default=str(DEFAULT_SELECTOR_OUTPUT_PATH), help="selector 输出 JSON 路径")
    parser.add_argument("--service-output", default=str(DEFAULT_SERVICE_OUTPUT_PATH), help="service 输出 JSON 路径")
    parser.add_argument("--use-demo", action="store_true", help="使用内置 mock 上游结果跑 demo")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    demo_context = {
        "graph_context": {
            "mock_note": "预留 Neo4j 图谱路径上下文，当前 career_path_plan demo 使用 mock。",
        },
        "sql_context": {
            "mock_note": "预留 SQL 岗位明细上下文，当前 career_path_plan demo 使用 mock。",
        },
    }

    if args.use_demo:
        result = run_career_path_plan_service(
            student_profile_result=build_demo_student_profile_result(),
            job_profile_result=build_demo_job_profile_result(),
            job_match_result=build_demo_job_match_result(),
            state_path=args.state_path,
            context_data=demo_context,
            builder_output_path=args.builder_output,
            selector_output_path=args.selector_output,
            service_output_path=args.service_output,
            extra_context={"demo_name": "career_path_plan_service_demo"},
        )
    elif args.student_profile_json and args.job_profile_json and args.job_match_json:
        result = run_career_path_plan_service(
            student_profile_result=load_json_file(args.student_profile_json),
            job_profile_result=load_json_file(args.job_profile_json),
            job_match_result=load_json_file(args.job_match_json),
            state_path=args.state_path,
            context_data=demo_context,
            builder_output_path=args.builder_output,
            selector_output_path=args.selector_output,
            service_output_path=args.service_output,
            extra_context={"demo_name": "career_path_plan_service_from_json"},
        )
    else:
        result = run_career_path_plan_service_from_state(
            state_path=args.state_path,
            context_data=demo_context,
            builder_output_path=args.builder_output,
            selector_output_path=args.selector_output,
            service_output_path=args.service_output,
            extra_context={"demo_name": "career_path_plan_service_from_state"},
        )

    print(json.dumps(result, ensure_ascii=False, indent=2))
