"""
job_match_service.py

job_match 模块业务服务层。

职责：
1. 调用 job_match_builder 构造 match_input_payload；
2. 调用 job_match_scorer 生成规则评分结果；
3. 通过统一大模型接口 call_llm(task_type="job_match", ...) 补充解释性分析；
4. 合并规则结果与模型结果；
5. 写回 student.json 的 job_match_result 字段。

边界约束：
- 不重写 llm_service 和 state_manager；
- 不重写 student_profile / job_profile；
- 只做服务编排、默认值补齐、异常兜底和状态写回。
"""

from __future__ import annotations

import argparse
import json
import logging
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from .job_match_builder import (
    build_demo_job_profile_result,
    build_demo_student_profile_result,
    build_match_input_payload,
)
from .job_match_scorer import score_match_input_payload
from llm_interface_layer.llm_service import call_llm
from llm_interface_layer.state_manager import StateManager


LOGGER = logging.getLogger(__name__)
DEFAULT_STATE_PATH = Path("outputs/state/student.json")
DEFAULT_BUILDER_OUTPUT_PATH = Path("outputs/state/job_match_input_payload.json")
DEFAULT_SCORER_OUTPUT_PATH = Path("outputs/state/job_match_score_result.json")
DEFAULT_SERVICE_OUTPUT_PATH = Path("outputs/state/job_match_service_result.json")


@dataclass
class JobMatchLLMSupplement:
    """大模型补充的人岗匹配解释字段。"""

    strengths: List[str] = field(default_factory=list)
    weaknesses: List[str] = field(default_factory=list)
    improvement_suggestions: List[str] = field(default_factory=list)
    recommendation: str = ""
    analysis_summary: str = ""


@dataclass
class JobMatchServiceResult:
    """最终写回 student.json 的 job_match_result 结构。"""

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
    score_weights: Dict[str, float] = field(default_factory=dict)
    rule_score_result: Dict[str, Any] = field(default_factory=dict)
    match_input_payload: Dict[str, Any] = field(default_factory=dict)
    llm_match_result: Dict[str, Any] = field(default_factory=dict)
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
    """安全文本清洗。"""
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


def safe_float(value: Any, default: float = 0.0) -> float:
    """安全转 float。"""
    text = clean_text(value)
    if not text:
        return default
    try:
        return float(text)
    except (TypeError, ValueError):
        return default


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


def build_default_recommendation(rule_score_result: Dict[str, Any]) -> str:
    """根据规则总分生成兜底推荐结论。"""
    score = safe_float(rule_score_result.get("overall_match_score"), default=0.0)
    if score >= 85:
        return "推荐优先投递该岗位，整体匹配度较高，可同步准备面试项目表达和岗位业务理解。"
    if score >= 70:
        return "可以投递该岗位，但建议优先补齐关键技能或项目短板后再重点冲刺。"
    if score >= 60:
        return "可作为尝试性投递岗位，但当前仍存在较明显能力缺口，建议先进行定向补强。"
    return "暂不建议将该岗位作为第一优先级目标，建议先选择门槛更贴近当前画像的岗位或进行系统补强。"


def build_default_analysis_summary(
    rule_score_result: Dict[str, Any],
    match_input_payload: Dict[str, Any],
) -> str:
    """生成兜底分析摘要。"""
    job_profile = safe_dict(match_input_payload.get("job_profile"))
    student_profile = safe_dict(match_input_payload.get("student_profile"))
    job_name = clean_text(job_profile.get("standard_job_name")) or "目标岗位"
    student_major = clean_text(student_profile.get("major")) or "当前专业"
    rule_summary = clean_text(rule_score_result.get("rule_summary"))
    if rule_summary:
        return f"针对{job_name}，候选人{student_major}背景与岗位要求的规则评估结果如下：{rule_summary}"
    return f"针对{job_name}，候选人{student_major}背景具备一定基础，但仍需结合技能、实践和职业方向进一步分析匹配度。"


def build_default_improvement_suggestions(
    rule_score_result: Dict[str, Any],
    llm_result: Dict[str, Any],
) -> List[str]:
    """当 LLM 未返回建议时，根据 missing_items / weaknesses 生成兜底建议。"""
    suggestions = [
        clean_text(item)
        for item in safe_list(llm_result.get("improvement_suggestions"))
        if clean_text(item)
    ]
    if suggestions:
        return dedup_keep_order(suggestions)

    missing_items = [safe_dict(item) for item in safe_list(rule_score_result.get("missing_items"))]
    generated = []
    for item in missing_items:
        dimension = clean_text(item.get("dimension"))
        required_item = clean_text(item.get("required_item"))
        if dimension == "hard_skills" and required_item:
            generated.append(f"优先补充岗位核心硬技能：{required_item}，并在项目经历中补充对应应用场景。")
        elif dimension == "tool_skills" and required_item:
            generated.append(f"补充工具技能 {required_item} 的实操证明，例如看板、分析报告或作品集。")
        elif dimension == "soft_skills" and required_item:
            generated.append(f"在项目/实习描述中强化 {required_item} 的行为证据。")
        elif dimension == "practice_experience" and required_item:
            generated.append(f"围绕岗位实践要求“{required_item}”补充项目或实习案例。")
        elif dimension == "major":
            generated.append("如果专业背景不完全对口，建议用课程项目、证书或领域项目补足岗位方向背书。")
        elif dimension == "education":
            generated.append("若学历略低于岗位门槛，建议优先通过高质量项目、实习成果或证书增强替代性竞争力。")

    if not generated:
        generated.append("建议围绕岗位要求继续补充可量化项目成果、关键技能证据和业务场景理解。")
    return dedup_keep_order(generated)[:8]


def normalize_llm_job_match_result(
    llm_result: Dict[str, Any],
    rule_score_result: Dict[str, Any],
    match_input_payload: Dict[str, Any],
) -> Dict[str, Any]:
    """对 call_llm('job_match', ...) 的返回做默认值补齐和新旧字段兼容。"""
    source = safe_dict(llm_result)
    matched_items = [safe_dict(item) for item in safe_list(rule_score_result.get("matched_items"))]
    missing_items = [safe_dict(item) for item in safe_list(rule_score_result.get("missing_items"))]

    strengths = [
        clean_text(item)
        for item in safe_list(source.get("strengths"))
        if clean_text(item)
    ]
    if not strengths:
        strengths = [
            f"已满足 {clean_text(item.get('required_item'))}"
            for item in matched_items
            if clean_text(item.get("required_item"))
        ][:8]

    weaknesses = [
        clean_text(item)
        for item in safe_list(source.get("weaknesses") or source.get("gaps"))
        if clean_text(item)
    ]
    if not weaknesses:
        weaknesses = [
            clean_text(item.get("reason")) or f"缺少 {clean_text(item.get('required_item'))}"
            for item in missing_items
            if clean_text(item.get("reason")) or clean_text(item.get("required_item"))
        ][:8]

    supplement = JobMatchLLMSupplement(
        strengths=dedup_keep_order(strengths),
        weaknesses=dedup_keep_order(weaknesses),
        improvement_suggestions=build_default_improvement_suggestions(rule_score_result, source),
        recommendation=clean_text(source.get("recommendation")) or build_default_recommendation(rule_score_result),
        analysis_summary=clean_text(source.get("analysis_summary") or source.get("summary"))
        or build_default_analysis_summary(rule_score_result, match_input_payload),
    )
    return asdict(supplement) | {"raw_llm_result": source}


def build_job_match_llm_input(
    match_input_payload: Dict[str, Any],
    rule_score_result: Dict[str, Any],
) -> Dict[str, Any]:
    """组装 job_match 大模型输入。"""
    return {
        "match_input_payload": deepcopy(safe_dict(match_input_payload)),
        "rule_score_result": deepcopy(safe_dict(rule_score_result)),
        "generation_requirements": {
            "strengths": "结合 matched_items 和岗位画像，总结候选人相对该岗位的核心优势点。",
            "weaknesses": "结合 missing_items 和岗位画像，总结候选人当前最关键的短板。",
            "improvement_suggestions": "针对短板给出可执行的补强建议，优先覆盖技能、项目、证书和表达优化。",
            "recommendation": "给出是否建议投递该岗位，以及投递优先级建议。",
            "analysis_summary": "输出一段简洁的人岗匹配分析摘要，适合后续报告模块引用。",
        },
        "output_schema_hint": asdict(JobMatchLLMSupplement()),
    }


def merge_job_match_results(
    match_input_payload: Dict[str, Any],
    rule_score_result: Dict[str, Any],
    llm_result: Dict[str, Any],
    service_warnings: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """合并规则评分结果与 LLM 补充结果。"""
    payload = safe_dict(match_input_payload)
    rule_result = safe_dict(rule_score_result)
    normalized_llm = normalize_llm_job_match_result(
        llm_result=llm_result,
        rule_score_result=rule_result,
        match_input_payload=payload,
    )

    merged_warnings = dedup_keep_order(
        [clean_text(item) for item in safe_list(payload.get("build_warnings")) if clean_text(item)]
        + [clean_text(item) for item in safe_list(service_warnings) if clean_text(item)]
    )

    result = JobMatchServiceResult(
        basic_requirement_score=safe_float(rule_result.get("basic_requirement_score"), default=0.0),
        vocational_skill_score=safe_float(rule_result.get("vocational_skill_score"), default=0.0),
        professional_quality_score=safe_float(rule_result.get("professional_quality_score"), default=0.0),
        development_potential_score=safe_float(rule_result.get("development_potential_score"), default=0.0),
        overall_match_score=safe_float(
            normalized_llm.get("overall_match_score")
            or rule_result.get("overall_match_score"),
            default=0.0,
        ),
        score_level=clean_text(rule_result.get("score_level")),
        matched_items=deepcopy([safe_dict(item) for item in safe_list(rule_result.get("matched_items"))]),
        missing_items=deepcopy([safe_dict(item) for item in safe_list(rule_result.get("missing_items"))]),
        strengths=dedup_keep_order(
            [clean_text(item) for item in safe_list(normalized_llm.get("strengths")) if clean_text(item)]
        ),
        weaknesses=dedup_keep_order(
            [clean_text(item) for item in safe_list(normalized_llm.get("weaknesses")) if clean_text(item)]
        ),
        improvement_suggestions=dedup_keep_order(
            [
                clean_text(item)
                for item in safe_list(normalized_llm.get("improvement_suggestions"))
                if clean_text(item)
            ]
        ),
        recommendation=clean_text(normalized_llm.get("recommendation")),
        analysis_summary=clean_text(normalized_llm.get("analysis_summary")),
        dimension_details=deepcopy(safe_dict(rule_result.get("dimension_details"))),
        score_weights=deepcopy(safe_dict(rule_result.get("score_weights"))),
        rule_score_result=deepcopy(rule_result),
        match_input_payload=deepcopy(payload),
        llm_match_result=deepcopy(safe_dict(normalized_llm.get("raw_llm_result"))),
        build_warnings=merged_warnings,
    )

    final_result = asdict(result)
    if not final_result["score_level"]:
        score = final_result["overall_match_score"]
        if score >= 90:
            final_result["score_level"] = "A-高度匹配"
        elif score >= 80:
            final_result["score_level"] = "B-较高匹配"
        elif score >= 70:
            final_result["score_level"] = "C-中等匹配"
        elif score >= 60:
            final_result["score_level"] = "D-勉强匹配"
        else:
            final_result["score_level"] = "E-匹配度较低"
    return final_result


class JobMatchService:
    """job_match 业务服务编排器。"""

    def __init__(self, state_manager: Optional[StateManager] = None) -> None:
        self.state_manager = state_manager or StateManager()

    def build_payload(
        self,
        student_profile_result: Dict[str, Any],
        job_profile_result: Dict[str, Any],
        output_path: Optional[str | Path] = DEFAULT_BUILDER_OUTPUT_PATH,
    ) -> Dict[str, Any]:
        """调用 builder 构造 match_input_payload。"""
        return build_match_input_payload(
            student_profile_result=student_profile_result,
            job_profile_result=job_profile_result,
            output_path=output_path,
        )

    def score_rules(
        self,
        match_input_payload: Dict[str, Any],
        output_path: Optional[str | Path] = DEFAULT_SCORER_OUTPUT_PATH,
    ) -> Dict[str, Any]:
        """调用 scorer 生成规则评分结果。"""
        return score_match_input_payload(
            match_input_payload=match_input_payload,
            output_path=output_path,
        )

    def call_job_match_llm(
        self,
        match_input_payload: Dict[str, Any],
        rule_score_result: Dict[str, Any],
        student_state: Optional[Dict[str, Any]] = None,
        context_data: Optional[Dict[str, Any]] = None,
        extra_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """通过统一大模型接口补充人岗匹配解释字段。"""
        input_data = build_job_match_llm_input(
            match_input_payload=match_input_payload,
            rule_score_result=rule_score_result,
        )
        merged_extra_context = {
            "service_name": "job_match_service",
            "expected_fields": [
                "strengths",
                "weaknesses",
                "improvement_suggestions",
                "recommendation",
                "analysis_summary",
            ],
        }
        if extra_context:
            merged_extra_context.update(deepcopy(extra_context))

        return call_llm(
            task_type="job_match",
            input_data=input_data,
            context_data=context_data,
            student_state=student_state,
            extra_context=merged_extra_context,
        )

    def run(
        self,
        student_profile_result: Dict[str, Any],
        job_profile_result: Dict[str, Any],
        student_state: Optional[Dict[str, Any]] = None,
        context_data: Optional[Dict[str, Any]] = None,
        state_path: Optional[str | Path] = DEFAULT_STATE_PATH,
        builder_output_path: Optional[str | Path] = DEFAULT_BUILDER_OUTPUT_PATH,
        scorer_output_path: Optional[str | Path] = DEFAULT_SCORER_OUTPUT_PATH,
        service_output_path: Optional[str | Path] = DEFAULT_SERVICE_OUTPUT_PATH,
        extra_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """执行完整 job_match 服务流程并写回 student.json。"""
        setup_logging()
        service_warnings = []

        if student_state is None:
            student_state = self.state_manager.load_state(state_path)

        LOGGER.info("Step 1/4: build match_input_payload")
        try:
            match_input_payload = self.build_payload(
                student_profile_result=student_profile_result,
                job_profile_result=job_profile_result,
                output_path=builder_output_path,
            )
        except Exception as exc:
            LOGGER.exception("job_match_builder failed")
            match_input_payload = {
                "student_profile": {},
                "job_profile": {},
                "comparable_schema": {},
                "matching_guidance": {},
                "build_warnings": [f"builder 执行失败: {exc}"],
            }
            service_warnings.append(f"builder 执行失败: {exc}")

        LOGGER.info("Step 2/4: score match rules")
        try:
            rule_score_result = self.score_rules(
                match_input_payload=match_input_payload,
                output_path=scorer_output_path,
            )
        except Exception as exc:
            LOGGER.exception("job_match_scorer failed")
            rule_score_result = {
                "basic_requirement_score": 0.0,
                "vocational_skill_score": 0.0,
                "professional_quality_score": 0.0,
                "development_potential_score": 0.0,
                "overall_match_score": 0.0,
                "score_level": "E-匹配度较低",
                "matched_items": [],
                "missing_items": [],
                "dimension_details": {},
                "score_weights": {},
                "rule_summary": "",
                "match_input_payload": deepcopy(match_input_payload),
            }
            service_warnings.append(f"scorer 执行失败: {exc}")

        LOGGER.info("Step 3/4: call LLM job_match")
        try:
            llm_result = self.call_job_match_llm(
                match_input_payload=match_input_payload,
                rule_score_result=rule_score_result,
                student_state=student_state,
                context_data=context_data,
                extra_context=extra_context,
            )
        except Exception as exc:
            LOGGER.exception("call_llm('job_match', ...) failed")
            llm_result = {}
            service_warnings.append(f"LLM 调用失败: {exc}")

        LOGGER.info("Step 4/4: merge results and update state")
        final_result = merge_job_match_results(
            match_input_payload=match_input_payload,
            rule_score_result=rule_score_result,
            llm_result=llm_result,
            service_warnings=service_warnings,
        )
        self.state_manager.update_state(
            task_type="job_match",
            task_result=final_result,
            state_path=state_path,
            student_state=student_state,
        )
        save_json(final_result, service_output_path)

        LOGGER.info(
            "job_match service finished. overall_match_score=%s, score_level=%s",
            final_result.get("overall_match_score"),
            final_result.get("score_level"),
        )
        return final_result

    def run_from_state(
        self,
        state_path: str | Path = DEFAULT_STATE_PATH,
        context_data: Optional[Dict[str, Any]] = None,
        builder_output_path: Optional[str | Path] = DEFAULT_BUILDER_OUTPUT_PATH,
        scorer_output_path: Optional[str | Path] = DEFAULT_SCORER_OUTPUT_PATH,
        service_output_path: Optional[str | Path] = DEFAULT_SERVICE_OUTPUT_PATH,
        extra_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """从 student.json 读取画像结果，执行人岗匹配并写回 job_match_result。"""
        student_state = self.state_manager.load_state(state_path)
        return self.run(
            student_profile_result=safe_dict(student_state.get("student_profile_result")),
            job_profile_result=safe_dict(student_state.get("job_profile_result")),
            student_state=student_state,
            context_data=context_data,
            state_path=state_path,
            builder_output_path=builder_output_path,
            scorer_output_path=scorer_output_path,
            service_output_path=service_output_path,
            extra_context=extra_context,
        )


def run_job_match_service(
    student_profile_result: Dict[str, Any],
    job_profile_result: Dict[str, Any],
    student_state: Optional[Dict[str, Any]] = None,
    context_data: Optional[Dict[str, Any]] = None,
    state_path: Optional[str | Path] = DEFAULT_STATE_PATH,
    builder_output_path: Optional[str | Path] = DEFAULT_BUILDER_OUTPUT_PATH,
    scorer_output_path: Optional[str | Path] = DEFAULT_SCORER_OUTPUT_PATH,
    service_output_path: Optional[str | Path] = DEFAULT_SERVICE_OUTPUT_PATH,
    extra_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """函数式服务入口。"""
    return JobMatchService().run(
        student_profile_result=student_profile_result,
        job_profile_result=job_profile_result,
        student_state=student_state,
        context_data=context_data,
        state_path=state_path,
        builder_output_path=builder_output_path,
        scorer_output_path=scorer_output_path,
        service_output_path=service_output_path,
        extra_context=extra_context,
    )


def run_job_match_service_from_state(
    state_path: str | Path = DEFAULT_STATE_PATH,
    context_data: Optional[Dict[str, Any]] = None,
    builder_output_path: Optional[str | Path] = DEFAULT_BUILDER_OUTPUT_PATH,
    scorer_output_path: Optional[str | Path] = DEFAULT_SCORER_OUTPUT_PATH,
    service_output_path: Optional[str | Path] = DEFAULT_SERVICE_OUTPUT_PATH,
    extra_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """函数式 state 驱动入口。"""
    return JobMatchService().run_from_state(
        state_path=state_path,
        context_data=context_data,
        builder_output_path=builder_output_path,
        scorer_output_path=scorer_output_path,
        service_output_path=service_output_path,
        extra_context=extra_context,
    )


def parse_args() -> argparse.Namespace:
    """命令行参数解析。"""
    parser = argparse.ArgumentParser(description="Run job_match service")
    parser.add_argument(
        "--state-path",
        default=str(DEFAULT_STATE_PATH),
        help="student.json 路径",
    )
    parser.add_argument(
        "--student-profile-json",
        default="",
        help="可选：单独的 student_profile_result JSON 路径；若提供则优先使用文件输入",
    )
    parser.add_argument(
        "--job-profile-json",
        default="",
        help="可选：单独的 job_profile_result JSON 路径；若提供则优先使用文件输入",
    )
    parser.add_argument(
        "--builder-output",
        default=str(DEFAULT_BUILDER_OUTPUT_PATH),
        help="builder 输出 JSON 路径",
    )
    parser.add_argument(
        "--scorer-output",
        default=str(DEFAULT_SCORER_OUTPUT_PATH),
        help="scorer 输出 JSON 路径",
    )
    parser.add_argument(
        "--service-output",
        default=str(DEFAULT_SERVICE_OUTPUT_PATH),
        help="service 输出 JSON 路径",
    )
    parser.add_argument(
        "--use-demo",
        action="store_true",
        help="使用内置 mock student/job profile 数据跑 demo",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    demo_context = {
        "graph_context": {
            "mock_note": "预留图谱上下文接入位，当前 job_match demo 使用 mock。",
        },
        "sql_context": {
            "mock_note": "预留 SQL 上下文接入位，当前 job_match demo 使用 mock。",
        },
    }

    if args.use_demo:
        result = run_job_match_service(
            student_profile_result=build_demo_student_profile_result(),
            job_profile_result=build_demo_job_profile_result(),
            state_path=args.state_path,
            context_data=demo_context,
            builder_output_path=args.builder_output,
            scorer_output_path=args.scorer_output,
            service_output_path=args.service_output,
            extra_context={"demo_name": "job_match_service_demo"},
        )
    elif args.student_profile_json and args.job_profile_json:
        result = run_job_match_service(
            student_profile_result=load_json_file(args.student_profile_json),
            job_profile_result=load_json_file(args.job_profile_json),
            state_path=args.state_path,
            context_data=demo_context,
            builder_output_path=args.builder_output,
            scorer_output_path=args.scorer_output,
            service_output_path=args.service_output,
            extra_context={"demo_name": "job_match_service_from_json"},
        )
    else:
        result = run_job_match_service_from_state(
            state_path=args.state_path,
            context_data=demo_context,
            builder_output_path=args.builder_output,
            scorer_output_path=args.scorer_output,
            service_output_path=args.service_output,
            extra_context={"demo_name": "job_match_service_from_state"},
        )

    print(json.dumps(result, ensure_ascii=False, indent=2))
