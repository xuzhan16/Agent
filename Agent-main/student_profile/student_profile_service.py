"""
student_profile_service.py

student_profile 模块业务服务层。

职责：
1. 调用 student_profile_builder 生成中间特征；
2. 调用 student_profile_scorer 生成规则评分；
3. 通过统一大模型接口 call_llm("student_profile", ...) 生成补充画像；
4. 合并规则结果与模型结果；
5. 写回 student.json 的 student_profile_result 字段。

边界约束：
- 不重写 llm_service 和 state_manager；
- 不重写 resume_parse；
- 不直接调用真实模型 API，只通过统一接口层 call_llm(...)。
"""

from __future__ import annotations

import argparse
import json
import logging
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from llm_interface_layer.llm_service import call_llm
from llm_interface_layer.state_manager import StateManager
from .student_profile_builder import build_profile_input_payload_from_state
from .student_profile_scorer import score_student_profile_payload


LOGGER = logging.getLogger(__name__)
DEFAULT_STATE_PATH = Path("outputs/state/student.json")
DEFAULT_BUILDER_OUTPUT_PATH = Path("outputs/state/student_profile_input_payload.json")
DEFAULT_SCORER_OUTPUT_PATH = Path("outputs/state/student_profile_score_result.json")
DEFAULT_SERVICE_OUTPUT_PATH = Path("outputs/state/student_profile_service_result.json")


@dataclass
class StudentProfileLLMSupplement:
    """大模型补充画像字段。"""

    soft_skills: List[str] = field(default_factory=list)
    potential_profile: Dict[str, Any] = field(default_factory=dict)
    strengths: List[str] = field(default_factory=list)
    weaknesses: List[str] = field(default_factory=list)
    missing_dimensions: List[str] = field(default_factory=list)
    summary: str = ""
    ability_evidence: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StudentProfileServiceResult:
    """最终写回 student.json 的 student_profile_result 结构。"""

    skill_profile: Dict[str, Any] = field(default_factory=dict)
    certificate_profile: List[str] = field(default_factory=list)
    soft_skill_profile: Dict[str, Any] = field(default_factory=dict)
    soft_skills: List[str] = field(default_factory=list)
    potential_profile: Dict[str, Any] = field(default_factory=dict)
    complete_score: float = 0.0
    competitiveness_score: float = 0.0
    score_level: str = ""
    strengths: List[str] = field(default_factory=list)
    weaknesses: List[str] = field(default_factory=list)
    missing_dimensions: List[str] = field(default_factory=list)
    summary: str = ""
    ability_evidence: Dict[str, Any] = field(default_factory=dict)
    rule_score_result: Dict[str, Any] = field(default_factory=dict)
    profile_input_payload: Dict[str, Any] = field(default_factory=dict)
    llm_profile_result: Dict[str, Any] = field(default_factory=dict)
    build_warnings: List[str] = field(default_factory=list)


def setup_logging() -> None:
    """初始化简易日志。"""
    if LOGGER.handlers:
        return
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
    )


def _safe_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if value is None or value == "":
        return []
    return [value]


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _dedup_keep_order(values: List[Any]) -> List[Any]:
    """对标量/可 JSON 序列化对象做稳定去重。"""
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


def _save_json(data: Dict[str, Any], output_path: Optional[str | Path]) -> None:
    """按需保存 JSON 文件。"""
    if not output_path:
        return
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _infer_soft_skills_from_soft_skill_profile(soft_skill_profile: Dict[str, Any]) -> List[str]:
    """从旧版 soft_skill_profile 字典里兜底提取 soft_skills。"""
    result = []
    for key in soft_skill_profile.keys():
        cleaned = _clean_text(key)
        if cleaned:
            result.append(cleaned)
    return _dedup_keep_order(result)


def _build_default_skill_profile(profile_input_payload: Dict[str, Any]) -> Dict[str, Any]:
    """当模型未返回 skill_profile 时，用 builder 结果生成兜底结构。"""
    normalized_profile = _safe_dict(profile_input_payload.get("normalized_profile"))
    skills = _safe_list(normalized_profile.get("hard_skills")) + _safe_list(
        normalized_profile.get("tool_skills")
    )
    return {_clean_text(skill): "待模型评估" for skill in skills if _clean_text(skill)}


def _build_default_potential_profile(
    profile_input_payload: Dict[str, Any],
    rule_score_result: Dict[str, Any],
) -> Dict[str, Any]:
    """当模型未返回 potential_profile 时，根据规则分和标签生成兜底结构。"""
    normalized_profile = _safe_dict(profile_input_payload.get("normalized_profile"))
    occupation_hints = [
        _clean_text(item)
        for item in _safe_list(normalized_profile.get("occupation_hints"))
        if _clean_text(item)
    ]
    domain_tags = [
        _clean_text(item)
        for item in _safe_list(normalized_profile.get("domain_tags"))
        if _clean_text(item)
    ]
    competitiveness_score = float(rule_score_result.get("competitiveness_base_score") or 0.0)

    if competitiveness_score >= 80:
        growth_level = "较强"
    elif competitiveness_score >= 65:
        growth_level = "中等偏上"
    elif competitiveness_score >= 50:
        growth_level = "中等"
    else:
        growth_level = "待提升"

    return {
        "growth_level": growth_level,
        "preferred_directions": occupation_hints[:3],
        "domain_tags": domain_tags[:5],
        "basis_score": competitiveness_score,
        "reason": "根据规则评分、职业方向标签和领域标签生成的兜底潜力画像。",
    }


def _build_missing_dimensions_from_rule_reasons(rule_score_result: Dict[str, Any]) -> List[str]:
    """根据规则侧 weaknesses/suggestions 兜底生成缺失维度。"""
    reason_block = _safe_dict(rule_score_result.get("score_reasons"))
    weakness_text = " ".join(
        _clean_text(item)
        for item in _safe_list(reason_block.get("weaknesses")) + _safe_list(reason_block.get("suggestions"))
        if _clean_text(item)
    )

    dimensions = []
    if "技能" in weakness_text or "工具" in weakness_text:
        dimensions.append("核心技能/工具栈")
    if "项目" in weakness_text:
        dimensions.append("项目经历")
    if "实习" in weakness_text:
        dimensions.append("实习经历")
    if "证书" in weakness_text or "获奖" in weakness_text:
        dimensions.append("证书/竞赛背书")
    if "求职意向" in weakness_text or "职业方向" in weakness_text:
        dimensions.append("求职方向明确度")
    if "教育" in weakness_text or "学历" in weakness_text or "专业" in weakness_text:
        dimensions.append("教育背景完整度")
    return _dedup_keep_order(dimensions)


def _build_ability_evidence(
    profile_input_payload: Dict[str, Any],
    rule_score_result: Dict[str, Any],
) -> Dict[str, Any]:
    """生成能力证据兜底结构。"""
    explicit_profile = _safe_dict(profile_input_payload.get("explicit_profile"))
    normalized_profile = _safe_dict(profile_input_payload.get("normalized_profile"))
    practice_profile = _safe_dict(profile_input_payload.get("practice_profile"))

    return {
        "project_examples": deepcopy(_safe_list(explicit_profile.get("project_experience"))[:5]),
        "internship_examples": deepcopy(_safe_list(explicit_profile.get("internship_experience"))[:5]),
        "certificate_tags": deepcopy(_safe_list(normalized_profile.get("qualification_tags"))),
        "practice_tags": deepcopy(_safe_list(normalized_profile.get("experience_tags"))),
        "project_keywords": deepcopy(_safe_list(practice_profile.get("project_keywords"))),
        "internship_keywords": deepcopy(_safe_list(practice_profile.get("internship_keywords"))),
        "score_evidence": {
            "profile_completeness_score": float(rule_score_result.get("profile_completeness_score") or 0.0),
            "competitiveness_base_score": float(rule_score_result.get("competitiveness_base_score") or 0.0),
            "score_level": _clean_text(rule_score_result.get("score_level")),
        },
    }


def normalize_llm_student_profile_result(
    llm_result: Dict[str, Any],
    profile_input_payload: Dict[str, Any],
    rule_score_result: Dict[str, Any],
) -> Dict[str, Any]:
    """对 call_llm('student_profile', ...) 的返回做默认值补齐和新旧字段兼容。"""
    llm_result = _safe_dict(llm_result)
    normalized_profile = _safe_dict(profile_input_payload.get("normalized_profile"))
    explicit_profile = _safe_dict(profile_input_payload.get("explicit_profile"))

    skill_profile = _safe_dict(llm_result.get("skill_profile")) or _build_default_skill_profile(
        profile_input_payload
    )
    certificate_profile = _dedup_keep_order(
        [
            _clean_text(item)
            for item in _safe_list(
                llm_result.get(
                    "certificate_profile",
                    explicit_profile.get("certificates", []),
                )
            )
            if _clean_text(item)
        ]
    )
    soft_skill_profile = _safe_dict(llm_result.get("soft_skill_profile"))

    soft_skills = _dedup_keep_order(
        [
            _clean_text(item)
            for item in _safe_list(llm_result.get("soft_skills"))
            if _clean_text(item)
        ]
        + _infer_soft_skills_from_soft_skill_profile(soft_skill_profile)
    )

    potential_profile = _safe_dict(llm_result.get("potential_profile")) or _build_default_potential_profile(
        profile_input_payload,
        rule_score_result,
    )
    strengths = _dedup_keep_order(
        [_clean_text(item) for item in _safe_list(llm_result.get("strengths")) if _clean_text(item)]
    )
    weaknesses = _dedup_keep_order(
        [_clean_text(item) for item in _safe_list(llm_result.get("weaknesses")) if _clean_text(item)]
    )
    missing_dimensions = _dedup_keep_order(
        [
            _clean_text(item)
            for item in _safe_list(llm_result.get("missing_dimensions"))
            if _clean_text(item)
        ]
        + _build_missing_dimensions_from_rule_reasons(rule_score_result)
    )
    summary = _clean_text(llm_result.get("summary"))
    if not summary:
        occupation_hints = [
            _clean_text(item)
            for item in _safe_list(normalized_profile.get("occupation_hints"))
            if _clean_text(item)
        ]
        target_hint = occupation_hints[0] if occupation_hints else "待明确方向"
        summary = f"该学生在{target_hint}方向已有一定基础，建议结合技能、项目和实习短板继续补强。"

    ability_evidence = _safe_dict(llm_result.get("ability_evidence"))
    if not ability_evidence:
        ability_evidence = _build_ability_evidence(profile_input_payload, rule_score_result)

    return asdict(
        StudentProfileLLMSupplement(
            soft_skills=soft_skills,
            potential_profile=potential_profile,
            strengths=strengths,
            weaknesses=weaknesses,
            missing_dimensions=missing_dimensions,
            summary=summary,
            ability_evidence=ability_evidence,
        )
    ) | {
        "skill_profile": skill_profile,
        "certificate_profile": certificate_profile,
        "soft_skill_profile": soft_skill_profile,
        "raw_llm_result": llm_result,
    }


def build_student_profile_llm_input(
    profile_input_payload: Dict[str, Any],
    rule_score_result: Dict[str, Any],
) -> Dict[str, Any]:
    """组装 student_profile 任务的大模型输入。"""
    explicit_profile = _safe_dict(profile_input_payload.get("explicit_profile"))
    normalized_profile = _safe_dict(profile_input_payload.get("normalized_profile"))
    practice_profile = _safe_dict(profile_input_payload.get("practice_profile"))
    score_reasons = _safe_dict(rule_score_result.get("score_reasons"))

    project_examples = []
    for item in _safe_list(explicit_profile.get("project_experience"))[:2]:
        item_dict = _safe_dict(item)
        project_examples.append(
            {
                "name": _clean_text(item_dict.get("name") or item_dict.get("project_name")),
                "role": _clean_text(item_dict.get("role")),
                "desc": _clean_text(item_dict.get("desc") or item_dict.get("description")),
            }
        )

    internship_examples = []
    for item in _safe_list(explicit_profile.get("internship_experience"))[:2]:
        item_dict = _safe_dict(item)
        internship_examples.append(
            {
                "company": _clean_text(item_dict.get("company")),
                "role": _clean_text(item_dict.get("role")),
                "desc": _clean_text(item_dict.get("desc") or item_dict.get("description")),
            }
        )

    return {
        "student_snapshot": {
            "basic_info": {
                "school": _clean_text(explicit_profile.get("school")),
                "major": _clean_text(explicit_profile.get("major")),
                "degree": _clean_text(explicit_profile.get("degree")),
                "graduation_year": _clean_text(explicit_profile.get("graduation_year")),
                "target_job": _clean_text(explicit_profile.get("target_job")),
            },
            "skills": {
                "hard_skills": [
                    _clean_text(item)
                    for item in _safe_list(normalized_profile.get("hard_skills"))
                    if _clean_text(item)
                ][:12],
                "tool_skills": [
                    _clean_text(item)
                    for item in _safe_list(normalized_profile.get("tool_skills"))
                    if _clean_text(item)
                ][:10],
                "qualification_tags": [
                    _clean_text(item)
                    for item in _safe_list(normalized_profile.get("qualification_tags"))
                    if _clean_text(item)
                ][:10],
            },
            "experience_summary": {
                "project_count": len(_safe_list(explicit_profile.get("project_experience"))),
                "internship_count": len(_safe_list(explicit_profile.get("internship_experience"))),
                "project_keywords": deepcopy(_safe_list(practice_profile.get("project_keywords"))[:8]),
                "internship_keywords": deepcopy(_safe_list(practice_profile.get("internship_keywords"))[:8]),
                "project_examples": project_examples,
                "internship_examples": internship_examples,
            },
            "career_hints": {
                "occupation_hints": deepcopy(_safe_list(normalized_profile.get("occupation_hints"))[:6]),
                "domain_tags": deepcopy(_safe_list(normalized_profile.get("domain_tags"))[:6]),
                "experience_tags": deepcopy(_safe_list(normalized_profile.get("experience_tags"))[:8]),
            },
        },
        "rule_score_snapshot": {
            "profile_completeness_score": float(rule_score_result.get("profile_completeness_score") or 0.0),
            "competitiveness_base_score": float(rule_score_result.get("competitiveness_base_score") or 0.0),
            "score_level": _clean_text(rule_score_result.get("score_level")),
            "strengths": deepcopy(_safe_list(score_reasons.get("strengths"))[:6]),
            "weaknesses": deepcopy(_safe_list(score_reasons.get("weaknesses"))[:6]),
            "suggestions": deepcopy(_safe_list(score_reasons.get("suggestions"))[:6]),
        },
        "generation_requirements": {
            "soft_skills": "从项目、实习、自我评价和经历表述中归纳软技能标签。",
            "potential_profile": "结合专业、技能、实践、方向聚焦度，给出潜力画像。",
            "strengths": "总结学生相对优势，尽量可落到可证明证据。",
            "weaknesses": "总结当前主要短板，避免泛泛而谈。",
            "missing_dimensions": "列出简历画像中仍缺失或薄弱的能力维度。",
            "summary": "给出适合后续人岗匹配使用的一段简洁画像总结。",
            "ability_evidence": "按项目/实习/证书/技能证据组织可追溯依据。",
        },
        "output_schema_hint": asdict(StudentProfileLLMSupplement()),
    }


def merge_rule_and_llm_result(
    profile_input_payload: Dict[str, Any],
    rule_score_result: Dict[str, Any],
    llm_result: Dict[str, Any],
    build_warnings: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """合并规则评分结果和模型补充结果。"""
    normalized_llm_result = normalize_llm_student_profile_result(
        llm_result=llm_result,
        profile_input_payload=profile_input_payload,
        rule_score_result=rule_score_result,
    )
    rule_reasons = _safe_dict(rule_score_result.get("score_reasons"))

    merged_strengths = _dedup_keep_order(
        [
            _clean_text(item)
            for item in _safe_list(rule_reasons.get("strengths")) + _safe_list(normalized_llm_result.get("strengths"))
            if _clean_text(item)
        ]
    )
    merged_weaknesses = _dedup_keep_order(
        [
            _clean_text(item)
            for item in _safe_list(rule_reasons.get("weaknesses")) + _safe_list(normalized_llm_result.get("weaknesses"))
            if _clean_text(item)
        ]
    )

    result = StudentProfileServiceResult(
        skill_profile=_safe_dict(normalized_llm_result.get("skill_profile")),
        certificate_profile=[
            _clean_text(item)
            for item in _safe_list(normalized_llm_result.get("certificate_profile"))
            if _clean_text(item)
        ],
        soft_skill_profile=_safe_dict(normalized_llm_result.get("soft_skill_profile")),
        soft_skills=[
            _clean_text(item)
            for item in _safe_list(normalized_llm_result.get("soft_skills"))
            if _clean_text(item)
        ],
        potential_profile=_safe_dict(normalized_llm_result.get("potential_profile")),
        complete_score=float(rule_score_result.get("profile_completeness_score") or 0.0),
        competitiveness_score=float(rule_score_result.get("competitiveness_base_score") or 0.0),
        score_level=_clean_text(rule_score_result.get("score_level")),
        strengths=merged_strengths,
        weaknesses=merged_weaknesses,
        missing_dimensions=[
            _clean_text(item)
            for item in _safe_list(normalized_llm_result.get("missing_dimensions"))
            if _clean_text(item)
        ],
        summary=_clean_text(normalized_llm_result.get("summary")),
        ability_evidence=_safe_dict(normalized_llm_result.get("ability_evidence")),
        rule_score_result=deepcopy(rule_score_result),
        profile_input_payload=deepcopy(profile_input_payload),
        llm_profile_result=_safe_dict(normalized_llm_result.get("raw_llm_result")),
        build_warnings=_dedup_keep_order(
            [
                _clean_text(item)
                for item in _safe_list(build_warnings)
                + _safe_list(_safe_dict(profile_input_payload.get("evidence_summary")).get("parse_warnings"))
                if _clean_text(item)
            ]
        ),
    )
    return asdict(result)


class StudentProfileService:
    """student_profile 服务层编排器。"""

    def __init__(self, state_manager: Optional[StateManager] = None) -> None:
        self.state_manager = state_manager or StateManager()

    def build_profile_payload(
        self,
        student_state: Dict[str, Any],
        builder_output_path: Optional[str | Path] = DEFAULT_BUILDER_OUTPUT_PATH,
    ) -> Dict[str, Any]:
        """调用 builder 生成中间特征。"""
        profile_input_payload = build_profile_input_payload_from_state(student_state)
        _save_json(profile_input_payload, builder_output_path)
        return profile_input_payload

    def score_profile_payload(
        self,
        profile_input_payload: Dict[str, Any],
        scorer_output_path: Optional[str | Path] = DEFAULT_SCORER_OUTPUT_PATH,
    ) -> Dict[str, Any]:
        """调用 scorer 生成规则评分。"""
        rule_score_result = score_student_profile_payload(profile_input_payload)
        _save_json(rule_score_result, scorer_output_path)
        return rule_score_result

    def call_student_profile_llm(
        self,
        profile_input_payload: Dict[str, Any],
        rule_score_result: Dict[str, Any],
        student_state: Dict[str, Any],
        context_data: Optional[Dict[str, Any]] = None,
        extra_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """调用统一大模型接口补充学生画像字段。"""
        llm_input = build_student_profile_llm_input(profile_input_payload, rule_score_result)
        merged_extra_context = {
            "service_name": "student_profile_service",
            "expected_fields": [
                "soft_skills",
                "potential_profile",
                "strengths",
                "weaknesses",
                "missing_dimensions",
                "summary",
                "ability_evidence",
            ],
        }
        if extra_context:
            merged_extra_context.update(deepcopy(extra_context))

        return call_llm(
            task_type="student_profile",
            input_data=llm_input,
            context_data=context_data,
            student_state=student_state,
            extra_context=merged_extra_context,
        )

    def update_student_state(
        self,
        merged_profile_result: Dict[str, Any],
        student_state: Dict[str, Any],
        state_path: str | Path = DEFAULT_STATE_PATH,
    ) -> Dict[str, Any]:
        """写回 student.json 的 student_profile_result 字段。"""
        return self.state_manager.update_state(
            task_type="student_profile",
            task_result=merged_profile_result,
            state_path=state_path,
            student_state=student_state,
        )

    def run(
        self,
        state_path: str | Path = DEFAULT_STATE_PATH,
        context_data: Optional[Dict[str, Any]] = None,
        extra_context: Optional[Dict[str, Any]] = None,
        builder_output_path: Optional[str | Path] = DEFAULT_BUILDER_OUTPUT_PATH,
        scorer_output_path: Optional[str | Path] = DEFAULT_SCORER_OUTPUT_PATH,
        service_output_path: Optional[str | Path] = DEFAULT_SERVICE_OUTPUT_PATH,
    ) -> Dict[str, Any]:
        """
        执行完整 student_profile 服务流程。

        返回：
        {
          "student_profile_result": {...},
          "student_state": {...},
          "profile_input_payload": {...},
          "rule_score_result": {...},
          "llm_result": {...}
        }
        """
        setup_logging()
        state_path = Path(state_path)
        build_warnings: List[str] = []

        LOGGER.info("Start loading student state: %s", state_path)
        student_state = self.state_manager.load_state(state_path)

        try:
            LOGGER.info("Step 1/4: building profile_input_payload")
            profile_input_payload = self.build_profile_payload(
                student_state=student_state,
                builder_output_path=builder_output_path,
            )
        except Exception as exc:
            LOGGER.exception("student_profile_builder failed")
            build_warnings.append(f"builder 执行失败: {exc}")
            profile_input_payload = {}

        try:
            LOGGER.info("Step 2/4: scoring profile payload by rules")
            rule_score_result = self.score_profile_payload(
                profile_input_payload=profile_input_payload,
                scorer_output_path=scorer_output_path,
            )
        except Exception as exc:
            LOGGER.exception("student_profile_scorer failed")
            build_warnings.append(f"scorer 执行失败: {exc}")
            rule_score_result = {
                "profile_completeness_score": 0.0,
                "competitiveness_base_score": 0.0,
                "score_level": "D-当前竞争力偏弱",
                "completeness_detail": {},
                "competitiveness_detail": {},
                "score_reasons": {
                    "strengths": [],
                    "weaknesses": ["规则评分失败"],
                    "suggestions": ["请检查 builder 输出结构和 scorer 输入格式。"],
                },
            }

        try:
            LOGGER.info("Step 3/4: calling LLM student_profile")
            llm_result = self.call_student_profile_llm(
                profile_input_payload=profile_input_payload,
                rule_score_result=rule_score_result,
                student_state=student_state,
                context_data=context_data,
                extra_context=extra_context,
            )
        except Exception as exc:
            LOGGER.exception("call_llm('student_profile', ...) failed")
            build_warnings.append(f"LLM 调用失败: {exc}")
            llm_result = {}

        LOGGER.info("Step 4/4: merging results and updating student.json")
        merged_profile_result = merge_rule_and_llm_result(
            profile_input_payload=profile_input_payload,
            rule_score_result=rule_score_result,
            llm_result=llm_result,
            build_warnings=build_warnings,
        )
        updated_state = self.update_student_state(
            merged_profile_result=merged_profile_result,
            student_state=student_state,
            state_path=state_path,
        )

        response_bundle = {
            "student_profile_result": merged_profile_result,
            "student_state": updated_state,
            "profile_input_payload": profile_input_payload,
            "rule_score_result": rule_score_result,
            "llm_result": llm_result,
        }
        _save_json(response_bundle, service_output_path)
        LOGGER.info(
            "student_profile service finished. complete_score=%s, competitiveness_score=%s",
            merged_profile_result.get("complete_score"),
            merged_profile_result.get("competitiveness_score"),
        )
        return response_bundle


def run_student_profile_service(
    state_path: str | Path = DEFAULT_STATE_PATH,
    context_data: Optional[Dict[str, Any]] = None,
    extra_context: Optional[Dict[str, Any]] = None,
    builder_output_path: Optional[str | Path] = DEFAULT_BUILDER_OUTPUT_PATH,
    scorer_output_path: Optional[str | Path] = DEFAULT_SCORER_OUTPUT_PATH,
    service_output_path: Optional[str | Path] = DEFAULT_SERVICE_OUTPUT_PATH,
) -> Dict[str, Any]:
    """函数式入口，方便直接在业务流程中调用。"""
    service = StudentProfileService()
    return service.run(
        state_path=state_path,
        context_data=context_data,
        extra_context=extra_context,
        builder_output_path=builder_output_path,
        scorer_output_path=scorer_output_path,
        service_output_path=service_output_path,
    )


def parse_args() -> argparse.Namespace:
    """命令行参数解析。"""
    parser = argparse.ArgumentParser(description="Student profile service")
    parser.add_argument(
        "--state-path",
        default=str(DEFAULT_STATE_PATH),
        help="student.json 文件路径",
    )
    parser.add_argument(
        "--builder-output",
        default=str(DEFAULT_BUILDER_OUTPUT_PATH),
        help="builder 中间特征输出路径",
    )
    parser.add_argument(
        "--scorer-output",
        default=str(DEFAULT_SCORER_OUTPUT_PATH),
        help="scorer 规则评分输出路径",
    )
    parser.add_argument(
        "--service-output",
        default=str(DEFAULT_SERVICE_OUTPUT_PATH),
        help="service 汇总结果输出路径",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    result = run_student_profile_service(
        state_path=args.state_path,
        builder_output_path=args.builder_output,
        scorer_output_path=args.scorer_output,
        service_output_path=args.service_output,
    )
    print(json.dumps(result["student_profile_result"], ensure_ascii=False, indent=2))


