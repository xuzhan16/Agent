"""
career_report_service.py

career_report 模块业务服务层。

职责：
1. 调用 career_report_builder 构造 report_input_payload；
2. 调用 career_report_formatter 生成固定章节骨架与结构化章节草稿；
3. 调用统一大模型接口 call_llm(task_type="career_report", ...) 做章节润色和最终报告成文；
4. 合并程序草稿和 LLM 输出；
5. 执行完整性检查、输出编辑建议；
6. 写回 student_api_state.json 的 career_report_result 字段。
"""

from __future__ import annotations

import argparse
import json
import logging
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from .career_report_builder import (
    build_demo_career_path_plan_result,
    build_demo_job_match_result,
    build_demo_job_profile_result,
    build_demo_student_profile_result,
    build_report_input_payload,
)
from .career_report_formatter import (
    REPORT_SECTION_TITLES,
    build_report_sections_draft,
    render_report_sections_markdown,
)
from llm_interface_layer.llm_service import call_llm
from llm_interface_layer.state_manager import StateManager
from semantic_retrieval.semantic_retriever import SemanticJobKnowledgeRetriever


LOGGER = logging.getLogger(__name__)
DEFAULT_STATE_PATH = Path("student_api_state.json")
DEFAULT_BUILDER_OUTPUT_PATH = Path("outputs/state/career_report_input_payload.json")
DEFAULT_FORMATTER_OUTPUT_PATH = Path("outputs/state/career_report_sections_draft.json")
DEFAULT_SERVICE_OUTPUT_PATH = Path("outputs/state/career_report_service_result.json")
DEFAULT_SEMANTIC_TOP_K = 3


@dataclass
class CompletenessCheckResult:
    """报告完整性检查结果。"""

    is_complete: bool = True
    missing_sections: List[str] = field(default_factory=list)


@dataclass
class CareerReportServiceResult:
    """最终写回 student_api_state.json 的 career_report_result 结构。"""

    report_title: str = ""
    report_sections: List[Dict[str, Any]] = field(default_factory=list)
    report_summary: str = ""
    report_text_markdown: str = ""
    edit_suggestions: List[str] = field(default_factory=list)
    completeness_check: Dict[str, Any] = field(default_factory=dict)
    report_input_payload: Dict[str, Any] = field(default_factory=dict)
    report_sections_draft: List[Dict[str, Any]] = field(default_factory=list)
    llm_report_result: Dict[str, Any] = field(default_factory=dict)
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
    text = str(value).replace("\u00a0", " ").replace("\u3000", " ").strip()
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


def normalize_section_list(value: Any) -> List[Dict[str, Any]]:
    """将 LLM 或草稿章节统一成 [{section_title, section_content}] 格式。"""
    if isinstance(value, list):
        result = []
        for item in value:
            item_dict = safe_dict(item)
            title = clean_text(item_dict.get("section_title") or item_dict.get("title"))
            content = clean_text(item_dict.get("section_content") or item_dict.get("content"))
            if title:
                result.append({"section_title": title, "section_content": content})
        return result

    if isinstance(value, dict):
        return [
            {"section_title": clean_text(key), "section_content": clean_text(val)}
            for key, val in value.items()
            if clean_text(key)
        ]
    return []


def section_map_from_list(sections: List[Dict[str, Any]]) -> Dict[str, str]:
    """将章节数组转为 title -> content。"""
    result = {}
    for section in safe_list(sections):
        section_dict = safe_dict(section)
        title = clean_text(section_dict.get("section_title"))
        content = clean_text(section_dict.get("section_content"))
        if title:
            result[title] = content
    return result


def get_path_context(report_input_payload: Dict[str, Any]) -> Dict[str, Any]:
    """提取报告侧路径上下文。"""
    payload = safe_dict(report_input_payload)
    path_context = safe_dict(payload.get("path_context"))
    plan_snapshot = safe_dict(payload.get("career_path_plan_snapshot"))
    career_path = safe_dict(plan_snapshot.get("career_path"))
    return {
        "target_path_data_status": clean_text(
            path_context.get("target_path_data_status")
            or plan_snapshot.get("target_path_data_status")
            or career_path.get("target_path_data_status")
        ),
        "target_path_data_message": clean_text(
            path_context.get("target_path_data_message")
            or plan_snapshot.get("target_path_data_message")
            or career_path.get("target_path_data_message")
        ),
        "path_strategy": clean_text(
            path_context.get("path_strategy")
            or plan_snapshot.get("path_strategy")
            or career_path.get("path_strategy")
        ),
    }


def sanitize_report_text(text: Any) -> str:
    """最终报告文本清洗，避免 Python 对象字符串和多余空白泄漏。"""
    cleaned = clean_text(text)
    replacements = {
        "dict_keys([])": "暂无明确记录",
        "dict_values([])": "暂无明确记录",
        "dict_items([])": "暂无明确记录",
        "set()": "暂无明确记录",
    }
    for old, new in replacements.items():
        cleaned = cleaned.replace(old, new)
    while "暂无明确记录、暂无明确记录" in cleaned:
        cleaned = cleaned.replace("暂无明确记录、暂无明确记录", "暂无明确记录")
    return cleaned


def enforce_fact_safe_sections(
    report_sections: List[Dict[str, Any]],
    report_sections_draft: List[Dict[str, Any]],
    report_input_payload: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """对事实敏感章节使用程序草稿兜底，防止 LLM 覆盖岗位画像或编造路径。"""
    draft_map = section_map_from_list(report_sections_draft)
    path_context = get_path_context(report_input_payload)
    no_target_path = (
        path_context.get("target_path_data_status") == "missing"
        or path_context.get("path_strategy") == "no_target_path_data"
    )
    target_context = safe_dict(report_input_payload.get("target_job_profile_context"))
    job_match_context = safe_dict(report_input_payload.get("job_match_context"))
    has_target_asset = bool(target_context.get("asset_found"))
    has_contest_asset = bool(safe_dict(job_match_context.get("target_job_match")).get("asset_found"))
    force_titles = set()
    if has_target_asset:
        force_titles.add("目标岗位画像与职业探索")
    if has_contest_asset:
        force_titles.add("人岗匹配分析")
    if no_target_path:
        force_titles.update({"职业目标设定与职业路径规划", "分阶段行动计划", "总结与建议"})

    sanitized_sections = []
    for section in safe_list(report_sections):
        section_dict = safe_dict(section)
        title = clean_text(section_dict.get("section_title"))
        content = sanitize_report_text(section_dict.get("section_content"))
        if title in force_titles and clean_text(draft_map.get(title)):
            content = sanitize_report_text(draft_map.get(title))
        sanitized_sections.append({"section_title": title, "section_content": content})

    existing_titles = {clean_text(item.get("section_title")) for item in sanitized_sections}
    for title in REPORT_SECTION_TITLES:
        if title not in existing_titles and clean_text(draft_map.get(title)):
            sanitized_sections.append(
                {"section_title": title, "section_content": sanitize_report_text(draft_map.get(title))}
            )
    return sanitized_sections


def extract_semantic_context(context_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """从 context_data 中提取语义知识快照。"""
    semantic_context = safe_dict(safe_dict(context_data).get("semantic_context"))
    hits = []
    for item in safe_list(semantic_context.get("hits"))[:DEFAULT_SEMANTIC_TOP_K]:
        item_dict = safe_dict(item)
        hits.append(
            {
                "standard_job_name": clean_text(item_dict.get("standard_job_name")),
                "job_category": clean_text(item_dict.get("job_category")),
                "job_level": clean_text(item_dict.get("job_level")),
                "score": item_dict.get("score", 0.0),
                "doc_text_excerpt": clean_text(item_dict.get("doc_text_excerpt")),
                "hard_skills": deepcopy(safe_list(item_dict.get("hard_skills"))[:8]),
                "vertical_paths": deepcopy(safe_list(item_dict.get("vertical_paths"))[:4]),
                "transfer_paths": deepcopy(safe_list(item_dict.get("transfer_paths"))[:4]),
            }
        )
    return {
        "query": clean_text(semantic_context.get("query")),
        "top_k": len(hits),
        "hits": hits,
    }


def build_semantic_query_for_career_report(
    report_input_payload: Dict[str, Any],
) -> str:
    """构造 career_report 阶段的岗位语义检索 query。"""
    payload = safe_dict(report_input_payload)
    report_meta = safe_dict(payload.get("report_meta"))
    student_snapshot = safe_dict(payload.get("student_snapshot"))
    job_snapshot = safe_dict(payload.get("job_snapshot"))
    job_match_snapshot = safe_dict(payload.get("job_match_snapshot"))
    path_snapshot = safe_dict(payload.get("career_path_plan_snapshot"))
    career_goal = safe_dict(path_snapshot.get("career_goal"))
    career_path = safe_dict(path_snapshot.get("career_path"))
    generation_context = safe_dict(payload.get("report_generation_context"))

    gap_terms = dedup_keep_order(
        clean_text(safe_dict(item).get("required_item"))
        for item in safe_list(job_match_snapshot.get("missing_items"))
        if clean_text(safe_dict(item).get("required_item"))
    )
    query_parts = [
        f"报告目标岗位：{clean_text(report_meta.get('target_job_name') or career_goal.get('primary_target_job') or job_snapshot.get('standard_job_name'))}",
        f"岗位类别：{clean_text(job_snapshot.get('job_category'))}",
        f"岗位层级：{clean_text(job_snapshot.get('job_level'))}",
        f"岗位核心技能：{'、'.join(safe_list(job_snapshot.get('hard_skills'))[:10])}",
        f"学生摘要：{clean_text(student_snapshot.get('summary'))}",
        f"匹配摘要：{clean_text(job_match_snapshot.get('analysis_summary'))}",
        f"职业规划摘要：{clean_text(path_snapshot.get('decision_summary'))}",
        f"主路径：{' -> '.join(safe_list(career_path.get('direct_path'))[:4])}",
        f"过渡路径：{' -> '.join(safe_list(career_path.get('transition_path'))[:4])}",
        f"长期路径：{' -> '.join(safe_list(career_path.get('long_term_path'))[:5])}",
        f"关键缺口：{'、'.join(gap_terms[:6]) if gap_terms else ''}",
        f"行动建议摘要：{clean_text(generation_context.get('recommendation'))}",
    ]
    return "\n".join(part for part in query_parts if clean_text(part))


def build_semantic_context_for_career_report(
    report_input_payload: Dict[str, Any],
    top_k: int = DEFAULT_SEMANTIC_TOP_K,
) -> Dict[str, Any]:
    """基于岗位知识语义库，为 career_report 构造 semantic_context。"""
    query_text = build_semantic_query_for_career_report(report_input_payload)
    if not clean_text(query_text):
        return {}

    project_root = Path(__file__).resolve().parent.parent
    retriever = SemanticJobKnowledgeRetriever.from_project_root(project_root)
    semantic_context = retriever.build_semantic_context(
        query_text=query_text,
        top_k=top_k,
        min_score=0.0,
    )
    if not safe_list(semantic_context.get("hits")):
        return {}
    return semantic_context


def build_career_report_llm_input(
    report_input_payload: Dict[str, Any],
    report_sections_draft: List[Dict[str, Any]],
    report_text_draft: str,
    context_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """组装 career_report 大模型输入。"""
    del report_text_draft

    section_outline = [
        {
            "section_title": clean_text(safe_dict(item).get("section_title")),
            "section_content": clean_text(safe_dict(item).get("section_content")),
        }
        for item in safe_list(report_sections_draft)
        if clean_text(safe_dict(item).get("section_title"))
    ]
    semantic_context = extract_semantic_context(context_data)
    graph_context = safe_dict(safe_dict(context_data).get("graph_context"))
    sql_context = safe_dict(safe_dict(context_data).get("sql_context"))

    return {
        "report_meta": deepcopy(safe_dict(report_input_payload.get("report_meta"))),
        "target_job_profile_context": deepcopy(
            safe_dict(report_input_payload.get("target_job_profile_context"))
        ),
        "job_match_context": deepcopy(safe_dict(report_input_payload.get("job_match_context"))),
        "path_context": deepcopy(safe_dict(report_input_payload.get("path_context"))),
        "upstream_snapshots": {
            "student_snapshot": deepcopy(safe_dict(report_input_payload.get("student_snapshot"))),
            "job_snapshot": deepcopy(safe_dict(report_input_payload.get("job_snapshot"))),
            "job_match_snapshot": deepcopy(safe_dict(report_input_payload.get("job_match_snapshot"))),
            "career_path_plan_snapshot": deepcopy(
                safe_dict(report_input_payload.get("career_path_plan_snapshot"))
            ),
        },
        "report_generation_context": deepcopy(
            safe_dict(report_input_payload.get("report_generation_context"))
        ),
        "knowledge_context_snapshot": {
            "graph_context": {
                "job_core": deepcopy(safe_dict(graph_context.get("job_core"))),
                "required_skills": deepcopy(safe_list(graph_context.get("required_skills"))[:10]),
                "related_jobs": deepcopy(safe_list(graph_context.get("related_jobs"))[:6]),
                "promote_paths": deepcopy(safe_list(graph_context.get("promote_paths"))[:5]),
                "transfer_paths": deepcopy(safe_list(graph_context.get("transfer_paths"))[:5]),
            },
            "sql_context": {
                "salary_stats": deepcopy(safe_dict(sql_context.get("salary_stats"))),
                "top_cities": deepcopy(safe_list(sql_context.get("top_cities"))[:5]),
                "top_industries": deepcopy(safe_list(sql_context.get("top_industries"))[:5]),
                "company_samples": deepcopy(safe_list(sql_context.get("company_samples"))[:6]),
            },
            "semantic_context": deepcopy(semantic_context),
        },
        "report_sections_draft": section_outline,
        "report_outline": {
            "section_titles": [
                clean_text(item.get("section_title"))
                for item in section_outline
                if clean_text(item.get("section_title"))
            ],
            "section_count": len(section_outline),
        },
        "generation_requirements": {
            "task_goal": "在保留固定章节骨架和事实内容不变的前提下，对各章节进行润色、增强段落衔接，生成可信的职业发展报告。",
            "must_keep_sections": REPORT_SECTION_TITLES,
            "report_title": "输出清晰正式的报告标题。",
            "report_summary": "输出一段可直接放在报告开头的摘要。",
            "report_sections": "返回固定章节顺序的结构化章节数组，字段为 section_title 与 section_content。",
            "report_text_markdown": "输出完整 Markdown 报告文本。",
            "target_profile_rule": "target_job_profile_context 是目标岗位画像唯一主来源；semantic_context 只能作为相似岗位参考，禁止把相似岗位写成目标岗位画像。",
            "semantic_boundary": "如果 semantic_context 命中岗位与目标岗位不同，只能写为相似岗位参考，不能写成“目标岗位属于该岗位”。",
            "path_rule": "当 path_context.target_path_data_status=missing 或 path_strategy=no_target_path_data 时，禁止生成任何目标岗位路径，禁止出现高级XX、XX负责人、当前学生画像 -> 目标岗位等伪路径。",
            "score_rule": "旧规则综合分和赛题评测结果必须分开解释；不能因为旧规则分高就忽略学历、专业、证书、技能知识点硬性核验。",
            "asset_rule": "如果岗位评测资产不足，必须提示可信度限制；如果资产存在，必须展示 hard_info_evaluation、skill_knowledge_match、contest_evaluation。",
            "representative_path_rule": "报告中不展示 representative_promotion_paths，全局代表路径只属于前端职业路径页面。",
            "goal_decision_rule": (
                "career_path_plan_snapshot.career_goal 中的 primary_plan_job/primary_target_job 是规则层已经确定的主路径岗位，"
                "user_target_job 是用户原始目标，system_recommended_job 是系统推荐岗位。"
                "LLM 只能解释为什么这样安排，不能重新选择岗位或改写分数。"
                "如果主路径岗位与用户原目标不同，必须说明原目标作为中期冲刺或备选目标被保留。"
            ),
        },
        "output_schema_hint": {
            "report_title": "",
            "report_summary": "",
            "report_sections": [
                {"section_title": "学生基本情况与能力画像", "section_content": ""}
            ],
            "report_text_markdown": "",
        },
    }


def normalize_llm_career_report_result(
    llm_result: Dict[str, Any],
    report_input_payload: Dict[str, Any],
    report_sections_draft: List[Dict[str, Any]],
    report_text_draft: str,
) -> Dict[str, Any]:
    """对 call_llm('career_report', ...) 返回做字段兼容和默认值补齐。"""
    source = safe_dict(llm_result)
    meta = safe_dict(report_input_payload.get("report_meta"))
    generation_context = safe_dict(report_input_payload.get("report_generation_context"))

    report_title = clean_text(source.get("report_title")) or clean_text(meta.get("report_title")) or "大学生职业生涯发展报告"

    report_sections = normalize_section_list(source.get("report_sections"))
    if not report_sections:
        report_sections = [
            {
                "section_title": clean_text(item.get("section_title")),
                "section_content": clean_text(item.get("section_content")),
            }
            for item in safe_list(report_sections_draft)
            if clean_text(safe_dict(item).get("section_title"))
        ]

    report_summary = clean_text(
        source.get("report_summary")
        or source.get("match_summary")
        or source.get("summary")
        or generation_context.get("decision_summary")
        or generation_context.get("match_summary")
    )
    if not report_summary:
        report_summary = "本报告基于学生能力画像、目标岗位画像、人岗匹配结果和职业路径规划结果，形成面向目标岗位的职业发展建议。"

    report_text_markdown = clean_text(source.get("report_text_markdown") or source.get("report_text"))
    if not report_text_markdown:
        report_text_markdown = render_report_sections_markdown(
            report_title=report_title,
            report_sections=report_sections,
            report_summary=report_summary,
        )

    return {
        "report_title": report_title,
        "report_sections": report_sections,
        "report_summary": report_summary,
        "report_text_markdown": report_text_markdown,
        "raw_llm_result": deepcopy(source),
    }


def check_report_completeness(report_sections: List[Dict[str, Any]]) -> Dict[str, Any]:
    """检查是否缺少固定章节，或章节内容为空。"""
    section_map = {
        clean_text(safe_dict(section).get("section_title")): clean_text(safe_dict(section).get("section_content"))
        for section in safe_list(report_sections)
        if clean_text(safe_dict(section).get("section_title"))
    }
    missing_sections = [
        title
        for title in REPORT_SECTION_TITLES
        if not clean_text(section_map.get(title))
    ]
    result = CompletenessCheckResult(
        is_complete=len(missing_sections) == 0,
        missing_sections=missing_sections,
    )
    return asdict(result)


def build_edit_suggestions(
    report_sections: List[Dict[str, Any]],
    completeness_check: Dict[str, Any],
    report_input_payload: Dict[str, Any],
) -> List[str]:
    """生成可编辑建议。"""
    suggestions = []
    payload = safe_dict(report_input_payload)
    build_warnings = [clean_text(item) for item in safe_list(payload.get("build_warnings")) if clean_text(item)]
    suggestions.extend(build_warnings)

    missing_sections = [
        clean_text(item)
        for item in safe_list(safe_dict(completeness_check).get("missing_sections"))
        if clean_text(item)
    ]
    for section_title in missing_sections:
        suggestions.append(f"章节“{section_title}”内容缺失或过短，建议补充更完整的结构化信息后重新生成。")

    for section in safe_list(report_sections):
        section_dict = safe_dict(section)
        section_title = clean_text(section_dict.get("section_title"))
        section_content = clean_text(section_dict.get("section_content"))
        if section_title and len(section_content) < 30:
            suggestions.append(f"章节“{section_title}”内容偏短，建议补充更具体的事实、案例或行动建议。")

    student_snapshot = safe_dict(payload.get("student_snapshot"))
    if not normalize_section_list(report_sections):
        suggestions.append("当前未形成结构化 report_sections，请先检查 formatter 草稿生成结果或 LLM 返回格式。")
    if not clean_text(student_snapshot.get("summary")):
        suggestions.append("学生画像 summary 为空，建议补充学生能力概述，以提升“学生基本情况与能力画像”章节质量。")

    return dedup_keep_order(suggestions)[:20]


def merge_career_report_results(
    report_input_payload: Dict[str, Any],
    report_sections_draft: List[Dict[str, Any]],
    llm_result: Dict[str, Any],
    service_warnings: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """合并草稿章节、LLM 结果、完整性检查和编辑建议。"""
    payload = safe_dict(report_input_payload)
    draft_meta = safe_dict(payload.get("report_meta"))
    draft_summary = clean_text(safe_dict(payload.get("report_generation_context")).get("decision_summary"))
    draft_text = render_report_sections_markdown(
        report_title=clean_text(draft_meta.get("report_title")),
        report_sections=report_sections_draft,
        report_summary=draft_summary,
    )
    normalized_llm = normalize_llm_career_report_result(
        llm_result=llm_result,
        report_input_payload=payload,
        report_sections_draft=report_sections_draft,
        report_text_draft=draft_text,
    )
    normalized_llm["report_sections"] = enforce_fact_safe_sections(
        report_sections=safe_list(normalized_llm.get("report_sections")),
        report_sections_draft=report_sections_draft,
        report_input_payload=payload,
    )
    path_context = get_path_context(payload)
    if (
        path_context.get("target_path_data_status") == "missing"
        or path_context.get("path_strategy") == "no_target_path_data"
    ):
        plan_snapshot = safe_dict(payload.get("career_path_plan_snapshot"))
        career_goal = safe_dict(plan_snapshot.get("career_goal"))
        target_job_name = clean_text(safe_dict(payload.get("report_meta")).get("target_job_name")) or "目标岗位"
        primary_plan_job = clean_text(career_goal.get("primary_plan_job") or career_goal.get("primary_target_job") or target_job_name)
        user_target_job = clean_text(career_goal.get("user_target_job"))
        if user_target_job and primary_plan_job and user_target_job != primary_plan_job:
            normalized_llm["report_summary"] = (
                f"系统建议以{primary_plan_job}作为短期主路径，同时保留{user_target_job}作为中期补强后的冲刺目标。"
                "当前主路径岗位暂无真实晋升/转岗路径数据，因此本报告不生成职业路径，只从岗位匹配、硬门槛评测、技能知识点缺口和行动计划角度给出建议。"
            )
        else:
            normalized_llm["report_summary"] = (
                f"当前建议以{primary_plan_job}作为求职目标，但本地岗位图谱和离线岗位画像中暂未沉淀该岗位明确晋升/转岗路径。"
                "因此本报告不生成目标岗位路径，仅从岗位匹配、硬门槛评测、技能知识点缺口和行动计划角度给出建议。"
            )
    normalized_llm["report_text_markdown"] = render_report_sections_markdown(
        report_title=clean_text(normalized_llm.get("report_title")),
        report_sections=safe_list(normalized_llm.get("report_sections")),
        report_summary=sanitize_report_text(normalized_llm.get("report_summary")),
    )

    completeness_check = check_report_completeness(normalized_llm["report_sections"])
    edit_suggestions = build_edit_suggestions(
        report_sections=normalized_llm["report_sections"],
        completeness_check=completeness_check,
        report_input_payload=payload,
    )

    result = CareerReportServiceResult(
        report_title=clean_text(normalized_llm.get("report_title")),
        report_sections=deepcopy(normalized_llm.get("report_sections")),
        report_summary=sanitize_report_text(normalized_llm.get("report_summary")),
        report_text_markdown=sanitize_report_text(normalized_llm.get("report_text_markdown")),
        edit_suggestions=dedup_keep_order(
            [clean_text(item) for item in edit_suggestions if clean_text(item)]
            + [clean_text(item) for item in safe_list(service_warnings) if clean_text(item)]
        ),
        completeness_check=deepcopy(completeness_check),
        report_input_payload=deepcopy(payload),
        report_sections_draft=deepcopy(safe_list(report_sections_draft)),
        llm_report_result=deepcopy(safe_dict(normalized_llm.get("raw_llm_result"))),
        build_warnings=dedup_keep_order(
            [clean_text(item) for item in safe_list(payload.get("build_warnings")) if clean_text(item)]
            + [clean_text(item) for item in safe_list(service_warnings) if clean_text(item)]
        ),
    )
    return asdict(result)


class CareerReportService:
    """career_report 业务服务编排器。"""

    def __init__(self, state_manager: Optional[StateManager] = None) -> None:
        self.state_manager = state_manager or StateManager()

    def build_payload(
        self,
        student_profile_result: Dict[str, Any],
        job_profile_result: Dict[str, Any],
        job_match_result: Dict[str, Any],
        career_path_plan_result: Dict[str, Any],
        output_path: Optional[str | Path] = DEFAULT_BUILDER_OUTPUT_PATH,
    ) -> Dict[str, Any]:
        """调用 builder 构造 report_input_payload。"""
        return build_report_input_payload(
            student_profile_result=student_profile_result,
            job_profile_result=job_profile_result,
            job_match_result=job_match_result,
            career_path_plan_result=career_path_plan_result,
            output_path=output_path,
        )

    def format_sections(
        self,
        report_input_payload: Dict[str, Any],
        output_path: Optional[str | Path] = DEFAULT_FORMATTER_OUTPUT_PATH,
    ) -> List[Dict[str, Any]]:
        """调用 formatter 生成固定章节草稿。"""
        return build_report_sections_draft(
            report_input_payload=report_input_payload,
            output_path=output_path,
        )

    def call_career_report_llm(
        self,
        report_input_payload: Dict[str, Any],
        report_sections_draft: List[Dict[str, Any]],
        student_state: Optional[Dict[str, Any]] = None,
        context_data: Optional[Dict[str, Any]] = None,
        extra_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """调用统一大模型接口做最终报告润色。"""
        meta = safe_dict(report_input_payload.get("report_meta"))
        generation_context = safe_dict(report_input_payload.get("report_generation_context"))
        report_text_draft = render_report_sections_markdown(
            report_title=clean_text(meta.get("report_title")),
            report_sections=report_sections_draft,
            report_summary=clean_text(generation_context.get("decision_summary")),
        )
        input_data = build_career_report_llm_input(
            report_input_payload=report_input_payload,
            report_sections_draft=report_sections_draft,
            report_text_draft=report_text_draft,
            context_data=context_data,
        )
        merged_extra_context = {
            "service_name": "career_report_service",
            "expected_fields": [
                "report_title",
                "report_summary",
                "report_sections",
                "report_text_markdown",
            ],
        }
        if extra_context:
            merged_extra_context.update(deepcopy(extra_context))

        return call_llm(
            task_type="career_report",
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
        career_path_plan_result: Dict[str, Any],
        student_state: Optional[Dict[str, Any]] = None,
        context_data: Optional[Dict[str, Any]] = None,
        state_path: Optional[str | Path] = DEFAULT_STATE_PATH,
        builder_output_path: Optional[str | Path] = DEFAULT_BUILDER_OUTPUT_PATH,
        formatter_output_path: Optional[str | Path] = DEFAULT_FORMATTER_OUTPUT_PATH,
        service_output_path: Optional[str | Path] = DEFAULT_SERVICE_OUTPUT_PATH,
        extra_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """执行完整 career_report 服务流程并写回 student_api_state.json。"""
        setup_logging()
        service_warnings = []
        merged_context_data = deepcopy(context_data) if isinstance(context_data, dict) else {}

        if student_state is None:
            student_state = self.state_manager.load_state(state_path)

        LOGGER.info("Step 1/4: build report_input_payload")
        try:
            report_input_payload = self.build_payload(
                student_profile_result=student_profile_result,
                job_profile_result=job_profile_result,
                job_match_result=job_match_result,
                career_path_plan_result=career_path_plan_result,
                output_path=builder_output_path,
            )
        except Exception as exc:
            LOGGER.exception("career_report_builder failed")
            report_input_payload = {
                "student_snapshot": {},
                "job_snapshot": {},
                "job_match_snapshot": {},
                "career_path_plan_snapshot": {},
                "report_meta": {"report_title": "大学生职业生涯发展报告"},
                "report_generation_context": {},
                "build_warnings": [f"builder 执行失败: {exc}"],
            }
            service_warnings.append(f"builder 执行失败: {exc}")

        if not safe_dict(merged_context_data.get("semantic_context")):
            try:
                semantic_context = build_semantic_context_for_career_report(
                    report_input_payload=report_input_payload,
                )
                if semantic_context:
                    merged_context_data["semantic_context"] = semantic_context
            except FileNotFoundError:
                LOGGER.info("semantic knowledge base not found, skip semantic retrieval for career_report")
            except Exception as exc:
                LOGGER.warning("semantic retrieval for career_report failed: %s", exc)
                service_warnings.append(f"语义知识检索失败: {exc}")

        semantic_snapshot = extract_semantic_context(merged_context_data)
        if semantic_snapshot:
            report_generation_context = safe_dict(report_input_payload.get("report_generation_context"))
            report_generation_context["semantic_fact_snapshot"] = deepcopy(semantic_snapshot)
            report_input_payload["report_generation_context"] = report_generation_context

        LOGGER.info("Step 2/4: format report section drafts")
        try:
            report_sections_draft = self.format_sections(
                report_input_payload=report_input_payload,
                output_path=formatter_output_path,
            )
        except Exception as exc:
            LOGGER.exception("career_report_formatter failed")
            report_sections_draft = [
                {
                    "section_title": title,
                    "section_content": "该章节草稿生成失败，建议检查上游结构化结果。",
                }
                for title in REPORT_SECTION_TITLES
            ]
            service_warnings.append(f"formatter 执行失败: {exc}")

        LOGGER.info("Step 3/4: call LLM career_report")
        try:
            llm_result = self.call_career_report_llm(
                report_input_payload=report_input_payload,
                report_sections_draft=report_sections_draft,
                student_state=student_state,
                context_data=merged_context_data,
                extra_context=extra_context,
            )
        except Exception as exc:
            LOGGER.exception("call_llm('career_report', ...) failed")
            llm_result = {}
            service_warnings.append(f"LLM 调用失败: {exc}")

        LOGGER.info("Step 4/4: merge report result and update state")
        final_result = merge_career_report_results(
            report_input_payload=report_input_payload,
            report_sections_draft=report_sections_draft,
            llm_result=llm_result,
            service_warnings=service_warnings,
        )
        self.state_manager.update_state(
            task_type="career_report",
            task_result=final_result,
            state_path=state_path,
            student_state=student_state,
        )
        save_json(final_result, service_output_path)

        LOGGER.info(
            "career_report service finished. title=%s, complete=%s",
            final_result.get("report_title"),
            safe_dict(final_result.get("completeness_check")).get("is_complete"),
        )
        return final_result

    def run_from_state(
        self,
        state_path: str | Path = DEFAULT_STATE_PATH,
        context_data: Optional[Dict[str, Any]] = None,
        builder_output_path: Optional[str | Path] = DEFAULT_BUILDER_OUTPUT_PATH,
        formatter_output_path: Optional[str | Path] = DEFAULT_FORMATTER_OUTPUT_PATH,
        service_output_path: Optional[str | Path] = DEFAULT_SERVICE_OUTPUT_PATH,
        extra_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """从 student_api_state.json 读取四个上游结果，执行报告生成并写回 state。"""
        student_state = self.state_manager.load_state(state_path)
        return self.run(
            student_profile_result=safe_dict(student_state.get("student_profile_result")),
            job_profile_result=safe_dict(student_state.get("job_profile_result")),
            job_match_result=safe_dict(student_state.get("job_match_result")),
            career_path_plan_result=safe_dict(student_state.get("career_path_plan_result")),
            student_state=student_state,
            context_data=context_data,
            state_path=state_path,
            builder_output_path=builder_output_path,
            formatter_output_path=formatter_output_path,
            service_output_path=service_output_path,
            extra_context=extra_context,
        )


def run_career_report_service(
    student_profile_result: Dict[str, Any],
    job_profile_result: Dict[str, Any],
    job_match_result: Dict[str, Any],
    career_path_plan_result: Dict[str, Any],
    student_state: Optional[Dict[str, Any]] = None,
    context_data: Optional[Dict[str, Any]] = None,
    state_path: Optional[str | Path] = DEFAULT_STATE_PATH,
    builder_output_path: Optional[str | Path] = DEFAULT_BUILDER_OUTPUT_PATH,
    formatter_output_path: Optional[str | Path] = DEFAULT_FORMATTER_OUTPUT_PATH,
    service_output_path: Optional[str | Path] = DEFAULT_SERVICE_OUTPUT_PATH,
    extra_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """函数式服务入口。"""
    return CareerReportService().run(
        student_profile_result=student_profile_result,
        job_profile_result=job_profile_result,
        job_match_result=job_match_result,
        career_path_plan_result=career_path_plan_result,
        student_state=student_state,
        context_data=context_data,
        state_path=state_path,
        builder_output_path=builder_output_path,
        formatter_output_path=formatter_output_path,
        service_output_path=service_output_path,
        extra_context=extra_context,
    )


def run_career_report_service_from_state(
    state_path: str | Path = DEFAULT_STATE_PATH,
    context_data: Optional[Dict[str, Any]] = None,
    builder_output_path: Optional[str | Path] = DEFAULT_BUILDER_OUTPUT_PATH,
    formatter_output_path: Optional[str | Path] = DEFAULT_FORMATTER_OUTPUT_PATH,
    service_output_path: Optional[str | Path] = DEFAULT_SERVICE_OUTPUT_PATH,
    extra_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """函数式 state 驱动入口。"""
    return CareerReportService().run_from_state(
        state_path=state_path,
        context_data=context_data,
        builder_output_path=builder_output_path,
        formatter_output_path=formatter_output_path,
        service_output_path=service_output_path,
        extra_context=extra_context,
    )


def parse_args() -> argparse.Namespace:
    """命令行参数解析。"""
    parser = argparse.ArgumentParser(description="Run career_report service")
    parser.add_argument("--state-path", default=str(DEFAULT_STATE_PATH), help="student_api_state.json 路径")
    parser.add_argument("--student-profile-json", default="", help="可选：单独的 student_profile_result JSON")
    parser.add_argument("--job-profile-json", default="", help="可选：单独的 job_profile_result JSON")
    parser.add_argument("--job-match-json", default="", help="可选：单独的 job_match_result JSON")
    parser.add_argument("--career-path-plan-json", default="", help="可选：单独的 career_path_plan_result JSON")
    parser.add_argument("--builder-output", default=str(DEFAULT_BUILDER_OUTPUT_PATH), help="builder 输出 JSON 路径")
    parser.add_argument("--formatter-output", default=str(DEFAULT_FORMATTER_OUTPUT_PATH), help="formatter 输出 JSON 路径")
    parser.add_argument("--service-output", default=str(DEFAULT_SERVICE_OUTPUT_PATH), help="service 输出 JSON 路径")
    parser.add_argument("--use-demo", action="store_true", help="使用内置 mock 上游结果跑 demo")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    demo_context = {
        "graph_context": {
            "mock_note": "预留 Neo4j 上下文接入位，当前 career_report demo 使用 mock。",
        },
        "sql_context": {
            "mock_note": "预留 SQL 岗位统计上下文接入位，当前 career_report demo 使用 mock。",
        },
    }

    if args.use_demo:
        result = run_career_report_service(
            student_profile_result=build_demo_student_profile_result(),
            job_profile_result=build_demo_job_profile_result(),
            job_match_result=build_demo_job_match_result(),
            career_path_plan_result=build_demo_career_path_plan_result(),
            state_path=args.state_path,
            context_data=demo_context,
            builder_output_path=args.builder_output,
            formatter_output_path=args.formatter_output,
            service_output_path=args.service_output,
            extra_context={"demo_name": "career_report_service_demo"},
        )
    elif (
        args.student_profile_json
        and args.job_profile_json
        and args.job_match_json
        and args.career_path_plan_json
    ):
        result = run_career_report_service(
            student_profile_result=load_json_file(args.student_profile_json),
            job_profile_result=load_json_file(args.job_profile_json),
            job_match_result=load_json_file(args.job_match_json),
            career_path_plan_result=load_json_file(args.career_path_plan_json),
            state_path=args.state_path,
            context_data=demo_context,
            builder_output_path=args.builder_output,
            formatter_output_path=args.formatter_output,
            service_output_path=args.service_output,
            extra_context={"demo_name": "career_report_service_from_json"},
        )
    else:
        result = run_career_report_service_from_state(
            state_path=args.state_path,
            context_data=demo_context,
            builder_output_path=args.builder_output,
            formatter_output_path=args.formatter_output,
            service_output_path=args.service_output,
            extra_context={"demo_name": "career_report_service_from_state"},
        )

    print(json.dumps(result, ensure_ascii=False, indent=2))
