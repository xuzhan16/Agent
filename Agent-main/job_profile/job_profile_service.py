"""
job_profile_service.py

job_profile 模块业务服务层。

职责：
1. 调用 job_profile_builder 构造岗位组中间特征；
2. 调用 job_profile_aggregator 生成岗位组聚合统计；
3. 调用统一大模型接口 call_llm(task_type="job_profile", ...) 补充语义画像字段；
4. 合并规则结果、聚合结果和模型结果；
5. 返回最终 job_profile_result；
6. 不重写 llm_service。

说明：
- 本文件只负责服务编排和结果融合，不直接实现复杂 LLM 接口细节；
- builder/aggregator 侧是规则先行，LLM 侧做补充和摘要生成；
- 若 LLM 调用失败，自动用规则侧结果兜底，保证流程可运行。
"""

from __future__ import annotations

import argparse
import json
import logging
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from .job_profile_aggregator import aggregate_job_profile_group, build_demo_dataframe as build_aggregator_demo_df
from .job_profile_builder import build_job_profile_input_payload
from .core_job_profile_service import build_job_profile_asset_context
from llm_interface_layer.llm_service import call_llm
from llm_interface_layer.state_manager import StateManager
from semantic_retrieval.semantic_retriever import SemanticJobKnowledgeRetriever

LOGGER = logging.getLogger(__name__)
DEFAULT_OUTPUT_PATH = Path("outputs/state/job_profile_service_result.json")
DEFAULT_STATE_PATH = Path("student_api_state.json")
DEFAULT_SEMANTIC_TOP_K = 3

@dataclass
class JobProfileLLMSupplement:
    """大模型补充字段的标准结构。"""

    job_category: str = ""
    job_level: str = ""
    soft_skills: List[str] = field(default_factory=list)
    suitable_student_profile: str = ""
    summary: str = ""
    vertical_paths: List[str] = field(default_factory=list)
    transfer_paths: List[str] = field(default_factory=list)

@dataclass
class JobProfileServiceResult:
    """最终 job_profile_result 结构。"""

    standard_job_name: str = ""
    job_category: str = ""
    job_level: str = ""
    degree_requirement: str = ""
    major_requirement: List[str] = field(default_factory=list)
    experience_requirement: List[str] = field(default_factory=list)
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
    degree_requirement_distribution: List[Dict[str, Any]] = field(default_factory=list)
    industry_distribution: List[Dict[str, Any]] = field(default_factory=list)
    city_distribution: List[Dict[str, Any]] = field(default_factory=list)
    salary_stats: Dict[str, Any] = field(default_factory=dict)
    group_summary: Dict[str, Any] = field(default_factory=dict)
    explicit_requirements: Dict[str, Any] = field(default_factory=dict)
    normalized_requirements: Dict[str, Any] = field(default_factory=dict)
    representative_samples: List[Dict[str, Any]] = field(default_factory=list)
    core_job_profiles: List[Dict[str, Any]] = field(default_factory=list)
    target_job_profile_assets: Dict[str, Any] = field(default_factory=dict)
    llm_profile_result: Dict[str, Any] = field(default_factory=dict)
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
    if isinstance(value, float) and pd.isna(value):
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
    if value is None or (isinstance(value, float) and pd.isna(value)) or value == "":
        return []
    return [value]

def dedup_keep_order(values: List[Any]) -> List[Any]:
    """对可 JSON 序列化对象做稳定去重。"""
    seen = set()
    result = []
    for item in values:
        if item is None or item == "":
            continue
        key = json.dumps(item, ensure_ascii=False, sort_keys=True) if isinstance(item, (dict, list)) else str(item)
        if key not in seen:
            seen.add(key)
            result.append(item)
    return result


def normalize_text_list(value: Any) -> List[str]:
    """统一字符串列表格式。"""
    return dedup_keep_order(clean_text(item) for item in safe_list(value) if clean_text(item))


def _normalize_path_strings(values: List[str], source_job_name: str) -> List[str]:
    """把图谱中的目标岗位名补成标准路径字符串。"""
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


def extract_path_knowledge(
    context_data: Optional[Dict[str, Any]],
    source_job_name: str = "",
) -> Dict[str, List[str]]:
    """从 context_data 中提取优先复用的离线路径/图谱路径知识。"""
    graph_context = safe_dict(safe_dict(context_data).get("graph_context"))
    source_job = clean_text(source_job_name)
    vertical_paths = dedup_keep_order(
        _normalize_path_strings(normalize_text_list(graph_context.get("vertical_paths")), source_job)
        + _normalize_path_strings(normalize_text_list(graph_context.get("promote_paths")), source_job)
        + _normalize_path_strings(
            normalize_text_list(graph_context.get("offline_profile_vertical_paths")),
            source_job,
        )
    )
    transfer_paths = dedup_keep_order(
        _normalize_path_strings(normalize_text_list(graph_context.get("transfer_paths")), source_job)
        + _normalize_path_strings(normalize_text_list(graph_context.get("lateral_paths")), source_job)
        + _normalize_path_strings(
            normalize_text_list(graph_context.get("offline_profile_transfer_paths")),
            source_job,
        )
    )
    return {
        "vertical_paths": vertical_paths,
        "transfer_paths": transfer_paths,
    }


def extract_graph_job_knowledge(
    context_data: Optional[Dict[str, Any]],
    source_job_name: str = "",
) -> Dict[str, Any]:
    """从图谱上下文中提取岗位骨架知识。"""
    graph_context = safe_dict(safe_dict(context_data).get("graph_context"))
    job_core = safe_dict(graph_context.get("job_core"))
    standard_job_name = clean_text(job_core.get("name") or source_job_name)
    return {
        "standard_job_name": standard_job_name,
        "job_category": clean_text(job_core.get("job_category")),
        "job_level": clean_text(job_core.get("job_level")),
        "degree_requirement": clean_text(job_core.get("degree_requirement")),
        "major_requirements": normalize_text_list(
            graph_context.get("major_requirements") or job_core.get("major_requirement")
        ),
        "experience_requirements": normalize_text_list(job_core.get("experience_requirement")),
        "required_skills": normalize_text_list(graph_context.get("required_skills")),
        "related_jobs": normalize_text_list(graph_context.get("related_jobs")),
        "raw_requirement_summary": clean_text(job_core.get("raw_requirement_summary")),
    }


def extract_sql_fact_context(context_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """从 SQL 上下文中提取岗位市场事实。"""
    sql_context = safe_dict(safe_dict(context_data).get("sql_context"))
    return {
        "job_count": sql_context.get("job_count", 0),
        "salary_stats": deepcopy(safe_dict(sql_context.get("salary_stats"))),
        "city_distribution": deepcopy(safe_list(sql_context.get("city_distribution"))),
        "industry_distribution": deepcopy(safe_list(sql_context.get("industry_distribution"))),
        "company_type_distribution": deepcopy(safe_list(sql_context.get("company_type_distribution"))),
        "company_size_distribution": deepcopy(safe_list(sql_context.get("company_size_distribution"))),
        "company_samples": deepcopy(safe_list(sql_context.get("company_samples"))),
        "representative_samples": deepcopy(safe_list(sql_context.get("representative_samples"))),
    }


def extract_semantic_context(context_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """从 context_data 中提取岗位语义知识快照。"""
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


def build_semantic_query_for_job_profile(
    builder_payload: Dict[str, Any],
    aggregation_result: Dict[str, Any],
    context_data: Optional[Dict[str, Any]] = None,
) -> str:
    """构造 job_profile 阶段的岗位语义检索 query。"""
    builder_payload = safe_dict(builder_payload)
    aggregation_result = safe_dict(aggregation_result)
    normalized_req = safe_dict(builder_payload.get("normalized_requirements"))
    graph_job_knowledge = extract_graph_job_knowledge(
        context_data,
        source_job_name=clean_text(builder_payload.get("standard_job_name")),
    )

    hard_skills = dedup_keep_order(
        normalize_text_list(graph_job_knowledge.get("required_skills"))
        + normalize_text_list(normalized_req.get("hard_skill_tags"))
    )
    degree_requirement = clean_text(
        graph_job_knowledge.get("degree_requirement")
        or " / ".join(normalize_text_list(normalized_req.get("degree_tags")))
    )
    major_requirement = dedup_keep_order(
        normalize_text_list(graph_job_knowledge.get("major_requirements"))
        + normalize_text_list(normalized_req.get("major_tags"))
    )
    query_parts = [
        f"标准岗位：{clean_text(graph_job_knowledge.get('standard_job_name') or builder_payload.get('standard_job_name'))}",
        f"岗位类别：{clean_text(graph_job_knowledge.get('job_category'))}",
        f"岗位层级：{clean_text(graph_job_knowledge.get('job_level'))}",
        f"学历要求：{degree_requirement}",
        f"专业要求：{'、'.join(major_requirement[:6]) if major_requirement else ''}",
        f"核心技能：{'、'.join(hard_skills[:10]) if hard_skills else ''}",
        f"岗位摘要：{clean_text(graph_job_knowledge.get('raw_requirement_summary'))}",
    ]
    if safe_list(aggregation_result.get("skill_frequency")):
        query_parts.append(
            "高频技能："
            + "、".join(
                clean_text(safe_dict(item).get("name"))
                for item in safe_list(aggregation_result.get("skill_frequency"))[:6]
                if clean_text(safe_dict(item).get("name"))
            )
        )
    return "\n".join(part for part in query_parts if clean_text(part))


def build_semantic_context_for_job_profile(
    builder_payload: Dict[str, Any],
    aggregation_result: Dict[str, Any],
    context_data: Optional[Dict[str, Any]] = None,
    top_k: int = DEFAULT_SEMANTIC_TOP_K,
) -> Dict[str, Any]:
    """基于岗位知识语义库，为 job_profile 构造 semantic_context。"""
    query_text = build_semantic_query_for_job_profile(
        builder_payload=builder_payload,
        aggregation_result=aggregation_result,
        context_data=context_data,
    )
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

def save_json(data: Dict[str, Any], output_path: Optional[str | Path]) -> None:
    """按需保存 JSON。"""
    if not output_path:
        return
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def build_degree_requirement_text(
    builder_payload: Dict[str, Any],
    aggregation_result: Dict[str, Any],
    llm_result: Dict[str, Any],
    graph_job_knowledge: Optional[Dict[str, Any]] = None,
) -> str:
    """融合规则、聚合和 LLM 的学历要求字段。"""
    graph_degree = clean_text(safe_dict(graph_job_knowledge).get("degree_requirement"))
    if graph_degree:
        return graph_degree

    llm_degree = clean_text(llm_result.get("required_degree") or llm_result.get("degree_requirement"))
    if llm_degree:
        return llm_degree

    normalized_req = safe_dict(builder_payload.get("normalized_requirements"))
    degree_tags = [clean_text(item) for item in safe_list(normalized_req.get("degree_tags")) if clean_text(item)]
    if degree_tags:
        return " / ".join(dedup_keep_order(degree_tags))

    degree_dist = safe_list(aggregation_result.get("degree_requirement_distribution"))
    if degree_dist:
        top_degree = clean_text(safe_dict(degree_dist[0]).get("name"))
        if top_degree and top_degree != "未明确":
            return top_degree
    return ""

def normalize_llm_job_profile_result(
    llm_result: Dict[str, Any],
    builder_payload: Dict[str, Any],
    context_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """对 call_llm('job_profile', ...) 返回做默认值补齐和字段兼容。"""
    source = safe_dict(llm_result)
    normalized_req = safe_dict(builder_payload.get("normalized_requirements"))
    standard_job_name = clean_text(builder_payload.get("standard_job_name"))
    path_knowledge = extract_path_knowledge(context_data, source_job_name=standard_job_name)
    graph_job_knowledge = extract_graph_job_knowledge(context_data, source_job_name=standard_job_name)
    semantic_context = extract_semantic_context(context_data)

    soft_skills = dedup_keep_order(
        [clean_text(item) for item in safe_list(source.get("soft_skills")) if clean_text(item)]
        + [clean_text(item) for item in safe_list(normalized_req.get("soft_skill_tags")) if clean_text(item)]
    )
    vertical_paths = dedup_keep_order(path_knowledge.get("vertical_paths", []))
    transfer_paths = dedup_keep_order(path_knowledge.get("transfer_paths", []))

    summary = clean_text(source.get("summary") or source.get("job_summary"))
    if not summary and clean_text(graph_job_knowledge.get("raw_requirement_summary")):
        summary = clean_text(graph_job_knowledge.get("raw_requirement_summary"))
    if not summary and safe_list(semantic_context.get("hits")):
        top_hit = safe_dict(safe_list(semantic_context.get("hits"))[0])
        summary = clean_text(top_hit.get("doc_text_excerpt"))
    if not summary and standard_job_name:
        summary = f"{standard_job_name} 岗位通常要求具备相关专业背景、核心技能储备、业务理解能力和一定实践经验。"

    suitable_student_profile = clean_text(source.get("suitable_student_profile"))
    if not suitable_student_profile and safe_list(semantic_context.get("hits")):
        top_hit = safe_dict(safe_list(semantic_context.get("hits"))[0])
        semantic_skills = [clean_text(item) for item in safe_list(top_hit.get("hard_skills")) if clean_text(item)]
        if semantic_skills:
            suitable_student_profile = (
                f"适合具备{'、'.join(semantic_skills[:5])}等相关技能基础，且有一定项目或实践经历的学生。"
            )
    if not suitable_student_profile:
        major_tags = [clean_text(item) for item in safe_list(normalized_req.get("major_tags")) if clean_text(item)]
        hard_skills = [clean_text(item) for item in safe_list(normalized_req.get("hard_skill_tags")) if clean_text(item)]
        major_desc = "、".join(major_tags[:3]) if major_tags else "相关专业"
        skill_desc = "、".join(hard_skills[:5]) if hard_skills else "岗位核心技能"
        suitable_student_profile = f"适合{major_desc}背景，且具备{skill_desc}基础，并有项目或实习实践经历的学生。"

    normalized = JobProfileLLMSupplement(
        job_category=clean_text(source.get("job_category")),
        job_level=clean_text(source.get("job_level")),
        soft_skills=soft_skills,
        suitable_student_profile=suitable_student_profile,
        summary=summary,
        vertical_paths=vertical_paths,
        transfer_paths=transfer_paths,
    )
    return asdict(normalized) | {"raw_llm_result": source}

def merge_job_profile_results(
    builder_payload: Dict[str, Any],
    aggregation_result: Dict[str, Any],
    llm_result: Dict[str, Any],
    context_data: Optional[Dict[str, Any]] = None,
    service_warnings: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """合并 builder 结果、aggregator 结果和 LLM 结果。"""
    builder_payload = safe_dict(builder_payload)
    aggregation_result = safe_dict(aggregation_result)
    normalized_req = safe_dict(builder_payload.get("normalized_requirements"))
    explicit_req = safe_dict(builder_payload.get("explicit_requirements"))
    graph_job_knowledge = extract_graph_job_knowledge(
        context_data,
        source_job_name=clean_text(builder_payload.get("standard_job_name")),
    )
    sql_fact_context = extract_sql_fact_context(context_data)
    normalized_llm = normalize_llm_job_profile_result(llm_result, builder_payload, context_data=context_data)

    merged_hard_skills = dedup_keep_order(
        [clean_text(item) for item in safe_list(graph_job_knowledge.get("required_skills")) if clean_text(item)]
        + [clean_text(item) for item in safe_list(normalized_req.get("hard_skill_tags")) if clean_text(item)]
        + [clean_text(item) for item in safe_list(safe_dict(llm_result).get("required_skills")) if clean_text(item)]
    )
    merged_tools = dedup_keep_order(
        [clean_text(item) for item in safe_list(normalized_req.get("tool_skill_tags")) if clean_text(item)]
    )
    merged_major_req = dedup_keep_order(
        [clean_text(item) for item in safe_list(graph_job_knowledge.get("major_requirements")) if clean_text(item)]
        + [clean_text(item) for item in safe_list(normalized_req.get("major_tags")) if clean_text(item)]
        + [clean_text(item) for item in safe_list(safe_dict(llm_result).get("preferred_majors")) if clean_text(item)]
    )
    merged_certificate_req = dedup_keep_order(
        [clean_text(item) for item in safe_list(normalized_req.get("certificate_tags")) if clean_text(item)]
        + [clean_text(item) for item in safe_list(safe_dict(llm_result).get("required_certificates")) if clean_text(item)]
    )
    merged_practice_req = dedup_keep_order(
        [clean_text(item) for item in safe_list(normalized_req.get("practice_tags")) if clean_text(item)]
    )
    merged_experience_req = dedup_keep_order(
        [clean_text(item) for item in safe_list(graph_job_knowledge.get("experience_requirements")) if clean_text(item)]
        + [clean_text(item) for item in safe_list(normalized_req.get("experience_tags")) if clean_text(item)]
    )
    merged_soft_skills = dedup_keep_order(
        [clean_text(item) for item in safe_list(normalized_llm.get("soft_skills")) if clean_text(item)]
    )
    merged_warnings = dedup_keep_order(
        [clean_text(item) for item in safe_list(builder_payload.get("build_warnings")) if clean_text(item)]
        + [clean_text(item) for item in safe_list(aggregation_result.get("aggregation_warnings")) if clean_text(item)]
        + [clean_text(item) for item in safe_list(service_warnings) if clean_text(item)]
    )
    group_summary = deepcopy(safe_dict(builder_payload.get("group_summary")))
    if sql_fact_context.get("job_count"):
        group_summary["job_count"] = sql_fact_context.get("job_count")
    if graph_job_knowledge.get("related_jobs"):
        group_summary["graph_related_jobs"] = deepcopy(graph_job_knowledge.get("related_jobs"))

    result = JobProfileServiceResult(
        standard_job_name=clean_text(
            graph_job_knowledge.get("standard_job_name")
            or builder_payload.get("standard_job_name")
            or aggregation_result.get("standard_job_name")
        ),
        job_category=clean_text(graph_job_knowledge.get("job_category") or normalized_llm.get("job_category")),
        job_level=clean_text(graph_job_knowledge.get("job_level") or normalized_llm.get("job_level")),
        degree_requirement=build_degree_requirement_text(
            builder_payload,
            aggregation_result,
            llm_result,
            graph_job_knowledge=graph_job_knowledge,
        ),
        major_requirement=merged_major_req,
        experience_requirement=merged_experience_req,
        hard_skills=merged_hard_skills,
        tools_or_tech_stack=merged_tools,
        certificate_requirement=merged_certificate_req,
        practice_requirement=merged_practice_req,
        soft_skills=merged_soft_skills,
        suitable_student_profile=clean_text(normalized_llm.get("suitable_student_profile")),
        summary=clean_text(normalized_llm.get("summary")),
        vertical_paths=[
            clean_text(item)
            for item in safe_list(normalized_llm.get("vertical_paths"))
            if clean_text(item)
        ],
        transfer_paths=[
            clean_text(item)
            for item in safe_list(normalized_llm.get("transfer_paths"))
            if clean_text(item)
        ],
        skill_frequency=deepcopy(safe_list(aggregation_result.get("skill_frequency"))),
        degree_requirement_distribution=deepcopy(
            safe_list(aggregation_result.get("degree_requirement_distribution"))
        ),
        industry_distribution=deepcopy(
            sql_fact_context.get("industry_distribution")
            or safe_list(aggregation_result.get("industry_distribution"))
        ),
        city_distribution=deepcopy(
            sql_fact_context.get("city_distribution")
            or safe_list(aggregation_result.get("city_distribution"))
        ),
        salary_stats=deepcopy(
            sql_fact_context.get("salary_stats")
            or safe_dict(aggregation_result.get("salary_stats"))
        ),
        group_summary=group_summary,
        explicit_requirements=deepcopy(explicit_req),
        normalized_requirements=deepcopy(normalized_req),
        representative_samples=deepcopy(
            sql_fact_context.get("representative_samples")
            or safe_list(builder_payload.get("representative_samples"))
        ),
        llm_profile_result=deepcopy(safe_dict(normalized_llm.get("raw_llm_result"))),
        build_warnings=merged_warnings,
    )

    final_result = asdict(result)
    try:
        asset_context = build_job_profile_asset_context(final_result.get("standard_job_name"))
        final_result["core_job_profiles"] = deepcopy(safe_list(asset_context.get("core_job_profiles")))
        final_result["target_job_profile_assets"] = deepcopy(
            safe_dict(asset_context.get("target_job_profile_assets"))
        )
        asset_warnings = [
            clean_text(item)
            for item in safe_list(asset_context.get("asset_warnings"))
            if clean_text(item)
        ]
        if asset_warnings:
            final_result["build_warnings"] = dedup_keep_order(final_result.get("build_warnings", []) + asset_warnings)
    except Exception as exc:
        LOGGER.warning("job_profile asset context build failed: %s", exc)
        final_result["build_warnings"] = dedup_keep_order(
            final_result.get("build_warnings", []) + [f"岗位后处理资产接入失败: {exc}"]
        )
    return final_result

def build_job_profile_llm_input(
    builder_payload: Dict[str, Any],
    aggregation_result: Dict[str, Any],
    context_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """组装 job_profile 大模型输入。"""
    normalized_req = safe_dict(builder_payload.get("normalized_requirements"))
    group_summary = safe_dict(builder_payload.get("group_summary"))
    graph_job_knowledge = extract_graph_job_knowledge(
        context_data,
        source_job_name=clean_text(builder_payload.get("standard_job_name")),
    )
    sql_fact_context = extract_sql_fact_context(context_data)
    path_knowledge = extract_path_knowledge(
        context_data,
        source_job_name=clean_text(builder_payload.get("standard_job_name")),
    )
    semantic_context = extract_semantic_context(context_data)

    representative_samples = []
    for item in safe_list(builder_payload.get("representative_samples"))[:3]:
        item_dict = safe_dict(item)
        representative_samples.append(
            {
                "job_name_raw": clean_text(item_dict.get("job_name_raw") or item_dict.get("job_name")),
                "city": clean_text(item_dict.get("city")),
                "company_name": clean_text(item_dict.get("company_name_clean") or item_dict.get("company_name")),
                "salary": clean_text(item_dict.get("salary_raw")),
            }
        )

    return {
        "target_job_name": clean_text(builder_payload.get("standard_job_name")),
        "job_requirement_snapshot": {
            "group_summary": deepcopy(group_summary),
            "degree_tags": deepcopy(safe_list(normalized_req.get("degree_tags"))[:5]),
            "major_tags": deepcopy(safe_list(normalized_req.get("major_tags"))[:8]),
            "experience_tags": deepcopy(safe_list(normalized_req.get("experience_tags"))[:6]),
            "hard_skill_tags": deepcopy(safe_list(normalized_req.get("hard_skill_tags"))[:12]),
            "tool_skill_tags": deepcopy(safe_list(normalized_req.get("tool_skill_tags"))[:10]),
            "certificate_tags": deepcopy(safe_list(normalized_req.get("certificate_tags"))[:8]),
            "soft_skill_tags": deepcopy(safe_list(normalized_req.get("soft_skill_tags"))[:8]),
            "practice_tags": deepcopy(safe_list(normalized_req.get("practice_tags"))[:8]),
            "representative_samples": representative_samples,
        },
        "aggregation_snapshot": {
            "job_count": aggregation_result.get("job_count", 0),
            "skill_frequency": deepcopy(safe_list(aggregation_result.get("skill_frequency"))[:10]),
            "degree_requirement_distribution": deepcopy(
                safe_list(aggregation_result.get("degree_requirement_distribution"))[:5]
            ),
            "industry_distribution": deepcopy(safe_list(aggregation_result.get("industry_distribution"))[:5]),
            "city_distribution": deepcopy(safe_list(aggregation_result.get("city_distribution"))[:5]),
            "salary_stats": deepcopy(safe_dict(aggregation_result.get("salary_stats"))),
        },
        "graph_knowledge_snapshot": {
            "standard_job_name": clean_text(graph_job_knowledge.get("standard_job_name")),
            "job_category": clean_text(graph_job_knowledge.get("job_category")),
            "job_level": clean_text(graph_job_knowledge.get("job_level")),
            "degree_requirement": clean_text(graph_job_knowledge.get("degree_requirement")),
            "major_requirements": deepcopy(safe_list(graph_job_knowledge.get("major_requirements"))[:8]),
            "required_skills": deepcopy(safe_list(graph_job_knowledge.get("required_skills"))[:12]),
            "related_jobs": deepcopy(safe_list(graph_job_knowledge.get("related_jobs"))[:6]),
            "raw_requirement_summary": clean_text(graph_job_knowledge.get("raw_requirement_summary")),
        },
        "sql_fact_snapshot": {
            "job_count": sql_fact_context.get("job_count", 0),
            "salary_stats": deepcopy(safe_dict(sql_fact_context.get("salary_stats"))),
            "city_distribution": deepcopy(safe_list(sql_fact_context.get("city_distribution"))[:5]),
            "industry_distribution": deepcopy(safe_list(sql_fact_context.get("industry_distribution"))[:5]),
            "company_type_distribution": deepcopy(
                safe_list(sql_fact_context.get("company_type_distribution"))[:5]
            ),
            "company_size_distribution": deepcopy(
                safe_list(sql_fact_context.get("company_size_distribution"))[:5]
            ),
            "company_samples": deepcopy(safe_list(sql_fact_context.get("company_samples"))[:6]),
            "representative_samples": deepcopy(safe_list(sql_fact_context.get("representative_samples"))[:3]),
        },
        "path_knowledge_snapshot": {
            "vertical_paths": deepcopy(path_knowledge.get("vertical_paths", [])[:5]),
            "transfer_paths": deepcopy(path_knowledge.get("transfer_paths", [])[:5]),
            "instruction": "以上路径若已存在，视为离线沉淀的岗位知识，优先复用；仅在明显缺失时补充新的常见路径建议。",
        },
        "semantic_knowledge_snapshot": deepcopy(semantic_context),
        "generation_requirements": {
            "job_category": "优先复用 graph_knowledge_snapshot 中的岗位类别，仅在图谱字段缺失时做归纳补全。",
            "job_level": "优先复用 graph_knowledge_snapshot 中的岗位层级，仅在图谱字段缺失时做补充推断。",
            "soft_skills": "根据岗位描述和要求句归纳 3-8 个软技能标签。",
            "suitable_student_profile": "描述适合投递该岗位的学生画像，要求具体可执行。",
            "summary": "结合图谱岗位骨架、SQL 市场事实和 semantic_knowledge_snapshot，输出一段简洁岗位画像摘要。",
            "vertical_paths": "只允许沿用 path_knowledge_snapshot 中已有的真实纵向晋升路径；如果缺失，请返回空数组，不要补造“高级XX/XX负责人”等路径。",
            "transfer_paths": "只允许沿用 path_knowledge_snapshot 中已有的真实横向转岗路径；如果缺失，请返回空数组，不要补造“相近业务岗位/相近技术岗位”等路径。",
        },
        "output_schema_hint": asdict(JobProfileLLMSupplement()),
    }

class JobProfileService:
    """job_profile 业务服务编排器。"""

    def __init__(self, state_manager: Optional[StateManager] = None) -> None:
        self.state_manager = state_manager or StateManager()

    def build_features(
        self,
        df: pd.DataFrame,
        standard_job_name: str,
    ) -> Dict[str, Any]:
        """调用 builder 构造岗位组中间特征。"""
        return build_job_profile_input_payload(
            df=df,
            standard_job_name=standard_job_name,
            output_path=None,
        )

    def aggregate_group(
        self,
        df: pd.DataFrame,
        standard_job_name: str,
    ) -> Dict[str, Any]:
        """调用 aggregator 生成岗位组聚合统计。"""
        return aggregate_job_profile_group(
            df=df,
            standard_job_name=standard_job_name,
        )

    def call_job_profile_llm(
        self,
        builder_payload: Dict[str, Any],
        aggregation_result: Dict[str, Any],
        student_state: Optional[Dict[str, Any]] = None,
        context_data: Optional[Dict[str, Any]] = None,
        extra_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """通过统一大模型接口补充岗位画像字段。"""
        input_data = build_job_profile_llm_input(
            builder_payload=builder_payload,
            aggregation_result=aggregation_result,
            context_data=context_data,
        )
        merged_extra_context = {
            "service_name": "job_profile_service",
            "expected_fields": [
                "job_category",
                "job_level",
                "soft_skills",
                "suitable_student_profile",
                "summary",
                "vertical_paths",
                "transfer_paths",
            ],
        }
        if extra_context:
            merged_extra_context.update(deepcopy(extra_context))

        return call_llm(
            task_type="job_profile",
            input_data=input_data,
            context_data=context_data,
            student_state=student_state,
            extra_context=merged_extra_context,
        )

    def update_student_state(
        self,
        job_profile_result: Dict[str, Any],
        state_path: str | Path = DEFAULT_STATE_PATH,
        student_state: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """写回 student_api_state.json 的 job_profile_result 字段。"""
        return self.state_manager.update_state(
            task_type="job_profile",
            task_result=job_profile_result,
            state_path=state_path,
            student_state=student_state,
        )

    def run(
        self,
        df: pd.DataFrame,
        standard_job_name: str,
        state_path: str | Path = DEFAULT_STATE_PATH,
        context_data: Optional[Dict[str, Any]] = None,
        extra_context: Optional[Dict[str, Any]] = None,
        output_path: Optional[str | Path] = DEFAULT_OUTPUT_PATH,
    ) -> Dict[str, Any]:
        """执行完整 job_profile 服务流程。"""
        setup_logging()
        state_path = Path(state_path)
        service_warnings = []
        merged_context_data = deepcopy(context_data) if isinstance(context_data, dict) else {}
        student_state = self.state_manager.load_state(state_path)

        LOGGER.info("Step 1/4: build job profile payload for %s", standard_job_name)
        try:
            builder_payload = self.build_features(df, standard_job_name)
        except Exception as exc:
            LOGGER.exception("job_profile_builder failed")
            builder_payload = {
                "standard_job_name": clean_text(standard_job_name),
                "group_summary": {},
                "explicit_requirements": {},
                "normalized_requirements": {},
                "representative_samples": [],
                "source_columns": list(df.columns) if df is not None else [],
                "build_warnings": [f"builder 执行失败: {exc}"],
            }
            service_warnings.append(f"builder 执行失败: {exc}")

        LOGGER.info("Step 2/4: aggregate job profile group for %s", standard_job_name)
        try:
            aggregation_result = self.aggregate_group(df, standard_job_name)
        except Exception as exc:
            LOGGER.exception("job_profile_aggregator failed")
            aggregation_result = {
                "standard_job_name": clean_text(standard_job_name),
                "job_count": 0,
                "skill_frequency": [],
                "degree_requirement_distribution": [],
                "industry_distribution": [],
                "city_distribution": [],
                "salary_stats": {},
                "top_company_types": [],
                "top_company_sizes": [],
                "source_columns": list(df.columns) if df is not None else [],
                "aggregation_warnings": [f"aggregator 执行失败: {exc}"],
            }
            service_warnings.append(f"aggregator 执行失败: {exc}")

        if not safe_dict(merged_context_data.get("semantic_context")):
            try:
                semantic_context = build_semantic_context_for_job_profile(
                    builder_payload=builder_payload,
                    aggregation_result=aggregation_result,
                    context_data=merged_context_data,
                )
                if semantic_context:
                    merged_context_data["semantic_context"] = semantic_context
            except FileNotFoundError:
                LOGGER.info("semantic knowledge base not found, skip semantic retrieval for job_profile")
            except Exception as exc:
                LOGGER.warning("semantic retrieval for job_profile failed: %s", exc)
                service_warnings.append(f"语义知识检索失败: {exc}")

        LOGGER.info("Step 3/4: call LLM job_profile for %s", standard_job_name)
        try:
            llm_result = self.call_job_profile_llm(
                builder_payload=builder_payload,
                aggregation_result=aggregation_result,
                student_state=student_state,
                context_data=merged_context_data,
                extra_context=extra_context,
            )
        except Exception as exc:
            LOGGER.exception("call_llm('job_profile', ...) failed")
            llm_result = {}
            service_warnings.append(f"LLM 调用失败: {exc}")

        LOGGER.info("Step 4/4: merge job profile results for %s", standard_job_name)
        final_result = merge_job_profile_results(
            builder_payload=builder_payload,
            aggregation_result=aggregation_result,
            llm_result=llm_result,
            context_data=merged_context_data,
            service_warnings=service_warnings,
        )

        self.update_student_state(
            job_profile_result=final_result,
            state_path=state_path,
            student_state=student_state,
        )
        save_json(final_result, output_path)

        LOGGER.info(
            "job_profile service finished. job=%s, job_count=%s, hard_skills=%s",
            final_result.get("standard_job_name"),
            safe_dict(final_result.get("group_summary")).get("job_count", 0),
            len(safe_list(final_result.get("hard_skills"))),
        )
        return final_result

def run_job_profile_service(
    df: pd.DataFrame,
    standard_job_name: str,
    state_path: str | Path = DEFAULT_STATE_PATH,
    context_data: Optional[Dict[str, Any]] = None,
    extra_context: Optional[Dict[str, Any]] = None,
    output_path: Optional[str | Path] = DEFAULT_OUTPUT_PATH,
) -> Dict[str, Any]:
    """函数式入口，方便主流程直接调用。"""
    return JobProfileService().run(
        df=df,
        standard_job_name=standard_job_name,
        state_path=state_path,
        context_data=context_data,
        extra_context=extra_context,
        output_path=output_path,
    )

def parse_args() -> argparse.Namespace:
    """命令行参数解析。"""
    parser = argparse.ArgumentParser(description="Build final job_profile_result by builder + aggregator + LLM")
    parser.add_argument(
        "--input-csv",
        default="",
        help="可选：清洗后的岗位 CSV 路径；不传则使用内置 demo 数据",
    )
    parser.add_argument(
        "--standard-job-name",
        default="数据分析师",
        help="目标标准岗位名称",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_PATH),
        help="job_profile_result JSON 输出路径",
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    if args.input_csv:
        source_df = pd.read_csv(args.input_csv)
    else:
        source_df = build_aggregator_demo_df()

    result = run_job_profile_service(
        df=source_df,
        standard_job_name=args.standard_job_name,
        context_data={
            "graph_context": {
                "mock_note": "这里预留 Neo4j 岗位关系上下文，当前 demo 使用 mock。",
                "upstream_jobs": ["数据分析实习生"],
                "downstream_jobs": ["高级数据分析师", "数据分析负责人"],
            },
            "sql_context": {
                "mock_note": "这里预留 SQL 薪资/城市/企业统计上下文，当前 demo 使用 mock。",
            },
        },
        extra_context={
            "demo_name": "job_profile_service_demo",
        },
        output_path=args.output,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


