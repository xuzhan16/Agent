"""
main_pipeline.py - 全流程统一调度入口

主流程：
1. resume_parse：读取简历并写回 state；
2. student_profile：调用 builder + scorer + LLM 补充能力画像；
3. job_profile：从 SQLite/Neo4j 拉取岗位知识，先规则聚合再补充语义画像；
4. job_match：基于规则评分结果 + LLM 解释生成人岗匹配分析；
5. career_path_plan：先 selector 选目标和路径，再由 LLM 做规划解释；
6. career_report：先 formatter 组章节草稿，再由 LLM 做润色成文。

设计原则：
- 尽量复用 service 层已有的“规则先行 + LLM 补充”能力；
- 避免主流程再次拼接大而全的 prompt；
- 保持真实模型链路与前端接口稳定可用。
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd

from career_path_plan.career_path_plan_service import run_career_path_plan_service_from_state
from career_report.career_report_service import run_career_report_service_from_state
from db_helper import query_neo4j, query_sqlite
from job_match.job_match_service import run_job_match_service_from_state
from job_profile.job_profile_service import run_job_profile_service
from llm_interface_layer.state_manager import StateManager
from resume_parse_module.resume_parser import process_resume_file
from student_profile.student_profile_service import run_student_profile_service

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# 配置数据库连接 (按项目默认路径)
SQLITE_DB_PATH = "outputs/sql/jobs.db"
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "ChangeThisPassword_123!")
PIPELINE_STATUS_FILE_NAME = "pipeline_state.json"
TOTAL_PIPELINE_STEPS = 6


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _dedup_keep_order(values: Iterable[Any]) -> List[str]:
    seen = set()
    result: List[str] = []
    for value in values:
        cleaned = _clean_text(value)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
    return result


def _parse_json_text_list(value: Any) -> List[str]:
    text = _clean_text(value)
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            loaded = json.loads(text)
            if isinstance(loaded, list):
                return _dedup_keep_order(loaded)
        except json.JSONDecodeError:
            pass
    return _dedup_keep_order(text.split(","))


def _safe_float(value: Any) -> float | None:
    text = _clean_text(value)
    if not text:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _resolve_effective_target_job(
    state_manager: StateManager,
    state_path: str | Path,
    initial_target_job: str,
) -> str:
    """
    以“简历解析结果中的求职意向”为主，外部传入岗位为兜底，解析出本次流水线的实际目标岗位。
    """
    state = state_manager.load_state(state_path)
    resume_parse_result = (
        state.get("resume_parse_result")
        if isinstance(state.get("resume_parse_result"), dict)
        else {}
    )
    resume_target_job = _clean_text(resume_parse_result.get("target_job_intention"))
    fallback_target_job = _clean_text(initial_target_job)
    effective_target_job = resume_target_job or fallback_target_job

    basic_info = state.get("basic_info") if isinstance(state.get("basic_info"), dict) else {}
    if effective_target_job:
        basic_info["target_job"] = effective_target_job
    state["basic_info"] = basic_info

    if effective_target_job and not resume_target_job:
        resume_parse_result["target_job_intention"] = effective_target_job
    state["resume_parse_result"] = resume_parse_result

    state_manager.save_state(state, state_path)
    return effective_target_job


def _resolve_target_job_from_student_profile(
    state_manager: StateManager,
    state_path: str | Path,
    student_profile_bundle: Dict[str, Any],
) -> str:
    """
    仅当简历未解析出目标岗位时，尝试从学生画像中已有的 occupation_hints 兜底出一个候选岗位。
    """
    profile_input_payload = (
        student_profile_bundle.get("profile_input_payload")
        if isinstance(student_profile_bundle.get("profile_input_payload"), dict)
        else {}
    )
    normalized_profile = (
        profile_input_payload.get("normalized_profile")
        if isinstance(profile_input_payload.get("normalized_profile"), dict)
        else {}
    )
    occupation_hints = _dedup_keep_order(normalized_profile.get("occupation_hints", []))
    inferred_target_job = occupation_hints[0] if occupation_hints else ""
    if not inferred_target_job:
        return ""

    state = state_manager.load_state(state_path)
    basic_info = state.get("basic_info") if isinstance(state.get("basic_info"), dict) else {}
    basic_info["target_job"] = inferred_target_job
    state["basic_info"] = basic_info

    resume_parse_result = (
        state.get("resume_parse_result")
        if isinstance(state.get("resume_parse_result"), dict)
        else {}
    )
    if not _clean_text(resume_parse_result.get("target_job_intention")):
        resume_parse_result["target_job_intention"] = inferred_target_job
    state["resume_parse_result"] = resume_parse_result

    state_manager.save_state(state, state_path)
    return inferred_target_job


def _query_job_profile_rows(initial_target_job: str) -> List[Dict[str, Any]]:
    """
    查询岗位画像规则层需要的关键字段，避免 service 因上游列缺失而退化。
    """
    return query_sqlite(
        db_path=SQLITE_DB_PATH,
        query="""
        SELECT
            standard_job_name,
            job_name_raw AS job_name,
            city,
            district AS province,
            industry,
            company_name_clean AS company_name,
            company_type,
            company_size,
            salary_month_min AS salary_min_month,
            salary_month_max AS salary_max_month,
            job_desc_clean AS job_desc,
            company_desc_clean AS company_desc,
            updated_at_std AS update_date
        FROM job_detail
        WHERE standard_job_name = ?
        LIMIT 80
        """,
        parameters=(initial_target_job,),
    )


def _build_job_profile_dataframe(
    sql_rows: List[Dict[str, Any]],
    initial_target_job: str,
) -> pd.DataFrame:
    columns = [
        "standard_job_name",
        "job_name",
        "city",
        "province",
        "industry",
        "company_name",
        "company_type",
        "company_size",
        "salary_min_month",
        "salary_max_month",
        "job_desc",
        "company_desc",
        "update_date",
        "salary_raw",
    ]
    if not sql_rows:
        return pd.DataFrame(columns=columns)

    normalized_rows = []
    for row in sql_rows:
        normalized = {column: row.get(column, "") for column in columns}
        normalized["standard_job_name"] = _clean_text(
            row.get("standard_job_name") or initial_target_job
        )
        salary_min = row.get("salary_min_month")
        salary_max = row.get("salary_max_month")
        if salary_min or salary_max:
            normalized["salary_raw"] = f"{salary_min or '?'}-{salary_max or '?'}"
        normalized_rows.append(normalized)

    return pd.DataFrame(normalized_rows)


def _query_job_graph_context(initial_target_job: str) -> Dict[str, Any]:
    core_rows = query_neo4j(
        uri=NEO4J_URI,
        user=NEO4J_USER,
        password=NEO4J_PASSWORD,
        query="""
        MATCH (j:Job {name: $job})
        OPTIONAL MATCH (j)-[:REQUIRES_DEGREE]->(d:Degree)
        OPTIONAL MATCH (j)-[:PREFERS_MAJOR]->(m:Major)
        RETURN
            j.name AS name,
            j.job_category AS job_category,
            j.job_level AS job_level,
            j.degree_requirement AS degree_requirement,
            j.major_requirement AS major_requirement,
            j.experience_requirement AS experience_requirement,
            j.raw_requirement_summary AS raw_requirement_summary,
            collect(DISTINCT d.name) AS degree_requirements,
            collect(DISTINCT m.name) AS major_requirements
        LIMIT 1
        """,
        parameters={"job": initial_target_job},
    )
    required_skills = [
        item.get("skill")
        for item in query_neo4j(
            uri=NEO4J_URI,
            user=NEO4J_USER,
            password=NEO4J_PASSWORD,
            query="""
            MATCH (j:Job {name: $job})-[:REQUIRES_SKILL]->(s:Skill)
            RETURN s.name AS skill
            LIMIT 10
            """,
            parameters={"job": initial_target_job},
        )
    ]
    transfer_paths = [
        item.get("next_job")
        for item in query_neo4j(
            uri=NEO4J_URI,
            user=NEO4J_USER,
            password=NEO4J_PASSWORD,
            query="""
            MATCH (j:Job {name: $job})-[:TRANSFER_TO]->(target:Job)
            RETURN target.name AS next_job
            LIMIT 5
            """,
            parameters={"job": initial_target_job},
        )
    ]
    promote_paths = [
        item.get("next_job")
        for item in query_neo4j(
            uri=NEO4J_URI,
            user=NEO4J_USER,
            password=NEO4J_PASSWORD,
            query="""
            MATCH (j:Job {name: $job})-[:PROMOTE_TO]->(target:Job)
            RETURN target.name AS next_job
            LIMIT 5
            """,
            parameters={"job": initial_target_job},
        )
    ]
    core_row = core_rows[0] if core_rows else {}
    job_core = {
        "name": _clean_text(core_row.get("name") or initial_target_job),
        "job_category": _clean_text(core_row.get("job_category")),
        "job_level": _clean_text(core_row.get("job_level")),
        "degree_requirement": _clean_text(core_row.get("degree_requirement")),
        "major_requirement": _clean_text(core_row.get("major_requirement")),
        "experience_requirement": _clean_text(core_row.get("experience_requirement")),
        "raw_requirement_summary": _clean_text(core_row.get("raw_requirement_summary")),
    }
    degree_requirements = _dedup_keep_order(
        list(core_row.get("degree_requirements") or [])
        + [job_core.get("degree_requirement")]
    )
    major_requirements = _dedup_keep_order(
        list(core_row.get("major_requirements") or [])
        + _parse_json_text_list(job_core.get("major_requirement"))
    )
    related_jobs = _dedup_keep_order(promote_paths + transfer_paths)
    return {
        "job_core": job_core,
        "required_skills": _dedup_keep_order(required_skills),
        "degree_requirements": degree_requirements,
        "major_requirements": major_requirements,
        "transfer_paths": _dedup_keep_order(transfer_paths),
        "promote_paths": _dedup_keep_order(promote_paths),
        "related_jobs": related_jobs,
    }


def _query_job_path_knowledge(initial_target_job: str) -> Dict[str, List[str]]:
    """从 SQLite 的 job_profile 表读取离线路径知识，作为 Neo4j 的补充来源。"""
    rows = query_sqlite(
        db_path=SQLITE_DB_PATH,
        query="""
        SELECT
            vertical_paths_json,
            transfer_paths_json
        FROM job_profile
        WHERE standard_job_name = ?
        LIMIT 1
        """,
        parameters=(initial_target_job,),
    )
    if not rows:
        return {"vertical_paths": [], "transfer_paths": []}
    row = rows[0]
    return {
        "vertical_paths": _parse_json_text_list(row.get("vertical_paths_json")),
        "transfer_paths": _parse_json_text_list(row.get("transfer_paths_json")),
    }


def _build_compact_sql_context(sql_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    def _build_distribution(key: str, limit: int = 8) -> List[Dict[str, Any]]:
        counts: Dict[str, int] = {}
        total = 0
        for row in sql_rows:
            name = _clean_text(row.get(key))
            if not name:
                continue
            counts[name] = counts.get(name, 0) + 1
            total += 1
        ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        return [
            {
                "name": name,
                "count": count,
                "ratio": round(count / total, 4) if total else 0.0,
            }
            for name, count in ranked[:limit]
        ]

    salary_mins = []
    salary_maxs = []
    salary_mids = []
    representative_samples = []
    for row in sql_rows[:5]:
        salary_min = row.get("salary_min_month")
        salary_max = row.get("salary_max_month")
        salary_text = ""
        if salary_min or salary_max:
            salary_text = f"{salary_min or '?'}-{salary_max or '?'}"
        representative_samples.append(
            {
                "job_name": _clean_text(row.get("job_name")),
                "city": _clean_text(row.get("city")),
                "company_name": _clean_text(row.get("company_name")),
                "salary_raw": salary_text,
                "update_date": _clean_text(row.get("update_date")),
            }
        )
        salary_min_float = _safe_float(salary_min)
        salary_max_float = _safe_float(salary_max)
        if salary_min_float is not None:
            salary_mins.append(salary_min_float)
        if salary_max_float is not None:
            salary_maxs.append(salary_max_float)
        if salary_min_float is not None and salary_max_float is not None:
            salary_mids.append((salary_min_float + salary_max_float) / 2)

    salary_stats: Dict[str, Any] = {}
    if salary_mins or salary_maxs:
        salary_stats = {
            "salary_min_month_avg": round(sum(salary_mins) / len(salary_mins), 2) if salary_mins else 0.0,
            "salary_max_month_avg": round(sum(salary_maxs) / len(salary_maxs), 2) if salary_maxs else 0.0,
            "salary_mid_month_avg": round(sum(salary_mids) / len(salary_mids), 2) if salary_mids else 0.0,
            "valid_salary_count": max(len(salary_mins), len(salary_maxs)),
        }

    city_distribution = _build_distribution("city")
    industry_distribution = _build_distribution("industry")
    company_type_distribution = _build_distribution("company_type", limit=6)
    company_size_distribution = _build_distribution("company_size", limit=6)
    company_samples = _dedup_keep_order(row.get("company_name") for row in sql_rows)[:8]
    return {
        "job_count": len(sql_rows),
        "salary_stats": salary_stats,
        "city_distribution": city_distribution,
        "industry_distribution": industry_distribution,
        "company_type_distribution": company_type_distribution,
        "company_size_distribution": company_size_distribution,
        "top_cities": [item["name"] for item in city_distribution[:8]],
        "top_industries": [item["name"] for item in industry_distribution[:8]],
        "top_company_types": [item["name"] for item in company_type_distribution[:6]],
        "top_company_sizes": [item["name"] for item in company_size_distribution[:6]],
        "company_samples": company_samples,
        "representative_samples": representative_samples,
    }


def _export_report_markdown(report_result: Dict[str, Any], state_path: str | Path) -> None:
    report_text = ""
    if isinstance(report_result, dict):
        report_text = _clean_text(
            report_result.get("report_text_markdown") or report_result.get("report_text")
        )
    if not report_text:
        return

    output_path = Path(state_path).resolve().with_name("final_report.md")
    output_path.write_text(report_text, encoding="utf-8")
    logger.info("最终报告已导出为 %s", output_path)


def _pipeline_status_path(state_path: str | Path) -> Path:
    return Path(state_path).resolve().with_name(PIPELINE_STATUS_FILE_NAME)


def _write_pipeline_status(
    state_path: str | Path,
    status: str,
    current_step: int,
    step_name: str,
    error: str | None = None,
) -> None:
    payload = {
        "status": status,
        "current_step": current_step,
        "total_steps": TOTAL_PIPELINE_STEPS,
        "step_name": step_name,
        "error": error,
        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    path = _pipeline_status_path(state_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def run_pipeline(
    resume_path: str,
    initial_target_job: str,
    state_path: str = "student_api_state.json",
) -> Dict[str, Any]:
    state_manager = StateManager()
    state_path = str(state_path)
    current_step = 0
    current_step_name = "准备开始"

    def _mark_step(step: int, step_name: str) -> None:
        nonlocal current_step, current_step_name
        current_step = step
        current_step_name = step_name
        _write_pipeline_status(
            state_path=state_path,
            status="running",
            current_step=step,
            step_name=step_name,
            error=None,
        )

    try:
        _mark_step(1, "简历解析")
        logger.info("启动第 1 步: resume_parse")
        process_resume_file(
            file_path=resume_path,
            state_path=state_path,
        )
        effective_target_job = _resolve_effective_target_job(
            state_manager=state_manager,
            state_path=state_path,
            initial_target_job=initial_target_job,
        )
        logger.info(
            "本次流水线目标岗位=%s",
            effective_target_job or "未从简历中解析到明确目标岗位",
        )

        _mark_step(2, "学生画像")
        logger.info("启动第 2 步: student_profile")
        student_profile_bundle = run_student_profile_service(
            state_path=state_path,
        )
        logger.info(
            "student_profile 完成，summary长度=%s",
            len(_clean_text(student_profile_bundle.get("student_profile_result", {}).get("summary"))),
        )

        if not effective_target_job:
            inferred_target_job = _resolve_target_job_from_student_profile(
                state_manager=state_manager,
                state_path=state_path,
                student_profile_bundle=student_profile_bundle,
            )
            if inferred_target_job:
                effective_target_job = inferred_target_job
                logger.info("简历未明确目标岗位，已根据学生画像 occupation_hints 兜底为=%s", effective_target_job)

        _mark_step(3, "岗位画像")
        logger.info("启动第 3 步: job_profile，拉取后端数据库知识")
        graph_context = _query_job_graph_context(effective_target_job) if effective_target_job else {
            "job_core": {},
            "required_skills": [],
            "degree_requirements": [],
            "major_requirements": [],
            "transfer_paths": [],
            "promote_paths": [],
            "related_jobs": [],
        }
        sql_rows = _query_job_profile_rows(effective_target_job) if effective_target_job else []
        offline_path_knowledge = _query_job_path_knowledge(effective_target_job) if effective_target_job else {
            "vertical_paths": [],
            "transfer_paths": [],
        }
        graph_context = {
            "job_core": graph_context.get("job_core", {}),
            "required_skills": _dedup_keep_order(graph_context.get("required_skills", [])),
            "degree_requirements": _dedup_keep_order(graph_context.get("degree_requirements", [])),
            "major_requirements": _dedup_keep_order(graph_context.get("major_requirements", [])),
            "promote_paths": _dedup_keep_order(graph_context.get("promote_paths", [])),
            "transfer_paths": _dedup_keep_order(graph_context.get("transfer_paths", [])),
            "related_jobs": _dedup_keep_order(graph_context.get("related_jobs", [])),
            "offline_profile_vertical_paths": _dedup_keep_order(
                offline_path_knowledge.get("vertical_paths", [])
            ),
            "offline_profile_transfer_paths": _dedup_keep_order(
                offline_path_knowledge.get("transfer_paths", [])
            ),
        }
        job_profile_df = _build_job_profile_dataframe(sql_rows, effective_target_job)
        job_profile_context = {
            "graph_context": graph_context,
            "sql_context": _build_compact_sql_context(sql_rows),
        }
        job_profile_result = run_job_profile_service(
            df=job_profile_df,
            standard_job_name=effective_target_job,
            state_path=state_path,
            context_data=job_profile_context,
        )
        logger.info(
            "job_profile 完成，hard_skills=%s",
            len(job_profile_result.get("hard_skills", []))
            if isinstance(job_profile_result, dict)
            else 0,
        )

        _mark_step(4, "岗位匹配")
        logger.info("启动第 4 步: job_match")
        job_match_result = run_job_match_service_from_state(
            state_path=state_path,
            context_data=job_profile_context,
        )
        logger.info(
            "job_match 完成，overall_match_score=%s",
            job_match_result.get("overall_match_score")
            if isinstance(job_match_result, dict)
            else "",
        )

        _mark_step(5, "职业路径规划")
        logger.info("启动第 5 步: career_path_plan，引入图谱晋升换岗数据")
        plan_context = {
            "graph_context": graph_context,
            "sql_context": job_profile_context.get("sql_context", {}),
        }
        career_path_plan_result = run_career_path_plan_service_from_state(
            state_path=state_path,
            context_data=plan_context,
        )
        logger.info(
            "career_path_plan 完成，primary_target_job=%s",
            career_path_plan_result.get("primary_target_job")
            if isinstance(career_path_plan_result, dict)
            else "",
        )

        _mark_step(6, "职业报告生成")
        logger.info("启动第 6 步: career_report")
        career_report_result = run_career_report_service_from_state(
            state_path=state_path,
            context_data=plan_context,
        )
        _export_report_markdown(career_report_result, state_path)

        _write_pipeline_status(
            state_path=state_path,
            status="completed",
            current_step=TOTAL_PIPELINE_STEPS,
            step_name="流程完成",
            error=None,
        )
        logger.info("=========== 流水线圆满结束 ===========")
        return state_manager.load_state(state_path)
    except Exception as exc:
        logger.exception("流水线执行失败")
        _write_pipeline_status(
            state_path=state_path,
            status="failed",
            current_step=max(1, current_step),
            step_name=current_step_name,
            error=str(exc),
        )
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--resume", type=str, required=True, help="简历文件路径")
    parser.add_argument("--job", type=str, required=True, help="目标应聘岗位")
    parser.add_argument("--out", type=str, default="student_api_state.json")
    args = parser.parse_args()

    run_pipeline(args.resume, args.job, args.out)
