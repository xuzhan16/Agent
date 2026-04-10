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
- 保持 mock LLM 链路与前端接口可继续使用。
"""
from __future__ import annotations

import argparse
import logging
import os
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
            province,
            industry,
            company_name_clean AS company_name,
            company_type,
            company_size,
            salary_month_min AS salary_min_month,
            salary_month_max AS salary_max_month,
            job_desc,
            company_desc,
            update_date
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


def _query_job_graph_context(initial_target_job: str) -> Dict[str, List[str]]:
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
    return {
        "required_skills": _dedup_keep_order(required_skills),
        "transfer_paths": _dedup_keep_order(transfer_paths),
        "promote_paths": _dedup_keep_order(promote_paths),
    }


def _build_compact_sql_context(sql_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    cities = _dedup_keep_order(row.get("city") for row in sql_rows)[:8]
    industries = _dedup_keep_order(row.get("industry") for row in sql_rows)[:8]
    company_types = _dedup_keep_order(row.get("company_type") for row in sql_rows)[:6]
    return {
        "job_count": len(sql_rows),
        "top_cities": cities,
        "top_industries": industries,
        "top_company_types": company_types,
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


def run_pipeline(
    resume_path: str,
    initial_target_job: str,
    state_path: str = "student.json",
) -> Dict[str, Any]:
    state_manager = StateManager()
    state_path = str(state_path)

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

    logger.info("启动第 3 步: job_profile，拉取后端数据库知识")
    sql_rows = _query_job_profile_rows(effective_target_job) if effective_target_job else []
    graph_context = _query_job_graph_context(effective_target_job) if effective_target_job else {
        "required_skills": [],
        "transfer_paths": [],
        "promote_paths": [],
    }
    job_profile_df = _build_job_profile_dataframe(sql_rows, effective_target_job)
    job_profile_context = {
        "sql_context": _build_compact_sql_context(sql_rows),
        "graph_context": graph_context,
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

    logger.info("启动第 4 步: job_match")
    job_match_result = run_job_match_service_from_state(
        state_path=state_path,
    )
    logger.info(
        "job_match 完成，overall_match_score=%s",
        job_match_result.get("overall_match_score")
        if isinstance(job_match_result, dict)
        else "",
    )

    logger.info("启动第 5 步: career_path_plan，引入图谱晋升换岗数据")
    plan_context = {
        "graph_context": {
            "transfer_paths": graph_context.get("transfer_paths", []),
            "promote_paths": graph_context.get("promote_paths", []),
        },
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

    logger.info("启动第 6 步: career_report")
    career_report_result = run_career_report_service_from_state(
        state_path=state_path,
    )
    _export_report_markdown(career_report_result, state_path)

    logger.info("=========== 流水线圆满结束 ===========")
    return state_manager.load_state(state_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--resume", type=str, required=True, help="简历文件路径")
    parser.add_argument("--job", type=str, required=True, help="目标应聘岗位")
    parser.add_argument("--out", type=str, default="student.json")
    args = parser.parse_args()

    run_pipeline(args.resume, args.job, args.out)
