"""
job_data_pipeline.py

统一封装岗位底库的数据处理流水线：
1. 原始 Excel 清洗
2. 非计算机岗位过滤
3. 岗位去重 / 标准岗位归一
4. 岗位画像与路径关系抽取
5. 导出岗位知识 JSON
6. 构建本地 embedding
7. 导出 SQLite
8. 导出 Neo4j CSV

设计目标：
- 给脚本和后端接口提供统一入口；
- 尽量复用已有 job_data 模块；
- 不影响学生侧主链路。
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict

from job_data.data_cleaning import process_job_excel
from job_data.build_embedding_index import process_build_embedding_index
from job_data.export_to_json_kb import process_export_to_json_kb
from job_data.export_to_neo4j import process_export_to_neo4j
from job_data.export_to_sql import process_export_to_sql
from job_data.job_dedup import process_job_dedup
from job_data.job_extract import DEFAULT_GROUP_SAMPLE_SIZE, process_job_extract
from job_data.non_cs_filter import process_non_cs_filter
from semantic_retrieval.embedding_store import DEFAULT_HASH_EMBEDDING_MODEL


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_INPUT_FILE = "20260226105856_457.xls"
DEFAULT_INTERMEDIATE_DIR = "outputs/intermediate"
DEFAULT_SQL_DB_PATH = "outputs/sql/jobs.db"
DEFAULT_NEO4J_OUTPUT_DIR = "outputs/neo4j"
DEFAULT_KNOWLEDGE_OUTPUT_DIR = "outputs/knowledge"
DEFAULT_KNOWLEDGE_JSON_NAME = "job_knowledge.jsonl"
DEFAULT_EXTRACT_MAX_WORKERS = 1
PROGRESS_BAR_WIDTH = 28


def resolve_project_path(path_value: str | Path) -> Path:
    """把相对路径统一解析到项目根目录下。"""
    path = Path(path_value)
    if path.is_absolute():
        return path
    return (PROJECT_ROOT / path).resolve()


def build_pipeline_output_paths(intermediate_dir: str | Path) -> Dict[str, Path]:
    """统一生成中间产物路径。"""
    base_dir = resolve_project_path(intermediate_dir)
    base_dir.mkdir(parents=True, exist_ok=True)
    return {
        "jobs_cleaned": base_dir / "jobs_cleaned.csv",
        "jobs_cs_filtered": base_dir / "jobs_cs_filtered.csv",
        "job_cs_filter_audit": base_dir / "job_cs_filter_audit.csv",
        "jobs_dedup_result": base_dir / "jobs_dedup_result.csv",
        "job_name_mapping": base_dir / "job_name_mapping.csv",
        "job_dedup_pairs": base_dir / "job_dedup_pairs.csv",
        "jobs_extracted_full": base_dir / "jobs_extracted_full.csv",
    }


def summarize_graph_tables(graph_tables: Dict[str, Any]) -> Dict[str, int]:
    """提取 Neo4j 导出结果的行数摘要。"""
    summary = {}
    for key, value in graph_tables.items():
        try:
            summary[key] = int(len(value))
        except Exception:
            summary[key] = 0
    return summary


def render_progress_bar(current: int, total: int, width: int = PROGRESS_BAR_WIDTH) -> str:
    """渲染简单控制台进度条。"""
    if total <= 0:
        return "[----------------------------]   0.00% (0/0)"
    ratio = max(0.0, min(1.0, float(current) / float(total)))
    filled = int(round(width * ratio))
    bar = "#" * filled + "-" * (width - filled)
    return f"[{bar}] {ratio * 100:6.2f}% ({current}/{total})"


def print_pipeline_progress(current: int, total: int, stage_name: str) -> None:
    """打印流水线阶段进度。"""
    print(f"[pipeline] {render_progress_bar(current, total)} {stage_name}", flush=True)


def run_job_data_pipeline(
    input_path: str | Path,
    intermediate_dir: str | Path = DEFAULT_INTERMEDIATE_DIR,
    sql_db_path: str | Path = DEFAULT_SQL_DB_PATH,
    neo4j_output_dir: str | Path = DEFAULT_NEO4J_OUTPUT_DIR,
    knowledge_output_dir: str | Path = DEFAULT_KNOWLEDGE_OUTPUT_DIR,
    sheet_name: str | int = 0,
    log_every: int = 50,
    max_workers: int = DEFAULT_EXTRACT_MAX_WORKERS,
    group_sample_size: int = DEFAULT_GROUP_SAMPLE_SIZE,
    embedding_model_name: str = DEFAULT_HASH_EMBEDDING_MODEL,
) -> Dict[str, Any]:
    """
    统一执行岗位底库处理流水线。

    返回处理摘要，方便接口层直接回传。
    """
    resolved_input_path = resolve_project_path(input_path)
    if not resolved_input_path.exists():
        raise FileNotFoundError(f"输入文件不存在: {resolved_input_path}")

    output_paths = build_pipeline_output_paths(intermediate_dir)
    resolved_sql_db_path = resolve_project_path(sql_db_path)
    resolved_sql_db_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_neo4j_output_dir = resolve_project_path(neo4j_output_dir)
    resolved_neo4j_output_dir.mkdir(parents=True, exist_ok=True)
    resolved_knowledge_output_dir = resolve_project_path(knowledge_output_dir)
    resolved_knowledge_output_dir.mkdir(parents=True, exist_ok=True)
    knowledge_json_path = resolved_knowledge_output_dir / DEFAULT_KNOWLEDGE_JSON_NAME

    total_stages = 8
    completed_stages = 0
    print_pipeline_progress(completed_stages, total_stages, "开始执行岗位底库流水线")

    cleaned_df = process_job_excel(
        input_excel_path=str(resolved_input_path),
        output_csv_path=str(output_paths["jobs_cleaned"]),
        sheet_name=sheet_name,
    )
    completed_stages += 1
    print_pipeline_progress(completed_stages, total_stages, "已完成：原始 Excel 清洗")

    filtered_df, filter_audit_df, filter_stats = process_non_cs_filter(
        df=cleaned_df,
        output_filtered_csv=str(output_paths["jobs_cs_filtered"]),
        output_audit_csv=str(output_paths["job_cs_filter_audit"]),
    )
    completed_stages += 1
    print_pipeline_progress(completed_stages, total_stages, "已完成：非计算机岗位过滤")

    dedup_df, mapping_df, pair_results_df = process_job_dedup(
        df=filtered_df,
        output_data_csv=str(output_paths["jobs_dedup_result"]),
        output_mapping_csv=str(output_paths["job_name_mapping"]),
        output_pair_csv=str(output_paths["job_dedup_pairs"]),
    )
    completed_stages += 1
    print_pipeline_progress(completed_stages, total_stages, "已完成：岗位去重与标准岗位归一")

    extracted_merged_df, extracted_df = process_job_extract(
        df=dedup_df,
        output_csv_path=str(output_paths["jobs_extracted_full"]),
        log_every=log_every,
        max_workers=max_workers,
        group_sample_size=group_sample_size,
    )
    completed_stages += 1
    print_pipeline_progress(completed_stages, total_stages, "已完成：岗位画像与路径关系抽取")

    knowledge_records = process_export_to_json_kb(
        df=extracted_merged_df,
        output_path=str(knowledge_json_path),
    )
    completed_stages += 1
    print_pipeline_progress(completed_stages, total_stages, "已完成：导出岗位知识 JSON")

    embedding_summary = process_build_embedding_index(
        input_json_path=str(knowledge_json_path),
        output_dir=str(resolved_knowledge_output_dir),
        embedding_model_name=embedding_model_name,
    )
    completed_stages += 1
    print_pipeline_progress(completed_stages, total_stages, "已完成：构建本地 embedding")

    process_export_to_sql(
        df=extracted_merged_df,
        db_path=str(resolved_sql_db_path),
    )
    completed_stages += 1
    print_pipeline_progress(completed_stages, total_stages, "已完成：导出 SQLite")

    graph_tables = process_export_to_neo4j(
        df=extracted_merged_df,
        output_dir=str(resolved_neo4j_output_dir),
    )
    completed_stages += 1
    print_pipeline_progress(completed_stages, total_stages, "已完成：导出 Neo4j CSV")

    return {
        "input_file": str(resolved_input_path),
        "sheet_name": sheet_name,
        "rows": {
            "cleaned_rows": int(len(cleaned_df)),
            "cs_filtered_rows": int(len(filtered_df)),
            "filtered_out_rows": int(len(cleaned_df) - len(filtered_df)),
            "cs_filter_audit_rows": int(len(filter_audit_df)),
            "llm_filter_review_rows": int(filter_stats.get("llm_reviewed_rows", 0)),
            "dedup_rows": int(len(dedup_df)),
            "mapping_rows": int(len(mapping_df)),
            "pair_rows": int(len(pair_results_df)),
            "extracted_rows": int(len(extracted_df)),
            "extract_success_rows": int(extracted_merged_df.get("extract_success", []).sum())
            if "extract_success" in extracted_merged_df.columns
            else 0,
        },
        "outputs": {
            "jobs_cleaned_csv": str(output_paths["jobs_cleaned"]),
            "jobs_cs_filtered_csv": str(output_paths["jobs_cs_filtered"]),
            "job_cs_filter_audit_csv": str(output_paths["job_cs_filter_audit"]),
            "jobs_dedup_result_csv": str(output_paths["jobs_dedup_result"]),
            "job_name_mapping_csv": str(output_paths["job_name_mapping"]),
            "job_dedup_pairs_csv": str(output_paths["job_dedup_pairs"]),
            "jobs_extracted_full_csv": str(output_paths["jobs_extracted_full"]),
            "job_knowledge_json": str(knowledge_json_path),
            "knowledge_output_dir": str(resolved_knowledge_output_dir),
            "sqlite_db": str(resolved_sql_db_path),
            "neo4j_output_dir": str(resolved_neo4j_output_dir),
        },
        "knowledge_summary": {
            "document_count": int(len(knowledge_records)),
            "embedding_model": embedding_summary.get("model_name"),
            "embedding_encoder_type": embedding_summary.get("encoder_type"),
            "embedding_dimension": embedding_summary.get("dimension"),
        },
        "filter_summary": filter_stats,
        "graph_summary": summarize_graph_tables(graph_tables),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="统一执行岗位底库数据处理流水线")
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT_FILE,
        help="原始岗位 Excel 文件路径",
    )
    parser.add_argument(
        "--intermediate-dir",
        default=DEFAULT_INTERMEDIATE_DIR,
        help="中间产物输出目录",
    )
    parser.add_argument(
        "--db-path",
        default=DEFAULT_SQL_DB_PATH,
        help="SQLite 输出路径",
    )
    parser.add_argument(
        "--neo4j-output-dir",
        default=DEFAULT_NEO4J_OUTPUT_DIR,
        help="Neo4j CSV 输出目录",
    )
    parser.add_argument(
        "--knowledge-output-dir",
        default=DEFAULT_KNOWLEDGE_OUTPUT_DIR,
        help="岗位知识 JSON 与 embedding 输出目录",
    )
    parser.add_argument(
        "--sheet-name",
        default=0,
        help="Excel sheet 名称或索引",
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=50,
        help="岗位抽取阶段的进度日志间隔",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_EXTRACT_MAX_WORKERS,
        help="岗位抽取阶段的并发线程数；超时频繁时建议保持 1",
    )
    parser.add_argument(
        "--group-sample-size",
        type=int,
        default=DEFAULT_GROUP_SAMPLE_SIZE,
        help="岗位抽取阶段每组抽样的 JD 数量",
    )
    parser.add_argument(
        "--embedding-model",
        default=DEFAULT_HASH_EMBEDDING_MODEL,
        help="本地 embedding 模型名；若环境缺依赖则自动回退到内置哈希向量方案",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_job_data_pipeline(
        input_path=args.input,
        intermediate_dir=args.intermediate_dir,
        sql_db_path=args.db_path,
        neo4j_output_dir=args.neo4j_output_dir,
        knowledge_output_dir=args.knowledge_output_dir,
        sheet_name=args.sheet_name,
        log_every=args.log_every,
        max_workers=args.max_workers,
        group_sample_size=args.group_sample_size,
        embedding_model_name=args.embedding_model,
    )
    print("Job data pipeline finished.")
    for key, value in result.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
