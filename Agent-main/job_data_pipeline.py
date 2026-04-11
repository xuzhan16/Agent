"""
job_data_pipeline.py

统一封装岗位底库的数据处理流水线：
1. 原始 Excel 清洗
2. 岗位去重 / 标准岗位归一
3. 岗位画像与路径关系抽取
4. 导出 SQLite
5. 导出 Neo4j CSV

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
from job_data.export_to_neo4j import process_export_to_neo4j
from job_data.export_to_sql import process_export_to_sql
from job_data.job_dedup import process_job_dedup
from job_data.job_extract import DEFAULT_GROUP_SAMPLE_SIZE, process_job_extract


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_INPUT_FILE = "20260226105856_457.xls"
DEFAULT_INTERMEDIATE_DIR = "outputs/intermediate"
DEFAULT_SQL_DB_PATH = "outputs/sql/jobs.db"
DEFAULT_NEO4J_OUTPUT_DIR = "outputs/neo4j"


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


def run_job_data_pipeline(
    input_path: str | Path,
    intermediate_dir: str | Path = DEFAULT_INTERMEDIATE_DIR,
    sql_db_path: str | Path = DEFAULT_SQL_DB_PATH,
    neo4j_output_dir: str | Path = DEFAULT_NEO4J_OUTPUT_DIR,
    sheet_name: str | int = 0,
    log_every: int = 50,
    max_workers: int = 4,
    group_sample_size: int = DEFAULT_GROUP_SAMPLE_SIZE,
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

    cleaned_df = process_job_excel(
        input_excel_path=str(resolved_input_path),
        output_csv_path=str(output_paths["jobs_cleaned"]),
        sheet_name=sheet_name,
    )

    dedup_df, mapping_df, pair_results_df = process_job_dedup(
        df=cleaned_df,
        output_data_csv=str(output_paths["jobs_dedup_result"]),
        output_mapping_csv=str(output_paths["job_name_mapping"]),
        output_pair_csv=str(output_paths["job_dedup_pairs"]),
    )

    extracted_merged_df, extracted_df = process_job_extract(
        df=dedup_df,
        output_csv_path=str(output_paths["jobs_extracted_full"]),
        log_every=log_every,
        max_workers=max_workers,
        group_sample_size=group_sample_size,
    )

    process_export_to_sql(
        df=extracted_merged_df,
        db_path=str(resolved_sql_db_path),
    )

    graph_tables = process_export_to_neo4j(
        df=extracted_merged_df,
        output_dir=str(resolved_neo4j_output_dir),
    )

    return {
        "input_file": str(resolved_input_path),
        "sheet_name": sheet_name,
        "rows": {
            "cleaned_rows": int(len(cleaned_df)),
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
            "jobs_dedup_result_csv": str(output_paths["jobs_dedup_result"]),
            "job_name_mapping_csv": str(output_paths["job_name_mapping"]),
            "job_dedup_pairs_csv": str(output_paths["job_dedup_pairs"]),
            "jobs_extracted_full_csv": str(output_paths["jobs_extracted_full"]),
            "sqlite_db": str(resolved_sql_db_path),
            "neo4j_output_dir": str(resolved_neo4j_output_dir),
        },
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
        default=4,
        help="岗位抽取阶段的并发线程数",
    )
    parser.add_argument(
        "--group-sample-size",
        type=int,
        default=DEFAULT_GROUP_SAMPLE_SIZE,
        help="岗位抽取阶段每组抽样的 JD 数量",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_job_data_pipeline(
        input_path=args.input,
        intermediate_dir=args.intermediate_dir,
        sql_db_path=args.db_path,
        neo4j_output_dir=args.neo4j_output_dir,
        sheet_name=args.sheet_name,
        log_every=args.log_every,
        max_workers=args.max_workers,
        group_sample_size=args.group_sample_size,
    )
    print("Job data pipeline finished.")
    for key, value in result.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
