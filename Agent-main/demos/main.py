"""
main.py - 项目主流程调度中心

该文件是整个数据处理流水线的入口点，负责协调和串联 ``job_data`` 包内各子模块：
1. data_cleaning: 原始 Excel 格式化、空值处理与基础字段提取。
2. job_dedup: 岗位名称归一化与重复记录识别（规则 + 语义）。
3. job_extract: 调用 LLM 从岗位描述抽取结构化画像。
4. export_to_neo4j / export_to_sql: 导出图谱 CSV 与 SQLite。

建议在项目根目录执行：``python demos/main.py`` 或 ``python -m demos.main``。

主要工作流顺序：
[输入 Excel] -> 基础清洗 -> 归一化与去重 -> 结构化画像抽取 -> 导出 Neo4j/SQLite -> [最终输出]
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from job_data.data_cleaning import process_job_excel
from job_data.export_to_neo4j import process_export_to_neo4j
from job_data.export_to_sql import process_export_to_sql
from job_data.job_dedup import process_job_dedup
from job_data.job_extract import process_job_extract


def log(message: str) -> None:
    """
    统一的日志输出格式，带时间戳。
    
    Args:
        message: 需要记录的日志信息。
    """
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time}] {message}")


@dataclass
class PipelinePaths:
    """
    路径管理配置类，集中管理项目中所有的输入、输出和中间文件路径。
    使用 dataclass 确保路径配置的结构化和易用性。
    """

    input_excel: Path             # 输入的原始岗位 Excel 文件路径
    output_root: Path             # 输出根目录
    intermediate_dir: Path        # 中间过程文件存放目录
    neo4j_dir: Path               # Neo4j 导出文件存放目录
    sql_dir: Path                 # SQL 导出文件存放目录
    cleaned_csv: Path             # 第一步：清洗后的数据路径
    dedup_result_csv: Path        # 第二步：去重后的数据路径
    job_mapping_csv: Path         # 第二步：岗位名称映射关系表路径
    dedup_pairs_csv: Path         # 第二步：去重判断明细表路径
    extracted_csv: Path           # 第三步：带完整画像的明细数据路径
    extracted_profile_csv: Path   # 第三步：纯岗位画像数据路径
    sqlite_db_path: Path          # 第五步：生成的 SQLite 数据库路径

    @classmethod
    def from_args(
        cls,
        input_excel: str,
        output_dir: str,
        sqlite_db_name: str,
    ) -> "PipelinePaths":
        """
        根据命令行参数或配置初始化路径对象。
        """
        output_root = Path(output_dir)
        intermediate_dir = output_root / "intermediate"
        neo4j_dir = output_root / "neo4j"
        sql_dir = output_root / "sql"

        return cls(
            input_excel=Path(input_excel),
            output_root=output_root,
            intermediate_dir=intermediate_dir,
            neo4j_dir=neo4j_dir,
            sql_dir=sql_dir,
            cleaned_csv=intermediate_dir / "jobs_cleaned.csv",
            dedup_result_csv=intermediate_dir / "jobs_dedup_result.csv",
            job_mapping_csv=intermediate_dir / "job_name_mapping.csv",
            dedup_pairs_csv=intermediate_dir / "job_dedup_pairs.csv",
            extracted_csv=intermediate_dir / "jobs_extracted_full.csv",
            extracted_profile_csv=intermediate_dir / "job_profile_extracted.csv",
            sqlite_db_path=sql_dir / sqlite_db_name,
        )

    def ensure_dirs(self) -> None:
        """
        创建所有必要的输出和中间目录，防止文件保存时报错。
        """
        self.output_root.mkdir(parents=True, exist_ok=True)
        self.intermediate_dir.mkdir(parents=True, exist_ok=True)
        self.neo4j_dir.mkdir(parents=True, exist_ok=True)
        self.sql_dir.mkdir(parents=True, exist_ok=True)


def save_dataframe(df: pd.DataFrame, output_path: Path, description: str) -> None:
    """
    通用的 DataFrame 保存辅助函数，支持自动创建父目录并带 UTF-8 BOM 以支持 Excel 直接打开。
    
    Args:
        df: 要保存的 DataFrame。
        output_path: 保存路径。
        description: 日志描述文字。
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    log(f"{description} saved: {output_path}")


def print_pipeline_config(paths: PipelinePaths, sheet_name: int | str, log_every: int, extract_max_workers: int) -> None:
    """
    在流水线启动前打印当前的所有关键配置，方便排查问题。
    """
    llm_api_base_url = os.getenv("LLM_API_BASE_URL", "").strip() or os.getenv("LLM_BASE_URL", "").strip()
    llm_model = os.getenv("LLM_MODEL_NAME", "").strip() or os.getenv("LLM_MODEL", "").strip()
    log("Pipeline configuration:")
    log(f"  input_excel: {paths.input_excel}")
    log(f"  output_root: {paths.output_root}")
    log(f"  sheet_name: {sheet_name}")
    log(f"  llm_api_base_url: {llm_api_base_url}")
    log(f"  llm_model: {llm_model}")
    log(f"  log_every: {log_every}")
    log(f"  extract_max_workers: {extract_max_workers}")


def run_pipeline(
    input_excel: str,
    output_dir: str,
    sheet_name: int | str = 0,
    sqlite_db_name: str = "jobs.db",
    dedup_threshold: float = 0.60,
    log_every: int = 50,
    extract_max_workers: int = 4,
) -> None:
    """
    执行完整的数据处理流水线。
    
    Args:
        input_excel: 输入文件。
        output_dir: 输出根目录。
        sheet_name: Excel 的 Sheet 索引或名称。
        sqlite_db_name: 生成的数据库文件名。
        dedup_threshold: 语义去重的置信度阈值。
        log_every: 每处理多少条/多少个唯一抽取任务输出一次进度日志。
        extract_max_workers: 岗位画像抽取阶段并发调用大模型的线程数。
    """
    paths = PipelinePaths.from_args(
        input_excel=input_excel,
        output_dir=output_dir,
        sqlite_db_name=sqlite_db_name,
    )
    paths.ensure_dirs()

    if not paths.input_excel.exists():
        raise FileNotFoundError(f"Input Excel file not found: {paths.input_excel}")

    print_pipeline_config(paths, sheet_name=sheet_name, log_every=log_every, extract_max_workers=extract_max_workers)

    # Step 1: 基础清洗 - 处理 Excel 格式、统一列名、解析薪资等
    log("Step 1/5: Start data cleaning.")
    clean_df = process_job_excel(
        input_excel_path=str(paths.input_excel),
        output_csv_path=str(paths.cleaned_csv),
        sheet_name=sheet_name,
    )
    log(f"Step 1/5: Data cleaning finished. Rows after cleaning: {len(clean_df)}")

    # Step 2: 岗位归一与去重 - 识别语义相同的岗位并打标
    log("Step 2/5: Start job deduplication and normalization.")
    dedup_df, mapping_df, pair_df = process_job_dedup(
        df=clean_df,
        output_data_csv=str(paths.dedup_result_csv),
        output_mapping_csv=str(paths.job_mapping_csv),
        output_pair_csv=str(paths.dedup_pairs_csv),
        positive_confidence_threshold=dedup_threshold,
    )
    log(f"Step 2/5: Job dedup finished. Rows after dedup: {len(dedup_df)}")
    log(f"Step 2/5: Job mapping rows: {len(mapping_df)}")
    log(f"Step 2/5: Candidate pair judgement rows: {len(pair_df)}")

    # Step 3: 岗位画像抽取 - 调用 LLM 提取技能、学历、证书等详细信息
    log("Step 3/5: Start job profile extraction.")
    extracted_df, extracted_profile_df = process_job_extract(
        df=dedup_df,
        output_csv_path=str(paths.extracted_csv),
        log_every=log_every,
        max_workers=extract_max_workers,
    )
    save_dataframe(
        extracted_profile_df,
        paths.extracted_profile_csv,
        description="Job profile extraction result",
    )
    success_count = 0
    if "extract_success" in extracted_df.columns:
        success_count = int(extracted_df["extract_success"].fillna(False).sum())
    log(
        "Step 3/5: Job profile extraction finished. "
        f"Rows: {len(extracted_df)}, success rows: {success_count}"
    )

    # Step 4: 导出 Neo4j CSV - 生成节点和关系文件，用于构建知识图谱
    log("Step 4/5: Start exporting Neo4j CSV files.")
    graph_tables = process_export_to_neo4j(extracted_df, str(paths.neo4j_dir))
    log(
        "Step 4/5: Neo4j export finished. "
        f"Export tables: {', '.join(sorted(graph_tables.keys()))}"
    )

    # Step 5: 导出 SQLite 数据库 - 供 Web 应用快速查询
    log("Step 5/5: Start exporting SQLite database.")
    process_export_to_sql(extracted_df, str(paths.sqlite_db_path))
    log(f"Step 5/5: SQLite export finished. Database: {paths.sqlite_db_path}")

    log("Pipeline finished successfully.")
    log(f"Cleaned CSV: {paths.cleaned_csv}")
    log(f"Dedup Result CSV: {paths.dedup_result_csv}")
    log(f"Job Mapping CSV: {paths.job_mapping_csv}")
    log(f"Dedup Pairs CSV: {paths.dedup_pairs_csv}")
    log(f"Extracted Full CSV: {paths.extracted_csv}")
    log(f"Extracted Profile CSV: {paths.extracted_profile_csv}")
    log(f"Neo4j Dir: {paths.neo4j_dir}")
    log(f"SQLite DB: {paths.sqlite_db_path}")


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description="岗位数据处理主流程")
    parser.add_argument(
        "--input",
        default="20260226105856_457.xls",
        help="输入原始 Excel 文件路径",
    )
    parser.add_argument(
        "--sheet-name",
        default=0,
        help="Excel 的 sheet 名称或索引",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="输出目录",
    )
    parser.add_argument(
        "--sqlite-db-name",
        default="jobs.db",
        help="SQLite 数据库文件名",
    )
    parser.add_argument(
        "--dedup-threshold",
        type=float,
        default=0.60,
        help="岗位归一时的合并置信度阈值",
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=50,
        help="岗位画像抽取时每处理多少个唯一任务打印一次日志",
    )
    parser.add_argument(
        "--extract-max-workers",
        type=int,
        default=4,
        help="岗位画像抽取阶段并发调用大模型的线程数；如果接口限流明显可调小",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_pipeline(
        input_excel=args.input,
        output_dir=args.output_dir,
        sheet_name=args.sheet_name,
        sqlite_db_name=args.sqlite_db_name,
        dedup_threshold=args.dedup_threshold,
        log_every=args.log_every,
        extract_max_workers=args.extract_max_workers,
    )





