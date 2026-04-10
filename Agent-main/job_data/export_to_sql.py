"""
export_to_sql.py

导出处理后的岗位数据到 SQLite。

表设计：
1. job_detail
   存岗位明细，包含原始字段、清洗字段、归一字段等。
2. job_profile
   存岗位画像抽取结果。
3. job_mapping
   存原始岗位名与标准岗位名映射。
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd


def clean_text(value: object) -> str:
    """基础文本清洗。"""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null"}:
        return ""
    return text


def json_dumps_safe(value: object) -> str:
    """
    将 list/dict 安全转为 JSON 字符串。
    其他标量值保持为字符串。
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)

    text = clean_text(value)
    if not text:
        return ""

    # 已经是 JSON 风格的字符串则原样保留
    if (text.startswith("[") and text.endswith("]")) or (text.startswith("{") and text.endswith("}")):
        return text
    return text


def normalize_scalar(value: object) -> str:
    """统一把值转成可写入 SQLite 的字符串。"""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    return str(value)


def get_first_existing_value(record: pd.Series, candidates: Sequence[str]) -> object:
    """从多个候选字段里取第一个存在且非空的值。"""
    for field in candidates:
        if field in record.index:
            value = record.get(field, "")
            if clean_text(value):
                return value
    return ""


def ensure_columns(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    """确保 DataFrame 拥有指定列，缺失则补空。"""
    result = df.copy()
    for col in columns:
        if col not in result.columns:
            result[col] = ""
    return result


def load_table(input_path: str) -> pd.DataFrame:
    """支持 CSV / Excel 输入。"""
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, dtype=str).fillna("")
    return pd.read_csv(path, dtype=str).fillna("")


def create_connection(db_path: str) -> sqlite3.Connection:
    """创建 SQLite 连接。"""
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(str(db_file))


def create_tables(conn: sqlite3.Connection) -> None:
    """自动建表。"""
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS job_detail (
            record_id TEXT PRIMARY KEY,
            source_row_no TEXT,
            job_code TEXT,
            job_url TEXT,
            job_name_raw TEXT,
            job_name_clean TEXT,
            normalized_job_name TEXT,
            standard_job_name TEXT,
            is_same_standard_job TEXT,
            mapping_confidence TEXT,
            mapping_merge_reason TEXT,
            company_name_raw TEXT,
            company_name_clean TEXT,
            company_type TEXT,
            company_type_norm TEXT,
            company_size TEXT,
            company_size_norm TEXT,
            industry TEXT,
            city TEXT,
            district TEXT,
            job_address_raw TEXT,
            job_address_norm TEXT,
            salary_raw TEXT,
            salary_range_clean TEXT,
            salary_min TEXT,
            salary_max TEXT,
            salary_unit TEXT,
            salary_month_min TEXT,
            salary_month_max TEXT,
            updated_at_raw TEXT,
            updated_at_std TEXT,
            job_desc_raw TEXT,
            job_desc_clean TEXT,
            company_desc_raw TEXT,
            company_desc_clean TEXT,
            is_abnormal TEXT,
            abnormal_reasons TEXT,
            raw_payload_json TEXT
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS job_profile (
            record_id TEXT PRIMARY KEY,
            standard_job_name TEXT,
            job_category TEXT,
            degree_requirement TEXT,
            major_requirement TEXT,
            experience_requirement TEXT,
            hard_skills_json TEXT,
            tools_or_tech_stack_json TEXT,
            certificate_requirement_json TEXT,
            soft_skills_json TEXT,
            practice_requirement TEXT,
            job_level TEXT,
            suitable_student_profile TEXT,
            raw_requirement_summary TEXT,
            extract_success TEXT,
            extract_error TEXT,
            job_extract_json TEXT
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS job_mapping (
            mapping_id TEXT PRIMARY KEY,
            raw_job_name TEXT,
            normalized_job_name TEXT,
            standard_job_name TEXT,
            is_same_standard_job TEXT,
            confidence TEXT,
            merge_reason TEXT,
            occurrence_count TEXT,
            cluster_size TEXT,
            job_family TEXT
        )
        """
    )

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_job_detail_standard_job_name ON job_detail(standard_job_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_job_detail_company_name ON job_detail(company_name_clean)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_job_detail_city ON job_detail(city)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_job_profile_standard_job_name ON job_profile(standard_job_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_job_mapping_standard_job_name ON job_mapping(standard_job_name)")
    conn.commit()


def clear_tables(conn: sqlite3.Connection, table_names: Sequence[str]) -> None:
    """清空旧数据，便于重新导入。"""
    cursor = conn.cursor()
    for table_name in table_names:
        cursor.execute(f"DELETE FROM {table_name}")
    conn.commit()


def build_job_detail_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """从处理后的总表构建 job_detail DataFrame。"""
    working_df = df.copy()

    rows: List[Dict[str, Any]] = []
    for _, row in working_df.iterrows():
        record_id = clean_text(
            get_first_existing_value(row, ["record_id"])
        ) or clean_text(get_first_existing_value(row, ["job_code", "job_url", "job_title", "job_name"]))

        row_dict = row.to_dict()
        rows.append(
            {
                "record_id": record_id,
                "source_row_no": normalize_scalar(get_first_existing_value(row, ["source_row_no"])),
                "job_code": normalize_scalar(get_first_existing_value(row, ["job_code"])),
                "job_url": normalize_scalar(get_first_existing_value(row, ["job_url"])),
                "job_name_raw": normalize_scalar(get_first_existing_value(row, ["job_title_raw", "job_name_raw", "job_title", "job_name"])),
                "job_name_clean": normalize_scalar(get_first_existing_value(row, ["job_title", "job_name", "job_title_norm"])),
                "normalized_job_name": normalize_scalar(get_first_existing_value(row, ["normalized_job_name", "job_title_norm", "job_title_rule_normalized"])),
                "standard_job_name": normalize_scalar(get_first_existing_value(row, ["standard_job_name", "normalized_job_title"])),
                "is_same_standard_job": normalize_scalar(get_first_existing_value(row, ["is_same_standard_job"])),
                "mapping_confidence": normalize_scalar(get_first_existing_value(row, ["confidence", "title_normalize_confidence"])),
                "mapping_merge_reason": normalize_scalar(get_first_existing_value(row, ["merge_reason"])),
                "company_name_raw": normalize_scalar(get_first_existing_value(row, ["company_name_raw", "company_name"])),
                "company_name_clean": normalize_scalar(get_first_existing_value(row, ["company_name", "company_name_norm"])),
                "company_type": normalize_scalar(get_first_existing_value(row, ["company_type"])),
                "company_type_norm": normalize_scalar(get_first_existing_value(row, ["company_type_norm"])),
                "company_size": normalize_scalar(get_first_existing_value(row, ["company_size"])),
                "company_size_norm": normalize_scalar(get_first_existing_value(row, ["company_size_norm"])),
                "industry": normalize_scalar(get_first_existing_value(row, ["industry"])),
                "city": normalize_scalar(get_first_existing_value(row, ["city"])),
                "district": normalize_scalar(get_first_existing_value(row, ["district"])),
                "job_address_raw": normalize_scalar(get_first_existing_value(row, ["job_address_raw", "job_address"])),
                "job_address_norm": normalize_scalar(get_first_existing_value(row, ["job_address_norm"])),
                "salary_raw": normalize_scalar(get_first_existing_value(row, ["salary_raw", "salary_range_raw", "salary_range"])),
                "salary_range_clean": normalize_scalar(get_first_existing_value(row, ["salary_range_clean"])),
                "salary_min": normalize_scalar(get_first_existing_value(row, ["salary_min"])),
                "salary_max": normalize_scalar(get_first_existing_value(row, ["salary_max"])),
                "salary_unit": normalize_scalar(get_first_existing_value(row, ["salary_unit"])),
                "salary_month_min": normalize_scalar(get_first_existing_value(row, ["salary_month_min", "monthly_salary_min"])),
                "salary_month_max": normalize_scalar(get_first_existing_value(row, ["salary_month_max", "monthly_salary_max"])),
                "updated_at_raw": normalize_scalar(get_first_existing_value(row, ["updated_at_raw", "updated_at"])),
                "updated_at_std": normalize_scalar(get_first_existing_value(row, ["updated_at_std", "updated_at_norm"])),
                "job_desc_raw": normalize_scalar(get_first_existing_value(row, ["job_description_raw", "job_desc_raw", "job_description", "job_desc"])),
                "job_desc_clean": normalize_scalar(get_first_existing_value(row, ["job_description_clean", "job_desc", "job_description_text"])),
                "company_desc_raw": normalize_scalar(get_first_existing_value(row, ["company_description_raw", "company_desc_raw", "company_description", "company_desc"])),
                "company_desc_clean": normalize_scalar(get_first_existing_value(row, ["company_description_clean", "company_desc", "company_description_text"])),
                "is_abnormal": normalize_scalar(get_first_existing_value(row, ["is_abnormal"])),
                "abnormal_reasons": normalize_scalar(get_first_existing_value(row, ["abnormal_reasons"])),
                "raw_payload_json": json.dumps(row_dict, ensure_ascii=False),
            }
        )

    return pd.DataFrame(rows).drop_duplicates(subset=["record_id"])


def build_job_profile_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """从处理后的总表构建 job_profile DataFrame。"""
    rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        record_id = clean_text(
            get_first_existing_value(row, ["record_id"])
        ) or clean_text(get_first_existing_value(row, ["job_code", "job_url", "job_title", "job_name"]))

        rows.append(
            {
                "record_id": record_id,
                "standard_job_name": normalize_scalar(get_first_existing_value(row, ["standard_job_name", "normalized_job_title"])),
                "job_category": normalize_scalar(get_first_existing_value(row, ["job_category", "job_family", "job_family_extracted"])),
                "degree_requirement": normalize_scalar(get_first_existing_value(row, ["degree_requirement", "education_requirement"])),
                "major_requirement": normalize_scalar(get_first_existing_value(row, ["major_requirement"])),
                "experience_requirement": normalize_scalar(get_first_existing_value(row, ["experience_requirement"])),
                "hard_skills_json": json_dumps_safe(get_first_existing_value(row, ["hard_skills", "skills_json"])),
                "tools_or_tech_stack_json": json_dumps_safe(get_first_existing_value(row, ["tools_or_tech_stack"])),
                "certificate_requirement_json": json_dumps_safe(get_first_existing_value(row, ["certificate_requirement", "certificates_json"])),
                "soft_skills_json": json_dumps_safe(get_first_existing_value(row, ["soft_skills", "soft_skills_json"])),
                "practice_requirement": normalize_scalar(get_first_existing_value(row, ["practice_requirement"])),
                "job_level": normalize_scalar(get_first_existing_value(row, ["job_level"])),
                "suitable_student_profile": normalize_scalar(get_first_existing_value(row, ["suitable_student_profile"])),
                "raw_requirement_summary": normalize_scalar(get_first_existing_value(row, ["raw_requirement_summary", "job_summary"])),
                "extract_success": normalize_scalar(get_first_existing_value(row, ["extract_success"])),
                "extract_error": normalize_scalar(get_first_existing_value(row, ["extract_error"])),
                "job_extract_json": json_dumps_safe(get_first_existing_value(row, ["job_extract_json", "portrait_json"])),
            }
        )

    return pd.DataFrame(rows).drop_duplicates(subset=["record_id"])


def build_job_mapping_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """从处理后的总表构建 job_mapping DataFrame。"""
    rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        raw_job_name = clean_text(get_first_existing_value(row, ["job_title", "job_name", "job_title_raw"]))
        normalized_job_name = clean_text(get_first_existing_value(row, ["normalized_job_name", "job_title_norm", "job_title_rule_normalized"]))
        standard_job_name = clean_text(get_first_existing_value(row, ["standard_job_name", "normalized_job_title"]))

        mapping_key = "|".join([raw_job_name, normalized_job_name, standard_job_name])
        rows.append(
            {
                "mapping_id": mapping_key,
                "raw_job_name": raw_job_name,
                "normalized_job_name": normalized_job_name,
                "standard_job_name": standard_job_name,
                "is_same_standard_job": normalize_scalar(get_first_existing_value(row, ["is_same_standard_job"])),
                "confidence": normalize_scalar(get_first_existing_value(row, ["confidence", "title_normalize_confidence"])),
                "merge_reason": normalize_scalar(get_first_existing_value(row, ["merge_reason"])),
                "occurrence_count": normalize_scalar(get_first_existing_value(row, ["occurrence_count"])),
                "cluster_size": normalize_scalar(get_first_existing_value(row, ["cluster_size"])),
                "job_family": normalize_scalar(get_first_existing_value(row, ["job_family", "job_category"])),
            }
        )

    mapping_df = pd.DataFrame(rows).drop_duplicates(subset=["mapping_id"])
    return mapping_df[mapping_df["raw_job_name"].astype(str).str.strip() != ""].copy()


def dataframe_to_records(df: pd.DataFrame, columns: Sequence[str]) -> List[Tuple[Any, ...]]:
    """将 DataFrame 转为 executemany 可用的记录列表。"""
    prepared_df = ensure_columns(df, columns)
    records: List[Tuple[Any, ...]] = []
    for _, row in prepared_df.iterrows():
        records.append(tuple(row[col] for col in columns))
    return records


def insert_dataframe(
    conn: sqlite3.Connection,
    table_name: str,
    df: pd.DataFrame,
    columns: Sequence[str],
) -> None:
    """将 DataFrame 批量写入 SQLite。"""
    records = dataframe_to_records(df, columns)
    if not records:
        print(f"[sql] Skip empty table: {table_name}")
        return

    placeholders = ", ".join(["?"] * len(columns))
    column_sql = ", ".join(columns)
    sql = f"INSERT OR REPLACE INTO {table_name} ({column_sql}) VALUES ({placeholders})"

    conn.executemany(sql, records)
    conn.commit()
    print(f"[sql] Inserted {len(records)} rows into {table_name}")


def export_to_sqlite(df: pd.DataFrame, db_path: str) -> None:
    """
    主流程函数：
    - 建表
    - 构建三张表的数据
    - 批量写入 SQLite
    """
    conn = create_connection(db_path)
    create_tables(conn)
    clear_tables(conn, ["job_detail", "job_profile", "job_mapping"])

    job_detail_df = build_job_detail_dataframe(df)
    job_profile_df = build_job_profile_dataframe(df)
    job_mapping_df = build_job_mapping_dataframe(df)

    job_detail_columns = [
        "record_id",
        "source_row_no",
        "job_code",
        "job_url",
        "job_name_raw",
        "job_name_clean",
        "normalized_job_name",
        "standard_job_name",
        "is_same_standard_job",
        "mapping_confidence",
        "mapping_merge_reason",
        "company_name_raw",
        "company_name_clean",
        "company_type",
        "company_type_norm",
        "company_size",
        "company_size_norm",
        "industry",
        "city",
        "district",
        "job_address_raw",
        "job_address_norm",
        "salary_raw",
        "salary_range_clean",
        "salary_min",
        "salary_max",
        "salary_unit",
        "salary_month_min",
        "salary_month_max",
        "updated_at_raw",
        "updated_at_std",
        "job_desc_raw",
        "job_desc_clean",
        "company_desc_raw",
        "company_desc_clean",
        "is_abnormal",
        "abnormal_reasons",
        "raw_payload_json",
    ]

    job_profile_columns = [
        "record_id",
        "standard_job_name",
        "job_category",
        "degree_requirement",
        "major_requirement",
        "experience_requirement",
        "hard_skills_json",
        "tools_or_tech_stack_json",
        "certificate_requirement_json",
        "soft_skills_json",
        "practice_requirement",
        "job_level",
        "suitable_student_profile",
        "raw_requirement_summary",
        "extract_success",
        "extract_error",
        "job_extract_json",
    ]

    job_mapping_columns = [
        "mapping_id",
        "raw_job_name",
        "normalized_job_name",
        "standard_job_name",
        "is_same_standard_job",
        "confidence",
        "merge_reason",
        "occurrence_count",
        "cluster_size",
        "job_family",
    ]

    insert_dataframe(conn, "job_detail", job_detail_df, job_detail_columns)
    insert_dataframe(conn, "job_profile", job_profile_df, job_profile_columns)
    insert_dataframe(conn, "job_mapping", job_mapping_df, job_mapping_columns)

    conn.close()
    print(f"[sql] SQLite export finished: {db_path}")


def build_demo_dataframe() -> pd.DataFrame:
    """
    插入示例：
    提供一个可直接运行测试的小样本 DataFrame。
    """
    return pd.DataFrame(
        [
            {
                "record_id": "r001",
                "source_row_no": "1",
                "job_code": "J001",
                "job_url": "https://example.com/job/1",
                "job_title": "Java开发工程师",
                "job_title_norm": "java开发工程师",
                "normalized_job_name": "java开发工程师",
                "standard_job_name": "Java开发工程师",
                "is_same_standard_job": "1",
                "confidence": "0.92",
                "merge_reason": "llm_same_normalized_title",
                "company_name": "某科技公司",
                "company_name_norm": "某科技公司",
                "company_type": "民营",
                "company_type_norm": "民营",
                "company_size": "100-499人",
                "company_size_norm": "100-499人",
                "industry": "计算机软件",
                "city": "深圳",
                "district": "南山区",
                "job_address": "深圳-南山区",
                "job_address_norm": "深圳-南山区",
                "salary_range_raw": "15-25K",
                "salary_range_clean": "15-25K",
                "salary_min": "15000",
                "salary_max": "25000",
                "salary_unit": "month",
                "salary_month_min": "15000",
                "salary_month_max": "25000",
                "updated_at": "2026-04-02",
                "updated_at_std": "2026-04-02",
                "job_description": "负责 Java 后端系统开发",
                "job_description_clean": "负责 Java 后端系统开发",
                "company_description": "一家教育信息化科技公司",
                "company_description_clean": "一家教育信息化科技公司",
                "is_abnormal": "0",
                "abnormal_reasons": "",
                "job_category": "Java开发",
                "degree_requirement": "本科及以上",
                "major_requirement": "计算机相关专业",
                "experience_requirement": "1年以上",
                "hard_skills": ["Java", "Spring Boot", "MySQL"],
                "tools_or_tech_stack": ["Java", "Spring Boot", "MySQL", "Redis"],
                "certificate_requirement": [],
                "soft_skills": ["沟通能力", "团队协作"],
                "practice_requirement": "有项目经验优先",
                "job_level": "普通",
                "suitable_student_profile": "适合计算机相关专业、有后端开发基础的学生",
                "raw_requirement_summary": "要求掌握 Java、Spring Boot、MySQL。",
                "extract_success": "1",
                "extract_error": "",
                "job_extract_json": json.dumps(
                    {
                        "standard_job_name": "Java开发工程师",
                        "job_category": "Java开发",
                        "degree_requirement": "本科及以上",
                    },
                    ensure_ascii=False,
                ),
            }
        ]
    )


def process_export_to_sql(
    df: pd.DataFrame,
    db_path: str,
) -> None:
    """对外主流程封装。"""
    export_to_sqlite(df, db_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="导出处理后的岗位数据到 SQLite")
    parser.add_argument(
        "--input",
        default="outputs/jobs_extracted.csv",
        help="输入处理后的岗位数据文件路径，支持 CSV / Excel",
    )
    parser.add_argument(
        "--db-path",
        default="outputs/sql/jobs.db",
        help="输出 SQLite 数据库文件路径",
    )
    parser.add_argument(
        "--use-demo",
        action="store_true",
        help="使用内置 demo 数据进行插入测试",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.use_demo:
        print("[sql] Using demo dataframe for test insert.")
        df = build_demo_dataframe()
    else:
        print(f"[sql] Loading input file: {args.input}")
        df = load_table(args.input)

    process_export_to_sql(df=df, db_path=args.db_path)
    print(f"[sql] Input rows: {len(df)}")
    print(f"[sql] Database file: {args.db_path}")


if __name__ == "__main__":
    main()
