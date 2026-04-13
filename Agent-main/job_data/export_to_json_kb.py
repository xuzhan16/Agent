"""
export_to_json_kb.py

将岗位预处理结果导出为可交付的岗位知识 JSON 文档库。

设计目标：
1. 以标准岗位为粒度生成知识文档；
2. 复用 job_extract 后的结构化字段；
3. 为后续本地 embedding 和语义检索提供稳定输入。
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from job_data.job_extract import clean_text, normalize_list_value, normalize_path_relation_details


DEFAULT_OUTPUT_PATH = Path("outputs/knowledge/job_knowledge.jsonl")


def normalize_dict_list(value: Any) -> List[Dict[str, Any]]:
    """统一 dict 列表格式。"""
    items = normalize_path_relation_details(value)
    if items:
        return items
    if isinstance(value, list):
        return [dict(item) for item in value if isinstance(item, dict)]
    text = clean_text(value)
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [dict(item) for item in parsed if isinstance(item, dict)]
        except json.JSONDecodeError:
            return []
    return []


def collect_aliases(group_df: pd.DataFrame) -> List[str]:
    """从岗位组中收集岗位别名。"""
    aliases: List[str] = []
    seen = set()
    for column_name in ["job_name", "job_title", "job_title_norm", "normalized_job_title", "standard_job_name"]:
        if column_name not in group_df.columns:
            continue
        for raw_value in group_df[column_name].tolist():
            alias = clean_text(raw_value)
            if not alias or alias in seen:
                continue
            seen.add(alias)
            aliases.append(alias)
    return aliases


def build_doc_id(standard_job_name: str) -> str:
    normalized = clean_text(standard_job_name)
    digest = hashlib.md5(normalized.encode("utf-8")).hexdigest()[:12]
    return f"job_{digest}"


def build_doc_text(record: Dict[str, Any]) -> str:
    """将结构化字段拼接成适合 embedding 的自然语言知识文本。"""
    job_name = clean_text(record.get("standard_job_name"))
    job_category = clean_text(record.get("job_category"))
    job_level = clean_text(record.get("job_level"))
    degree_requirement = clean_text(record.get("degree_requirement"))
    major_requirement = clean_text(record.get("major_requirement"))
    experience_requirement = clean_text(record.get("experience_requirement"))
    hard_skills = normalize_list_value(record.get("hard_skills"))
    tools_or_tech_stack = normalize_list_value(record.get("tools_or_tech_stack"))
    soft_skills = normalize_list_value(record.get("soft_skills"))
    certificates = normalize_list_value(record.get("certificate_requirement"))
    practice_requirement = clean_text(record.get("practice_requirement"))
    requirement_summary = clean_text(record.get("raw_requirement_summary"))
    vertical_paths = normalize_list_value(record.get("vertical_paths"))
    transfer_paths = normalize_list_value(record.get("transfer_paths"))

    parts: List[str] = []
    if job_name:
        parts.append(f"{job_name}属于{job_category or '未明确分类'}岗位，岗位层级为{job_level or '未明确层级'}。")
    if degree_requirement or major_requirement:
        parts.append(
            f"常见学历要求为{degree_requirement or '未明确'}，专业要求为{major_requirement or '未明确'}。"
        )
    if experience_requirement:
        parts.append(f"经验要求：{experience_requirement}。")
    if hard_skills:
        parts.append(f"核心硬技能包括：{'、'.join(hard_skills[:10])}。")
    if tools_or_tech_stack:
        parts.append(f"常见工具或技术栈包括：{'、'.join(tools_or_tech_stack[:10])}。")
    if soft_skills:
        parts.append(f"软技能要求包括：{'、'.join(soft_skills[:8])}。")
    if certificates:
        parts.append(f"证书要求包括：{'、'.join(certificates[:8])}。")
    if practice_requirement:
        parts.append(f"实践要求：{practice_requirement}。")
    if requirement_summary:
        parts.append(f"岗位摘要：{requirement_summary}")
    if vertical_paths:
        parts.append(f"常见纵向发展路径：{'；'.join(vertical_paths[:5])}。")
    if transfer_paths:
        parts.append(f"常见横向转岗路径：{'；'.join(transfer_paths[:5])}。")
    return " ".join(part for part in parts if clean_text(part))


def build_job_knowledge_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """把岗位抽取结果转换成标准岗位知识文档。"""
    if df is None or df.empty:
        return []

    work_df = df.copy()
    standard_job_column = ""
    for candidate in ["standard_job_name", "standard_job_name_y", "standard_job_name_x", "normalized_job_name"]:
        if candidate in work_df.columns:
            standard_job_column = candidate
            break
    if not standard_job_column:
        raise ValueError("缺少 standard_job_name 字段，无法导出岗位知识库")

    grouped = work_df.groupby(standard_job_column, dropna=False, sort=True)
    documents: List[Dict[str, Any]] = []
    for standard_job_name, group_df in grouped:
        normalized_job_name = clean_text(standard_job_name)
        if not normalized_job_name:
            continue
        first_row = group_df.iloc[0].to_dict()
        source_count = int(len(group_df))
        success_ratio = 1.0
        if "extract_success" in group_df.columns:
            success_values = group_df["extract_success"].fillna(False).astype(bool)
            success_ratio = float(success_values.mean()) if len(success_values) else 1.0

        document = {
            "doc_id": build_doc_id(normalized_job_name),
            "doc_type": "job_profile",
            "standard_job_name": normalized_job_name,
            "aliases": collect_aliases(group_df),
            "job_category": clean_text(first_row.get("job_category")),
            "job_level": clean_text(first_row.get("job_level")),
            "degree_requirement": clean_text(first_row.get("degree_requirement")),
            "major_requirement": clean_text(first_row.get("major_requirement")),
            "certificate_requirement": normalize_list_value(first_row.get("certificate_requirement")),
            "hard_skills": normalize_list_value(first_row.get("hard_skills")),
            "tools_or_tech_stack": normalize_list_value(first_row.get("tools_or_tech_stack")),
            "soft_skills": normalize_list_value(first_row.get("soft_skills")),
            "practice_requirement": clean_text(first_row.get("practice_requirement")),
            "experience_requirement": clean_text(first_row.get("experience_requirement")),
            "raw_requirement_summary": clean_text(first_row.get("raw_requirement_summary")),
            "vertical_paths": normalize_list_value(first_row.get("vertical_paths")),
            "transfer_paths": normalize_list_value(first_row.get("transfer_paths")),
            "path_relation_details": normalize_dict_list(first_row.get("path_relation_details")),
            "source_count": source_count,
            "confidence": round(max(0.6, success_ratio), 2),
        }
        document["doc_text"] = build_doc_text(document)
        documents.append(document)
    return documents


def save_job_knowledge_records(records: List[Dict[str, Any]], output_path: str | Path) -> None:
    """保存岗位知识文档。"""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() == ".json":
        with path.open("w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        return

    with path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False))
            f.write("\n")


def process_export_to_json_kb(
    df: pd.DataFrame,
    output_path: str | Path = DEFAULT_OUTPUT_PATH,
) -> List[Dict[str, Any]]:
    """主流程：导出岗位知识文档库。"""
    records = build_job_knowledge_records(df)
    save_job_knowledge_records(records, output_path)
    return records


def load_input_table(input_path: str | Path) -> pd.DataFrame:
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"input file not found: {path}")
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    return pd.read_csv(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="导出岗位知识 JSON 文档库")
    parser.add_argument("--input", default="outputs/intermediate/jobs_extracted_full.csv", help="岗位抽取结果输入文件")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="岗位知识 JSON/JSONL 输出路径")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    df = load_input_table(args.input)
    records = process_export_to_json_kb(df=df, output_path=args.output)
    print(f"[export_to_json_kb] finished. documents={len(records)} output={args.output}")


if __name__ == "__main__":
    main()
