"""
export_to_neo4j.py

导出 Neo4j 知识图谱可导入的 CSV 文件。

第一版图谱设计：
节点：
- Job
- Skill
- Degree
- Major
- Industry

关系：
- Job -> Skill : REQUIRES_SKILL
- Job -> Degree : REQUIRES_DEGREE
- Job -> Major : PREFERS_MAJOR
- Job -> Industry : BELONGS_TO_INDUSTRY
- Job -> Job : PROMOTE_TO
- Job -> Job : TRANSFER_TO
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set

import pandas as pd


def clean_text(value: object) -> str:
    """基础文本清洗。"""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null"}:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def stable_id(prefix: str, value: str) -> str:
    """为节点生成稳定 ID。"""
    text = clean_text(value)
    return f"{prefix}_{hashlib.md5(text.encode('utf-8')).hexdigest()[:16]}"


def get_first_existing_value(record: pd.Series, candidates: Sequence[str]) -> str:
    """从多个候选字段中取第一个非空值。"""
    for field in candidates:
        if field in record.index:
            text = clean_text(record.get(field, ""))
            if text:
                return text
    return ""


def pick_first_nonempty(values: Sequence[object]) -> str:
    """从一组值中取第一个非空值。"""
    for value in values:
        text = clean_text(value)
        if text:
            return text
    return ""


def most_common_nonempty(values: Sequence[object]) -> str:
    """获取最常见的非空值。"""
    cleaned = [clean_text(value) for value in values if clean_text(value)]
    if not cleaned:
        return ""
    return pd.Series(cleaned).value_counts().index[0]


def normalize_list_value(value: object) -> List[str]:
    """
    将列表字段统一转为字符串列表。

    支持：
    - list
    - JSON 数组字符串
    - 逗号/顿号/分号/竖线分隔字符串
    """
    if value is None:
        return []

    if isinstance(value, list):
        return sorted({clean_text(item) for item in value if clean_text(item)})

    text = clean_text(value)
    if not text:
        return []

    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return sorted({clean_text(item) for item in parsed if clean_text(item)})
        except json.JSONDecodeError:
            pass

    parts = re.split(r"[、,，;/；|]+", text)
    return sorted({clean_text(part) for part in parts if clean_text(part)})


def normalize_degree_values(value: object) -> List[str]:
    """
    规范化学历节点。

    这里不做复杂学历本体，只抽取粗粒度常见学历要求。
    """
    text = clean_text(value)
    if not text:
        return []

    if "学历不限" in text:
        return ["学历不限"]

    results: List[str] = []
    patterns = [
        ("博士及以上", r"博士.*及以上"),
        ("硕士及以上", r"硕士.*及以上"),
        ("本科及以上", r"本科.*及以上"),
        ("大专及以上", r"(大专|专科).*及以上"),
        ("博士", r"博士"),
        ("硕士", r"硕士"),
        ("本科", r"本科"),
        ("大专", r"(大专|专科)"),
        ("中专", r"中专"),
    ]
    for degree_name, pattern in patterns:
        if re.search(pattern, text):
            results.append(degree_name)

    if results:
        return sorted(set(results))

    # 如果无法规则抽取，则保留原文为一个粗粒度节点
    return [text]


def normalize_major_values(value: object) -> List[str]:
    """
    规范化专业节点。

    不追求复杂本体，优先把“相关专业/某某类专业”拆成可用的粗粒度专业名。
    """
    text = clean_text(value)
    if not text:
        return []

    if "专业不限" in text:
        return ["专业不限"]

    json_list = normalize_list_value(text)
    if len(json_list) > 1:
        values = json_list
    else:
        normalized = text
        normalized = re.sub(r"(等)?相关类专业", "", normalized)
        normalized = re.sub(r"(等)?相关专业", "", normalized)
        normalized = re.sub(r"(专业优先|优先考虑|优先)", "", normalized)
        normalized = normalized.replace("与", "、").replace("和", "、")
        values = [clean_text(part) for part in re.split(r"[、,，;/；|]+", normalized)]

    values = [value for value in values if clean_text(value)]
    return sorted(set(values))


def parse_path_targets(value: object, source_job_name: str) -> List[str]:
    """
    解析 vertical_paths / transfer_paths 字段。

    兼容：
    - JSON 数组
    - 逗号分隔
    - 使用 -> / → / => 表示路径
    """
    if value is None:
        return []

    if isinstance(value, list):
        raw_items = value
    else:
        text = clean_text(value)
        if not text:
            return []

        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
                raw_items = parsed if isinstance(parsed, list) else [text]
            except json.JSONDecodeError:
                raw_items = [text]
        else:
            raw_items = [text]

    targets: List[str] = []
    for item in raw_items:
        item_text = clean_text(item)
        if not item_text:
            continue

        if re.search(r"->|→|=>|＞|＞>", item_text):
            parts = [clean_text(part) for part in re.split(r"\s*(?:->|→|=>|＞|＞>)\s*", item_text)]
            parts = [part for part in parts if part]
            targets.extend(parts)
        else:
            parts = [clean_text(part) for part in re.split(r"[、,，;/；|]+", item_text)]
            targets.extend([part for part in parts if part])

    source_clean = clean_text(source_job_name)
    return sorted({target for target in targets if target and target != source_clean})


def get_job_node_name(record: pd.Series) -> str:
    """从记录中提取 Job 节点名称。"""
    return get_first_existing_value(
        record,
        ["standard_job_name", "job_name", "normalized_job_name", "job_title_norm", "job_title"],
    )


def build_base_job_nodes(df: pd.DataFrame) -> pd.DataFrame:
    """
    构建 Job 节点表。

    以 standard_job_name 为主键，做粗粒度聚合。
    """
    working_df = df.copy()
    working_df["_job_node_name"] = working_df.apply(get_job_node_name, axis=1)
    working_df = working_df[working_df["_job_node_name"].astype(str).str.strip() != ""].copy()

    rows = []
    for job_name, group in working_df.groupby("_job_node_name", dropna=False):
        job_name = clean_text(job_name)
        job_category_candidates: List[object] = []
        if "job_category" in group.columns:
            job_category_candidates.extend(group["job_category"].tolist())
        if "job_family" in group.columns:
            job_category_candidates.extend(group["job_family"].tolist())
        if "job_family_extracted" in group.columns:
            job_category_candidates.extend(group["job_family_extracted"].tolist())
        job_category = most_common_nonempty(job_category_candidates)

        job_level = most_common_nonempty(group["job_level"].tolist()) if "job_level" in group.columns else ""
        degree_candidates: List[object] = []
        if "degree_requirement" in group.columns:
            degree_candidates.extend(group["degree_requirement"].tolist())
        if "education_requirement" in group.columns:
            degree_candidates.extend(group["education_requirement"].tolist())
        degree_requirement = most_common_nonempty(degree_candidates)

        major_requirement = most_common_nonempty(group["major_requirement"].tolist()) if "major_requirement" in group.columns else ""
        experience_requirement = most_common_nonempty(group["experience_requirement"].tolist()) if "experience_requirement" in group.columns else ""
        summary_candidates: List[object] = []
        if "raw_requirement_summary" in group.columns:
            summary_candidates.extend(group["raw_requirement_summary"].tolist())
        if "job_summary" in group.columns:
            summary_candidates.extend(group["job_summary"].tolist())
        if "job_desc" in group.columns:
            summary_candidates.extend(group["job_desc"].tolist())
        if "job_description_clean" in group.columns:
            summary_candidates.extend(group["job_description_clean"].tolist())
        if "job_description" in group.columns:
            summary_candidates.extend(group["job_description"].tolist())
        raw_requirement_summary = pick_first_nonempty(summary_candidates)

        rows.append(
            {
                "job_id:ID(Job-ID)": stable_id("job", job_name),
                "name": job_name,
                "job_category": job_category,
                "job_level": job_level,
                "degree_requirement": degree_requirement,
                "major_requirement": major_requirement,
                "experience_requirement": experience_requirement,
                "raw_requirement_summary": raw_requirement_summary,
                "occurrence_count:int": int(len(group)),
                ":LABEL": "Job",
            }
        )

    return pd.DataFrame(
        rows,
        columns=[
            "job_id:ID(Job-ID)",
            "name",
            "job_category",
            "job_level",
            "degree_requirement",
            "major_requirement",
            "experience_requirement",
            "raw_requirement_summary",
            "occurrence_count:int",
            ":LABEL",
        ],
    ).drop_duplicates(subset=["job_id:ID(Job-ID)"])


def ensure_job_node(
    jobs_df: pd.DataFrame,
    job_name: str,
) -> pd.DataFrame:
    """如果路径关系中出现了未在主数据中出现的岗位，则补一个最小 Job 节点。"""
    job_name = clean_text(job_name)
    if not job_name:
        return jobs_df

    job_id = stable_id("job", job_name)
    if not jobs_df.empty and job_id in set(jobs_df["job_id:ID(Job-ID)"].tolist()):
        return jobs_df

    extra_row = pd.DataFrame(
        [
            {
                "job_id:ID(Job-ID)": job_id,
                "name": job_name,
                "job_category": "",
                "job_level": "",
                "degree_requirement": "",
                "major_requirement": "",
                "experience_requirement": "",
                "raw_requirement_summary": "",
                "occurrence_count:int": 0,
                ":LABEL": "Job",
            }
        ]
    )
    return pd.concat([jobs_df, extra_row], ignore_index=True)


def build_graph_tables(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    从岗位数据构建节点和关系表。
    """
    working_df = df.copy()
    working_df["_job_node_name"] = working_df.apply(get_job_node_name, axis=1)
    working_df = working_df[working_df["_job_node_name"].astype(str).str.strip() != ""].copy()

    print(f"[neo4j] Building graph from rows: {len(working_df)}")

    jobs_df = build_base_job_nodes(working_df)
    job_name_to_id = {
        row["name"]: row["job_id:ID(Job-ID)"] for _, row in jobs_df.iterrows()
    }

    skill_names: Set[str] = set()
    degree_names: Set[str] = set()
    major_names: Set[str] = set()
    industry_names: Set[str] = set()

    rel_requires_skill_rows: List[Dict[str, str]] = []
    rel_requires_degree_rows: List[Dict[str, str]] = []
    rel_prefers_major_rows: List[Dict[str, str]] = []
    rel_belongs_industry_rows: List[Dict[str, str]] = []
    rel_promote_to_rows: List[Dict[str, str]] = []
    rel_transfer_to_rows: List[Dict[str, str]] = []

    for _, row in working_df.iterrows():
        source_job_name = clean_text(row["_job_node_name"])
        if not source_job_name:
            continue
        source_job_id = job_name_to_id.get(source_job_name) or stable_id("job", source_job_name)

        # Skill：hard_skills + tools_or_tech_stack
        hard_skills = normalize_list_value(row.get("hard_skills", ""))
        tools = normalize_list_value(row.get("tools_or_tech_stack", ""))
        skill_values = sorted(set(hard_skills + tools))
        for skill in skill_values:
            skill_names.add(skill)
            rel_requires_skill_rows.append(
                {
                    ":START_ID(Job-ID)": source_job_id,
                    ":END_ID(Skill-ID)": stable_id("skill", skill),
                    ":TYPE": "REQUIRES_SKILL",
                }
            )

        # Degree
        degree_value = get_first_existing_value(row, ["degree_requirement", "education_requirement"])
        for degree in normalize_degree_values(degree_value):
            degree_names.add(degree)
            rel_requires_degree_rows.append(
                {
                    ":START_ID(Job-ID)": source_job_id,
                    ":END_ID(Degree-ID)": stable_id("degree", degree),
                    ":TYPE": "REQUIRES_DEGREE",
                }
            )

        # Major
        for major in normalize_major_values(row.get("major_requirement", "")):
            major_names.add(major)
            rel_prefers_major_rows.append(
                {
                    ":START_ID(Job-ID)": source_job_id,
                    ":END_ID(Major-ID)": stable_id("major", major),
                    ":TYPE": "PREFERS_MAJOR",
                }
            )

        # Industry
        industry = get_first_existing_value(row, ["industry"])
        if industry:
            industry_names.add(industry)
            rel_belongs_industry_rows.append(
                {
                    ":START_ID(Job-ID)": source_job_id,
                    ":END_ID(Industry-ID)": stable_id("industry", industry),
                    ":TYPE": "BELONGS_TO_INDUSTRY",
                }
            )

        # Vertical paths
        for target_job_name in parse_path_targets(row.get("vertical_paths", ""), source_job_name):
            jobs_df = ensure_job_node(jobs_df, target_job_name)
            job_name_to_id[target_job_name] = stable_id("job", target_job_name)
            rel_promote_to_rows.append(
                {
                    ":START_ID(Job-ID)": source_job_id,
                    ":END_ID(Job-ID)": job_name_to_id[target_job_name],
                    ":TYPE": "PROMOTE_TO",
                }
            )

        # Transfer paths
        for target_job_name in parse_path_targets(row.get("transfer_paths", ""), source_job_name):
            jobs_df = ensure_job_node(jobs_df, target_job_name)
            job_name_to_id[target_job_name] = stable_id("job", target_job_name)
            rel_transfer_to_rows.append(
                {
                    ":START_ID(Job-ID)": source_job_id,
                    ":END_ID(Job-ID)": job_name_to_id[target_job_name],
                    ":TYPE": "TRANSFER_TO",
                }
            )

    # 节点表
    skills_df = pd.DataFrame(
        [
            {"skill_id:ID(Skill-ID)": stable_id("skill", name), "name": name, ":LABEL": "Skill"}
            for name in sorted(skill_names)
        ],
        columns=["skill_id:ID(Skill-ID)", "name", ":LABEL"],
    ).drop_duplicates()

    degrees_df = pd.DataFrame(
        [
            {"degree_id:ID(Degree-ID)": stable_id("degree", name), "name": name, ":LABEL": "Degree"}
            for name in sorted(degree_names)
        ],
        columns=["degree_id:ID(Degree-ID)", "name", ":LABEL"],
    ).drop_duplicates()

    majors_df = pd.DataFrame(
        [
            {"major_id:ID(Major-ID)": stable_id("major", name), "name": name, ":LABEL": "Major"}
            for name in sorted(major_names)
        ],
        columns=["major_id:ID(Major-ID)", "name", ":LABEL"],
    ).drop_duplicates()

    industries_df = pd.DataFrame(
        [
            {"industry_id:ID(Industry-ID)": stable_id("industry", name), "name": name, ":LABEL": "Industry"}
            for name in sorted(industry_names)
        ],
        columns=["industry_id:ID(Industry-ID)", "name", ":LABEL"],
    ).drop_duplicates()

    # 关系表
    rel_requires_skill_df = pd.DataFrame(
        rel_requires_skill_rows,
        columns=[":START_ID(Job-ID)", ":END_ID(Skill-ID)", ":TYPE"],
    ).drop_duplicates()

    rel_requires_degree_df = pd.DataFrame(
        rel_requires_degree_rows,
        columns=[":START_ID(Job-ID)", ":END_ID(Degree-ID)", ":TYPE"],
    ).drop_duplicates()

    rel_prefers_major_df = pd.DataFrame(
        rel_prefers_major_rows,
        columns=[":START_ID(Job-ID)", ":END_ID(Major-ID)", ":TYPE"],
    ).drop_duplicates()

    rel_belongs_industry_df = pd.DataFrame(
        rel_belongs_industry_rows,
        columns=[":START_ID(Job-ID)", ":END_ID(Industry-ID)", ":TYPE"],
    ).drop_duplicates()

    rel_promote_to_df = pd.DataFrame(
        rel_promote_to_rows,
        columns=[":START_ID(Job-ID)", ":END_ID(Job-ID)", ":TYPE"],
    ).drop_duplicates()

    rel_transfer_to_df = pd.DataFrame(
        rel_transfer_to_rows,
        columns=[":START_ID(Job-ID)", ":END_ID(Job-ID)", ":TYPE"],
    ).drop_duplicates()

    jobs_df = jobs_df.drop_duplicates(subset=["job_id:ID(Job-ID)"]).sort_values(by=["name"]).reset_index(drop=True)

    print(
        "[neo4j] Nodes -> "
        f"Job: {len(jobs_df)}, Skill: {len(skills_df)}, Degree: {len(degrees_df)}, "
        f"Major: {len(majors_df)}, Industry: {len(industries_df)}"
    )
    print(
        "[neo4j] Relations -> "
        f"REQUIRES_SKILL: {len(rel_requires_skill_df)}, "
        f"REQUIRES_DEGREE: {len(rel_requires_degree_df)}, "
        f"PREFERS_MAJOR: {len(rel_prefers_major_df)}, "
        f"BELONGS_TO_INDUSTRY: {len(rel_belongs_industry_df)}, "
        f"PROMOTE_TO: {len(rel_promote_to_df)}, "
        f"TRANSFER_TO: {len(rel_transfer_to_df)}"
    )

    return {
        "jobs": jobs_df,
        "skills": skills_df,
        "degrees": degrees_df,
        "majors": majors_df,
        "industries": industries_df,
        "rel_requires_skill": rel_requires_skill_df,
        "rel_requires_degree": rel_requires_degree_df,
        "rel_prefers_major": rel_prefers_major_df,
        "rel_belongs_industry": rel_belongs_industry_df,
        "rel_promote_to": rel_promote_to_df,
        "rel_transfer_to": rel_transfer_to_df,
    }


def export_graph_csvs(graph_tables: Dict[str, pd.DataFrame], output_dir: str) -> None:
    """把节点和关系表导出为 Neo4j CSV。"""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    file_map = {
        "jobs": "jobs.csv",
        "skills": "skills.csv",
        "degrees": "degrees.csv",
        "majors": "majors.csv",
        "industries": "industries.csv",
        "rel_requires_skill": "rel_requires_skill.csv",
        "rel_requires_degree": "rel_requires_degree.csv",
        "rel_prefers_major": "rel_prefers_major.csv",
        "rel_belongs_industry": "rel_belongs_industry.csv",
        "rel_promote_to": "rel_promote_to.csv",
        "rel_transfer_to": "rel_transfer_to.csv",
    }

    for key, file_name in file_map.items():
        table = graph_tables.get(key)
        if table is None:
            continue
        table.to_csv(output_path / file_name, index=False, encoding="utf-8-sig")

    print(f"[neo4j] CSV files exported to: {output_dir}")


def load_table(input_path: str) -> pd.DataFrame:
    """支持 CSV / Excel 输入。"""
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, dtype=str).fillna("")
    return pd.read_csv(path, dtype=str).fillna("")


def process_export_to_neo4j(df: pd.DataFrame, output_dir: str) -> Dict[str, pd.DataFrame]:
    """主流程函数。"""
    graph_tables = build_graph_tables(df)
    export_graph_csvs(graph_tables, output_dir)
    return graph_tables


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="导出 Neo4j 知识图谱导入 CSV")
    parser.add_argument(
        "--input",
        default="outputs/jobs_extracted.csv",
        help="输入处理后的岗位数据文件路径，支持 CSV / Excel",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs/neo4j",
        help="输出 Neo4j CSV 目录",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    df = load_table(args.input)
    graph_tables = process_export_to_neo4j(df, args.output_dir)
    print("[neo4j] Finished.")
    print(f"[neo4j] Input rows: {len(df)}")
    print(f"[neo4j] Exported files: {len(graph_tables)}")


if __name__ == "__main__":
    main()
