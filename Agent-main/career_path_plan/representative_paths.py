"""
representative_paths.py

Build global representative promotion paths from real graph facts.

The paths in this module are intentionally independent from the user's target
job. They are used to satisfy the contest requirement that the overall system
shows at least three jobs with real promotion relations.
"""

from __future__ import annotations

import csv
import os
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from db_helper import query_neo4j


NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "ChangeThisPassword_123!")
DEFAULT_MISSING_MESSAGE = "当前图谱中可用代表晋升路径不足 3 个，建议后续补充岗位关系数据。"


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\u00a0", " ").replace("\u3000", " ").strip()
    text = re.sub(r"\s+", " ", text)
    if text.lower() in {"", "nan", "none", "null", "n/a", "na", "-"}:
        return ""
    return text


def dedup_keep_order(values: Iterable[Any]) -> List[str]:
    seen = set()
    result: List[str] = []
    for value in values:
        cleaned = clean_text(value)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
    return result


def is_valid_job_name(value: Any) -> bool:
    text = clean_text(value)
    if not text:
        return False
    if text.startswith("job_"):
        return False
    if re.fullmatch(r"[0-9a-fA-F]{12,}", text):
        return False
    return bool(re.search(r"[\u4e00-\u9fffA-Za-z]", text))


def build_representative_item(
    source_job: str,
    promote_targets: List[str],
    source: str,
) -> Dict[str, Any]:
    targets = [target for target in dedup_keep_order(promote_targets) if is_valid_job_name(target)]
    return {
        "source_job": clean_text(source_job),
        "promote_targets": targets,
        "edge_count": len(targets),
        "source": source,
        "selection_reason": "该岗位存在多条真实 PROMOTE_TO 晋升关系，适合作为代表性晋升路径展示。",
    }


def query_representative_paths_from_neo4j(limit: int) -> List[Dict[str, Any]]:
    rows = query_neo4j(
        uri=NEO4J_URI,
        user=NEO4J_USER,
        password=NEO4J_PASSWORD,
        query="""
        MATCH (j:Job)-[:PROMOTE_TO]->(target:Job)
        WITH j.name AS source_job,
             collect(DISTINCT target.name) AS targets,
             count(DISTINCT target) AS edge_count
        WHERE source_job IS NOT NULL AND source_job <> "" AND edge_count > 0
        RETURN source_job, targets, edge_count
        ORDER BY edge_count DESC, source_job ASC
        LIMIT $limit
        """,
        parameters={"limit": int(limit)},
    )
    result = []
    for row in rows:
        source_job = clean_text(row.get("source_job"))
        targets = row.get("targets") if isinstance(row.get("targets"), list) else []
        if not is_valid_job_name(source_job):
            continue
        item = build_representative_item(source_job, targets, source="neo4j.PROMOTE_TO")
        if item["edge_count"] > 0:
            result.append(item)
    return result[:limit]


def find_csv_column(fieldnames: List[str], marker: str, fallback: str = "") -> str:
    for fieldname in fieldnames:
        if marker in fieldname:
            return fieldname
    return fallback


def load_job_name_map(jobs_csv: Path) -> Dict[str, str]:
    if not jobs_csv.exists():
        return {}
    with jobs_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        id_col = find_csv_column(reader.fieldnames or [], ":ID(Job-ID)")
        name_col = "name"
        job_names = {}
        for row in reader:
            job_id = clean_text(row.get(id_col))
            job_name = clean_text(row.get(name_col))
            if job_id and job_name:
                job_names[job_id] = job_name
        return job_names


def query_representative_paths_from_csv(project_root: Path, limit: int) -> List[Dict[str, Any]]:
    neo4j_dir = project_root / "outputs" / "neo4j"
    job_names = load_job_name_map(neo4j_dir / "jobs.csv")
    promote_csv = neo4j_dir / "rel_promote_to.csv"
    if not promote_csv.exists() or not job_names:
        return []

    grouped: Dict[str, List[str]] = defaultdict(list)
    with promote_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        start_col = find_csv_column(reader.fieldnames or [], ":START_ID(Job-ID)")
        end_col = find_csv_column(reader.fieldnames or [], ":END_ID(Job-ID)")
        for row in reader:
            source_job = clean_text(job_names.get(clean_text(row.get(start_col))))
            target_job = clean_text(job_names.get(clean_text(row.get(end_col))))
            if is_valid_job_name(source_job) and is_valid_job_name(target_job):
                grouped[source_job].append(target_job)

    ranked = sorted(
        grouped.items(),
        key=lambda item: (-len(dedup_keep_order(item[1])), item[0]),
    )
    result = []
    for source_job, targets in ranked:
        item = build_representative_item(
            source_job=source_job,
            promote_targets=targets,
            source="csv.PROMOTE_TO",
        )
        if item["edge_count"] > 0:
            result.append(item)
        if len(result) >= limit:
            break
    return result


def build_representative_promotion_paths(
    limit: int = 3,
    project_root: Optional[str | Path] = None,
) -> Dict[str, Any]:
    """Return representative promotion paths from Neo4j, then CSV fallback."""
    safe_limit = max(3, int(limit or 3))
    root = Path(project_root) if project_root else Path(__file__).resolve().parent.parent

    paths = query_representative_paths_from_neo4j(limit=safe_limit)
    if not paths:
        paths = query_representative_paths_from_csv(project_root=root, limit=safe_limit)

    status = "available" if len(paths) >= 3 else "insufficient"
    return {
        "representative_promotion_paths": paths,
        "representative_path_count": len(paths),
        "representative_path_status": status,
        "representative_path_message": ""
        if status == "available"
        else DEFAULT_MISSING_MESSAGE,
    }
