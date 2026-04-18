"""
job_path_graph_service.py

Build the full job path graph for the frontend from real graph facts.

The service never fabricates career paths. Neo4j is the primary source, and
the generated Neo4j import CSV files are used only as a local fallback.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from db_helper import query_neo4j


NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD") or os.getenv("NEO4J_AUTH_PASSWORD") or "12345678"

RELATION_LABELS = {
    "PROMOTE_TO": ("晋升", "promotion"),
    "TRANSFER_TO": ("转岗", "transfer"),
}


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\u00a0", " ").replace("\u3000", " ").strip()
    text = re.sub(r"\s+", " ", text)
    if text.lower() in {"", "nan", "none", "null", "n/a", "na", "-"}:
        return ""
    return text


def safe_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    return [value]


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


def parse_list_field(value: Any) -> List[str]:
    if isinstance(value, list):
        return dedup_keep_order(value)
    text = clean_text(value)
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return dedup_keep_order(parsed)
    except Exception:
        pass
    parts = re.split(r"[、,，;/；|]+", text)
    return dedup_keep_order(parts)


def is_valid_job_name(value: Any) -> bool:
    text = clean_text(value)
    if not text:
        return False
    if text.startswith("job_"):
        return False
    if re.fullmatch(r"[0-9a-fA-F]{12,}", text):
        return False
    return bool(re.search(r"[\u4e00-\u9fffA-Za-z]", text))


def find_csv_column(fieldnames: List[str], marker: str, fallback: str = "") -> str:
    for fieldname in fieldnames:
        if marker in fieldname:
            return fieldname
    return fallback


def edge_id(source: str, relation: str, target: str) -> str:
    digest = hashlib.sha1(f"{source}|{relation}|{target}".encode("utf-8")).hexdigest()[:12]
    return f"edge_{digest}"


def build_node(job_name: str, props: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    props = props or {}
    name = clean_text(
        job_name
        or props.get("name")
        or props.get("standard_job_name")
        or props.get("job_name")
    )
    return {
        "id": name,
        "label": name,
        "node_type": "job",
        "job_category": clean_text(props.get("job_category")),
        "job_level": clean_text(props.get("job_level")),
        "degree_requirement": clean_text(props.get("degree_requirement")),
        "major_requirement": parse_list_field(props.get("major_requirement")),
        "occurrence_count": props.get("occurrence_count") or props.get("occurrence_count:int"),
        "is_isolated": False,
    }


def build_edge(
    source: str,
    target: str,
    relation: str,
    props: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    props = props or {}
    normalized_relation = clean_text(relation).upper()
    label, edge_type = RELATION_LABELS.get(normalized_relation, (normalized_relation, normalized_relation.lower()))
    return {
        "id": edge_id(source, normalized_relation, target),
        "source": source,
        "target": target,
        "relation": normalized_relation,
        "label": label,
        "edge_type": edge_type,
        "source_name": source,
        "target_name": target,
        "reason": clean_text(props.get("reason")),
        "confidence": props.get("confidence"),
    }


def build_stats(nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]) -> Dict[str, int]:
    promote_count = sum(1 for edge in edges if edge.get("relation") == "PROMOTE_TO")
    transfer_count = sum(1 for edge in edges if edge.get("relation") == "TRANSFER_TO")
    return {
        "job_node_count": len(nodes),
        "promote_edge_count": promote_count,
        "transfer_edge_count": transfer_count,
        "total_edge_count": len(edges),
    }


def normalize_graph(
    node_map: Dict[str, Dict[str, Any]],
    edges: List[Dict[str, Any]],
    source: str,
    empty_message: str,
) -> Dict[str, Any]:
    connected_names = {edge["source"] for edge in edges} | {edge["target"] for edge in edges}
    nodes = [
        node_map[name]
        for name in sorted(connected_names)
        if name in node_map and is_valid_job_name(name)
    ]
    unique_edges: Dict[str, Dict[str, Any]] = {}
    for edge in edges:
        if not is_valid_job_name(edge.get("source")) or not is_valid_job_name(edge.get("target")):
            continue
        if edge.get("source") not in connected_names or edge.get("target") not in connected_names:
            continue
        unique_edges[edge["id"]] = edge
    clean_edges = list(unique_edges.values())

    stats = build_stats(nodes, clean_edges)
    status = "available" if clean_edges else "empty"
    return {
        "graph_status": status,
        "source": source,
        "stats": stats,
        "nodes": nodes,
        "edges": clean_edges,
        "message": f"已从 {source} 读取岗位路径关系。" if status == "available" else empty_message,
    }


def query_graph_from_neo4j() -> Dict[str, Any]:
    rows = query_neo4j(
        uri=NEO4J_URI,
        user=NEO4J_USER,
        password=NEO4J_PASSWORD,
        query="""
        MATCH (a:Job)-[r:PROMOTE_TO|TRANSFER_TO]->(b:Job)
        RETURN
          coalesce(a.name, a.standard_job_name, a.job_name) AS source_job,
          coalesce(b.name, b.standard_job_name, b.job_name) AS target_job,
          type(r) AS relation,
          properties(a) AS source_props,
          properties(b) AS target_props,
          properties(r) AS relation_props
        ORDER BY relation ASC, source_job ASC, target_job ASC
        """,
    )
    if not rows:
        return {
            "graph_status": "empty",
            "source": "neo4j",
            "stats": build_stats([], []),
            "nodes": [],
            "edges": [],
            "message": "Neo4j 中暂未查询到 PROMOTE_TO / TRANSFER_TO 路径关系。",
        }

    node_map: Dict[str, Dict[str, Any]] = {}
    edges: List[Dict[str, Any]] = []
    for row in rows:
        source_job = clean_text(row.get("source_job"))
        target_job = clean_text(row.get("target_job"))
        relation = clean_text(row.get("relation")).upper()
        if relation not in RELATION_LABELS:
            continue
        if not is_valid_job_name(source_job) or not is_valid_job_name(target_job):
            continue
        source_props = row.get("source_props") if isinstance(row.get("source_props"), dict) else {}
        target_props = row.get("target_props") if isinstance(row.get("target_props"), dict) else {}
        relation_props = row.get("relation_props") if isinstance(row.get("relation_props"), dict) else {}
        node_map[source_job] = build_node(source_job, source_props)
        node_map[target_job] = build_node(target_job, target_props)
        edges.append(build_edge(source_job, target_job, relation, relation_props))

    return normalize_graph(
        node_map=node_map,
        edges=edges,
        source="neo4j",
        empty_message="Neo4j 中暂未查询到 PROMOTE_TO / TRANSFER_TO 路径关系。",
    )


def load_job_nodes_from_csv(jobs_csv: Path) -> Tuple[Dict[str, str], Dict[str, Dict[str, Any]]]:
    if not jobs_csv.exists():
        return {}, {}
    with jobs_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        id_col = find_csv_column(fieldnames, ":ID(Job-ID)")
        name_col = "name"
        job_id_to_name: Dict[str, str] = {}
        node_map: Dict[str, Dict[str, Any]] = {}
        for row in reader:
            job_id = clean_text(row.get(id_col))
            job_name = clean_text(row.get(name_col))
            if not job_id or not is_valid_job_name(job_name):
                continue
            job_id_to_name[job_id] = job_name
            node_map[job_name] = build_node(job_name, row)
        return job_id_to_name, node_map


def load_edges_from_csv(rel_csv: Path, relation: str, job_id_to_name: Dict[str, str]) -> List[Dict[str, Any]]:
    if not rel_csv.exists() or not job_id_to_name:
        return []
    edges: List[Dict[str, Any]] = []
    with rel_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        start_col = find_csv_column(fieldnames, ":START_ID(Job-ID)")
        end_col = find_csv_column(fieldnames, ":END_ID(Job-ID)")
        type_col = find_csv_column(fieldnames, ":TYPE", fallback=":TYPE")
        for row in reader:
            rel_type = clean_text(row.get(type_col) or relation).upper()
            if rel_type != relation:
                rel_type = relation
            source_job = clean_text(job_id_to_name.get(clean_text(row.get(start_col))))
            target_job = clean_text(job_id_to_name.get(clean_text(row.get(end_col))))
            if not is_valid_job_name(source_job) or not is_valid_job_name(target_job):
                continue
            edges.append(build_edge(source_job, target_job, rel_type, row))
    return edges


def query_graph_from_csv(project_root: Path) -> Dict[str, Any]:
    neo4j_dir = project_root / "outputs" / "neo4j"
    jobs_csv = neo4j_dir / "jobs.csv"
    promote_csv = neo4j_dir / "rel_promote_to.csv"
    transfer_csv = neo4j_dir / "rel_transfer_to.csv"

    job_id_to_name, node_map = load_job_nodes_from_csv(jobs_csv)
    if not job_id_to_name or not node_map:
        return {
            "graph_status": "unavailable",
            "source": "none",
            "stats": build_stats([], []),
            "nodes": [],
            "edges": [],
            "message": "Neo4j 不可用，且本地图谱 jobs.csv 不存在或为空。",
        }

    edges = [
        *load_edges_from_csv(promote_csv, "PROMOTE_TO", job_id_to_name),
        *load_edges_from_csv(transfer_csv, "TRANSFER_TO", job_id_to_name),
    ]
    if not edges:
        return {
            "graph_status": "empty",
            "source": "csv_fallback",
            "stats": build_stats([], []),
            "nodes": [],
            "edges": [],
            "message": "本地图谱 CSV 中暂未读取到 PROMOTE_TO / TRANSFER_TO 路径关系。",
        }

    return normalize_graph(
        node_map=node_map,
        edges=edges,
        source="csv_fallback",
        empty_message="本地图谱 CSV 中暂未读取到 PROMOTE_TO / TRANSFER_TO 路径关系。",
    )


def build_full_job_path_graph(
    project_root: Optional[str | Path] = None,
    prefer_neo4j: bool = True,
) -> Dict[str, Any]:
    """Build a frontend-ready full job path graph."""
    root = Path(project_root) if project_root else Path(__file__).resolve().parent
    if prefer_neo4j:
        try:
            graph = query_graph_from_neo4j()
            if graph.get("graph_status") == "available":
                return graph
        except Exception as exc:
            graph = {
                "graph_status": "unavailable",
                "source": "neo4j",
                "stats": build_stats([], []),
                "nodes": [],
                "edges": [],
                "message": f"Neo4j 查询失败，准备使用 CSV 兜底：{exc}",
            }

    csv_graph = query_graph_from_csv(root)
    if csv_graph.get("graph_status") == "available":
        if prefer_neo4j:
            csv_graph["message"] = "Neo4j 暂不可用或暂无路径关系，已从本地图谱 CSV 读取岗位路径关系。"
        return csv_graph

    return csv_graph
