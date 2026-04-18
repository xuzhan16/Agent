"""
submission_smoke_check.py

Submission-readiness smoke checks for the career-planning project.

This script intentionally avoids rerunning the expensive LLM/data pipeline. It
checks whether the already-generated local knowledge bases and key state files
are present, coherent, and suitable for a contest/demo handoff.
"""

from __future__ import annotations

import csv
import json
import sqlite3
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "submission_check"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def read_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def status_item(name: str, status: str, message: str, details: Dict[str, Any] | None = None) -> Dict[str, Any]:
    return {
        "name": name,
        "status": status,
        "message": message,
        "details": details or {},
    }


def count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return sum(1 for _ in csv.DictReader(f))


def check_files() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    required_files = {
        "jobs.db": PROJECT_ROOT / "outputs" / "sql" / "jobs.db",
        "job_requirement_stats.json": PROJECT_ROOT / "outputs" / "match_assets" / "job_requirement_stats.json",
        "core_jobs.json": PROJECT_ROOT / "outputs" / "match_assets" / "core_jobs.json",
        "job_skill_knowledge_assets.json": PROJECT_ROOT / "outputs" / "match_assets" / "job_skill_knowledge_assets.json",
        "job_ability_assets.json": PROJECT_ROOT / "outputs" / "match_assets" / "job_ability_assets.json",
        "job_knowledge.jsonl": PROJECT_ROOT / "outputs" / "knowledge" / "job_knowledge.jsonl",
        "neo4j/jobs.csv": PROJECT_ROOT / "outputs" / "neo4j" / "jobs.csv",
        "neo4j/rel_promote_to.csv": PROJECT_ROOT / "outputs" / "neo4j" / "rel_promote_to.csv",
        "neo4j/rel_transfer_to.csv": PROJECT_ROOT / "outputs" / "neo4j" / "rel_transfer_to.csv",
    }
    missing = []
    for label, path in required_files.items():
        if not path.exists() or path.stat().st_size <= 0:
            missing.append(label)
    items.append(
        status_item(
            "文件资产检查",
            "FAIL" if missing else "PASS",
            "缺少关键资产文件" if missing else "关键资产文件存在且非空",
            {"missing": missing},
        )
    )

    core_payload = read_json(required_files["core_jobs.json"]) or {}
    core_jobs = core_payload.get("jobs") if isinstance(core_payload, dict) else []
    items.append(
        status_item(
            "10 个核心岗位资产",
            "PASS" if isinstance(core_jobs, list) and len(core_jobs) == 10 else "FAIL",
            f"核心岗位数量：{len(core_jobs) if isinstance(core_jobs, list) else 0}",
        )
    )

    ability_payload = read_json(required_files["job_ability_assets.json"]) or {}
    ability_jobs = ability_payload.get("jobs") if isinstance(ability_payload, dict) else {}
    incomplete = []
    if isinstance(ability_jobs, dict):
        for job_name, item in ability_jobs.items():
            dims = (item or {}).get("ability_requirements", {})
            if not isinstance(dims, dict) or len(dims) < 7:
                incomplete.append(job_name)
    items.append(
        status_item(
            "七维能力画像资产",
            "PASS" if isinstance(ability_jobs, dict) and ability_jobs and not incomplete else "FAIL",
            f"岗位能力画像数量：{len(ability_jobs) if isinstance(ability_jobs, dict) else 0}",
            {"incomplete_jobs": incomplete[:20]},
        )
    )
    return items


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE name = ? AND type IN ('table', 'view') LIMIT 1",
        (table,),
    ).fetchone() is not None


def check_sqlite() -> List[Dict[str, Any]]:
    db_path = PROJECT_ROOT / "outputs" / "sql" / "jobs.db"
    if not db_path.exists() or db_path.stat().st_size <= 0:
        return [status_item("SQLite jobs.db", "FAIL", "jobs.db 不存在或为空")]

    items: List[Dict[str, Any]] = []
    conn = sqlite3.connect(db_path)
    try:
        detail_count = conn.execute("SELECT COUNT(*) FROM job_detail").fetchone()[0] if table_exists(conn, "job_detail") else 0
        std_non_empty = 0
        if detail_count:
            std_non_empty = conn.execute(
                "SELECT COUNT(*) FROM job_detail WHERE standard_job_name IS NOT NULL AND TRIM(CAST(standard_job_name AS TEXT)) != ''"
            ).fetchone()[0]
        ratio = std_non_empty / detail_count if detail_count else 0.0
        items.append(
            status_item(
                "SQLite 标准岗位名非空率",
                "PASS" if ratio >= 0.95 else "FAIL",
                f"job_detail 行数 {detail_count}，standard_job_name 非空 {std_non_empty}（{ratio:.1%}）",
            )
        )

        view_ok = table_exists(conn, "job_market_view")
        items.append(
            status_item(
                "job_market_view 统一查询视图",
                "PASS" if view_ok else "WARN",
                "job_market_view 可查询" if view_ok else "未发现 job_market_view，AI SQL 会使用临时视图兜底",
            )
        )

        source_table = "job_market_view" if view_ok else "job_detail"
        city_count = conn.execute(f"SELECT COUNT(*) FROM {source_table} WHERE city LIKE '%北京%'").fetchone()[0]
        java_count = conn.execute(f"SELECT COUNT(*) FROM {source_table} WHERE standard_job_name LIKE '%Java%' OR standard_job_name LIKE '%java%'").fetchone()[0]
        frontend_count = conn.execute(f"SELECT COUNT(*) FROM {source_table} WHERE standard_job_name LIKE '%前端%'").fetchone()[0]
        salary_count = conn.execute(f"SELECT COUNT(*) FROM {source_table} WHERE salary_month_max IS NOT NULL").fetchone()[0]
        items.append(
            status_item(
                "SQLite 市场事实查询",
                "PASS" if city_count and (java_count or frontend_count) and salary_count else "FAIL",
                "北京公司、Java/前端岗位、薪资字段可查询",
                {
                    "beijing_rows": city_count,
                    "java_rows": java_count,
                    "frontend_rows": frontend_count,
                    "salary_rows": salary_count,
                },
            )
        )
    except Exception as exc:
        items.append(status_item("SQLite 检查", "FAIL", f"SQLite 检查失败：{exc}"))
    finally:
        conn.close()
    return items


def check_neo4j() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    csv_promote = count_csv_rows(PROJECT_ROOT / "outputs" / "neo4j" / "rel_promote_to.csv")
    csv_transfer = count_csv_rows(PROJECT_ROOT / "outputs" / "neo4j" / "rel_transfer_to.csv")
    items.append(
        status_item(
            "Neo4j CSV fallback",
            "PASS" if csv_promote >= 3 else "FAIL",
            f"CSV 晋升关系 {csv_promote} 条，转岗关系 {csv_transfer} 条",
        )
    )

    try:
        from job_path_graph_service import build_full_job_path_graph

        graph = build_full_job_path_graph(PROJECT_ROOT, prefer_neo4j=True, scope="curated")
        stats = graph.get("stats", {})
        promote_count = int(stats.get("promote_edge_count") or 0)
        total_count = int(stats.get("total_edge_count") or 0)
        items.append(
            status_item(
                "精选岗位路径图谱",
                "PASS" if promote_count >= 3 and total_count >= 3 else "WARN",
                f"精选图谱晋升关系 {promote_count} 条，总关系 {total_count} 条",
                {
                    "source": graph.get("source"),
                    "scope": graph.get("graph_scope"),
                    "raw_node_count": graph.get("raw_node_count"),
                    "raw_edge_count": graph.get("raw_edge_count"),
                    "filtered_node_count": graph.get("filtered_node_count"),
                    "filtered_edge_count": graph.get("filtered_edge_count"),
                },
            )
        )
    except Exception as exc:
        items.append(status_item("精选岗位路径图谱", "WARN", f"图谱服务检查失败，将依赖 CSV fallback：{exc}"))
    return items


def check_state_outputs() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    state_files = {
        "student_profile": PROJECT_ROOT / "outputs" / "state" / "student_profile_service_result.json",
        "job_profile": PROJECT_ROOT / "outputs" / "state" / "job_profile_service_result.json",
        "job_match": PROJECT_ROOT / "outputs" / "state" / "job_match_service_result.json",
        "career_path": PROJECT_ROOT / "outputs" / "state" / "career_path_plan_service_result.json",
        "career_report": PROJECT_ROOT / "outputs" / "state" / "career_report_service_result.json",
        "report_data": PROJECT_ROOT / "shared_reports" / "report_data.json",
    }
    missing = [name for name, path in state_files.items() if not path.exists()]
    items.append(
        status_item(
            "主链路状态文件",
            "WARN" if missing else "PASS",
            "部分状态文件缺失，需上传简历后刷新主链路" if missing else "主链路状态文件存在",
            {"missing": missing},
        )
    )

    job_profile = read_json(state_files["job_profile"]) or {}
    core_profiles = job_profile.get("core_job_profiles") or []
    target_assets = job_profile.get("target_job_profile_assets") or {}
    items.append(
        status_item(
            "岗位画像输出结构",
            "PASS" if core_profiles and target_assets else "WARN",
            f"core_job_profiles={len(core_profiles) if isinstance(core_profiles, list) else 0}",
            {"target_has_ability": bool((target_assets or {}).get("ability_radar"))},
        )
    )

    match = read_json(state_files["job_match"]) or {}
    if isinstance(match, list):
        match = match[0] if match else {}
    target_match = match.get("target_job_match") if isinstance(match, dict) else {}
    target_match = target_match or {}
    skill_ok = bool((target_match.get("skill_knowledge_match") or {}).get("required_knowledge_points"))
    hard_ok = bool(target_match.get("hard_info_evaluation"))
    ability_ok = bool((target_match.get("ability_match") or {}).get("dimensions"))
    items.append(
        status_item(
            "人岗匹配赛题结构",
            "PASS" if hard_ok and skill_ok and ability_ok else "WARN",
            "检查 hard_info_evaluation / skill_knowledge_match / ability_match",
            {"hard_info": hard_ok, "skill_knowledge": skill_ok, "ability_match": ability_ok},
        )
    )

    career_path = read_json(state_files["career_path"]) or {}
    pseudo_terms = ["高级XX", "XX负责人", "当前学生画像 -> 目标岗位", "相近业务岗位", "相近技术岗位"]
    path_text = json.dumps(career_path, ensure_ascii=False)
    items.append(
        status_item(
            "职业路径不造伪路径",
            "PASS" if not any(term in path_text for term in pseudo_terms) else "FAIL",
            "未发现典型伪路径话术" if not any(term in path_text for term in pseudo_terms) else "发现典型伪路径话术",
        )
    )

    report_data = read_json(state_files["report_data"]) or read_json(state_files["career_report"]) or {}
    report_text = json.dumps(report_data, ensure_ascii=False)
    report_terms = ["学历", "专业", "证书", "知识点", "七维能力", "能力"]
    matched_terms = [term for term in report_terms if term in report_text]
    items.append(
        status_item(
            "报告赛题字段覆盖",
            "PASS" if len(matched_terms) >= 4 else "WARN",
            f"报告中命中字段：{', '.join(matched_terms) if matched_terms else '暂无'}",
        )
    )
    return items


def check_frontend_files() -> List[Dict[str, Any]]:
    frontend_root = PROJECT_ROOT.parent / "frontend"
    files = [
        frontend_root / "src" / "pages" / "JobProfile.tsx",
        frontend_root / "src" / "pages" / "JobMatching.tsx",
        frontend_root / "src" / "pages" / "JobPathGraph.tsx",
        frontend_root / "src" / "pages" / "AIAssistant.tsx",
    ]
    missing = [str(path) for path in files if not path.exists()]
    return [
        status_item(
            "前端关键页面文件",
            "FAIL" if missing else "PASS",
            "前端关键页面存在" if not missing else "缺少前端关键页面",
            {"missing": missing},
        )
    ]


def write_reports(items: List[Dict[str, Any]]) -> Tuple[Path, Path]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summary = Counter(item["status"] for item in items)
    payload = {
        "version": "v1",
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "summary": dict(summary),
        "items": items,
    }
    json_path = OUTPUT_DIR / "submission_smoke_report.json"
    md_path = OUTPUT_DIR / "submission_smoke_report.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# 赛题提交前 Smoke Check",
        "",
        f"- 生成时间：{payload['generated_at']}",
        f"- PASS：{summary.get('PASS', 0)}",
        f"- WARN：{summary.get('WARN', 0)}",
        f"- FAIL：{summary.get('FAIL', 0)}",
        "",
        "| 状态 | 检查项 | 说明 |",
        "| --- | --- | --- |",
    ]
    for item in items:
        lines.append(f"| {item['status']} | {item['name']} | {item['message']} |")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path


def main() -> int:
    items: List[Dict[str, Any]] = []
    items.extend(check_files())
    items.extend(check_sqlite())
    items.extend(check_neo4j())
    items.extend(check_state_outputs())
    items.extend(check_frontend_files())
    json_path, md_path = write_reports(items)
    summary = Counter(item["status"] for item in items)
    print(json.dumps({"summary": dict(summary), "json_report": str(json_path), "markdown_report": str(md_path)}, ensure_ascii=False, indent=2))
    return 1 if summary.get("FAIL", 0) else 0


if __name__ == "__main__":
    sys.exit(main())
