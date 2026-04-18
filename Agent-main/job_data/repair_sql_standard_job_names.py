"""
repair_sql_standard_job_names.py

Lightweight submission-readiness repair for the SQLite job knowledge base.

The full job-data pipeline already produced jobs_extracted_full.csv with
standardized job names, but some historical jobs.db builds left
standard_job_name blank. This script backfills those names without rerunning the
expensive extraction pipeline, then creates a stable job_market_view for local
SQL/AI assistant queries.
"""

from __future__ import annotations

import argparse
import csv
import shutil
import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_CSV = PROJECT_ROOT / "outputs" / "intermediate" / "jobs_extracted_full.csv"
DEFAULT_DB_PATH = PROJECT_ROOT / "outputs" / "sql" / "jobs.db"
DEFAULT_BACKUP_DIR = PROJECT_ROOT / "outputs" / "backups" / "sql"


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\u00a0", " ").replace("\u3000", " ").strip()
    if text.lower() in {"", "nan", "none", "null", "n/a", "na", "-"}:
        return ""
    return text


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def preferred_standard_name(row: Dict[str, str]) -> str:
    return (
        clean_text(row.get("standard_job_name_y"))
        or clean_text(row.get("standard_job_name_x"))
        or clean_text(row.get("normalized_job_name"))
        or clean_text(row.get("job_title_norm"))
        or clean_text(row.get("job_title"))
    )


def most_common_mapping(pairs: Iterable[Tuple[str, str]]) -> Dict[str, str]:
    counters: Dict[str, Counter[str]] = {}
    for key, value in pairs:
        key = clean_text(key)
        value = clean_text(value)
        if not key or not value:
            continue
        counters.setdefault(key, Counter())[value] += 1
    return {key: counter.most_common(1)[0][0] for key, counter in counters.items() if counter}


def load_standard_name_maps(csv_path: Path) -> Tuple[List[Dict[str, str]], Dict[str, Dict[str, str]], Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    by_key: Dict[str, Dict[str, str]] = {
        "record_id": {},
        "source_row_no": {},
        "job_code": {},
        "job_url": {},
        "normalized_job_name": {},
        "job_title": {},
        "job_title_norm": {},
    }
    if not csv_path.exists():
        raise FileNotFoundError(f"source csv not found: {csv_path}")

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            standard = preferred_standard_name(row)
            if not standard:
                continue
            normalized_row = {key: clean_text(value) for key, value in row.items()}
            normalized_row["_standard_job_name"] = standard
            rows.append(normalized_row)
            for key in ["record_id", "source_row_no", "job_code", "job_url"]:
                value = clean_text(row.get(key))
                if value and value not in by_key[key]:
                    by_key[key][value] = standard

    by_key["normalized_job_name"] = most_common_mapping(
        (row.get("normalized_job_name", ""), row.get("_standard_job_name", "")) for row in rows
    )
    by_key["job_title"] = most_common_mapping(
        (row.get("job_title", ""), row.get("_standard_job_name", "")) for row in rows
    )
    by_key["job_title_norm"] = most_common_mapping(
        (row.get("job_title_norm", ""), row.get("_standard_job_name", "")) for row in rows
    )
    flat_mapping = most_common_mapping(
        (row.get("normalized_job_name", "") or row.get("job_title_norm", "") or row.get("job_title", ""), row.get("_standard_job_name", ""))
        for row in rows
    )
    return rows, by_key, flat_mapping


def get_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    try:
        return [clean_text(row[1]) for row in conn.execute(f"PRAGMA table_info({quote_ident(table)})").fetchall()]
    except sqlite3.Error:
        return []


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE name = ? AND type IN ('table', 'view') LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def resolve_from_row(row: sqlite3.Row, maps: Dict[str, Dict[str, str]], key_order: List[str]) -> Tuple[str, str]:
    row_dict = dict(row)
    for key in key_order:
        value = clean_text(row_dict.get(key))
        if value and value in maps.get(key, {}):
            return maps[key][value], key
    return "", ""


def update_table_standard_names(
    conn: sqlite3.Connection,
    table: str,
    maps: Dict[str, Dict[str, str]],
    key_order: List[str],
) -> Dict[str, Any]:
    if not table_exists(conn, table):
        return {"table": table, "exists": False, "updated": 0, "remaining_empty": 0, "warnings": [f"{table} not found"]}
    columns = get_columns(conn, table)
    if "standard_job_name" not in columns:
        return {"table": table, "exists": True, "updated": 0, "remaining_empty": 0, "warnings": [f"{table}.standard_job_name not found"]}

    selectable = ["rowid", "standard_job_name"] + [key for key in key_order if key in columns]
    rows = conn.execute(
        f"SELECT {', '.join(quote_ident(item) for item in selectable)} "
        f"FROM {quote_ident(table)} "
        "WHERE standard_job_name IS NULL OR TRIM(CAST(standard_job_name AS TEXT)) = ''"
    ).fetchall()

    updated = 0
    method_counter: Counter[str] = Counter()
    for row in rows:
        standard, method = resolve_from_row(row, maps, key_order)
        if not standard:
            continue
        conn.execute(
            f"UPDATE {quote_ident(table)} SET standard_job_name = ? WHERE rowid = ?",
            (standard, row["rowid"]),
        )
        updated += 1
        method_counter[method] += 1

    remaining_empty = conn.execute(
        f"SELECT COUNT(*) FROM {quote_ident(table)} "
        "WHERE standard_job_name IS NULL OR TRIM(CAST(standard_job_name AS TEXT)) = ''"
    ).fetchone()[0]
    total = conn.execute(f"SELECT COUNT(*) FROM {quote_ident(table)}").fetchone()[0]
    return {
        "table": table,
        "exists": True,
        "total_rows": total,
        "updated": updated,
        "remaining_empty": int(remaining_empty),
        "non_empty": int(total - remaining_empty),
        "match_methods": dict(method_counter),
        "warnings": [] if updated else [f"{table} had no rows matched for backfill"],
    }


def create_job_market_view(conn: sqlite3.Connection) -> bool:
    if not table_exists(conn, "job_detail"):
        return False
    conn.execute("DROP VIEW IF EXISTS job_market_view")
    conn.execute(
        """
        CREATE VIEW job_market_view AS
        SELECT
          d.record_id,
          d.source_row_no,
          d.job_code,
          d.job_url,
          d.company_name_clean AS company_name,
          d.city,
          COALESCE(NULLIF(d.standard_job_name, ''), NULLIF(d.normalized_job_name, ''), NULLIF(d.job_name_clean, ''), NULLIF(d.job_name_raw, '')) AS standard_job_name,
          COALESCE(NULLIF(d.job_name_clean, ''), NULLIF(d.job_name_raw, ''), NULLIF(d.normalized_job_name, '')) AS job_title,
          d.normalized_job_name AS job_title_norm,
          CAST(NULLIF(d.salary_min, '') AS REAL) AS salary_min,
          CAST(NULLIF(d.salary_max, '') AS REAL) AS salary_max,
          CAST(NULLIF(d.salary_month_min, '') AS REAL) AS salary_month_min,
          CAST(NULLIF(d.salary_month_max, '') AS REAL) AS salary_month_max,
          d.industry,
          d.company_size,
          d.company_type,
          p.degree_requirement,
          p.major_requirement,
          p.certificate_requirement_json AS certificate_requirement,
          p.hard_skills_json AS hard_skills,
          p.tools_or_tech_stack_json AS tools_or_tech_stack,
          d.job_desc_clean AS job_description_clean
        FROM job_detail d
        LEFT JOIN job_profile p ON p.record_id = d.record_id
        """
    )
    return True


def top_standard_names(conn: sqlite3.Connection, table: str = "job_detail", limit: int = 20) -> List[Dict[str, Any]]:
    if not table_exists(conn, table):
        return []
    rows = conn.execute(
        f"""
        SELECT standard_job_name, COUNT(*) AS count
        FROM {quote_ident(table)}
        WHERE standard_job_name IS NOT NULL AND TRIM(CAST(standard_job_name AS TEXT)) != ''
        GROUP BY standard_job_name
        ORDER BY count DESC, standard_job_name ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [{"standard_job_name": row[0], "count": int(row[1])} for row in rows]


def backup_db(db_path: Path, backup_dir: Path) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    backup_path = backup_dir / f"{db_path.name}.bak_{timestamp}"
    shutil.copy2(db_path, backup_path)
    return backup_path


def repair_sql_standard_job_names(
    source_csv: Path = DEFAULT_SOURCE_CSV,
    db_path: Path = DEFAULT_DB_PATH,
    backup_dir: Path = DEFAULT_BACKUP_DIR,
    skip_backup: bool = False,
) -> Dict[str, Any]:
    if not db_path.exists():
        raise FileNotFoundError(f"jobs.db not found: {db_path}")
    csv_rows, maps, _ = load_standard_name_maps(source_csv)
    backup_path = None if skip_backup else backup_db(db_path, backup_dir)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        results = []
        results.append(
            update_table_standard_names(
                conn,
                "job_detail",
                maps,
                ["record_id", "source_row_no", "job_code", "job_url", "normalized_job_name", "job_title_norm", "job_title"],
            )
        )
        results.append(
            update_table_standard_names(
                conn,
                "job_profile",
                maps,
                ["record_id"],
            )
        )
        results.append(
            update_table_standard_names(
                conn,
                "job_mapping",
                maps,
                ["normalized_job_name", "raw_job_name"],
            )
        )
        view_created = create_job_market_view(conn)
        conn.commit()
        top20 = top_standard_names(conn)
    finally:
        conn.close()

    warnings: List[str] = []
    for result in results:
        warnings.extend(result.get("warnings", []))
    return {
        "version": "v1",
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "source_csv": str(source_csv),
        "db_path": str(db_path),
        "backup_path": str(backup_path) if backup_path else "",
        "csv_sample_count": len(csv_rows),
        "table_results": results,
        "job_market_view_created": view_created,
        "top_standard_jobs": top20,
        "warnings": warnings,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill standard_job_name in outputs/sql/jobs.db")
    parser.add_argument("--source-csv", default=str(DEFAULT_SOURCE_CSV))
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--backup-dir", default=str(DEFAULT_BACKUP_DIR))
    parser.add_argument("--skip-backup", action="store_true")
    args = parser.parse_args()

    result = repair_sql_standard_job_names(
        source_csv=Path(args.source_csv),
        db_path=Path(args.db_path),
        backup_dir=Path(args.backup_dir),
        skip_backup=args.skip_backup,
    )
    import json

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
