import logging
import os
import tempfile
import json
import shutil
import asyncio
import re
import uuid
import csv
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from urllib import error as urllib_error
from urllib import request as urllib_request

from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel

from job_data_pipeline import (
    DEFAULT_GROUP_SAMPLE_SIZE,
    DEFAULT_INPUT_FILE,
    DEFAULT_INTERMEDIATE_DIR,
    DEFAULT_KNOWLEDGE_OUTPUT_DIR,
    DEFAULT_NEO4J_OUTPUT_DIR,
    DEFAULT_SQL_DB_PATH,
    run_job_data_pipeline,
)
from main_pipeline import run_pipeline
from llm_interface_layer.config import DEFAULT_LLM_CONFIG
from semantic_retrieval.semantic_retriever import SemanticJobKnowledgeRetriever
from semantic_retrieval.embedding_store import DEFAULT_HASH_EMBEDDING_MODEL

app = FastAPI(title="AI Career Plan API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logger = logging.getLogger(__name__)

# 保存全局状态给前后端串联
STATE_FILE = "student_api_state.json"
REPORT_FILE_NAME = "final_report.md"
LATEST_REPORT_SNAPSHOT_FILE = "report_data.json"
REPORT_SNAPSHOT_DIR_NAME = "shared_reports"
REPORT_SNAPSHOT_PATTERN = re.compile(r"^(report_[A-Za-z0-9_-]+\.json|report_data\.json)$")
PIPELINE_STATUS_FILE = "pipeline_state.json"
AI_MEMORY_FILE = "ai_chat_memory.json"
AI_MAX_HISTORY_MESSAGES = 12
AI_MAX_CONTEXT_CHUNKS = 6
AI_CONTEXT_CHUNK_MAX_CHARS = 420
AI_SEMANTIC_TOP_K = 3
AI_SQL_MAX_ROWS = 15
AI_SQL_CONTEXT_ROWS = 8
AI_SQL_MAX_VALUE_CHARS = 120
AI_SQL_TABLE_NAME = "jobs"
AI_SQL_DB_SOURCE = "sqlite_jobs_db"
AI_SQL_CSV_SOURCE = "csv_fallback"
AI_SQL_VIEW_COLUMNS = [
    "company_name",
    "city",
    "standard_job_name",
    "standard_job_name_y",
    "standard_job_name_x",
    "job_title",
    "job_title_norm",
    "salary_min",
    "salary_max",
    "salary_month_min",
    "salary_month_max",
    "industry",
    "company_size",
    "company_type",
    "degree_requirement",
    "major_requirement",
    "certificate_requirement",
    "hard_skills",
    "tools_or_tech_stack",
    "job_description_clean",
]
AI_SQL_PREFERRED_TABLES = ["job_detail", "jobs", "job_samples", "job_postings", "job_market_assets"]
AI_SQL_FIELD_ALIASES = {
    "company_name": ["company_name", "company_name_clean", "company_name_raw", "company", "companyName", "公司名称"],
    "city": ["city", "work_city", "工作城市", "城市"],
    "standard_job_name": ["standard_job_name", "standard_job_name_y", "standard_job_name_x", "job_name", "normalized_job_name"],
    "job_title": ["job_title", "job_name_clean", "job_name_raw", "title", "position_name", "岗位名称"],
    "job_title_norm": ["job_title_norm", "normalized_job_name", "job_name_clean", "job_name_raw"],
    "salary_min": ["salary_month_min", "salary_min", "min_salary"],
    "salary_max": ["salary_month_max", "salary_max", "max_salary"],
    "salary_month_min": ["salary_month_min", "salary_min", "min_salary"],
    "salary_month_max": ["salary_month_max", "salary_max", "max_salary"],
    "industry": ["industry", "industry_name", "行业"],
    "company_size": ["company_size", "company_size_norm", "company_scale", "公司规模"],
    "company_type": ["company_type", "company_type_norm", "企业类型"],
    "degree_requirement": ["degree_requirement", "学历要求"],
    "major_requirement": ["major_requirement", "major_requirement_json", "专业要求"],
    "certificate_requirement": ["certificate_requirement", "certificate_requirement_json", "证书要求"],
    "hard_skills": ["hard_skills", "hard_skills_json", "专业技能", "技能要求"],
    "tools_or_tech_stack": ["tools_or_tech_stack", "tools_or_tech_stack_json", "技术栈"],
    "job_description_clean": ["job_description_clean", "job_desc_clean", "job_desc_raw", "岗位描述"],
}
AI_SQL_FORBIDDEN_PATTERN = re.compile(
    r"\b(insert|update|delete|drop|alter|create|attach|detach|pragma|vacuum|replace|truncate|grant|revoke|execute|exec)\b",
    re.IGNORECASE,
)
AI_SQL_FROM_JOIN_PATTERN = re.compile(r"\b(?:from|join)\s+([a-zA-Z_][\w]*)", re.IGNORECASE)
PERSONAL_CONTEXT_PATTERN = re.compile(r"我|我的|根据我的|按我的|当前|这个岗位|结合我的")
CITY_SUFFIX_PATTERN = re.compile(r"([\u4e00-\u9fff]{2,12}(?:市|省|自治区|特别行政区|自治州|地区|盟|县|区))")
COMMON_CITY_HINTS = [
    "北京",
    "上海",
    "广州",
    "深圳",
    "杭州",
    "苏州",
    "南京",
    "成都",
    "武汉",
    "西安",
    "重庆",
    "天津",
    "长沙",
    "郑州",
    "青岛",
    "厦门",
    "宁波",
    "福州",
    "东莞",
    "佛山",
    "珠海",
    "哈尔滨",
]


class ManualResumeRequest(BaseModel):
    resume_text: str
    file_name: str = "manual_resume.txt"


class ReportUpdateRequest(BaseModel):
    report_text: str


class JobDataProcessRequest(BaseModel):
    input_file: str = DEFAULT_INPUT_FILE
    intermediate_dir: str = DEFAULT_INTERMEDIATE_DIR
    sql_db_path: str = DEFAULT_SQL_DB_PATH
    neo4j_output_dir: str = DEFAULT_NEO4J_OUTPUT_DIR
    knowledge_output_dir: str = DEFAULT_KNOWLEDGE_OUTPUT_DIR
    sheet_name: Union[str, int] = 0
    log_every: int = 50
    max_workers: int = 4
    group_sample_size: int = DEFAULT_GROUP_SAMPLE_SIZE
    embedding_model: str = DEFAULT_HASH_EMBEDDING_MODEL


class AIChatRequest(BaseModel):
    message: str
    conversation_id: str = ""
    web_search_enabled: bool = False


class TargetJobConfirmRequest(BaseModel):
    requested_job_name: str
    confirmed_standard_job_name: str


def _clean_text(value):
    if value is None:
        return ""
    return str(value).strip()


def _safe_dict(value):
    return value if isinstance(value, dict) else {}


def _safe_list(value):
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    return [value]


def _state_file_path() -> Path:
    return Path(__file__).resolve().with_name(STATE_FILE)


def _report_file_path() -> Path:
    return Path(__file__).resolve().with_name(REPORT_FILE_NAME)


def _pipeline_status_file_path() -> Path:
    return _state_file_path().with_name(PIPELINE_STATUS_FILE)


def _report_snapshot_dir() -> Path:
    path = Path(__file__).resolve().with_name(REPORT_SNAPSHOT_DIR_NAME)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _project_root() -> Path:
    return Path(__file__).resolve().parent


def _resolve_project_path(path_value: str) -> str:
    path = Path(path_value)
    if path.is_absolute():
        return str(path)
    return str((_project_root() / path).resolve())


def _load_all_data() -> dict:
    state_path = _state_file_path()
    if not state_path.exists():
        return {}
    with state_path.open("r", encoding="utf-8") as f:
        loaded = json.load(f)
    return loaded if isinstance(loaded, dict) else {}


def _write_all_data(all_data: dict) -> None:
    state_path = _state_file_path()
    with state_path.open("w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)


def _write_report_file(report_text: str) -> None:
    if not _clean_text(report_text):
        return
    _report_file_path().write_text(report_text, encoding="utf-8")


def _state_last_updated() -> str:
    state_path = _state_file_path()
    if not state_path.exists():
        return ""
    return datetime.fromtimestamp(state_path.stat().st_mtime, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _build_report_snapshot_payload(report_detail: dict, file_name: str) -> dict:
    return {
        "file_name": file_name,
        "report_title": _clean_text(report_detail.get("report_title")),
        "report_summary": _clean_text(report_detail.get("report_summary")),
        "report_text": _clean_text(report_detail.get("report_text")),
        "report_sections": _safe_list(report_detail.get("report_sections")),
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


def _is_safe_report_snapshot_name(file_name: str) -> bool:
    sanitized = os.path.basename(_clean_text(file_name))
    if sanitized != _clean_text(file_name):
        return False
    return bool(REPORT_SNAPSHOT_PATTERN.fullmatch(sanitized))


def _load_report_snapshot(file_name: str) -> dict:
    sanitized = os.path.basename(_clean_text(file_name))
    if not _is_safe_report_snapshot_name(sanitized):
        raise HTTPException(status_code=400, detail="非法报告文件名")

    target_path = _report_snapshot_dir() / sanitized
    if not target_path.exists():
        raise HTTPException(status_code=404, detail="报告已过期或不存在")

    try:
        with target_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"报告读取失败: {exc}") from exc

    if not isinstance(payload, dict) or not _clean_text(payload.get("report_text")):
        raise HTTPException(status_code=404, detail="报告已过期或不存在")
    return payload


def _write_report_snapshot(report_detail: dict) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    unique_name = f"report_{timestamp}.json"
    payload = _build_report_snapshot_payload(report_detail, unique_name)

    report_dir = _report_snapshot_dir()
    unique_path = report_dir / unique_name
    latest_path = report_dir / LATEST_REPORT_SNAPSHOT_FILE

    with unique_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    with latest_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return unique_name


def _load_pipeline_status() -> dict:
    path = _pipeline_status_file_path()
    if not path.exists():
        return {
            "status": "idle",
            "current_step": 0,
            "total_steps": 6,
            "step_name": "未开始",
            "error": None,
            "updated_at": "",
        }
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return {
            "status": "idle",
            "current_step": 0,
            "total_steps": 6,
            "step_name": "未开始",
            "error": "进度文件读取失败",
            "updated_at": "",
        }
    return payload if isinstance(payload, dict) else {
        "status": "idle",
        "current_step": 0,
        "total_steps": 6,
        "step_name": "未开始",
        "error": "进度文件格式错误",
        "updated_at": "",
    }


def _ai_memory_file_path() -> Path:
    return _state_file_path().with_name(AI_MEMORY_FILE)


def _ai_context_file_paths() -> Dict[str, Path]:
    base_dir = _project_root()
    return {
        "report_data": base_dir / "shared_reports" / "report_data.json",
        "student_profile": base_dir / "outputs" / "state" / "student_profile_service_result.json",
        "job_match": base_dir / "outputs" / "state" / "job_match_service_result.json",
        "career_path": base_dir / "outputs" / "state" / "career_path_plan_service_result.json",
        "job_skill_knowledge_assets": base_dir / "outputs" / "match_assets" / "job_skill_knowledge_assets.json",
    }


def _safe_read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            loaded = json.load(f)
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return {}


def _match_asset_file_paths() -> Dict[str, Path]:
    asset_dir = _project_root() / "outputs" / "match_assets"
    return {
        "core_jobs": asset_dir / "core_jobs.json",
        "requirement_stats": asset_dir / "job_requirement_stats.json",
        "skill_assets": asset_dir / "job_skill_knowledge_assets.json",
        "manifest": asset_dir / "match_assets_manifest.json",
    }


def _build_job_profile_assets_payload() -> Dict[str, Any]:
    """读取岗位画像后处理资产，组装成前端可直接消费的全局岗位画像。"""
    paths = _match_asset_file_paths()
    core_root = _safe_read_json(paths["core_jobs"])
    requirement_root = _safe_read_json(paths["requirement_stats"])
    skill_root = _safe_read_json(paths["skill_assets"])
    manifest = _safe_read_json(paths["manifest"])

    missing_assets = [
        name
        for name, path in paths.items()
        if name != "manifest" and not path.exists()
    ]
    requirement_jobs = _safe_dict(requirement_root.get("jobs"))
    skill_jobs = _safe_dict(skill_root.get("jobs"))

    core_job_profiles = []
    for core_job in _safe_list(core_root.get("jobs")):
        core_job_dict = _safe_dict(core_job)
        job_name = _clean_text(core_job_dict.get("standard_job_name"))
        if not job_name:
            continue
        stats = _safe_dict(requirement_jobs.get(job_name))
        skills = _safe_dict(skill_jobs.get(job_name))
        core_job_profiles.append(
            {
                "standard_job_name": job_name,
                "sample_count": core_job_dict.get("sample_count") or stats.get("sample_count", 0),
                "job_category": _clean_text(core_job_dict.get("job_category") or stats.get("job_category")),
                "job_level_summary": _clean_text(
                    core_job_dict.get("job_level_summary") or stats.get("job_level_summary")
                ),
                "display_order": core_job_dict.get("display_order"),
                "selection_reason": _clean_text(core_job_dict.get("selection_reason")),
                "mainstream_degree": _clean_text(
                    core_job_dict.get("mainstream_degree") or stats.get("mainstream_degree")
                ),
                "mainstream_majors_summary": core_job_dict.get("mainstream_majors_summary")
                or _safe_list(stats.get("mainstream_majors")),
                "mainstream_cert_summary": core_job_dict.get("mainstream_cert_summary")
                or _safe_list(stats.get("mainstream_certificates")),
                "top_skills": _safe_list(core_job_dict.get("top_skills")),
                "degree_distribution": _safe_list(stats.get("degree_distribution")),
                "major_distribution": _safe_list(stats.get("major_distribution")),
                "certificate_distribution": _safe_list(stats.get("certificate_distribution")),
                "no_certificate_requirement_ratio": stats.get("no_certificate_requirement_ratio", 0.0),
                "degree_gate": _clean_text(stats.get("degree_gate")),
                "major_gate_set": _safe_list(stats.get("major_gate_set")),
                "must_have_certificates": _safe_list(stats.get("must_have_certificates")),
                "preferred_certificates": _safe_list(stats.get("preferred_certificates")),
                "hard_skills": _safe_list(skills.get("hard_skills")),
                "tools_or_tech_stack": _safe_list(skills.get("tools_or_tech_stack")),
                "required_knowledge_points": _safe_list(skills.get("required_knowledge_points")),
                "preferred_knowledge_points": _safe_list(skills.get("preferred_knowledge_points")),
                "source_quality": _safe_dict(stats.get("source_quality")),
            }
        )

    generated_at = (
        _clean_text(manifest.get("generated_at"))
        or _clean_text(core_root.get("generated_at"))
        or _clean_text(requirement_root.get("generated_at"))
        or _clean_text(skill_root.get("generated_at"))
    )
    summary = {
        "core_job_count": len(core_job_profiles),
        "standard_job_count": len(requirement_jobs),
        "sample_count": manifest.get("sample_count") or 0,
        "generated_at": generated_at,
    }
    return {
        "summary": summary,
        "core_job_profiles": core_job_profiles,
        "missing_assets": missing_assets,
    }


def _truncate_for_ai(value: Any, max_chars: int = AI_CONTEXT_CHUNK_MAX_CHARS) -> str:
    text = _clean_text(value)
    if len(text) <= max_chars:
        return text
    return f"{text[: max_chars - 1]}…"


def _extract_top_city_names(value: Any, limit: int = 5) -> List[str]:
    cities: List[str] = []
    for item in _safe_list(value):
        item_dict = _safe_dict(item)
        if item_dict:
            city_name = _clean_text(
                item_dict.get("city")
                or item_dict.get("city_name")
                or item_dict.get("name")
                or item_dict.get("value")
            )
        else:
            city_name = _clean_text(item)
        if not city_name or city_name in cities:
            continue
        cities.append(city_name)
        if len(cities) >= limit:
            break
    return cities


def _format_salary_stats_text(stats_value: Any) -> str:
    salary_stats = _safe_dict(stats_value)
    salary_mid = salary_stats.get("salary_mid_month_avg")
    salary_min = salary_stats.get("salary_min_month_avg")
    salary_max = salary_stats.get("salary_max_month_avg")
    if salary_mid not in (None, ""):
        return f"月均约 {salary_mid}"
    if salary_min not in (None, "") or salary_max not in (None, ""):
        return f"{salary_min or '?'} - {salary_max or '?'} / 月"
    return "暂无薪资信息"


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_json_load_list(text_value: str) -> List[Any]:
    normalized_text = _clean_text(text_value)
    if not normalized_text:
        return []
    try:
        parsed = json.loads(normalized_text)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


def _query_job_market_snapshot_via_sql(job_skill_assets_path: Path, target_job: str) -> Dict[str, Any]:
    if not job_skill_assets_path or (not job_skill_assets_path.exists()):
        return {}

    payload = _safe_read_json(job_skill_assets_path)
    jobs_root = _safe_dict(payload.get("jobs"))
    normalized_target_job = _clean_text(target_job)
    if not jobs_root or not normalized_target_job:
        return {}

    conn = sqlite3.connect(":memory:")
    try:
        conn.execute(
            """
            CREATE TABLE job_market_assets (
                standard_job_name TEXT PRIMARY KEY,
                salary_min_month_avg REAL,
                salary_max_month_avg REAL,
                salary_mid_month_avg REAL,
                city_distribution_json TEXT,
                top_cities_json TEXT,
                required_knowledge_points_json TEXT,
                preferred_knowledge_points_json TEXT
            )
            """
        )

        rows = []
        for key, raw_item in jobs_root.items():
            item = _safe_dict(raw_item)
            standard_job_name = _clean_text(item.get("standard_job_name") or key)
            if not standard_job_name:
                continue

            salary_stats = _safe_dict(item.get("salary_stats"))
            if not salary_stats:
                group_summary = _safe_dict(item.get("group_summary"))
                salary_stats = {
                    "salary_min_month_avg": group_summary.get("salary_min_month_avg"),
                    "salary_max_month_avg": group_summary.get("salary_max_month_avg"),
                    "salary_mid_month_avg": item.get("salary_mid_month_avg"),
                }

            city_distribution = item.get("city_distribution") or []
            top_cities = item.get("top_cities") or []
            required_knowledge_points = _safe_list(item.get("required_knowledge_points"))
            preferred_knowledge_points = _safe_list(item.get("preferred_knowledge_points"))

            rows.append(
                (
                    standard_job_name,
                    _safe_float(salary_stats.get("salary_min_month_avg")),
                    _safe_float(salary_stats.get("salary_max_month_avg")),
                    _safe_float(salary_stats.get("salary_mid_month_avg")),
                    json.dumps(_safe_list(city_distribution), ensure_ascii=False),
                    json.dumps(_safe_list(top_cities), ensure_ascii=False),
                    json.dumps(required_knowledge_points, ensure_ascii=False),
                    json.dumps(preferred_knowledge_points, ensure_ascii=False),
                )
            )

        if not rows:
            return {}

        conn.executemany(
            """
            INSERT OR REPLACE INTO job_market_assets(
                standard_job_name,
                salary_min_month_avg,
                salary_max_month_avg,
                salary_mid_month_avg,
                city_distribution_json,
                top_cities_json,
                required_knowledge_points_json,
                preferred_knowledge_points_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                standard_job_name,
                salary_min_month_avg,
                salary_max_month_avg,
                salary_mid_month_avg,
                city_distribution_json,
                top_cities_json,
                required_knowledge_points_json,
                preferred_knowledge_points_json
            FROM job_market_assets
            WHERE standard_job_name = ?
            LIMIT 1
            """,
            (normalized_target_job,),
        )
        row = cursor.fetchone()
        if not row:
            cursor.execute(
                """
                SELECT
                    standard_job_name,
                    salary_min_month_avg,
                    salary_max_month_avg,
                    salary_mid_month_avg,
                    city_distribution_json,
                    top_cities_json,
                    required_knowledge_points_json,
                    preferred_knowledge_points_json
                FROM job_market_assets
                WHERE standard_job_name LIKE ?
                ORDER BY LENGTH(standard_job_name) ASC
                LIMIT 1
                """,
                (f"%{normalized_target_job}%",),
            )
            row = cursor.fetchone()

        if not row:
            return {}

        city_distribution = _safe_json_load_list(row[4])
        top_cities = _safe_json_load_list(row[5])
        required_knowledge_points = [
            _clean_text(item) for item in _safe_json_load_list(row[6]) if _clean_text(item)
        ]
        preferred_knowledge_points = [
            _clean_text(item) for item in _safe_json_load_list(row[7]) if _clean_text(item)
        ]
        top_city_names = _extract_top_city_names(top_cities or city_distribution)

        salary_stats = {
            "salary_min_month_avg": row[1],
            "salary_max_month_avg": row[2],
            "salary_mid_month_avg": row[3],
        }
        return {
            "recommended_job": _clean_text(row[0]),
            "recommended_job_salary": _format_salary_stats_text(salary_stats),
            "recommended_job_location": "、".join(top_city_names) if top_city_names else "",
            "recommended_job_required_knowledge_points": required_knowledge_points[:8],
            "recommended_job_preferred_knowledge_points": preferred_knowledge_points[:8],
            "recommended_job_source": "job_skill_knowledge_assets_sql",
        }
    except sqlite3.Error as exc:
        logger.warning(f"SQL query on job_skill_knowledge_assets failed: {exc}")
        return {}
    finally:
        conn.close()


def _pick_first_text_by_keys(payloads: List[Dict[str, Any]], keys: List[str]) -> str:
    for payload in payloads:
        payload_dict = _safe_dict(payload)
        if not payload_dict:
            continue
        for key in keys:
            value = _clean_text(payload_dict.get(key))
            if value:
                return value
    return ""


def _extract_city_from_text(text: str) -> str:
    normalized = _clean_text(text)
    if not normalized:
        return ""
    city_match = CITY_SUFFIX_PATTERN.search(normalized)
    if city_match:
        return _clean_text(city_match.group(1))
    for city in COMMON_CITY_HINTS:
        if city in normalized:
            return city
    return ""


def _extract_city_from_keyword_lines(text: str, keywords: List[str]) -> str:
    normalized = _clean_text(text)
    if not normalized:
        return ""
    for line in re.split(r"[\r\n]+", normalized):
        line_text = _clean_text(line)
        if not line_text:
            continue
        if not any(keyword in line_text for keyword in keywords):
            continue
        line_parts = re.split(r"[:：]", line_text, maxsplit=1)
        candidate = _clean_text(line_parts[1] if len(line_parts) > 1 else line_text)
        if not candidate:
            continue
        city = _extract_city_from_text(candidate)
        if city:
            return city
        if len(candidate) <= 20:
            return candidate
    return ""


def _build_user_location_snapshot(student_profile_raw: Dict[str, Any]) -> Dict[str, Any]:
    student_state = _safe_dict(student_profile_raw.get("student_state"))
    resume_parse_result = _safe_dict(student_state.get("resume_parse_result"))
    resume_basic_info = _safe_dict(resume_parse_result.get("basic_info"))

    profile_input_payload = _safe_dict(student_profile_raw.get("profile_input_payload"))
    payload_basic_info = _safe_dict(profile_input_payload.get("basic_info"))
    explicit_profile = _safe_dict(profile_input_payload.get("explicit_profile"))
    source_snapshot = _safe_dict(profile_input_payload.get("source_snapshot"))
    source_resume_parse = _safe_dict(source_snapshot.get("resume_parse_result"))

    payload_candidates = [
        resume_parse_result,
        resume_basic_info,
        payload_basic_info,
        explicit_profile,
        source_resume_parse,
    ]
    user_address_keys = [
        "address",
        "current_address",
        "current_city",
        "location",
        "city",
        "residence",
        "residence_city",
        "home_address",
        "home_city",
        "live_city",
        "living_city",
        "hometown",
        "所在地",
        "现居地",
        "居住地",
        "地址",
    ]
    intention_address_keys = [
        "target_city",
        "expected_city",
        "desired_city",
        "job_city",
        "target_location",
        "expected_location",
        "intent_city",
        "employment_intention_city",
        "意向城市",
        "期望城市",
        "就业意愿地址",
        "工作地点",
        "求职地点",
    ]

    user_address = _pick_first_text_by_keys(payload_candidates, user_address_keys)
    employment_intention_address = _pick_first_text_by_keys(payload_candidates, intention_address_keys)
    target_job_intention = _clean_text(
        resume_parse_result.get("target_job_intention")
        or source_resume_parse.get("target_job_intention")
        or explicit_profile.get("target_job_intention")
        or payload_basic_info.get("target_job")
    )

    raw_resume_text = _clean_text(
        resume_parse_result.get("raw_resume_text")
        or source_resume_parse.get("raw_resume_text")
    )
    if not user_address:
        user_address = _extract_city_from_keyword_lines(
            raw_resume_text,
            ["现居", "居住", "所在地", "住址", "地址", "location"],
        )
    if not employment_intention_address:
        employment_intention_address = _extract_city_from_keyword_lines(
            raw_resume_text,
            ["意向城市", "期望城市", "期望地点", "工作地点", "就业意愿", "求职意向", "求职地点"],
        )
    if not employment_intention_address:
        employment_intention_address = _extract_city_from_text(target_job_intention)

    return {
        "user_address": user_address or "未提供",
        "employment_intention_address": employment_intention_address or "未提供",
        "target_job_intention": target_job_intention or "未提供",
    }


def _build_recommended_job_market_snapshot(
    job_match: Dict[str, Any],
    career_path: Dict[str, Any],
    job_skill_assets_path: Optional[Path] = None,
) -> Dict[str, Any]:
    recommended_job_match = _safe_dict(job_match.get("recommended_job_match"))
    recommendation_ranking = _safe_list(job_match.get("recommendation_ranking"))
    ranking_first = _safe_dict(recommendation_ranking[0]) if recommendation_ranking else {}

    career_plan_input = _safe_dict(career_path.get("career_plan_input_payload"))
    planner_context = _safe_dict(career_plan_input.get("planner_context"))
    market_fact_snapshot = _safe_dict(planner_context.get("market_fact_snapshot"))

    recommended_job = _clean_text(
        career_path.get("primary_target_job")
        or recommended_job_match.get("job_name")
        or ranking_first.get("job_name")
    )

    top_cities = _extract_top_city_names(market_fact_snapshot.get("top_cities"))
    salary_text = _format_salary_stats_text(market_fact_snapshot.get("salary_stats"))

    match_input_payload = _safe_dict(job_match.get("match_input_payload"))
    matching_guidance = _safe_dict(match_input_payload.get("matching_guidance"))
    job_profile = _safe_dict(match_input_payload.get("job_profile"))

    if not top_cities:
        top_cities = _extract_top_city_names(matching_guidance.get("city_distribution"))
    if not top_cities:
        top_cities = _extract_top_city_names(job_profile.get("city_distribution"))

    if salary_text == "暂无薪资信息":
        salary_text = _format_salary_stats_text(matching_guidance.get("salary_stats"))
    if salary_text == "暂无薪资信息":
        salary_text = _format_salary_stats_text(job_profile.get("salary_stats"))

    sql_market_snapshot: Dict[str, Any] = {}
    if job_skill_assets_path:
        sql_market_snapshot = _query_job_market_snapshot_via_sql(job_skill_assets_path, recommended_job)
        sql_job_name = _clean_text(sql_market_snapshot.get("recommended_job"))
        sql_location = _clean_text(sql_market_snapshot.get("recommended_job_location"))
        sql_salary = _clean_text(sql_market_snapshot.get("recommended_job_salary"))
        if sql_job_name:
            recommended_job = sql_job_name
        if sql_location:
            top_cities = _extract_top_city_names(sql_location.split("、"))
        if sql_salary and sql_salary != "暂无薪资信息":
            salary_text = sql_salary

    location_text = "、".join(top_cities) if top_cities else "暂无地点信息"
    return {
        "recommended_job": recommended_job or "未提供推荐岗位",
        "recommended_job_location": location_text,
        "recommended_job_salary": salary_text,
        "recommended_job_required_knowledge_points": _safe_list(
            sql_market_snapshot.get("recommended_job_required_knowledge_points")
        )[:8],
        "recommended_job_preferred_knowledge_points": _safe_list(
            sql_market_snapshot.get("recommended_job_preferred_knowledge_points")
        )[:8],
        "recommended_job_source": _clean_text(sql_market_snapshot.get("recommended_job_source")) or "state_snapshot",
    }


def _extract_query_keywords(question: str) -> List[str]:
    text = _clean_text(question)
    if not text:
        return []
    keywords: List[str] = []
    english_words = re.findall(r"[a-zA-Z0-9_]{2,}", text.lower())
    chinese_words = re.findall(r"[\u4e00-\u9fff]{2,}", text)
    for item in english_words + chinese_words:
        if item not in keywords:
            keywords.append(item)
    return keywords[:20]


def _build_ai_context_snapshot() -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {
        "loaded_files": [],
        "missing_files": [],
        "student_profile": {},
        "job_match": {},
        "career_path": {},
        "report_data": {},
    }
    file_paths = _ai_context_file_paths()

    report_payload = _safe_read_json(file_paths["report_data"])
    if report_payload:
        report_sections = []
        for section in _safe_list(report_payload.get("report_sections"))[:8]:
            section_dict = _safe_dict(section)
            title = _clean_text(section_dict.get("section_title") or section_dict.get("title"))
            content = _truncate_for_ai(section_dict.get("section_content") or section_dict.get("content"), 320)
            if title:
                report_sections.append({"title": title, "content": content})
        snapshot["report_data"] = {
            "report_title": _clean_text(report_payload.get("report_title")),
            "report_summary": _truncate_for_ai(report_payload.get("report_summary"), 500),
            "report_text_preview": _truncate_for_ai(report_payload.get("report_text"), 500),
            "report_sections": report_sections,
        }
        snapshot["loaded_files"].append("report_data.json")
    else:
        snapshot["missing_files"].append("report_data.json")

    student_profile_raw = _safe_read_json(file_paths["student_profile"])
    student_profile = _safe_dict(student_profile_raw.get("student_profile_result")) or student_profile_raw
    if student_profile:
        snapshot["student_profile"] = {
            "summary": _truncate_for_ai(student_profile.get("summary"), 500),
            "score_level": _clean_text(student_profile.get("score_level")),
            "complete_score": student_profile.get("complete_score"),
            "competitiveness_score": student_profile.get("competitiveness_score"),
            "strengths": _safe_list(student_profile.get("strengths"))[:5],
            "weaknesses": _safe_list(student_profile.get("weaknesses"))[:5],
            "preferred_directions": _safe_list(
                _safe_dict(student_profile.get("potential_profile")).get("preferred_directions")
            )[:5],
        }
        snapshot["student_profile"].update(_build_user_location_snapshot(student_profile_raw))
        snapshot["loaded_files"].append("student_profile_service_result.json")
    else:
        snapshot["missing_files"].append("student_profile_service_result.json")

    job_match_raw = _safe_read_json(file_paths["job_match"])
    job_match = _safe_dict(job_match_raw.get("job_match_result")) or job_match_raw
    if job_match:
        snapshot["job_match"] = {
            "overall_match_score": job_match.get("overall_match_score", job_match.get("overall_score")),
            "score_level": _clean_text(job_match.get("score_level")),
            "analysis_summary": _truncate_for_ai(job_match.get("analysis_summary"), 500),
            "recommendation": _truncate_for_ai(job_match.get("recommendation"), 500),
            "improvement_suggestions": _safe_list(job_match.get("improvement_suggestions"))[:5],
            "dimension_scores": {
                "basic_requirement_score": job_match.get("basic_requirement_score"),
                "vocational_skill_score": job_match.get("vocational_skill_score"),
                "professional_quality_score": job_match.get("professional_quality_score"),
                "development_potential_score": job_match.get("development_potential_score"),
            },
        }
        snapshot["loaded_files"].append("job_match_service_result.json")
    else:
        snapshot["missing_files"].append("job_match_service_result.json")

    career_path_raw = _safe_read_json(file_paths["career_path"])
    career_path = _safe_dict(career_path_raw.get("career_path_plan_result")) or career_path_raw
    if career_path:
        snapshot["career_path"] = {
            "primary_target_job": _clean_text(career_path.get("primary_target_job")),
            "goal_positioning": _truncate_for_ai(career_path.get("goal_positioning"), 500),
            "decision_summary": _truncate_for_ai(career_path.get("decision_summary"), 500),
            "short_term_plan": _safe_list(career_path.get("short_term_plan"))[:5],
            "mid_term_plan": _safe_list(career_path.get("mid_term_plan"))[:5],
            "fallback_strategy": _truncate_for_ai(career_path.get("fallback_strategy"), 500),
        }
        snapshot["loaded_files"].append("career_path_plan_service_result.json")
    else:
        snapshot["missing_files"].append("career_path_plan_service_result.json")

    if file_paths["job_skill_knowledge_assets"].exists():
        snapshot["loaded_files"].append("job_skill_knowledge_assets.json")
    else:
        snapshot["missing_files"].append("job_skill_knowledge_assets.json")

    snapshot["job_match"].update(
        _build_recommended_job_market_snapshot(
            job_match,
            career_path,
            file_paths["job_skill_knowledge_assets"],
        )
    )

    return snapshot


def _build_context_chunks(snapshot: Dict[str, Any]) -> List[Dict[str, str]]:
    chunks: List[Dict[str, str]] = []

    student_profile = _safe_dict(snapshot.get("student_profile"))
    if student_profile:
        chunks.append(
            {
                "source": "student_profile",
                "title": "学生画像摘要",
                "text": _truncate_for_ai(student_profile.get("summary"), 500),
            }
        )
        chunks.append(
            {
                "source": "student_profile",
                "title": "学生画像分数",
                "text": (
                    f"score_level={student_profile.get('score_level')}；"
                    f"complete_score={student_profile.get('complete_score')}；"
                    f"competitiveness_score={student_profile.get('competitiveness_score')}"
                ),
            }
        )
        chunks.append(
            {
                "source": "student_profile",
                "title": "用户地域与求职意向",
                "text": (
                    f"用户地址={_clean_text(student_profile.get('user_address')) or '未提供'}；"
                    f"就业意愿地址={_clean_text(student_profile.get('employment_intention_address')) or '未提供'}；"
                    f"目标岗位意向={_clean_text(student_profile.get('target_job_intention')) or '未提供'}"
                ),
            }
        )
        if student_profile.get("strengths"):
            chunks.append(
                {
                    "source": "student_profile",
                    "title": "学生画像优势",
                    "text": "；".join([_clean_text(item) for item in _safe_list(student_profile.get("strengths"))[:5]]),
                }
            )
        if student_profile.get("weaknesses"):
            chunks.append(
                {
                    "source": "student_profile",
                    "title": "学生画像短板",
                    "text": "；".join([_clean_text(item) for item in _safe_list(student_profile.get("weaknesses"))[:5]]),
                }
            )

    job_match = _safe_dict(snapshot.get("job_match"))
    if job_match:
        chunks.append(
            {
                "source": "job_match",
                "title": "人岗匹配结论",
                "text": _truncate_for_ai(job_match.get("analysis_summary"), 500),
            }
        )
        chunks.append(
            {
                "source": "job_match",
                "title": "人岗匹配推荐",
                "text": _truncate_for_ai(job_match.get("recommendation"), 500),
            }
        )
        chunks.append(
            {
                "source": "job_match",
                "title": "推荐岗位地域与薪资",
                "text": (
                    f"推荐岗位={_clean_text(job_match.get('recommended_job')) or '未提供推荐岗位'}；"
                    f"岗位地点={_clean_text(job_match.get('recommended_job_location')) or '暂无地点信息'}；"
                    f"岗位薪资={_clean_text(job_match.get('recommended_job_salary')) or '暂无薪资信息'}"
                ),
            }
        )
        if job_match.get("recommended_job_required_knowledge_points"):
            required_points = [
                _clean_text(item)
                for item in _safe_list(job_match.get("recommended_job_required_knowledge_points"))
                if _clean_text(item)
            ]
            if required_points:
                chunks.append(
                    {
                        "source": "job_match",
                        "title": "推荐岗位关键知识点",
                        "text": "；".join(required_points[:8]),
                    }
                )

    career_path = _safe_dict(snapshot.get("career_path"))
    if career_path:
        chunks.append(
            {
                "source": "career_path",
                "title": "职业路径总结",
                "text": _truncate_for_ai(career_path.get("decision_summary"), 500),
            }
        )
        if career_path.get("short_term_plan"):
            chunks.append(
                {
                    "source": "career_path",
                    "title": "短期行动计划",
                    "text": "；".join([_clean_text(item) for item in _safe_list(career_path.get("short_term_plan"))[:5]]),
                }
            )

    report_data = _safe_dict(snapshot.get("report_data"))
    if report_data:
        chunks.append(
            {
                "source": "report_data",
                "title": "报告摘要",
                "text": _truncate_for_ai(report_data.get("report_summary"), 500),
            }
        )
        for section in _safe_list(report_data.get("report_sections"))[:4]:
            section_dict = _safe_dict(section)
            title = _clean_text(section_dict.get("title"))
            content = _clean_text(section_dict.get("content"))
            if title and content:
                chunks.append(
                    {
                        "source": "report_data",
                        "title": f"报告章节:{title}",
                        "text": _truncate_for_ai(content, 380),
                    }
                )

    return [item for item in chunks if _clean_text(item.get("text"))]


def _score_context_chunk(chunk_text: str, keywords: List[str]) -> int:
    if not keywords:
        return 0
    normalized_text = _clean_text(chunk_text).lower()
    score = 0
    for keyword in keywords:
        normalized_kw = _clean_text(keyword).lower()
        if not normalized_kw:
            continue
        if normalized_kw in normalized_text:
            score += 2 if len(normalized_kw) >= 3 else 1
    return score


def _retrieve_context_chunks(question: str, snapshot: Dict[str, Any]) -> List[Dict[str, str]]:
    chunks = _build_context_chunks(snapshot)
    if not chunks:
        return []

    keywords = _extract_query_keywords(question)
    scored = []
    for index, chunk in enumerate(chunks):
        score = _score_context_chunk(chunk.get("text", ""), keywords)
        scored.append((score, index, chunk))

    scored.sort(key=lambda item: (item[0], -item[1]), reverse=True)
    selected = [item[2] for item in scored if item[0] > 0][:AI_MAX_CONTEXT_CHUNKS]

    # 若用户问题是第一人称/指代式，强制补充档案上下文。
    if PERSONAL_CONTEXT_PATTERN.search(_clean_text(question)):
        needed_sources = {"student_profile", "job_match", "career_path", "report_data"}
        selected_sources = {_clean_text(item.get("source")) for item in selected}
        for chunk in chunks:
            source = _clean_text(chunk.get("source"))
            if source in needed_sources and source not in selected_sources:
                selected.append(chunk)
                selected_sources.add(source)
            if len(selected) >= AI_MAX_CONTEXT_CHUNKS:
                break

    if not selected:
        selected = chunks[:AI_MAX_CONTEXT_CHUNKS]

    return selected[:AI_MAX_CONTEXT_CHUNKS]


def _format_context_for_prompt(
    snapshot: Dict[str, Any],
    chunks: List[Dict[str, str]],
    semantic_hits: Optional[List[Dict[str, Any]]] = None,
    sql_context: Optional[Dict[str, Any]] = None,
    path_graph_context: Optional[Dict[str, Any]] = None,
) -> str:
    loaded_files = "、".join(_safe_list(snapshot.get("loaded_files"))) or "无"
    missing_files = "、".join(_safe_list(snapshot.get("missing_files"))) or "无"
    context_lines = [
        f"已加载文件：{loaded_files}",
        f"缺失文件：{missing_files}",
        "召回片段：",
    ]
    if not chunks:
        context_lines.append("- 无可用片段（请先完成学生画像/匹配/路径/报告生成流程）")
    else:
        for chunk in chunks:
            context_lines.append(
                f"- [{chunk.get('source')}] {chunk.get('title')}: {chunk.get('text')}"
            )
    context_lines.append("语义知识召回：")
    if not semantic_hits:
        context_lines.append("- 无可用岗位语义知识片段（请先构建 JSON + embedding 知识库）")
    else:
        for hit in semantic_hits[:AI_SEMANTIC_TOP_K]:
            context_lines.append(
                f"- [semantic_kb] {hit.get('standard_job_name')} "
                f"(score={hit.get('score')}): {hit.get('doc_text_excerpt')}"
            )
    context_lines.append("SQL 查询上下文：")
    sql_context_dict = _safe_dict(sql_context)
    sql_context_text = _clean_text(sql_context_dict.get("context_text"))
    if sql_context_text:
        context_lines.extend(sql_context_text.splitlines())
    else:
        context_lines.append("- 未启用 SQL 查询或当前问题不需要结构化查询")
    context_lines.append("岗位路径图谱上下文：")
    path_context_dict = _safe_dict(path_graph_context)
    if bool(path_context_dict.get("enabled")):
        summary_text = _clean_text(path_context_dict.get("summary_text"))
        if summary_text:
            context_lines.append(f"- {summary_text}")
        for edge in _safe_list(path_context_dict.get("matched_edges"))[:AI_SQL_CONTEXT_ROWS]:
            edge_dict = _safe_dict(edge)
            context_lines.append(
                f"- {edge_dict.get('source_job')} -> {edge_dict.get('target_job')} "
                f"({edge_dict.get('label') or edge_dict.get('relation')})"
            )
    else:
        context_lines.append("- 未启用路径图谱查询或当前问题不需要图谱路径")
    return "\n".join(context_lines)


def _load_ai_memory_store() -> Dict[str, Any]:
    path = _ai_memory_file_path()
    if not path.exists():
        return {"conversations": {}}
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, dict) and isinstance(payload.get("conversations"), dict):
            return payload
    except Exception:
        pass
    return {"conversations": {}}


def _save_ai_memory_store(store: Dict[str, Any]) -> None:
    path = _ai_memory_file_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False, indent=2)


def _ensure_conversation_id(store: Dict[str, Any], conversation_id: str) -> str:
    normalized_id = _clean_text(conversation_id) or f"conv_{uuid.uuid4().hex[:12]}"
    conversations = _safe_dict(store.get("conversations"))
    if normalized_id not in conversations:
        conversations[normalized_id] = {
            "messages": [],
            "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
    store["conversations"] = conversations
    return normalized_id


def _append_conversation_message(store: Dict[str, Any], conversation_id: str, role: str, content: str) -> None:
    conversations = _safe_dict(store.get("conversations"))
    conversation = _safe_dict(conversations.get(conversation_id))
    messages = _safe_list(conversation.get("messages"))
    messages.append(
        {
            "role": role,
            "content": _clean_text(content),
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
    )
    conversation["messages"] = messages[-AI_MAX_HISTORY_MESSAGES:]
    conversation["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    conversations[conversation_id] = conversation
    store["conversations"] = conversations


def _get_recent_history(store: Dict[str, Any], conversation_id: str) -> List[Dict[str, str]]:
    conversations = _safe_dict(store.get("conversations"))
    conversation = _safe_dict(conversations.get(conversation_id))
    history: List[Dict[str, str]] = []
    for item in _safe_list(conversation.get("messages")):
        item_dict = _safe_dict(item)
        role = _clean_text(item_dict.get("role"))
        content = _clean_text(item_dict.get("content"))
        if role in {"user", "assistant"} and content:
            history.append({"role": role, "content": content})
    return history[-AI_MAX_HISTORY_MESSAGES:]


def _build_context_summary_line(snapshot: Dict[str, Any]) -> str:
    profile = _safe_dict(snapshot.get("student_profile"))
    match = _safe_dict(snapshot.get("job_match"))
    career = _safe_dict(snapshot.get("career_path"))

    summary_parts = []
    if _clean_text(profile.get("summary")):
        summary_parts.append("已读取学生画像")
    if match.get("overall_match_score") not in (None, ""):
        summary_parts.append(f"已读取岗位匹配（总分 {match.get('overall_match_score')}）")
    if _clean_text(career.get("primary_target_job")):
        summary_parts.append(f"已读取职业路径（目标岗位：{career.get('primary_target_job')}）")

    if not summary_parts:
        summary_parts.append("尚未读取到可用档案，请先完成画像/匹配/路径/报告生成")

    return "；".join(summary_parts)


def _semantic_knowledge_dir() -> Path:
    return _project_root() / "outputs" / "knowledge"


def _build_ai_semantic_query(question: str, snapshot: Dict[str, Any]) -> str:
    question_text = _clean_text(question)
    profile = _safe_dict(snapshot.get("student_profile"))
    match = _safe_dict(snapshot.get("job_match"))
    career = _safe_dict(snapshot.get("career_path"))
    report_data = _safe_dict(snapshot.get("report_data"))

    parts = [f"用户问题：{question_text}"]
    if _clean_text(career.get("primary_target_job")):
        parts.append(f"当前目标岗位：{career.get('primary_target_job')}")
    if _clean_text(profile.get("summary")):
        parts.append(f"学生画像摘要：{profile.get('summary')}")
    preferred_directions = _safe_list(profile.get("preferred_directions"))
    if preferred_directions:
        parts.append(f"偏好方向：{'、'.join([_clean_text(item) for item in preferred_directions[:5] if _clean_text(item)])}")
    if _clean_text(profile.get("user_address")):
        parts.append(f"用户地址：{profile.get('user_address')}")
    if _clean_text(profile.get("employment_intention_address")):
        parts.append(f"就业意愿地址：{profile.get('employment_intention_address')}")
    if _clean_text(match.get("analysis_summary")):
        parts.append(f"匹配分析：{match.get('analysis_summary')}")
    if _clean_text(match.get("recommendation")):
        parts.append(f"匹配建议：{match.get('recommendation')}")
    if _clean_text(match.get("recommended_job")):
        parts.append(f"推荐岗位：{match.get('recommended_job')}")
    if _clean_text(match.get("recommended_job_location")):
        parts.append(f"推荐岗位地点：{match.get('recommended_job_location')}")
    if _clean_text(match.get("recommended_job_salary")):
        parts.append(f"推荐岗位薪资：{match.get('recommended_job_salary')}")
    if _safe_list(match.get("recommended_job_required_knowledge_points")):
        points = [
            _clean_text(item)
            for item in _safe_list(match.get("recommended_job_required_knowledge_points"))
            if _clean_text(item)
        ]
        if points:
            parts.append(f"推荐岗位关键知识点：{'、'.join(points[:8])}")
    if _clean_text(report_data.get("report_summary")):
        parts.append(f"报告摘要：{report_data.get('report_summary')}")
    return "\n".join([item for item in parts if _clean_text(item)])


def _retrieve_semantic_hits(question: str, snapshot: Dict[str, Any], top_k: int = AI_SEMANTIC_TOP_K) -> List[Dict[str, Any]]:
    query_text = _build_ai_semantic_query(question, snapshot)
    if not _clean_text(query_text):
        return []
    try:
        retriever = SemanticJobKnowledgeRetriever(_semantic_knowledge_dir())
        return retriever.search(query_text=query_text, top_k=top_k, min_score=0.30)
    except FileNotFoundError:
        return []
    except Exception as exc:
        logger.warning(f"semantic retrieval unavailable for ai assistant: {exc}")
        return []


def _ai_sql_data_csv_path() -> Path:
    return _project_root() / "outputs" / "intermediate" / "jobs_extracted_full.csv"


def _ai_sql_data_db_path() -> Path:
    return _project_root() / DEFAULT_SQL_DB_PATH


def _read_sqlite_tables(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    tables: List[Dict[str, Any]] = []
    try:
        raw_tables = conn.execute(
            "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') ORDER BY name"
        ).fetchall()
    except sqlite3.Error:
        return tables

    for row in raw_tables:
        table_name = _clean_text(row[0])
        if not table_name or table_name.startswith("sqlite_"):
            continue
        try:
            columns = [
                _clean_text(item[1])
                for item in conn.execute(f"PRAGMA table_info({_quote_sql_identifier(table_name)})").fetchall()
                if _clean_text(item[1])
            ]
        except sqlite3.Error:
            columns = []
        try:
            row_count = int(conn.execute(f"SELECT COUNT(*) FROM {_quote_sql_identifier(table_name)}").fetchone()[0])
        except sqlite3.Error:
            row_count = 0
        tables.append(
            {
                "table_name": table_name,
                "type": _clean_text(row[1]),
                "row_count": row_count,
                "columns": columns,
            }
        )
    return tables


def _score_ai_sql_table(table: Dict[str, Any]) -> int:
    table_name = _clean_text(table.get("table_name"))
    columns = set(_safe_list(table.get("columns")))
    row_count = int(table.get("row_count") or 0)
    score = 0
    if row_count > 0:
        score += 100
    if table_name in AI_SQL_PREFERRED_TABLES:
        score += 80 - AI_SQL_PREFERRED_TABLES.index(table_name) * 8
    for aliases in AI_SQL_FIELD_ALIASES.values():
        if any(alias in columns for alias in aliases):
            score += 8
    score += min(row_count // 100, 30)
    return score


def inspect_jobs_db_schema(db_path: Path) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "available": False,
        "db_path": str(db_path),
        "tables": [],
        "preferred_table": "",
        "message": "",
    }
    if not db_path.exists() or db_path.stat().st_size <= 0:
        result["message"] = "jobs.db 不存在或为空"
        return result

    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            tables = _read_sqlite_tables(conn)
        finally:
            conn.close()
    except sqlite3.Error as exc:
        result["message"] = f"jobs.db 无法打开：{exc}"
        return result

    usable_tables = [table for table in tables if int(table.get("row_count") or 0) > 0]
    result["tables"] = tables
    if not usable_tables:
        result["message"] = "jobs.db 中没有可用数据表"
        return result

    preferred = sorted(usable_tables, key=_score_ai_sql_table, reverse=True)[0]
    if _score_ai_sql_table(preferred) < 120:
        result["message"] = "jobs.db 表结构不足以支撑岗位事实查询"
        return result

    result["available"] = True
    result["preferred_table"] = _clean_text(preferred.get("table_name"))
    result["message"] = "jobs.db 可用"
    return result


def _read_csv_header(csv_path: Path) -> List[str]:
    if not csv_path.exists():
        return []
    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            first_row = next(reader, [])
            return [_clean_text(item) for item in first_row if _clean_text(item)]
    except Exception:
        return []


def classify_ai_question_intent(question: str, history: Optional[List[Dict[str, str]]] = None) -> Dict[str, Any]:
    normalized = _clean_text(question)
    lowered = normalized.lower()
    result: Dict[str, Any] = {
        "intent": "general_advice",
        "confidence": 0.55,
        "query_domain": "local_context",
        "reason": "默认使用本地档案上下文回答",
        "should_use_sql": False,
        "should_use_path_graph": False,
        "should_use_semantic": False,
        "should_use_profile_context": True,
    }
    if not normalized:
        return result

    def _set(intent: str, confidence: float, domain: str, reason: str, sql: bool = False, path: bool = False, semantic: bool = False) -> Dict[str, Any]:
        result.update(
            {
                "intent": intent,
                "confidence": confidence,
                "query_domain": domain,
                "reason": reason,
                "should_use_sql": sql,
                "should_use_path_graph": path,
                "should_use_semantic": semantic,
                "should_use_profile_context": True,
            }
        )
        return result

    path_keywords = ["路径图谱", "岗位路径", "职业路径", "晋升", "转岗", "promote_to", "transfer_to", "图谱关系"]
    if any(keyword in lowered or keyword in normalized for keyword in path_keywords):
        return _set("career_path_question", 0.92, "path_graph", "用户询问岗位晋升/转岗/路径图谱关系", path=True)

    report_keywords = ["报告", "总结", "行动计划", "职业建议", "结论", "规划建议"]
    if any(keyword in normalized for keyword in report_keywords):
        return _set("report_question", 0.86, "report_state", "用户询问报告总结或行动建议")

    match_keywords = ["为什么推荐", "为什么不匹配", "匹配", "风险", "覆盖率", "赛题评测", "硬门槛", "最推荐"]
    if any(keyword in normalized for keyword in match_keywords):
        return _set("match_explanation", 0.88, "job_match_state", "用户询问人岗匹配、推荐原因或风险")

    profile_keywords = ["我的学历", "我的专业", "我的技能", "我的证书", "我的项目", "我的实习", "学生画像", "个人画像", "我会什么"]
    if any(keyword in normalized for keyword in profile_keywords):
        return _set("student_profile_question", 0.86, "student_profile_state", "用户询问学生画像或个人能力信息")

    requirement_keywords = ["岗位要求", "学历要求", "专业要求", "证书要求", "技能要求", "学历", "专业", "证书", "知识点", "需要什么", "要求是什么"]
    if any(keyword in normalized for keyword in requirement_keywords):
        return _set("job_requirement_question", 0.84, "semantic_job_knowledge", "用户询问岗位要求或知识点", semantic=True)

    company_keywords = ["公司", "企业", "厂", "投递清单", "投递公司"]
    salary_keywords = ["薪资", "工资", "收入", "月薪", "待遇", "k", "K", "万", "高薪"]
    city_keywords = ["城市", "地区", "地点", "北京", "上海", "广州", "深圳", "杭州", "成都", "南京", "苏州", "武汉"]
    industry_keywords = ["行业", "领域", "产业"]
    market_words = ["有哪些", "哪些", "查询", "筛选", "多少", "分布", "样本", "机会", "岗位"]
    if any(keyword in normalized for keyword in company_keywords):
        return _set("company_search", 0.9, "sql_market", "用户询问公司/企业样本", sql=True)
    if any(keyword in normalized for keyword in salary_keywords):
        return _set("salary_search", 0.88, "sql_market", "用户询问薪资或薪资排序", sql=True)
    if any(keyword in normalized for keyword in industry_keywords):
        return _set("industry_search", 0.82, "sql_market", "用户询问行业相关岗位样本", sql=True)
    if any(keyword in normalized for keyword in city_keywords) and any(word in normalized for word in market_words):
        return _set("city_market_search", 0.84, "sql_market", "用户询问城市岗位机会或本地样本", sql=True)

    return result


def _should_use_sql_query(question: str) -> bool:
    return bool(classify_ai_question_intent(question).get("should_use_sql"))


def _extract_sql_from_text(value: str) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    fenced = re.search(r"```(?:sql)?\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    if fenced:
        text = _clean_text(fenced.group(1))
    statements = [item.strip() for item in text.split(";") if _clean_text(item)]
    if len(statements) != 1:
        return ""
    return _clean_text(statements[0])


def _sanitize_readonly_sql(sql_text: str, table_name: str = AI_SQL_TABLE_NAME) -> str:
    sql = _extract_sql_from_text(sql_text)
    if not sql:
        return ""
    if "--" in sql or "/*" in sql or "*/" in sql:
        return ""
    if AI_SQL_FORBIDDEN_PATTERN.search(sql):
        return ""

    normalized = sql.strip()
    lowered = normalized.lower()
    if not lowered.startswith("select"):
        return ""

    table_names = AI_SQL_FROM_JOIN_PATTERN.findall(normalized)
    if not table_names:
        return ""
    for table in table_names:
        clean_table = _clean_text(table).strip('"`[]').lower()
        if clean_table != table_name.lower():
            return ""

    if " limit " not in f" {lowered} ":
        normalized = f"{normalized} LIMIT {AI_SQL_MAX_ROWS}"
    else:
        def _clamp_limit(match: re.Match) -> str:
            try:
                raw_value = int(match.group(1))
            except Exception:
                raw_value = AI_SQL_MAX_ROWS
            return f"LIMIT {min(max(raw_value, 1), AI_SQL_MAX_ROWS)}"

        normalized = re.sub(r"\blimit\s+(\d+)\b", _clamp_limit, normalized, flags=re.IGNORECASE)

    return normalized


def _convert_salary_token(value: str, unit: str) -> Optional[float]:
    number = _safe_float(value)
    if number is None:
        return None
    unit_text = _clean_text(unit).lower()
    if unit_text == "k":
        return number * 1000.0
    if unit_text in {"w", "万"}:
        return number * 10000.0
    return number


def _extract_salary_floor_from_question(question: str) -> Optional[float]:
    normalized = _clean_text(question)
    if not normalized:
        return None
    if not any(keyword in normalized for keyword in ["薪资", "工资", "收入", "月薪", "k", "K", "w", "W", "万"]):
        return None

    range_match = re.search(r"(\d+(?:\.\d+)?)\s*[-~到至]\s*(\d+(?:\.\d+)?)\s*(k|K|w|W|万)?", normalized)
    if range_match:
        return _convert_salary_token(range_match.group(1), range_match.group(3) or "")

    min_match = re.search(
        r"(?:不低于|不少于|至少|大于等于|>=|以上|达到|目标|期望|薪资|工资|月薪)\s*(\d+(?:\.\d+)?)\s*(k|K|w|W|万)?",
        normalized,
    )
    if min_match:
        return _convert_salary_token(min_match.group(1), min_match.group(2) or "")

    loose_match = re.search(r"(\d+(?:\.\d+)?)\s*(k|K|w|W|万)", normalized)
    if loose_match:
        return _convert_salary_token(loose_match.group(1), loose_match.group(2) or "")

    return None


def _extract_job_keyword_from_question(question: str) -> str:
    normalized = _clean_text(question)
    if not normalized:
        return ""

    lower_text = normalized.lower()
    priority_keywords = [
        "java",
        "python",
        "golang",
        "c++",
        "前端",
        "后端",
        "测试",
        "运维",
        "算法",
        "数据",
        "产品",
        "安卓",
        "android",
        "ios",
    ]
    for keyword in priority_keywords:
        if keyword in lower_text or keyword in normalized:
            return keyword

    pattern_match = re.search(r"([\u4e00-\u9fffA-Za-z0-9+#/]{2,20}(?:开发|测试|工程师|运维|算法|产品))", normalized)
    if pattern_match:
        return _clean_text(pattern_match.group(1))
    return ""


def _build_rule_based_sql(question: str, table_name: str = AI_SQL_TABLE_NAME) -> str:
    city = _extract_city_from_text(question)
    salary_floor = _extract_salary_floor_from_question(question)
    job_keyword = _extract_job_keyword_from_question(question)

    where_conditions = [
        "company_name IS NOT NULL",
        "TRIM(company_name) != ''",
    ]
    if city:
        city_sql = city.replace("'", "''")
        where_conditions.append(f"city LIKE '%{city_sql}%'")
    if salary_floor is not None:
        where_conditions.append(
            f"COALESCE(salary_month_max, salary_max, 0) >= {float(salary_floor):.2f}"
        )
    if job_keyword:
        keyword_sql = job_keyword.replace("'", "''")
        where_conditions.append(
            "(" +
            f"job_title LIKE '%{keyword_sql}%' OR "
            f"job_title_norm LIKE '%{keyword_sql}%' OR "
            f"standard_job_name_y LIKE '%{keyword_sql}%'" +
            ")"
        )

    where_sql = " AND ".join(where_conditions)
    return (
        "SELECT "
        "company_name, city, standard_job_name_y AS standard_job_name, job_title, "
        "salary_month_min, salary_month_max, industry, company_size, company_type "
        f"FROM {table_name} "
        f"WHERE {where_sql} "
        "ORDER BY COALESCE(salary_month_max, salary_max, 0) DESC, "
        "COALESCE(salary_month_min, salary_min, 0) DESC "
        f"LIMIT {AI_SQL_MAX_ROWS}"
    )


def _generate_sql_via_llm(question: str, history: List[Dict[str, str]], table_columns: List[str]) -> str:
    llm_config = DEFAULT_LLM_CONFIG
    columns_text = ", ".join(table_columns[:120])
    history_lines = []
    for item in history[-4:]:
        role = _clean_text(item.get("role"))
        content = _clean_text(item.get("content"))
        if role and content:
            history_lines.append(f"{role}: {content}")

    payload_messages = [
        {
            "role": "system",
            "content": (
                "你是 SQLite SQL 生成器，只输出一条 SQL，不要解释。"
                "必须满足：1) 只允许 SELECT；2) 只查询 jobs 表；3) 默认 LIMIT<=15；"
                "4) 优先返回公司、城市、岗位、薪资列；5) 不要输出 markdown。"
                f"\n可用字段：{columns_text}"
            ),
        },
        {
            "role": "user",
            "content": (
                f"对话历史（可选参考）：{'; '.join(history_lines) if history_lines else '无'}\n"
                f"用户当前问题：{_clean_text(question)}\n"
                "请生成一条可直接执行的 SQLite SQL。"
            ),
        },
    ]

    payload = {
        "model": llm_config.model_name,
        "temperature": 0.0,
        "max_tokens": 260,
        "messages": payload_messages,
    }
    headers = {"Content-Type": "application/json"}
    api_key = _clean_text(llm_config.api_key) or _clean_text(os.getenv(llm_config.api_key_env_name))
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    req = urllib_request.Request(
        llm_config.api_base_url.rstrip("/") + "/chat/completions",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers=headers,
        method="POST",
    )

    with urllib_request.urlopen(req, timeout=llm_config.timeout_seconds) as resp:
        response_payload = json.loads(resp.read().decode("utf-8"))

    content = _clean_text(
        _safe_dict(_safe_list(response_payload.get("choices"))[0]).get("message", {}).get("content")
        if _safe_list(response_payload.get("choices"))
        else ""
    )
    return _extract_sql_from_text(content)


def _plan_sql_query(question: str, history: List[Dict[str, str]], table_columns: List[str]) -> Dict[str, Any]:
    llm_error = ""
    llm_sql = ""
    try:
        llm_sql = _sanitize_readonly_sql(_generate_sql_via_llm(question, history, table_columns))
    except Exception as exc:
        llm_error = str(exc)

    if llm_sql:
        return {
            "sql": llm_sql,
            "source": "llm_sql",
            "llm_error": llm_error,
        }

    fallback_sql = _sanitize_readonly_sql(_build_rule_based_sql(question))
    return {
        "sql": fallback_sql,
        "source": "rule_fallback",
        "llm_error": llm_error or "LLM SQL 为空或未通过安全校验",
    }


def _quote_sql_identifier(name: str) -> str:
    return '"' + _clean_text(name).replace('"', '""') + '"'


def _find_column_name(columns: List[str], aliases: List[str]) -> str:
    column_lookup = {_clean_text(col).lower(): _clean_text(col) for col in columns if _clean_text(col)}
    for alias in aliases:
        matched = column_lookup.get(_clean_text(alias).lower())
        if matched:
            return matched
    return ""


def _sql_column_expr(table_alias: str, columns: List[str], semantic_name: str, default: str = "NULL") -> str:
    column = _find_column_name(columns, _safe_list(AI_SQL_FIELD_ALIASES.get(semantic_name)))
    if not column:
        return default
    return f"{table_alias}.{_quote_sql_identifier(column)}"


def _sql_nullif_text_expr(expr: str) -> str:
    expr_text = _clean_text(expr)
    if not expr_text or expr_text.upper() == "NULL":
        return "NULL"
    return f"NULLIF(TRIM(CAST({expr_text} AS TEXT)), '')"


def _sql_real_expr(expr: str) -> str:
    expr_text = _clean_text(expr)
    if not expr_text or expr_text.upper() == "NULL":
        return "NULL"
    return f"CAST(NULLIF(TRIM(CAST({expr_text} AS TEXT)), '') AS REAL)"


def _create_unified_jobs_view(conn: sqlite3.Connection, schema: Dict[str, Any]) -> str:
    preferred_table = _clean_text(schema.get("preferred_table"))
    tables = {
        _clean_text(table.get("table_name")): table
        for table in _safe_list(schema.get("tables"))
        if _clean_text(table.get("table_name"))
    }
    preferred_columns = _safe_list(_safe_dict(tables.get(preferred_table)).get("columns"))
    profile_columns = _safe_list(_safe_dict(tables.get("job_profile")).get("columns"))

    if not preferred_table or not preferred_columns:
        raise sqlite3.Error("jobs.db 未找到可用于岗位事实查询的明细表")

    join_clause = ""
    profile_alias = "d"
    if (
        preferred_table != "job_profile"
        and "job_profile" in tables
        and _find_column_name(preferred_columns, ["record_id"])
        and _find_column_name(profile_columns, ["record_id"])
    ):
        join_clause = (
            f" LEFT JOIN main.{_quote_sql_identifier('job_profile')} p "
            f"ON p.{_quote_sql_identifier('record_id')} = d.{_quote_sql_identifier('record_id')}"
        )
        profile_alias = "p"

    def detail_expr(semantic_name: str, default: str = "NULL") -> str:
        return _sql_column_expr("d", preferred_columns, semantic_name, default)

    def profile_expr(semantic_name: str, default: str = "NULL") -> str:
        if profile_alias == "p":
            expr = _sql_column_expr("p", profile_columns, semantic_name, "")
            if expr:
                return expr
        return detail_expr(semantic_name, default)

    standard_job_candidates = [
        detail_expr("standard_job_name", "NULL"),
        detail_expr("job_title_norm", "NULL"),
        detail_expr("job_title", "NULL"),
    ]
    standard_job_expr = "COALESCE(" + ", ".join(_sql_nullif_text_expr(expr) for expr in standard_job_candidates) + ")"
    company_name_expr = detail_expr("company_name", "''")
    select_exprs = [
        f"NULLIF({company_name_expr}, '') AS company_name",
        f"{detail_expr('city', 'NULL')} AS city",
        f"{standard_job_expr} AS standard_job_name",
        f"{standard_job_expr} AS standard_job_name_y",
        f"{standard_job_expr} AS standard_job_name_x",
        f"{detail_expr('job_title', 'NULL')} AS job_title",
        f"{detail_expr('job_title_norm', 'NULL')} AS job_title_norm",
        f"{_sql_real_expr(detail_expr('salary_min', 'NULL'))} AS salary_min",
        f"{_sql_real_expr(detail_expr('salary_max', 'NULL'))} AS salary_max",
        f"{_sql_real_expr(detail_expr('salary_month_min', 'NULL'))} AS salary_month_min",
        f"{_sql_real_expr(detail_expr('salary_month_max', 'NULL'))} AS salary_month_max",
        f"{detail_expr('industry', 'NULL')} AS industry",
        f"{detail_expr('company_size', 'NULL')} AS company_size",
        f"{detail_expr('company_type', 'NULL')} AS company_type",
        f"{profile_expr('degree_requirement', 'NULL')} AS degree_requirement",
        f"{profile_expr('major_requirement', 'NULL')} AS major_requirement",
        f"{profile_expr('certificate_requirement', 'NULL')} AS certificate_requirement",
        f"{profile_expr('hard_skills', 'NULL')} AS hard_skills",
        f"{profile_expr('tools_or_tech_stack', 'NULL')} AS tools_or_tech_stack",
        f"{detail_expr('job_description_clean', 'NULL')} AS job_description_clean",
    ]
    view_sql = (
        f"CREATE TEMP VIEW {_quote_sql_identifier(AI_SQL_TABLE_NAME)} AS "
        f"SELECT {', '.join(select_exprs)} "
        f"FROM main.{_quote_sql_identifier(preferred_table)} d{join_clause}"
    )
    conn.execute(view_sql)
    return preferred_table


def _execute_sql_on_jobs_db(db_path: Path, sql: str, max_rows: int = AI_SQL_MAX_ROWS) -> Dict[str, Any]:
    schema = inspect_jobs_db_schema(db_path)
    if not bool(schema.get("available")):
        return {
            "success": False,
            "error": _clean_text(schema.get("message")) or "jobs.db 不可用",
            "columns": [],
            "rows": [],
            "schema": schema,
        }

    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            db_table = _create_unified_jobs_view(conn, schema)
            cursor = conn.execute(sql)
            columns = [item[0] for item in (cursor.description or [])]
            fetched = cursor.fetchmany(max_rows)

            rows = []
            for row in fetched:
                row_dict = {}
                for col in columns:
                    value = row[col]
                    if isinstance(value, float) and value.is_integer():
                        value = int(value)
                    row_dict[col] = value
                rows.append(row_dict)

            return {
                "success": True,
                "error": "",
                "columns": columns,
                "rows": rows,
                "schema": schema,
                "db_table": db_table,
            }
        finally:
            conn.close()
    except Exception as exc:
        return {
            "success": False,
            "error": f"jobs.db 查询执行失败: {exc}",
            "columns": [],
            "rows": [],
            "schema": schema,
            "db_table": _clean_text(schema.get("preferred_table")),
        }


def _execute_sql_on_jobs_csv(csv_path: Path, sql: str, max_rows: int = AI_SQL_MAX_ROWS) -> Dict[str, Any]:
    if not csv_path.exists():
        return {"success": False, "error": f"SQL 数据源不存在：{csv_path.name}", "rows": [], "columns": []}

    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            fieldnames = [_clean_text(item) for item in (reader.fieldnames or []) if _clean_text(item)]
            if not fieldnames:
                return {"success": False, "error": "CSV 表头为空", "rows": [], "columns": []}

            real_columns = {
                "_source_index",
                "source_row_no",
                "salary_min",
                "salary_max",
                "salary_months",
                "salary_month_min",
                "salary_month_max",
                "company_size_min",
                "company_size_max",
                "completeness_score",
                "cs_filter_confidence",
                "cs_filter_cs_score",
                "cs_filter_non_cs_score",
                "confidence",
            }
            bool_columns = {
                "is_salary_negotiable",
                "is_abnormal",
                "is_duplicate_removed",
                "is_cs_related",
                "cs_filter_is_ambiguous_title",
                "is_same_standard_job",
                "extract_success",
            }

            conn = sqlite3.connect(":memory:")
            try:
                ddl_columns = []
                for col in fieldnames:
                    col_type = "REAL" if col in real_columns else ("INTEGER" if col in bool_columns else "TEXT")
                    ddl_columns.append(f"{_quote_sql_identifier(col)} {col_type}")
                conn.execute(f"CREATE TABLE {_quote_sql_identifier(AI_SQL_TABLE_NAME)} ({', '.join(ddl_columns)})")

                insert_sql = (
                    f"INSERT INTO {_quote_sql_identifier(AI_SQL_TABLE_NAME)} "
                    f"({', '.join(_quote_sql_identifier(col) for col in fieldnames)}) "
                    f"VALUES ({', '.join(['?'] * len(fieldnames))})"
                )

                batch_values = []
                for row in reader:
                    item_values = []
                    row_dict = row if isinstance(row, dict) else {}
                    for col in fieldnames:
                        raw_value = row_dict.get(col)
                        if col in real_columns:
                            item_values.append(_safe_float(raw_value))
                        elif col in bool_columns:
                            raw_text = _clean_text(raw_value).lower()
                            item_values.append(1 if raw_text in {"1", "true", "yes", "y"} else 0)
                        else:
                            item_values.append(_clean_text(raw_value))
                    batch_values.append(item_values)
                    if len(batch_values) >= 800:
                        conn.executemany(insert_sql, batch_values)
                        batch_values = []
                if batch_values:
                    conn.executemany(insert_sql, batch_values)

                cursor = conn.execute(sql)
                columns = [item[0] for item in (cursor.description or [])]
                fetched = cursor.fetchmany(max_rows)

                rows = []
                for row in fetched:
                    row_dict = {}
                    for index, col in enumerate(columns):
                        value = row[index] if index < len(row) else None
                        if isinstance(value, float) and value.is_integer():
                            value = int(value)
                        row_dict[col] = value
                    rows.append(row_dict)

                return {
                    "success": True,
                    "error": "",
                    "columns": columns,
                    "rows": rows,
                }
            finally:
                conn.close()
    except Exception as exc:
        return {
            "success": False,
            "error": f"SQL 查询执行失败: {exc}",
            "columns": [],
            "rows": [],
        }


def _truncate_sql_value(value: Any, max_chars: int = AI_SQL_MAX_VALUE_CHARS) -> str:
    text = _clean_text(value)
    if len(text) <= max_chars:
        return text
    return f"{text[: max_chars - 1]}…"


def _get_sql_row_value(row: Dict[str, Any], keys: List[str]) -> Any:
    row_dict = _safe_dict(row)
    for key in keys:
        value = row_dict.get(key)
        if _clean_text(value):
            return value
    return None


def _clean_sql_multi_value(value: Any, max_items: int = 3) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    parts = [
        _clean_text(part)
        for part in re.split(r"[,，、/；;]+", text)
        if _clean_text(part)
    ]
    unique_parts: List[str] = []
    for part in parts:
        if part not in unique_parts:
            unique_parts.append(part)
        if len(unique_parts) >= max_items:
            break
    return "、".join(unique_parts) if unique_parts else text


def _format_salary_number(value: Any) -> str:
    number = _safe_float(value)
    if number is None or number <= 0:
        return ""
    if number >= 1000:
        text = f"{number / 1000:.1f}".rstrip("0").rstrip(".")
        return f"{text}k"
    return str(int(number)) if float(number).is_integer() else f"{number:.1f}"


def _format_salary_range_from_row(row: Dict[str, Any]) -> str:
    min_value = _get_sql_row_value(row, ["salary_month_min", "salary_min"])
    max_value = _get_sql_row_value(row, ["salary_month_max", "salary_max"])
    min_text = _format_salary_number(min_value)
    max_text = _format_salary_number(max_value)
    if min_text and max_text:
        return f"{min_text}-{max_text}"
    if max_text:
        return f"最高约 {max_text}"
    if min_text:
        return f"{min_text} 起"
    return "暂无薪资"


def _infer_ai_chat_intent(question: str, sql_context: Optional[Dict[str, Any]] = None) -> str:
    normalized = _clean_text(question)
    if not normalized:
        return "general"
    if any(keyword in normalized for keyword in ["公司", "企业", "厂", "投递"]):
        return "company_search"
    if any(keyword in normalized for keyword in ["薪资", "工资", "收入", "月薪", "k", "K", "万"]):
        return "salary_job_search"
    if any(keyword in normalized for keyword in ["城市", "地区", "地点", "北京", "上海", "广州", "深圳"]):
        return "local_market_search"
    if bool(_safe_dict(sql_context).get("enabled")):
        return "job_market_search"
    return "general"


def _build_company_cards(rows: List[Dict[str, Any]], question: str, limit: int = 8) -> List[Dict[str, Any]]:
    cards: List[Dict[str, Any]] = []
    seen_companies = set()
    job_keyword = _extract_job_keyword_from_question(question)
    for row in rows:
        row_dict = _safe_dict(row)
        company_name = _clean_text(_get_sql_row_value(row_dict, ["company_name", "company_name_clean", "company_name_raw"]))
        if not company_name:
            continue
        city = _clean_text(_get_sql_row_value(row_dict, ["city", "work_city"]))
        dedupe_key = f"{company_name}|{city}".lower()
        if dedupe_key in seen_companies:
            continue
        seen_companies.add(dedupe_key)

        job_title = _clean_text(_get_sql_row_value(row_dict, ["job_title", "job_name_clean", "job_name_raw"]))
        standard_job_name = _clean_text(_get_sql_row_value(row_dict, ["standard_job_name", "standard_job_name_y", "standard_job_name_x"]))
        salary_range = _format_salary_range_from_row(row_dict)
        industry = _clean_sql_multi_value(_get_sql_row_value(row_dict, ["industry"]))
        company_size = _clean_sql_multi_value(_get_sql_row_value(row_dict, ["company_size"]))
        company_type = _clean_sql_multi_value(_get_sql_row_value(row_dict, ["company_type"]))
        direction_text = job_keyword or standard_job_name or job_title or "当前查询方向"
        reason_parts = []
        if city:
            reason_parts.append(f"位于{city}")
        if standard_job_name or job_title:
            reason_parts.append(f"岗位为{standard_job_name or job_title}")
        if salary_range != "暂无薪资":
            reason_parts.append(f"薪资区间{salary_range}")
        if industry:
            reason_parts.append(f"行业为{industry}")
        reason = "，".join(reason_parts) + f"，与“{direction_text}”查询条件相关。"

        cards.append(
            {
                "type": "company",
                "company_name": company_name,
                "city": city,
                "job_title": job_title,
                "standard_job_name": standard_job_name,
                "salary_range": salary_range,
                "industry": industry,
                "company_size": company_size,
                "company_type": company_type,
                "reason": reason,
                "match_reason": reason,
            }
        )
        if len(cards) >= limit:
            break
    return cards


def _build_sql_summary_stats(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    companies = set()
    cities: Dict[str, int] = {}
    industries: Dict[str, int] = {}
    salary_values: List[float] = []

    for row in rows:
        row_dict = _safe_dict(row)
        company_name = _clean_text(_get_sql_row_value(row_dict, ["company_name", "company_name_clean", "company_name_raw"]))
        if company_name:
            companies.add(company_name)
        city = _clean_text(_get_sql_row_value(row_dict, ["city"]))
        if city:
            cities[city] = cities.get(city, 0) + 1
        industry_text = _clean_sql_multi_value(_get_sql_row_value(row_dict, ["industry"]), max_items=5)
        for industry in [item for item in industry_text.split("、") if item]:
            industries[industry] = industries.get(industry, 0) + 1
        for key in ["salary_month_min", "salary_month_max", "salary_min", "salary_max"]:
            value = _safe_float(row_dict.get(key))
            if value is not None and value > 0:
                salary_values.append(value)

    top_industries = [
        {"name": name, "count": count}
        for name, count in sorted(industries.items(), key=lambda item: item[1], reverse=True)[:5]
    ]
    top_city = ""
    if cities:
        top_city = sorted(cities.items(), key=lambda item: item[1], reverse=True)[0][0]

    return {
        "company_count": len(companies),
        "job_count": len(rows),
        "top_city": top_city,
        "salary_max": max(salary_values) if salary_values else None,
        "salary_min": min(salary_values) if salary_values else None,
        "top_industries": top_industries,
    }


def _build_sql_result_table(rows: List[Dict[str, Any]], title: str = "候选公司列表") -> Dict[str, Any]:
    columns = ["公司", "城市", "岗位", "标准岗位", "薪资", "行业", "规模"]
    table_rows = []
    for row in rows[:AI_SQL_MAX_ROWS]:
        row_dict = _safe_dict(row)
        table_rows.append(
            {
                "公司": _clean_text(_get_sql_row_value(row_dict, ["company_name", "company_name_clean", "company_name_raw"])) or "暂无",
                "城市": _clean_text(_get_sql_row_value(row_dict, ["city"])) or "暂无",
                "岗位": _clean_text(_get_sql_row_value(row_dict, ["job_title", "job_name_clean", "job_name_raw"])) or "暂无",
                "标准岗位": _clean_text(_get_sql_row_value(row_dict, ["standard_job_name", "standard_job_name_y", "standard_job_name_x"])) or "暂无",
                "薪资": _format_salary_range_from_row(row_dict),
                "行业": _clean_sql_multi_value(_get_sql_row_value(row_dict, ["industry"])) or "暂无",
                "规模": _clean_sql_multi_value(_get_sql_row_value(row_dict, ["company_size"])) or "暂无",
            }
        )
    return {
        "title": title,
        "columns": columns,
        "rows": table_rows,
    }


def _build_query_condition_summary(question: str, sql_context: Dict[str, Any]) -> List[str]:
    conditions = []
    city = _extract_city_from_text(question)
    salary_floor = _extract_salary_floor_from_question(question)
    job_keyword = _extract_job_keyword_from_question(question)
    if city:
        conditions.append(f"城市：{city}")
    if job_keyword:
        conditions.append(f"岗位方向：{job_keyword}")
    if salary_floor is not None:
        conditions.append(f"薪资要求：{_format_salary_number(salary_floor)} 以上")
    data_source = _clean_text(sql_context.get("data_source"))
    if data_source == AI_SQL_DB_SOURCE:
        conditions.append("数据来源：jobs.db")
    elif data_source == AI_SQL_CSV_SOURCE:
        conditions.append("数据来源：CSV fallback")
    return conditions


def _should_use_semantic_for_intent(intent_result: Dict[str, Any]) -> bool:
    intent = _clean_text(intent_result.get("intent"))
    return bool(intent_result.get("should_use_semantic")) or intent in {"job_requirement_question", "match_explanation"}


def _extract_path_graph_keywords(question: str) -> List[str]:
    normalized = _clean_text(question)
    if not normalized:
        return []
    stop_words = {
        "岗位",
        "路径",
        "图谱",
        "关系",
        "晋升",
        "转岗",
        "有哪些",
        "什么",
        "查看",
        "查询",
        "职业",
    }
    keywords = []
    job_keyword = _extract_job_keyword_from_question(normalized)
    if job_keyword:
        keywords.append(job_keyword)
    for token in re.findall(r"[A-Za-z+#.]{2,20}|[\u4e00-\u9fff]{2,12}", normalized):
        cleaned = _clean_text(token)
        if cleaned and cleaned not in stop_words and cleaned not in keywords:
            keywords.append(cleaned)
    return keywords[:6]


def _build_path_graph_context(question: str, limit: int = 8) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "enabled": True,
        "path_graph_status": "unavailable",
        "source": "none",
        "stats": {},
        "matched_edges": [],
        "summary_text": "",
        "message": "",
    }
    try:
        from job_path_graph_service import build_full_job_path_graph

        graph = build_full_job_path_graph(project_root=_project_root(), prefer_neo4j=True)
    except Exception as exc:
        result["message"] = f"岗位路径图谱查询失败：{exc}"
        result["summary_text"] = "当前无法读取本地岗位路径图谱。"
        return result

    stats = _safe_dict(graph.get("stats"))
    edges = [_safe_dict(edge) for edge in _safe_list(graph.get("edges")) if _safe_dict(edge)]
    keywords = _extract_path_graph_keywords(question)
    normalized_question = _clean_text(question).lower()
    relation_filter = ""
    if "转岗" in normalized_question or "transfer_to" in normalized_question:
        relation_filter = "TRANSFER_TO"
    elif "晋升" in normalized_question or "promote_to" in normalized_question:
        relation_filter = "PROMOTE_TO"
    if relation_filter:
        edges = [edge for edge in edges if _clean_text(edge.get("relation")).upper() == relation_filter]

    def _edge_score(edge: Dict[str, Any]) -> int:
        source = _clean_text(edge.get("source_name") or edge.get("source"))
        target = _clean_text(edge.get("target_name") or edge.get("target"))
        relation = _clean_text(edge.get("relation"))
        combined = f"{source} {target} {relation}".lower()
        score = 0
        for keyword in keywords:
            if _clean_text(keyword).lower() in combined:
                score += 10
        if "PROMOTE" in relation:
            score += 2
        return score

    scored_edges = sorted(edges, key=lambda edge: (_edge_score(edge), _clean_text(edge.get("source"))), reverse=True)
    if keywords:
        matched_edges = [edge for edge in scored_edges if _edge_score(edge) > 0][:limit]
    else:
        matched_edges = scored_edges[:limit]

    normalized_edges = []
    for edge in matched_edges:
        relation = _clean_text(edge.get("relation"))
        normalized_edges.append(
            {
                "source_job": _clean_text(edge.get("source_name") or edge.get("source")),
                "target_job": _clean_text(edge.get("target_name") or edge.get("target")),
                "relation": relation,
                "label": "晋升" if relation == "PROMOTE_TO" else ("转岗" if relation == "TRANSFER_TO" else relation),
                "source": _clean_text(graph.get("source")),
            }
        )

    status = _clean_text(graph.get("graph_status")) or "unavailable"
    result.update(
        {
            "path_graph_status": status,
            "source": _clean_text(graph.get("source")) or "none",
            "stats": stats,
            "matched_edges": normalized_edges,
            "message": _clean_text(graph.get("message")),
        }
    )
    if normalized_edges:
        result["summary_text"] = (
            f"本地岗位路径图谱共包含 {stats.get('promote_edge_count', 0)} 条晋升关系、"
            f"{stats.get('transfer_edge_count', 0)} 条转岗关系；当前问题命中 {len(normalized_edges)} 条真实路径关系。"
        )
    elif status == "available":
        result["summary_text"] = "本地岗位路径图谱可用，但当前问题没有命中具体岗位路径；可尝试输入更明确的岗位名称。"
    else:
        result["summary_text"] = _clean_text(graph.get("message")) or "本地岗位路径图谱暂无可用关系。"
    return result


def _build_path_graph_product_answer(question: str, path_graph_context: Dict[str, Any]) -> str:
    path_context = _safe_dict(path_graph_context)
    stats = _safe_dict(path_context.get("stats"))
    edges = _safe_list(path_context.get("matched_edges"))
    source = _clean_text(path_context.get("source")) or "本地图谱"
    lines = [
        "📁 本地岗位路径图谱分析",
        f"本回答基于本地岗位路径图谱（来源：{source}），未使用联网搜索。",
    ]
    if stats:
        lines.append(
            f"图谱统计：岗位节点 {stats.get('job_node_count', 0)} 个，"
            f"晋升关系 {stats.get('promote_edge_count', 0)} 条，"
            f"转岗关系 {stats.get('transfer_edge_count', 0)} 条。"
        )
    if not edges:
        lines.append("当前问题没有命中可展示的真实岗位路径关系，系统不会强行生成不存在的路径。")
        message = _clean_text(path_context.get("message") or path_context.get("summary_text"))
        if message:
            lines.append(message)
        return "\n".join(lines)

    lines.append("命中的真实路径关系如下：")
    for index, edge in enumerate(edges[:8], start=1):
        edge_dict = _safe_dict(edge)
        lines.append(
            f"{index}. {edge_dict.get('source_job') or '未知岗位'} -> "
            f"{edge_dict.get('target_job') or '未知岗位'}（{edge_dict.get('label') or edge_dict.get('relation') or '关系'}）"
        )
    lines.append("说明：以上关系来自本地 Neo4j 或图谱 CSV 产物，只展示已有事实，不由 LLM 补造。")
    return "\n".join(lines)


def _build_sql_context_text(sql_plan: Dict[str, Any], sql_exec: Dict[str, Any], data_file_name: str) -> str:
    lines = [
        f"- 结构化查询数据源：{data_file_name}",
        f"- SQL 生成来源：{_clean_text(sql_plan.get('source')) or 'unknown'}",
    ]
    if _clean_text(sql_plan.get("llm_error")):
        lines.append(f"- SQL 生成备注：{_truncate_sql_value(sql_plan.get('llm_error'), 180)}")

    if not bool(sql_exec.get("success")):
        lines.append(f"- SQL 执行失败：{_clean_text(sql_exec.get('error')) or '未知错误'}")
        return "\n".join(lines)

    rows = _safe_list(sql_exec.get("rows"))
    lines.append(f"- SQL 查询结果行数：{len(rows)}")
    if not rows:
        lines.append("- 未查到匹配记录")
        return "\n".join(lines)

    lines.append(f"- SQL 结果预览（最多 {AI_SQL_CONTEXT_ROWS} 行）：")
    for row in rows[:AI_SQL_CONTEXT_ROWS]:
        row_dict = _safe_dict(row)
        if not row_dict:
            continue
        keys = list(row_dict.keys())[:8]
        row_text = "；".join(
            f"{key}={_truncate_sql_value(row_dict.get(key))}"
            for key in keys
        )
        lines.append(f"  - {row_text}")
    return "\n".join(lines)


def _build_sql_query_context(
    question: str,
    history: List[Dict[str, str]],
    intent_result: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "enabled": False,
        "context_text": "",
        "generated_sql": "",
        "sql_source": "",
        "row_count": 0,
        "rows": [],
        "columns": [],
        "company_cards": [],
        "summary_stats": {},
        "result_table": {},
        "error": "",
        "data_file": "",
        "data_source": "",
        "db_table": "",
        "schema": {},
    }

    intent = _safe_dict(intent_result) or classify_ai_question_intent(question, history)
    if not bool(intent.get("should_use_sql")):
        return result

    db_path = _ai_sql_data_db_path()
    csv_path = _ai_sql_data_csv_path()
    result["enabled"] = True

    db_schema = inspect_jobs_db_schema(db_path)
    use_db = bool(db_schema.get("available"))
    if use_db:
        result["data_source"] = AI_SQL_DB_SOURCE
        result["data_file"] = db_path.name
        result["db_table"] = _clean_text(db_schema.get("preferred_table"))
        result["schema"] = db_schema
        table_columns = AI_SQL_VIEW_COLUMNS
    else:
        result["data_source"] = AI_SQL_CSV_SOURCE
        result["data_file"] = csv_path.name
        result["schema"] = db_schema
        table_columns = _read_csv_header(csv_path)

    if not table_columns:
        result["error"] = f"SQL 数据源不可用：jobs.db 与 {csv_path.name} 均不可用"
        result["context_text"] = f"- SQL 数据源不可用：jobs.db 与 {csv_path.name} 均不可用"
        return result

    sql_plan = _plan_sql_query(question=question, history=history, table_columns=table_columns)
    sql_text = _clean_text(sql_plan.get("sql"))
    result["generated_sql"] = sql_text
    result["sql_source"] = _clean_text(sql_plan.get("source"))

    if not sql_text:
        result["error"] = _clean_text(sql_plan.get("llm_error")) or "SQL 生成失败"
        result["context_text"] = _build_sql_context_text(sql_plan, {"success": False, "error": result["error"]}, result["data_file"])
        return result

    sql_exec: Dict[str, Any]
    if use_db:
        sql_exec = _execute_sql_on_jobs_db(db_path=db_path, sql=sql_text, max_rows=AI_SQL_MAX_ROWS)
        if not bool(sql_exec.get("success")):
            csv_columns = _read_csv_header(csv_path)
            if csv_columns:
                csv_plan = _plan_sql_query(question=question, history=history, table_columns=csv_columns)
                csv_sql_text = _clean_text(csv_plan.get("sql"))
                if csv_sql_text:
                    sql_plan = csv_plan
                    sql_text = csv_sql_text
                    result["generated_sql"] = sql_text
                    result["sql_source"] = _clean_text(csv_plan.get("source"))
                    result["data_source"] = AI_SQL_CSV_SOURCE
                    result["data_file"] = csv_path.name
                    result["db_table"] = ""
                    sql_exec = _execute_sql_on_jobs_csv(csv_path=csv_path, sql=sql_text, max_rows=AI_SQL_MAX_ROWS)
    else:
        sql_exec = _execute_sql_on_jobs_csv(csv_path=csv_path, sql=sql_text, max_rows=AI_SQL_MAX_ROWS)

    rows = _safe_list(sql_exec.get("rows"))

    result["rows"] = rows
    result["columns"] = _safe_list(sql_exec.get("columns"))
    result["row_count"] = len(rows)
    result["error"] = _clean_text(sql_exec.get("error"))
    result["db_table"] = _clean_text(sql_exec.get("db_table")) or result["db_table"]
    result["schema"] = _safe_dict(sql_exec.get("schema")) or result["schema"]
    result["company_cards"] = _build_company_cards(rows, question)
    result["summary_stats"] = _build_sql_summary_stats(rows)
    result["result_table"] = _build_sql_result_table(rows)
    result["context_text"] = _build_sql_context_text(sql_plan, sql_exec, result["data_file"])

    return result


def _invoke_ai_chat_completion(
    user_message: str,
    history: List[Dict[str, str]],
    local_context_markdown: str,
) -> str:
    llm_config = DEFAULT_LLM_CONFIG
    payload_messages: List[Dict[str, str]] = [
        {
            "role": "system",
            "content": (
                "你是一位专业的职业规划 AI 助手。"
                "请优先使用【本地档案上下文】回答；如果上下文没有相关信息，要明确说明。\n"
                "回答时请尽量简洁，并标注信息来源（如：📁 本地分析）。"
                "不要在主回答中输出 SQL 原文、调试日志、完整召回片段列表或内部字段名；"
                "当前未启用联网搜索，回答必须说明基于本地数据；"
                "公司、薪资、城市、行业等事实只能基于结构化查询结果总结，不能凭空编造。\n\n"
                f"【本地档案上下文】\n{local_context_markdown}"
            ),
        }
    ]
    payload_messages.extend(history[-AI_MAX_HISTORY_MESSAGES:])
    payload_messages.append({"role": "user", "content": _clean_text(user_message)})

    payload = {
        "model": llm_config.model_name,
        "temperature": llm_config.temperature,
        "max_tokens": 1200,
        "messages": payload_messages,
    }

    headers = {"Content-Type": "application/json"}
    api_key = _clean_text(llm_config.api_key) or _clean_text(os.getenv(llm_config.api_key_env_name))
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    req = urllib_request.Request(
        llm_config.api_base_url.rstrip("/") + "/chat/completions",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers=headers,
        method="POST",
    )

    try:
        with urllib_request.urlopen(req, timeout=llm_config.timeout_seconds) as resp:
            response_payload = json.loads(resp.read().decode("utf-8"))
    except urllib_error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"AI HTTPError: status={exc.code}, reason={exc.reason}, body={error_body}") from exc

    answer = _clean_text(
        _safe_dict(_safe_list(response_payload.get("choices"))[0]).get("message", {}).get("content")
        if _safe_list(response_payload.get("choices"))
        else ""
    )
    if not answer or answer in {"{}", "[]"}:
        raise RuntimeError("AI 返回空内容")
    if answer.startswith("{") and answer.endswith("}"):
        raise RuntimeError("AI 返回了结构化 JSON，改走本地上下文回退")
    return answer


def _extract_name_from_snapshot(snapshot: Dict[str, Any]) -> str:
    report_data = _safe_dict(snapshot.get("report_data"))
    combined_text = "\n".join(
        [
            _clean_text(report_data.get("report_summary")),
            _clean_text(report_data.get("report_text_preview")),
            "\n".join(
                [
                    _clean_text(_safe_dict(item).get("content"))
                    for item in _safe_list(report_data.get("report_sections"))
                ]
            ),
        ]
    )
    match = re.search(r"学生姓名[:：]\s*([^\s，。；;]+)", combined_text)
    if match:
        return _clean_text(match.group(1))
    return ""


def _extract_mbti_from_snapshot(snapshot: Dict[str, Any]) -> str:
    report_data = _safe_dict(snapshot.get("report_data"))
    combined_text = "\n".join(
        [
            _clean_text(report_data.get("report_summary")),
            _clean_text(report_data.get("report_text_preview")),
            "\n".join(
                [
                    _clean_text(_safe_dict(item).get("content"))
                    for item in _safe_list(report_data.get("report_sections"))
                ]
            ),
        ]
    )
    match = re.search(r"MBTI[:：]\s*([A-Za-z]{4})", combined_text)
    if match:
        return _clean_text(match.group(1)).upper()
    return ""


def _looks_like_realtime_question(question: str) -> bool:
    normalized = _clean_text(question)
    if not normalized:
        return False
    realtime_keywords = ["今天", "最新", "实时", "天气", "新闻", "薪资", "招聘", "2026", "现在"]
    return any(keyword in normalized for keyword in realtime_keywords)


def _build_sql_product_answer(
    question: str,
    snapshot: Dict[str, Any],
    sql_query_context: Dict[str, Any],
    web_search_enabled: bool = False,
) -> str:
    sql_context = _safe_dict(sql_query_context)
    cards = _safe_list(sql_context.get("company_cards"))
    rows = _safe_list(sql_context.get("rows"))
    data_source = _clean_text(sql_context.get("data_source"))
    data_source_text = "jobs.db" if data_source == AI_SQL_DB_SOURCE else ("CSV fallback" if data_source == AI_SQL_CSV_SOURCE else "本地数据")
    conditions = _build_query_condition_summary(question, sql_context)
    match = _safe_dict(snapshot.get("job_match"))
    career = _safe_dict(snapshot.get("career_path"))
    recommended_job = (
        _clean_text(match.get("recommended_job"))
        or _clean_text(career.get("system_recommended_job"))
        or _clean_text(career.get("primary_target_job"))
    )

    lines = ["📁 本地岗位机会分析"]
    lines.append("本回答基于本地岗位样本数据，未使用联网搜索。")
    if conditions:
        lines.append("查询条件：" + "；".join(conditions))
    else:
        lines.append(f"数据来源：{data_source_text}")

    if not rows:
        error_text = _clean_text(sql_context.get("error"))
        if error_text:
            lines.append(f"当前没有形成可展示的岗位样本结果：{error_text}")
        else:
            lines.append("当前本地岗位样本中没有查到完全匹配的公司或岗位记录。")
        lines.append("你可以换成更具体的条件继续问，例如“北京 20k 以上有哪些前端岗位？”或“上海 Java 开发有哪些公司？”。")
        return "\n".join(lines)

    lines.append(f"我从{data_source_text}中查到 {len(rows)} 条候选岗位样本，并整理出以下公司优先关注：")
    for index, card in enumerate(cards[:5], start=1):
        card_dict = _safe_dict(card)
        lines.append(
            f"{index}. {card_dict.get('company_name') or '未知公司'}："
            f"{card_dict.get('city') or '城市未记录'}，"
            f"{card_dict.get('standard_job_name') or card_dict.get('job_title') or '岗位未记录'}，"
            f"薪资 {card_dict.get('salary_range') or '暂无薪资'}。"
        )

    if recommended_job:
        lines.append(
            f"结合当前档案，系统当前更关注的岗位方向是“{recommended_job}”。"
            "如果你想让结果更贴近个人匹配，可以继续追问“只看当前推荐岗位”或“按我的目标岗位筛选”。"
        )
    else:
        lines.append("如果你希望结果更贴近个人画像，可以继续补充岗位方向、城市或薪资要求。")

    lines.append("下一步你可以继续问：只看某个岗位方向、按薪资排序、只看本科可投，或帮你生成投递清单。")
    if (not web_search_enabled) and _looks_like_realtime_question(question):
        lines.append("说明：当前结果来自本地岗位样本，不是实时招聘网站数据。")
    return "\n".join(lines)


def _build_local_fallback_answer(
    question: str,
    snapshot: Dict[str, Any],
    chunks: List[Dict[str, str]],
    web_search_enabled: bool,
    semantic_hits: Optional[List[Dict[str, Any]]] = None,
    sql_query_context: Optional[Dict[str, Any]] = None,
) -> str:
    sql_context = _safe_dict(sql_query_context)
    if bool(sql_context.get("enabled")):
        return _build_sql_product_answer(
            question=question,
            snapshot=snapshot,
            sql_query_context=sql_context,
            web_search_enabled=web_search_enabled,
        )

    if not chunks:
        if semantic_hits:
            lines = ["📁 本地分析", "- 当前尚未加载完整档案，但已召回岗位语义知识片段："]
            for hit in semantic_hits[:AI_SEMANTIC_TOP_K]:
                lines.append(f"  - {hit.get('standard_job_name')}：{hit.get('doc_text_excerpt')}")
            return "\n".join(lines)
        return "📁 本地分析\n当前没有可用档案内容，请先完成学生画像、岗位匹配、职业路径和报告生成。"

    normalized_question = _clean_text(question)
    lines = ["📁 本地分析", "- 本回答基于本地学生画像、岗位匹配、路径规划或报告数据，未使用联网搜索。"]

    if "名字" in normalized_question or "姓名" in normalized_question:
        name = _extract_name_from_snapshot(snapshot)
        if name:
            lines.append(f"- 你的姓名是：{name}")
        else:
            lines.append("- 当前可用档案中未检索到明确姓名字段。")

    if "mbti" in normalized_question.lower() or "性格" in normalized_question:
        mbti = _extract_mbti_from_snapshot(snapshot)
        if mbti:
            lines.append(f"- 当前识别到的 MBTI 为：{mbti}")
        else:
            lines.append("- 当前档案中未找到明确 MBTI 字段。")

    career = _safe_dict(snapshot.get("career_path"))
    if "目标岗位" in normalized_question and _clean_text(career.get("primary_target_job")):
        lines.append(f"- 当前主目标岗位：{career.get('primary_target_job')}")

    profile = _safe_dict(snapshot.get("student_profile"))
    match = _safe_dict(snapshot.get("job_match"))
    if "匹配" in normalized_question and match.get("overall_match_score") not in (None, ""):
        lines.append(
            f"- 当前匹配分：{match.get('overall_match_score')}（{_clean_text(match.get('score_level'))}）"
        )
    if "推荐岗位" in normalized_question and _clean_text(match.get("recommended_job")):
        lines.append(f"- 当前推荐岗位：{match.get('recommended_job')}")
    if any(keyword in normalized_question for keyword in ["薪资", "工资", "收入"]):
        lines.append(f"- 推荐岗位薪资：{_clean_text(match.get('recommended_job_salary')) or '暂无薪资信息'}")
    if any(keyword in normalized_question for keyword in ["地点", "城市", "地区"]):
        lines.append(f"- 推荐岗位地点：{_clean_text(match.get('recommended_job_location')) or '暂无地点信息'}")
    if any(keyword in normalized_question for keyword in ["地址", "住址", "居住地"]):
        lines.append(f"- 用户地址：{_clean_text(profile.get('user_address')) or '未提供'}")
    if any(keyword in normalized_question for keyword in ["意愿地址", "意向地址", "期望地址", "就业意愿"]):
        lines.append(f"- 就业意愿地址：{_clean_text(profile.get('employment_intention_address')) or '未提供'}")

    sql_context = _safe_dict(sql_query_context)
    sql_rows = _safe_list(sql_context.get("rows"))
    sql_generated = _clean_text(sql_context.get("generated_sql"))
    if sql_generated:
        lines.append(f"- 已执行 SQL：{sql_generated}")
    if sql_rows:
        lines.append(f"- SQL 返回候选记录 {len(sql_rows)} 条（展示前 {min(5, len(sql_rows))} 条）：")
        for row in sql_rows[:5]:
            row_dict = _safe_dict(row)
            if not row_dict:
                continue
            keys = list(row_dict.keys())[:8]
            row_text = "；".join(
                f"{key}={_truncate_sql_value(row_dict.get(key))}"
                for key in keys
            )
            lines.append(f"  - {row_text}")
    elif _clean_text(sql_context.get("error")):
        lines.append(f"- SQL 查询状态：{sql_context.get('error')}")

    lines.append("- 结合当前问题召回到的关键档案片段：")
    for chunk in chunks[:4]:
        lines.append(f"  - [{chunk.get('source')}] {chunk.get('title')}：{chunk.get('text')}")

    if semantic_hits:
        lines.append("- 结合岗位语义知识库召回到的相关岗位片段：")
        for hit in semantic_hits[:AI_SEMANTIC_TOP_K]:
            lines.append(
                f"  - {hit.get('standard_job_name')}（相似度 {hit.get('score')}）：{hit.get('doc_text_excerpt')}"
            )

    if (not web_search_enabled) and _looks_like_realtime_question(normalized_question):
        lines.append("- 未开启联网搜索，我只能基于您当前的档案为您提供建议。")

    return "\n".join(lines)


def _normalize_section_list(value):
    if isinstance(value, list):
        result = []
        for item in value:
            item_dict = _safe_dict(item)
            title = _clean_text(item_dict.get("section_title") or item_dict.get("title"))
            content = _clean_text(item_dict.get("section_content") or item_dict.get("content"))
            if title:
                result.append({"section_title": title, "section_content": content})
        return result

    if isinstance(value, dict):
        result = []
        for key, item in value.items():
            item_dict = _safe_dict(item)
            title = _clean_text(item_dict.get("section_title") or item_dict.get("title") or key)
            content = _clean_text(item_dict.get("section_content") or item_dict.get("content") or item)
            if title:
                result.append({"section_title": title, "section_content": content})
        return result
    return []


def _build_resume_response(all_data: dict) -> dict:
    resume_res = _safe_dict(all_data.get("resume_parse_result"))
    basic_info = _safe_dict(resume_res.get("basic_info"))
    response = dict(resume_res)

    for field in ("name", "gender", "phone", "email", "school", "major", "degree", "graduation_year"):
        response[field] = _clean_text(response.get(field) or basic_info.get(field))

    response["skills"] = _safe_list(response.get("skills"))
    response["certificates"] = _safe_list(response.get("certificates"))
    response["project_experience"] = _safe_list(response.get("project_experience"))
    response["internship_experience"] = _safe_list(response.get("internship_experience"))

    response["position"] = _clean_text(
        response.get("position")
        or response.get("target_job_intention")
        or _safe_dict(all_data.get("basic_info")).get("target_job")
    )
    response["education"] = " / ".join(
        item
        for item in [
            _clean_text(response.get("school")),
            _clean_text(response.get("major")),
            _clean_text(response.get("degree")),
        ]
        if item
    )
    project_count = len(response["project_experience"])
    internship_count = len(response["internship_experience"])
    if project_count or internship_count:
        response["experience"] = f"{internship_count} 段实习 / {project_count} 个项目"
    else:
        response["experience"] = "暂无明确经历摘要"
    return response


def _format_salary_text(job_profile_result: dict) -> str:
    salary_stats = _safe_dict(job_profile_result.get("salary_stats"))
    salary_mid = salary_stats.get("salary_mid_month_avg")
    salary_min = salary_stats.get("salary_min_month_avg")
    salary_max = salary_stats.get("salary_max_month_avg")
    if salary_mid not in (None, ""):
        return f"月均约 {salary_mid}"
    if salary_min not in (None, "") or salary_max not in (None, ""):
        return f"{salary_min or '?'} - {salary_max or '?'} / 月"
    return "暂无薪资信息"


def _extract_report_text(report_res: dict) -> str:
    return _clean_text(report_res.get("report_text_markdown") or report_res.get("report_text"))


def _target_job_needs_confirmation(all_data: dict) -> bool:
    match_res = _safe_dict(all_data.get("job_match_result"))
    target_match = _safe_dict(match_res.get("target_job_match"))
    target_assets = _safe_dict(_safe_dict(all_data.get("job_profile_result")).get("target_job_profile_assets"))
    return (
        _clean_text(target_match.get("evaluation_status")) == "needs_confirmation"
        or _clean_text(target_match.get("resolution_status")) == "needs_confirmation"
        or _clean_text(_safe_dict(target_match.get("job_name_resolution")).get("resolution_status")) == "needs_confirmation"
        or _clean_text(target_assets.get("evaluation_status")) == "needs_confirmation"
        or _clean_text(target_assets.get("resolution_status")) == "needs_confirmation"
    )


def _build_report_detail(all_data: dict) -> dict:
    if _target_job_needs_confirmation(all_data):
        target_match = _safe_dict(_safe_dict(all_data.get("job_match_result")).get("target_job_match"))
        requested = (
            _clean_text(_safe_dict(target_match.get("job_name_resolution")).get("requested_job_name"))
            or _clean_text(target_match.get("job_name"))
            or _resolve_state_target_job(all_data)
        )
        message = (
            f"当前目标岗位“{requested}”尚未完成本地标准岗位确认。"
            "请先在人岗匹配页面选择一个最接近的本地标准岗位，再生成完整职业规划报告。"
        )
        return {
            "file_name": REPORT_FILE_NAME,
            "report_title": "职业规划报告待生成",
            "report_summary": message,
            "report_text": f"# 职业规划报告待生成\n\n{message}\n",
            "report_sections": [
                {
                    "section_title": "目标岗位待确认",
                    "section_content": message,
                }
            ],
            "edit_suggestions": ["确认本地标准岗位后重新生成职业路径和职业报告。"],
            "completeness_check": {"is_complete": False, "missing_sections": ["目标岗位标准化确认"]},
        }

    report_res = _safe_dict(all_data.get("career_report_result"))
    report_text = _extract_report_text(report_res)
    if report_text:
        _write_report_file(report_text)
    return {
        "file_name": _clean_text(all_data.get("latest_shared_report_file")) or REPORT_FILE_NAME,
        "report_title": _clean_text(report_res.get("report_title")) or "职业规划报告",
        "report_summary": _clean_text(
            report_res.get("report_summary")
            or report_res.get("action_summary")
            or report_res.get("match_summary")
            or report_res.get("path_summary")
        ),
        "report_text": report_text,
        "report_sections": _normalize_section_list(report_res.get("report_sections")),
        "edit_suggestions": _safe_list(report_res.get("edit_suggestions")),
        "completeness_check": _safe_dict(report_res.get("completeness_check")),
    }


def _resolve_state_target_job(all_data: dict) -> str:
    candidates = [
        _safe_dict(all_data.get("target_job_confirmation")).get("confirmed_standard_job_name"),
        _safe_dict(all_data.get("career_path_plan_result")).get("primary_target_job"),
        _safe_dict(all_data.get("job_profile_result")).get("standard_job_name"),
        _safe_dict(all_data.get("resume_parse_result")).get("target_job_intention"),
        _safe_dict(all_data.get("basic_info")).get("target_job"),
    ]
    for item in candidates:
        cleaned = _clean_text(item)
        if cleaned:
            return cleaned
    return "未明确目标岗位"

@app.post("/api/resume/parse")
async def parse_resume(resume: UploadFile = File(...)):
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{resume.filename}") as tmp:
            shutil.copyfileobj(resume.file, tmp)
            tmp_path = tmp.name
        
        # 跑全流水线 (目前前端一键跑完，我们放在这一步调用后端 pipeline)
        run_pipeline(tmp_path, "", str(_state_file_path()))
        
        # 返回第一步的数据
        all_data = _load_all_data()
        if all_data:
            resume_res = _build_resume_response(all_data)
        else:
            resume_res = {"name": "Test", "skills": [], "certificates": [], "project_experience": [], "internship_experience": []}
            
        return {
            "success": True,
            "message": "success",
            "data": resume_res
        }
    except Exception as e:
        logger.error(f"Error parsing resume: {e}")
        return {"success": False, "message": str(e), "data": {}}
    finally:
        try:
            if 'tmp_path' in locals() and os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except OSError:
            pass


@app.post("/api/resume/manual")
async def parse_manual_resume(req: ManualResumeRequest):
    resume_text = _clean_text(req.resume_text)
    if not resume_text:
        return {"success": False, "message": "简历文本不能为空", "data": {}}

    tmp_path = ""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt", mode="w", encoding="utf-8") as tmp:
            tmp.write(resume_text)
            tmp_path = tmp.name

        run_pipeline(tmp_path, "", str(_state_file_path()))
        all_data = _load_all_data()
        resume_res = _build_resume_response(all_data) if all_data else {}
        return {"success": True, "message": "success", "data": resume_res}
    except Exception as e:
        logger.error(f"Error parsing manual resume: {e}")
        return {"success": False, "message": str(e), "data": {}}
    finally:
        try:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except OSError:
            pass


@app.post("/api/data/process")
async def process_job_data(req: JobDataProcessRequest):
    try:
        result = await asyncio.to_thread(
            run_job_data_pipeline,
            input_path=_resolve_project_path(req.input_file),
            intermediate_dir=_resolve_project_path(req.intermediate_dir),
            sql_db_path=_resolve_project_path(req.sql_db_path),
            neo4j_output_dir=_resolve_project_path(req.neo4j_output_dir),
            knowledge_output_dir=_resolve_project_path(req.knowledge_output_dir),
            sheet_name=req.sheet_name,
            log_every=req.log_every,
            max_workers=req.max_workers,
            group_sample_size=req.group_sample_size,
            embedding_model_name=req.embedding_model,
        )
        return {"success": True, "message": "岗位底库数据处理完成", "data": result}
    except Exception as e:
        logger.error(f"Error processing job data pipeline: {e}")
        return {"success": False, "message": str(e), "data": {}}

@app.get("/api/student/profile")
@app.post("/api/student/profile")
async def build_student_profile():
    # 该接口仅为状态查询接口，不处理请求体输入。
    data = {}
    all_data = _load_all_data()
    if all_data:
        data = _safe_dict(all_data.get("student_profile_result"))
    return {
        "success": True,
        "status": "ok",
        "source": "state_file",
        "last_updated": _state_last_updated(),
        "data": data,
    }

@app.get("/api/job/match")
@app.post("/api/job/match")
async def match_jobs():
    # 该接口仅为状态查询接口，不处理请求体输入。
    data = []
    all_data = _load_all_data()
    if all_data:
        match_res = _safe_dict(all_data.get("job_match_result"))
        job_profile_result = _safe_dict(all_data.get("job_profile_result"))
        current_target_job = _resolve_state_target_job(all_data)

        if match_res:
            score = match_res.get("overall_match_score", match_res.get("overall_score", 0))
            level = _clean_text(match_res.get("score_level")) or ("高度匹配" if score >= 80 else ("较好匹配" if score >= 60 else "需转型突破"))

            reasons = []
            strengths = _safe_list(match_res.get("strengths"))
            for s in strengths:
                reasons.append(str(_safe_dict(s).get("description", s) if isinstance(s, dict) else s))

            weaknesses = _safe_list(match_res.get("weaknesses") or match_res.get("gaps"))
            for g in weaknesses:
                reasons.append(str(_safe_dict(g).get("description", g) if isinstance(g, dict) else g))

            if not reasons:
                for item in _safe_list(match_res.get("missing_items")):
                    if isinstance(item, dict):
                        reason = item.get("reason") or item.get("required_item")
                        if reason:
                            reasons.append(str(reason))

            representative_samples = _safe_list(job_profile_result.get("representative_samples"))
            company_name = "暂无公司信息"
            if representative_samples:
                company_name = _clean_text(_safe_dict(representative_samples[0]).get("company_name")) or company_name

            data = [{
                "job_name": current_target_job,
                "match_score": score,
                "match_level": level,
                "reasons": reasons,
                "company": company_name,
                "salary": _format_salary_text(job_profile_result),
                "strengths": _safe_list(match_res.get("strengths")),
                "weaknesses": _safe_list(match_res.get("weaknesses") or match_res.get("gaps")),
                "improvement_suggestions": _safe_list(match_res.get("improvement_suggestions")),
                "recommendation": _clean_text(match_res.get("recommendation")),
                "analysis_summary": _clean_text(match_res.get("analysis_summary") or match_res.get("summary")),
                "dimension_scores": {
                    "basic_requirement_score": match_res.get("basic_requirement_score"),
                    "vocational_skill_score": match_res.get("vocational_skill_score"),
                    "professional_quality_score": match_res.get("professional_quality_score"),
                    "development_potential_score": match_res.get("development_potential_score"),
                },
                "target_job_match": _safe_dict(match_res.get("target_job_match")),
                "recommended_job_match": _safe_dict(match_res.get("recommended_job_match")),
                "recommendation_ranking": _safe_list(match_res.get("recommendation_ranking")),
                "core_job_profiles": _safe_list(job_profile_result.get("core_job_profiles")),
                "target_job_profile_assets": _safe_dict(job_profile_result.get("target_job_profile_assets")),
                "target_job_confirmation": _safe_dict(all_data.get("target_job_confirmation")),
            }]

    if not data:
        return {
            "success": True,
            "status": "no_data",
            "source": "state_file",
            "last_updated": _state_last_updated(),
            "message": "暂无匹配数据，请先完善画像或稍后重试",
            "data": [],
        }

    return {
        "success": True,
        "status": "ok",
        "source": "state_file",
        "last_updated": _state_last_updated(),
        "data": data,
    }


@app.post("/api/job/confirm-target")
async def confirm_target_job(req: TargetJobConfirmRequest):
    """保存用户选择的本地标准岗位，并即时刷新目标岗位画像与赛题资产评估。"""
    requested_job_name = _clean_text(req.requested_job_name)
    confirmed_standard_job_name = _clean_text(req.confirmed_standard_job_name)
    if not requested_job_name or not confirmed_standard_job_name:
        return {"success": False, "message": "requested_job_name 和 confirmed_standard_job_name 不能为空", "data": {}}

    try:
        from job_match.match_asset_loader import MatchAssetLoader
        from job_match.target_job_confirmation_service import save_target_job_confirmation
        from job_profile.core_job_profile_service import build_target_job_profile_assets
        from job_match.contest_match_evaluator import evaluate_single_job

        confirmation = save_target_job_confirmation(
            requested_job_name=requested_job_name,
            confirmed_standard_job_name=confirmed_standard_job_name,
            project_root=_project_root(),
            state_path=_state_file_path(),
        )

        all_data = _load_all_data()
        loader = MatchAssetLoader(project_root=_project_root())
        job_profile_result = _safe_dict(all_data.get("job_profile_result"))
        job_match_result = _safe_dict(all_data.get("job_match_result"))
        match_payload = _safe_dict(job_match_result.get("match_input_payload"))
        student_profile = _safe_dict(match_payload.get("student_profile")) or _safe_dict(all_data.get("student_profile_result"))
        target_overall = _safe_float(job_match_result.get("overall_match_score")) or _safe_float(
            _safe_dict(job_match_result.get("rule_score_result")).get("overall_match_score")
        )

        target_assets = build_target_job_profile_assets(
            requested_job_name,
            loader=loader,
        )
        target_match = evaluate_single_job(
            job_name=requested_job_name,
            student_profile=student_profile,
            loader=loader,
            match_type="target_job",
            overall_match_score=target_overall,
        )

        job_profile_result["user_requested_job_name"] = requested_job_name
        job_profile_result["confirmed_standard_job_name"] = confirmed_standard_job_name
        job_profile_result["standard_job_name"] = confirmed_standard_job_name
        job_profile_result["target_job_profile_assets"] = target_assets
        all_data["job_profile_result"] = job_profile_result

        job_match_result["target_job_match"] = target_match
        if match_payload:
            match_job_profile = _safe_dict(match_payload.get("job_profile"))
            match_job_profile["standard_job_name"] = confirmed_standard_job_name
            match_job_profile["requested_job_name"] = requested_job_name
            match_job_profile["confirmed_standard_job_name"] = confirmed_standard_job_name
            match_payload["job_profile"] = match_job_profile
            job_match_result["match_input_payload"] = match_payload
        all_data["job_match_result"] = job_match_result
        all_data["target_job_confirmation"] = confirmation
        _write_all_data(all_data)

        return {
            "success": True,
            "message": "目标岗位标准化确认成功，已刷新目标岗位画像与赛题资产评估。",
            "data": {
                "target_job_confirmation": confirmation,
                "target_job_profile_assets": target_assets,
                "target_job_match": target_match,
            },
        }
    except Exception as exc:
        logger.error(f"Error confirming target job: {exc}")
        return {"success": False, "message": str(exc), "data": {}}


@app.get("/api/job/profile-assets")
async def job_profile_assets():
    payload = _build_job_profile_assets_payload()
    profiles = _safe_list(payload.get("core_job_profiles"))
    missing_assets = _safe_list(payload.get("missing_assets"))
    message = "岗位画像资产读取成功"
    status = "ok"
    if not profiles:
        status = "no_data"
        message = "暂无岗位画像资产，请先生成 outputs/match_assets 后处理产物"
    elif missing_assets:
        message = f"岗位画像资产部分缺失：{', '.join(str(item) for item in missing_assets)}"

    return {
        "success": True,
        "status": status,
        "source": "match_assets",
        "message": message,
        "data": {
            "summary": _safe_dict(payload.get("summary")),
            "core_job_profiles": profiles,
        },
    }


@app.get("/api/job-path-graph/all")
async def job_path_graph_all():
    """返回前端 G6 可渲染的全量岗位路径知识图谱。"""
    try:
        from job_path_graph_service import build_full_job_path_graph

        payload = build_full_job_path_graph(project_root=_project_root(), prefer_neo4j=True)
        return {
            "success": True,
            "status": _clean_text(payload.get("graph_status")) or "unavailable",
            "source": _clean_text(payload.get("source")) or "none",
            "message": _clean_text(payload.get("message")),
            "data": payload,
        }
    except Exception as exc:
        logger.error(f"Error building job path graph: {exc}")
        payload = {
            "graph_status": "unavailable",
            "source": "none",
            "stats": {
                "job_node_count": 0,
                "promote_edge_count": 0,
                "transfer_edge_count": 0,
                "total_edge_count": 0,
            },
            "nodes": [],
            "edges": [],
            "message": f"岗位路径图谱读取失败：{exc}",
        }
        return {
            "success": True,
            "status": "unavailable",
            "source": "none",
            "message": payload["message"],
            "data": payload,
        }


@app.get("/api/career/path")
@app.post("/api/career/path")
async def career_path():
    # 该接口仅为状态查询接口，不处理请求体输入。
    data = {}
    all_data = _load_all_data()
    if all_data:
        if _target_job_needs_confirmation(all_data):
            target_match = _safe_dict(_safe_dict(all_data.get("job_match_result")).get("target_job_match"))
            requested = (
                _clean_text(_safe_dict(target_match.get("job_name_resolution")).get("requested_job_name"))
                or _clean_text(target_match.get("job_name"))
                or _resolve_state_target_job(all_data)
            )
            data = {
                "primary_target_job": requested,
                "secondary_target_jobs": [],
                "goal_positioning": "当前目标岗位尚未完成本地标准岗位确认，暂不生成确定性职业路径。",
                "goal_reason": "请先在人岗匹配页面选择最接近的本地标准岗位，系统再基于标准岗位资产继续规划。",
                "path_strategy": "needs_target_confirmation",
                "target_path_data_status": "needs_confirmation",
                "target_path_data_message": "当前目标岗位需要先确认本地标准岗位，系统不会在未确认状态下生成职业路径。",
                "direct_path": [],
                "transition_path": [],
                "long_term_path": [],
                "representative_promotion_paths": [],
                "representative_path_count": 0,
                "representative_path_status": "",
                "representative_path_message": "",
                "short_term_plan": ["先确认本地标准岗位，再重新生成职业路径规划。"],
                "mid_term_plan": [],
                "risk_and_gap": ["目标岗位未完成标准化确认，岗位路径和赛题评测结论暂不完整。"],
                "fallback_strategy": "确认标准岗位后重新规划。",
                "target_selection_reason": [],
                "path_selection_reason": [],
            }
            return {
                "success": True,
                "status": "needs_confirmation",
                "source": "state_file",
                "last_updated": _state_last_updated(),
                "data": data,
            }

        raw = _safe_dict(all_data.get("career_path_plan_result"))
        service_raw = _safe_read_json(_project_root() / "outputs" / "state" / "career_path_plan_service_result.json")
        service_result = _safe_dict(service_raw.get("career_path_plan_result")) or service_raw
        if not raw:
            raw = service_result
        else:
            # 兼容旧状态文件：代表路径是全局图谱资产，可从最新 service_result 补齐。
            for key in (
                "target_path_data_status",
                "target_path_data_message",
                "representative_promotion_paths",
                "representative_path_count",
                "representative_path_status",
                "representative_path_message",
            ):
                if raw.get(key) in (None, "", []):
                    raw[key] = service_result.get(key)

        def flatten_list(obj_list):
            if not obj_list:
                return []
            res = []
            for item in obj_list:
                if isinstance(item, str):
                    res.append(item)
                elif isinstance(item, dict):
                    if "title" in item and "description" in item:
                        res.append(f"{item['title']}: {item['description']}")
                    elif "phase" in item and "content" in item:
                        res.append(f"{item['phase']}: {item['content']}")
                    elif "step" in item and "action" in item:
                        res.append(f"Step {item.get('step', '')}: {item['action']}")
                    elif "phase" in item and "actions" in item:
                        acts = "、".join(item["actions"]) if isinstance(item["actions"], list) else str(item["actions"])
                        res.append(f"{item['phase']}: {acts}")
                    else:
                        values = [str(v) for v in item.values() if isinstance(v, str)]
                        if values:
                            res.append(" - ".join(values))
                        else:
                            res.append(json.dumps(item, ensure_ascii=False))
                else:
                    res.append(str(item))
            return res

        data = {
            "primary_target_job": raw.get("primary_target_job", _resolve_state_target_job(all_data)),
            "secondary_target_jobs": raw.get("secondary_target_jobs", raw.get("backup_target_jobs", [])),
            "goal_positioning": _clean_text(raw.get("goal_positioning")),
            "goal_reason": _clean_text(raw.get("goal_reason")),
            "path_strategy": _clean_text(raw.get("path_strategy")),
            "target_path_data_status": _clean_text(raw.get("target_path_data_status")),
            "target_path_data_message": _clean_text(raw.get("target_path_data_message")),
            "direct_path": flatten_list(raw.get("direct_path", [])),
            "transition_path": flatten_list(raw.get("transition_path", [])),
            "long_term_path": flatten_list(raw.get("long_term_path", [])),
            "representative_promotion_paths": [
                {
                    "source_job": _clean_text(_safe_dict(item).get("source_job")),
                    "promote_targets": flatten_list(_safe_dict(item).get("promote_targets", [])),
                    "edge_count": _safe_dict(item).get("edge_count", 0),
                    "source": _clean_text(_safe_dict(item).get("source")),
                    "selection_reason": _clean_text(_safe_dict(item).get("selection_reason")),
                }
                for item in _safe_list(raw.get("representative_promotion_paths"))
                if _safe_dict(item)
            ],
            "representative_path_count": raw.get("representative_path_count", 0),
            "representative_path_status": _clean_text(raw.get("representative_path_status")),
            "representative_path_message": _clean_text(raw.get("representative_path_message")),
            "short_term_plan": flatten_list(raw.get("short_term_plan", [])),
            "mid_term_plan": flatten_list(raw.get("mid_term_plan", [])),
            "risk_and_gap": flatten_list(raw.get("risk_and_gap", raw.get("risk_notes", []))),
            "fallback_strategy": _clean_text(raw.get("fallback_strategy")),
            "target_selection_reason": flatten_list(raw.get("target_selection_reason", [])),
            "path_selection_reason": flatten_list(raw.get("path_selection_reason", [])),
        }
    return {
        "success": True,
        "status": "ok",
        "source": "state_file",
        "last_updated": _state_last_updated(),
        "data": data,
    }

@app.post("/api/report/generate")
async def generate_report():
    all_data = _load_all_data()
    report_detail = _build_report_detail(all_data)
    if not _clean_text(report_detail.get("report_text")):
        return {"success": False, "message": "当前没有可用报告，请先完成主流程生成报告。", "data": ""}
    try:
        snapshot_file_name = _write_report_snapshot(report_detail)
        all_data["latest_shared_report_file"] = snapshot_file_name
        _write_all_data(all_data)
        return {"success": True, "data": snapshot_file_name}
    except Exception as exc:
        logger.error(f"Error generating report snapshot: {exc}")
        return {"success": False, "message": f"报告快照保存失败: {exc}", "data": ""}

@app.get("/api/report")
async def get_report():
    all_data = _load_all_data()
    data = _build_report_detail(all_data).get("report_text", "")
    return {"success": True, "data": data}


@app.get("/api/report/detail")
async def get_report_detail():
    all_data = _load_all_data()
    return {
        "success": True,
        "source": "state_file",
        "last_updated": _state_last_updated(),
        "data": _build_report_detail(all_data),
    }


@app.get("/api/report/shared")
async def get_shared_report(file_name: str = Query(default="")):
    requested_name = _clean_text(file_name)

    if requested_name:
        payload = _load_report_snapshot(requested_name)
        return {"success": True, "data": payload.get("report_text", "")}

    latest_path = _report_snapshot_dir() / LATEST_REPORT_SNAPSHOT_FILE
    if latest_path.exists():
        payload = _load_report_snapshot(LATEST_REPORT_SNAPSHOT_FILE)
        return {"success": True, "data": payload.get("report_text", "")}

    # 兼容历史流程：若快照目录还没有文件，回退读取当前状态文件。
    all_data = _load_all_data()
    data = _build_report_detail(all_data).get("report_text", "")
    if not _clean_text(data):
        raise HTTPException(status_code=404, detail="报告已过期或不存在")
    return {"success": True, "data": data}


@app.get("/api/report/download")
async def download_report(file_name: str = Query(default=REPORT_FILE_NAME)):
    report_text = ""
    if _is_safe_report_snapshot_name(file_name):
        try:
            payload = _load_report_snapshot(file_name)
            report_text = _clean_text(payload.get("report_text"))
        except HTTPException as exc:
            if exc.status_code != 404:
                raise

    if not report_text:
        all_data = _load_all_data()
        report_text = _build_report_detail(all_data).get("report_text", "")

    if not report_text:
        raise HTTPException(status_code=404, detail="报告不存在")
    return Response(
        content=report_text,
        media_type="text/markdown; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{file_name or REPORT_FILE_NAME}"'},
    )


@app.get("/api/pipeline/status")
async def get_pipeline_status():
    return {
        "success": True,
        "data": _load_pipeline_status(),
    }


@app.post("/api/report/update")
async def update_report(req: ReportUpdateRequest):
    report_text = _clean_text(req.report_text)
    if not report_text:
        return {"success": False, "message": "报告内容不能为空", "data": ""}

    all_data = _load_all_data()
    report_res = _safe_dict(all_data.get("career_report_result"))
    report_res["report_text_markdown"] = report_text
    report_res["report_text"] = report_text
    all_data["career_report_result"] = report_res
    _write_all_data(all_data)
    _write_report_file(report_text)
    return {"success": True, "data": REPORT_FILE_NAME}


@app.get("/api/ai/context-summary")
async def get_ai_context_summary():
    snapshot = _build_ai_context_snapshot()
    return {
        "success": True,
        "data": {
            "summary": _build_context_summary_line(snapshot),
            "loaded_files": _safe_list(snapshot.get("loaded_files")),
            "missing_files": _safe_list(snapshot.get("missing_files")),
        },
    }


@app.post("/api/ai/chat")
async def handle_ai_chat(req: AIChatRequest):
    user_message = _clean_text(req.message)
    if not user_message:
        return {"success": False, "message": "message 不能为空", "data": {}}

    memory_store = _load_ai_memory_store()
    conversation_id = _ensure_conversation_id(memory_store, req.conversation_id)
    history = _get_recent_history(memory_store, conversation_id)

    snapshot = _build_ai_context_snapshot()
    intent_result = classify_ai_question_intent(user_message, history)
    context_chunks = _retrieve_context_chunks(user_message, snapshot)
    semantic_hits = _retrieve_semantic_hits(user_message, snapshot) if _should_use_semantic_for_intent(intent_result) else []
    sql_query_context = _build_sql_query_context(user_message, history, intent_result)
    path_graph_context = (
        _build_path_graph_context(user_message)
        if bool(intent_result.get("should_use_path_graph"))
        else {"enabled": False, "matched_edges": [], "stats": {}, "summary_text": ""}
    )
    context_markdown = _format_context_for_prompt(
        snapshot,
        context_chunks,
        semantic_hits,
        sql_query_context,
        path_graph_context,
    )

    answer = ""
    answer_source = "local_fallback"
    try:
        answer = _invoke_ai_chat_completion(
            user_message=user_message,
            history=history,
            local_context_markdown=context_markdown,
        )
        answer_source = "llm"
    except Exception as exc:
        logger.warning(f"AI chat model call failed, fallback to local answer: {exc}")
        answer = _build_local_fallback_answer(
            question=user_message,
            snapshot=snapshot,
            chunks=context_chunks,
            web_search_enabled=bool(req.web_search_enabled),
            semantic_hits=semantic_hits,
            sql_query_context=sql_query_context,
        )

    sql_context_dict = _safe_dict(sql_query_context)
    path_context_dict = _safe_dict(path_graph_context)
    if bool(sql_context_dict.get("enabled")):
        answer = _build_sql_product_answer(
            question=user_message,
            snapshot=snapshot,
            sql_query_context=sql_context_dict,
            web_search_enabled=bool(req.web_search_enabled),
        )
        answer_source = "local_sql_summary"
    elif bool(path_context_dict.get("enabled")):
        answer = _build_path_graph_product_answer(user_message, path_context_dict)
        answer_source = "local_path_graph_summary"

    _append_conversation_message(memory_store, conversation_id, "user", user_message)
    _append_conversation_message(memory_store, conversation_id, "assistant", answer)
    _save_ai_memory_store(memory_store)

    semantic_evidence = [
        hit for hit in _safe_list(semantic_hits)
        if (_safe_float(_safe_dict(hit).get("score")) or 0) >= 0.30
    ]
    sql_evidence = {
        "enabled": bool(sql_context_dict.get("enabled")),
        "data_source": _clean_text(sql_context_dict.get("data_source")),
        "db_table": _clean_text(sql_context_dict.get("db_table")),
        "data_file": _clean_text(sql_context_dict.get("data_file")),
        "sql_source": _clean_text(sql_context_dict.get("sql_source")),
        "generated_sql": _clean_text(sql_context_dict.get("generated_sql")),
        "row_count": sql_context_dict.get("row_count", 0),
        "error": _clean_text(sql_context_dict.get("error")),
    }
    path_graph_evidence = {
        "enabled": bool(path_context_dict.get("enabled")),
        "path_graph_status": _clean_text(path_context_dict.get("path_graph_status")),
        "source": _clean_text(path_context_dict.get("source")),
        "stats": _safe_dict(path_context_dict.get("stats")),
        "matched_edges": _safe_list(path_context_dict.get("matched_edges")),
        "summary_text": _clean_text(path_context_dict.get("summary_text")),
        "message": _clean_text(path_context_dict.get("message")),
    }
    local_sources_used = set()
    if bool(sql_context_dict.get("enabled")):
        local_sources_used.add("jobs_db" if _clean_text(sql_context_dict.get("data_source")) == AI_SQL_DB_SOURCE else "csv_fallback")
    if bool(path_context_dict.get("enabled")):
        local_sources_used.add("job_path_graph")
    if semantic_evidence:
        local_sources_used.add("semantic_kb")
    for chunk in context_chunks:
        source = _clean_text(_safe_dict(chunk).get("source"))
        if source == "student_profile":
            local_sources_used.add("student_profile")
        elif source == "job_match":
            local_sources_used.add("job_match")
        elif source == "career_path":
            local_sources_used.add("career_path")
        elif source == "report_data":
            local_sources_used.add("report_data")
    local_sources_used.add("offline_mode")

    return {
        "success": True,
        "data": {
            "conversation_id": conversation_id,
            "intent": _clean_text(intent_result.get("intent")) or _infer_ai_chat_intent(user_message, sql_context_dict),
            "intent_result": intent_result,
            "answer": answer,
            "source": answer_source,
            "context_summary": _build_context_summary_line(snapshot),
            "used_context_sources": list(
                {
                    *{chunk.get("source") for chunk in context_chunks if chunk.get("source")},
                    *({"semantic_kb"} if semantic_hits else set()),
                    *({"sql_query"} if _clean_text(_safe_dict(sql_query_context).get("generated_sql")) else set()),
                    *({"path_graph"} if bool(path_context_dict.get("enabled")) else set()),
                }
            ),
            "loaded_files": _safe_list(snapshot.get("loaded_files")),
            "missing_files": _safe_list(snapshot.get("missing_files")),
            "local_sources_used": sorted(local_sources_used),
            "result_cards": _safe_list(sql_context_dict.get("company_cards")),
            "result_table": _safe_dict(sql_context_dict.get("result_table")),
            "summary_stats": _safe_dict(sql_context_dict.get("summary_stats")),
            "evidence": {
                "context_chunks": context_chunks[:AI_MAX_CONTEXT_CHUNKS],
                "semantic_hits": semantic_evidence,
                "sql": sql_evidence,
                "path_graph": path_graph_evidence,
            },
            "sql_debug": {
                **sql_evidence,
            },
        },
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api_server:app", host="127.0.0.1", port=8000, reload=True)
