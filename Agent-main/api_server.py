import logging
import os
import tempfile
import json
import shutil
import asyncio
import re
import uuid
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
        return retriever.search(query_text=query_text, top_k=top_k, min_score=0.0)
    except FileNotFoundError:
        return []
    except Exception as exc:
        logger.warning(f"semantic retrieval unavailable for ai assistant: {exc}")
        return []


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
                "回答时请尽量简洁，并标注信息来源（如：📁 本地分析）。\n\n"
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


def _build_local_fallback_answer(
    question: str,
    snapshot: Dict[str, Any],
    chunks: List[Dict[str, str]],
    web_search_enabled: bool,
    semantic_hits: Optional[List[Dict[str, Any]]] = None,
) -> str:
    if not chunks:
        if semantic_hits:
            lines = ["📁 本地分析", "- 当前尚未加载完整档案，但已召回岗位语义知识片段："]
            for hit in semantic_hits[:AI_SEMANTIC_TOP_K]:
                lines.append(f"  - {hit.get('standard_job_name')}：{hit.get('doc_text_excerpt')}")
            return "\n".join(lines)
        return "📁 本地分析\n当前没有可用档案内容，请先完成学生画像、岗位匹配、职业路径和报告生成。"

    normalized_question = _clean_text(question)
    lines = ["📁 本地分析"]

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


def _build_report_detail(all_data: dict) -> dict:
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


@app.get("/api/career/path")
@app.post("/api/career/path")
async def career_path():
    # 该接口仅为状态查询接口，不处理请求体输入。
    data = {}
    all_data = _load_all_data()
    if all_data:
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

    snapshot = _build_ai_context_snapshot()
    context_chunks = _retrieve_context_chunks(user_message, snapshot)
    semantic_hits = _retrieve_semantic_hits(user_message, snapshot)
    context_markdown = _format_context_for_prompt(snapshot, context_chunks, semantic_hits)

    memory_store = _load_ai_memory_store()
    conversation_id = _ensure_conversation_id(memory_store, req.conversation_id)
    history = _get_recent_history(memory_store, conversation_id)

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
        )

    _append_conversation_message(memory_store, conversation_id, "user", user_message)
    _append_conversation_message(memory_store, conversation_id, "assistant", answer)
    _save_ai_memory_store(memory_store)

    return {
        "success": True,
        "data": {
            "conversation_id": conversation_id,
            "answer": answer,
            "source": answer_source,
            "context_summary": _build_context_summary_line(snapshot),
            "used_context_sources": list(
                {
                    *{chunk.get("source") for chunk in context_chunks if chunk.get("source")},
                    *({"semantic_kb"} if semantic_hits else set()),
                }
            ),
            "loaded_files": _safe_list(snapshot.get("loaded_files")),
            "missing_files": _safe_list(snapshot.get("missing_files")),
        },
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api_server:app", host="127.0.0.1", port=8000, reload=True)
