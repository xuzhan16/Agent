import logging
import os
import tempfile
import json
import shutil
import asyncio
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Union
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


def _truncate_for_ai(value: Any, max_chars: int = AI_CONTEXT_CHUNK_MAX_CHARS) -> str:
    text = _clean_text(value)
    if len(text) <= max_chars:
        return text
    return f"{text[: max_chars - 1]}…"


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
    semantic_hits: List[Dict[str, Any]] | None = None,
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
    if _clean_text(match.get("analysis_summary")):
        parts.append(f"匹配分析：{match.get('analysis_summary')}")
    if _clean_text(match.get("recommendation")):
        parts.append(f"匹配建议：{match.get('recommendation')}")
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
    semantic_hits: List[Dict[str, Any]] | None = None,
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

    match = _safe_dict(snapshot.get("job_match"))
    if "匹配" in normalized_question and match.get("overall_match_score") not in (None, ""):
        lines.append(
            f"- 当前匹配分：{match.get('overall_match_score')}（{_clean_text(match.get('score_level'))}）"
        )

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

@app.get("/api/career/path")
@app.post("/api/career/path")
async def career_path():
    # 该接口仅为状态查询接口，不处理请求体输入。
    data = {}
    all_data = _load_all_data()
    if all_data:
        raw = _safe_dict(all_data.get("career_path_plan_result"))

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
            "direct_path": flatten_list(raw.get("direct_path", [])),
            "transition_path": flatten_list(raw.get("transition_path", [])),
            "long_term_path": flatten_list(raw.get("long_term_path", [])),
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
