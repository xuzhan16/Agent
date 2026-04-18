"""
target_job_confirmation_service.py

Conservative target-job confirmation helpers.

This layer sits between raw user intent and local standard job assets:
- resolved: a single standard job can be used safely.
- needs_confirmation: several plausible local standard jobs exist, user must choose.
- unresolved: no reliable local standard job candidate exists.
"""

from __future__ import annotations

import difflib
import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from .job_name_normalizer import extract_job_tokens, normalize_job_name_key
from .match_asset_loader import MatchAssetLoader, clean_text, safe_dict, safe_list


RESOLUTION_RESOLVED = "resolved"
RESOLUTION_NEEDS_CONFIRMATION = "needs_confirmation"
RESOLUTION_UNRESOLVED = "unresolved"
EVALUATION_NEEDS_CONFIRMATION = "needs_confirmation"

DEFAULT_STATE_FILE = "student_api_state.json"

# These labels are intentionally broad: even if one candidate looks likely,
# asking the user is safer than silently locking a wide career intent.
BROAD_CONFIRMATION_KEYS = {
    "后端",
    "后端开发",
    "服务端",
    "服务端开发",
    "软件开发",
    "开发",
    "产品",
    "测试",
    "运营",
    "项目管理",
    "数据分析",
    "数据",
}


def dedup_keep_order(values: Iterable[Any]) -> List[Any]:
    seen = set()
    result = []
    for value in values:
        if isinstance(value, dict):
            key = json.dumps(value, ensure_ascii=False, sort_keys=True)
            item = value
        else:
            item = clean_text(value)
            key = item
        if not item or key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def parse_list_like(value: Any) -> List[str]:
    if isinstance(value, list):
        return [clean_text(item) for item in value if clean_text(item)]
    text = clean_text(value)
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            loaded = json.loads(text)
            if isinstance(loaded, list):
                return [clean_text(item) for item in loaded if clean_text(item)]
        except Exception:
            pass
    return [clean_text(part) for part in text.replace("，", "、").replace(",", "、").split("、") if clean_text(part)]


def _state_file_path(project_root: Optional[str | Path] = None, state_path: Optional[str | Path] = None) -> Path:
    if state_path:
        return Path(state_path)
    root = Path(project_root) if project_root else Path(__file__).resolve().parent.parent
    return root / DEFAULT_STATE_FILE


def load_state(path: str | Path) -> Dict[str, Any]:
    state_path = Path(path)
    if not state_path.exists():
        return {}
    try:
        with state_path.open("r", encoding="utf-8") as f:
            loaded = json.load(f)
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return {}


def save_state(path: str | Path, state: Dict[str, Any]) -> None:
    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with state_path.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def get_target_job_confirmation(
    state: Optional[Dict[str, Any]] = None,
    *,
    project_root: Optional[str | Path] = None,
    state_path: Optional[str | Path] = None,
) -> Dict[str, Any]:
    source_state = state if isinstance(state, dict) else load_state(_state_file_path(project_root, state_path))
    return safe_dict(source_state.get("target_job_confirmation"))


def _confirmation_matches_request(confirmation: Dict[str, Any], requested_job_name: str) -> bool:
    confirmed = clean_text(confirmation.get("confirmed_standard_job_name"))
    if not confirmed:
        return False
    original = clean_text(confirmation.get("requested_job_name"))
    if not requested_job_name:
        return True
    if not original:
        return True
    return normalize_job_name_key(original) == normalize_job_name_key(requested_job_name)


def get_confirmed_standard_job(
    requested_job_name: Any,
    standard_job_names: Iterable[Any],
    state: Optional[Dict[str, Any]] = None,
    *,
    project_root: Optional[str | Path] = None,
    state_path: Optional[str | Path] = None,
) -> Dict[str, Any]:
    """Return saved confirmation if it is valid for current request and assets."""
    confirmation = get_target_job_confirmation(state, project_root=project_root, state_path=state_path)
    confirmed_name = clean_text(confirmation.get("confirmed_standard_job_name"))
    standards = set(clean_text(name) for name in standard_job_names if clean_text(name))
    requested = clean_text(requested_job_name)
    if confirmed_name and confirmed_name in standards and _confirmation_matches_request(confirmation, requested):
        return deepcopy(confirmation)
    return {}


def _core_job_map(loader: MatchAssetLoader) -> Dict[str, Dict[str, Any]]:
    return {
        clean_text(item.get("standard_job_name")): safe_dict(item)
        for item in loader.core_jobs()
        if clean_text(item.get("standard_job_name"))
    }


def _normalize_terms(values: Iterable[Any]) -> List[str]:
    terms: List[str] = []
    for value in values:
        for item in parse_list_like(value):
            key = normalize_job_name_key(item, drop_level_words=False)
            if key:
                terms.append(key)
    return dedup_keep_order(terms)


def extract_student_candidate_terms(student_profile: Dict[str, Any]) -> List[str]:
    """Extract deterministic student-side terms for candidate ranking."""
    profile = safe_dict(student_profile)
    raw = safe_dict(profile.get("raw_student_profile_result"))
    input_payload = safe_dict(raw.get("profile_input_payload"))
    normalized_profile = safe_dict(input_payload.get("normalized_profile"))
    explicit_profile = safe_dict(input_payload.get("explicit_profile"))
    ability_evidence = safe_dict(raw.get("ability_evidence"))

    values: List[Any] = []
    for key in [
        "hard_skills",
        "tool_skills",
        "skills",
        "certificates",
        "occupation_hints",
        "domain_tags",
        "experience_tags",
        "summary",
    ]:
        values.extend(parse_list_like(profile.get(key)))
    for key in ["hard_skills", "tool_skills", "occupation_hints", "domain_tags", "experience_tags"]:
        values.extend(parse_list_like(normalized_profile.get(key)))
    for key in ["certificates", "project_experience", "internship_experience"]:
        values.extend(parse_list_like(explicit_profile.get(key)))
    for key in ["project_examples", "internship_examples"]:
        values.extend(parse_list_like(ability_evidence.get(key)))
    return _normalize_terms(values)


def _candidate_asset_terms(
    job_name: str,
    stats: Dict[str, Any],
    skill_assets: Dict[str, Any],
    core_job: Dict[str, Any],
) -> List[str]:
    values: List[Any] = [job_name]
    for source in (stats, skill_assets, core_job):
        for key in [
            "job_category",
            "job_level_summary",
            "mainstream_degree",
            "mainstream_majors",
            "mainstream_certificates",
            "major_gate_set",
            "must_have_certificates",
            "preferred_certificates",
            "hard_skills",
            "tools_or_tech_stack",
            "top_skills",
            "required_knowledge_points",
            "preferred_knowledge_points",
        ]:
            values.extend(parse_list_like(safe_dict(source).get(key)))
    return _normalize_terms(values)


def _name_similarity(requested_job_name: str, candidate_name: str) -> float:
    requested_key = normalize_job_name_key(requested_job_name)
    candidate_key = normalize_job_name_key(candidate_name)
    if not requested_key or not candidate_key:
        return 0.0
    score = difflib.SequenceMatcher(None, requested_key, candidate_key).ratio()
    if requested_key in candidate_key or candidate_key in requested_key:
        score = max(score, 0.82)
    requested_tokens = extract_job_tokens(requested_job_name)
    if requested_tokens:
        token_hits = sum(1 for token in requested_tokens if token and token in candidate_key)
        score = max(score, token_hits / max(len(requested_tokens), 1))
    return round(float(min(score, 1.0)), 4)


def _term_overlap(left_terms: List[str], right_terms: List[str]) -> float:
    if not left_terms or not right_terms:
        return 0.0
    hits = 0
    for term in left_terms:
        if any(term == other or term in other or other in term for other in right_terms):
            hits += 1
    return round(hits / max(len(left_terms), 1), 4)


def _asset_quality(stats: Dict[str, Any], skill_assets: Dict[str, Any]) -> float:
    score = 0.0
    if safe_list(stats.get("degree_distribution")):
        score += 0.2
    if safe_list(stats.get("major_distribution")):
        score += 0.2
    if safe_list(stats.get("certificate_distribution")) or stats.get("no_certificate_requirement_ratio") is not None:
        score += 0.15
    if safe_list(skill_assets.get("required_knowledge_points")):
        score += 0.3
    if safe_list(skill_assets.get("preferred_knowledge_points")):
        score += 0.15
    return round(min(score, 1.0), 4)


def _candidate_reason(
    requested_job_name: str,
    candidate_name: str,
    name_score: float,
    skill_overlap: float,
    required_points: List[str],
) -> str:
    reason_parts = []
    if name_score >= 0.75:
        reason_parts.append(f"岗位名称与“{requested_job_name}”相近")
    if skill_overlap > 0:
        reason_parts.append("学生技能/经历与该岗位知识点存在重合")
    if required_points:
        reason_parts.append(f"该岗位沉淀了 {len(required_points)} 个必备知识点")
    if not reason_parts:
        reason_parts.append("该岗位是本地资产中的可能相关标准岗位")
    return f"{candidate_name}：{'，'.join(reason_parts)}。"


def _expand_candidate_names(
    requested_job_name: str,
    initial_candidates: List[str],
    loader: MatchAssetLoader,
    max_names: int = 12,
) -> List[str]:
    names = dedup_keep_order(initial_candidates)
    if len(names) >= max_names:
        return names[:max_names]

    requested_tokens = extract_job_tokens(requested_job_name)
    requested_key = normalize_job_name_key(requested_job_name)
    scored: List[tuple[str, float]] = []
    for name in loader.all_standard_job_names():
        if name in names:
            continue
        candidate_key = normalize_job_name_key(name, drop_level_words=False)
        token_hit = 0.0
        if requested_tokens:
            token_hit = sum(1 for token in requested_tokens if token in candidate_key) / len(requested_tokens)
        sim = _name_similarity(requested_job_name, name)
        if requested_key and (requested_key in candidate_key or candidate_key in requested_key):
            sim = max(sim, 0.78)
        score = max(sim, token_hit)
        if score >= 0.45:
            scored.append((name, score))
    scored.sort(key=lambda item: item[1], reverse=True)
    for name, _ in scored:
        names.append(name)
        if len(names) >= max_names:
            break
    return dedup_keep_order(names)[:max_names]


def build_candidate_jobs(
    requested_job_name: Any,
    loader: MatchAssetLoader,
    student_profile: Optional[Dict[str, Any]] = None,
    initial_candidates: Optional[List[Any]] = None,
    top_k: int = 5,
) -> List[Dict[str, Any]]:
    """Build rich candidate standard jobs for frontend confirmation."""
    requested = clean_text(requested_job_name)
    if not requested:
        return []

    core_map = _core_job_map(loader)
    student_terms = extract_student_candidate_terms(safe_dict(student_profile))
    candidate_names = _expand_candidate_names(
        requested,
        [clean_text(item) for item in safe_list(initial_candidates) if clean_text(item)],
        loader,
    )

    candidates: List[Dict[str, Any]] = []
    for job_name in candidate_names:
        stats = loader.get_requirement_stats_by_standard_name(job_name)
        skills = loader.get_skill_assets_by_standard_name(job_name)
        core = core_map.get(job_name, {})
        if not stats and not skills and not core:
            continue

        required_points = safe_list(skills.get("required_knowledge_points"))
        preferred_points = safe_list(skills.get("preferred_knowledge_points"))
        candidate_terms = _candidate_asset_terms(job_name, stats, skills, core)
        name_score = _name_similarity(requested, job_name)
        skill_score = _term_overlap(student_terms, candidate_terms)
        quality_score = _asset_quality(stats, skills)
        sample_count = int(stats.get("sample_count") or core.get("sample_count") or 0)
        sample_boost = min(sample_count / 300.0, 1.0)
        core_boost = 1.0 if job_name in core_map else 0.0
        candidate_score = (
            name_score * 0.35
            + skill_score * 0.35
            + quality_score * 0.20
            + ((sample_boost * 0.7) + (core_boost * 0.3)) * 0.10
        )
        mainstream_majors = safe_list(stats.get("mainstream_majors")) or safe_list(stats.get("major_gate_set"))
        mainstream_certificates = safe_list(stats.get("mainstream_certificates")) or safe_list(stats.get("preferred_certificates"))
        top_skills = (
            safe_list(core.get("top_skills"))
            or safe_list(skills.get("hard_skills"))
            or safe_list(skills.get("tools_or_tech_stack"))
            or required_points
        )
        candidates.append(
            {
                "standard_job_name": job_name,
                "candidate_score": round(float(candidate_score), 4),
                "sample_count": sample_count,
                "job_category": clean_text(stats.get("job_category") or core.get("job_category")),
                "mainstream_degree": clean_text(stats.get("mainstream_degree") or core.get("mainstream_degree")),
                "mainstream_majors": deepcopy(mainstream_majors[:8]),
                "mainstream_certificates": deepcopy(mainstream_certificates[:8]),
                "top_skills": deepcopy(top_skills[:8]),
                "required_knowledge_points": deepcopy(required_points[:12]),
                "preferred_knowledge_points": deepcopy(preferred_points[:12]),
                "match_reason": _candidate_reason(requested, job_name, name_score, skill_score, required_points),
                "is_core_job": bool(job_name in core_map),
            }
        )

    candidates.sort(key=lambda item: item.get("candidate_score", 0.0), reverse=True)
    return [item for item in candidates if item.get("candidate_score", 0.0) >= 0.16][:top_k]


def _should_force_confirmation(requested_job_name: str, resolution: Dict[str, Any]) -> bool:
    requested_key = normalize_job_name_key(requested_job_name, drop_level_words=False)
    if requested_key in BROAD_CONFIRMATION_KEYS:
        return True
    method = clean_text(resolution.get("resolution_method"))
    if method == "token_unique" and len(requested_key) <= 3 and not any(ch.encode().isalpha() for ch in requested_key):
        return True
    return False


def resolve_job_with_confirmation(
    job_name: Any,
    loader: MatchAssetLoader,
    student_profile: Optional[Dict[str, Any]] = None,
    state: Optional[Dict[str, Any]] = None,
    *,
    project_root: Optional[str | Path] = None,
    state_path: Optional[str | Path] = None,
) -> Dict[str, Any]:
    """Resolve a job name with saved user confirmation and rich candidate fallback."""
    requested = clean_text(job_name)
    standards = loader.all_standard_job_names()

    confirmation = get_confirmed_standard_job(
        requested,
        standards,
        state=state,
        project_root=project_root or loader.project_root,
        state_path=state_path,
    )
    confirmed_name = clean_text(confirmation.get("confirmed_standard_job_name"))
    if confirmed_name:
        return {
            "resolution_status": RESOLUTION_RESOLVED,
            "requested_job_name": requested,
            "resolved_standard_job_name": confirmed_name,
            "confirmed_standard_job_name": confirmed_name,
            "asset_found": True,
            "resolution_method": "user_confirmed",
            "resolution_confidence": 1.0,
            "candidate_jobs": [],
            "candidate_job_names": [],
            "confirmation_source": clean_text(confirmation.get("confirmation_source")) or "user_selected",
            "confirmed_at": clean_text(confirmation.get("confirmed_at")),
            "message": f"已使用用户确认的本地标准岗位：{confirmed_name}。",
        }

    resolution = loader.resolve_job_name(requested)
    candidate_names = [clean_text(item) for item in safe_list(resolution.get("candidate_jobs")) if clean_text(item)]

    if resolution.get("asset_found") and not _should_force_confirmation(requested, resolution):
        resolved = clean_text(resolution.get("resolved_standard_job_name"))
        return {
            **deepcopy(resolution),
            "resolution_status": RESOLUTION_RESOLVED,
            "candidate_jobs": [],
            "candidate_job_names": [],
            "message": f"已命中本地标准岗位：{resolved}。",
        }

    if resolution.get("asset_found"):
        resolved = clean_text(resolution.get("resolved_standard_job_name"))
        candidate_names = dedup_keep_order([resolved] + candidate_names)

    candidates = build_candidate_jobs(
        requested_job_name=requested,
        loader=loader,
        student_profile=student_profile,
        initial_candidates=candidate_names,
    )
    if candidates:
        return {
            **deepcopy(resolution),
            "resolution_status": RESOLUTION_NEEDS_CONFIRMATION,
            "resolved_standard_job_name": "",
            "asset_found": False,
            "resolution_confidence": 0.0,
            "candidate_jobs": candidates,
            "candidate_job_names": [item.get("standard_job_name") for item in candidates if item.get("standard_job_name")],
            "message": "当前目标岗位未唯一命中本地标准岗位，请选择一个最接近的岗位用于后续评估。",
        }

    return {
        **deepcopy(resolution),
        "resolution_status": RESOLUTION_UNRESOLVED,
        "resolved_standard_job_name": "",
        "asset_found": False,
        "candidate_jobs": [],
        "candidate_job_names": [],
        "message": "当前目标岗位未命中本地岗位资产，且暂无可靠候选岗位。",
    }


def save_target_job_confirmation(
    requested_job_name: Any,
    confirmed_standard_job_name: Any,
    *,
    project_root: Optional[str | Path] = None,
    state_path: Optional[str | Path] = None,
) -> Dict[str, Any]:
    """Persist user-selected standard job in student_api_state.json."""
    requested = clean_text(requested_job_name)
    confirmed = clean_text(confirmed_standard_job_name)
    path = _state_file_path(project_root, state_path)
    state = load_state(path)
    loader = MatchAssetLoader(project_root=project_root or path.parent)
    if confirmed not in set(loader.all_standard_job_names()):
        raise ValueError(f"confirmed_standard_job_name 不存在于本地岗位资产: {confirmed}")

    confirmation = {
        "requested_job_name": requested,
        "confirmed_standard_job_name": confirmed,
        "confirmation_source": "user_selected",
        "confirmed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    state["target_job_confirmation"] = confirmation
    save_state(path, state)
    return confirmation
