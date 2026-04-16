"""
job_name_normalizer.py

Conservative standard-job-name resolver for local match assets.
"""

from __future__ import annotations

import difflib
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple


SYMBOL_PATTERN = re.compile(r"[\s\-_/\\|｜,，;；:：()（）\[\]【】{}<>《》\"'“”‘’·]+")
WEAK_WORD_PATTERN = re.compile(r"(岗位|职位|职务|方向|招聘|急聘|校招|社招|热招|岗位要求)")
LEVEL_WORD_PATTERN = re.compile(r"(初级|中级|高级|资深|专家级|实习生|实习)")

JOB_KEYWORDS = [
    "c/c++",
    "c++",
    "c#",
    "java",
    "python",
    "android",
    "ios",
    "web",
    "app",
    "qa",
    "嵌入式软件",
    "嵌入式",
    "软件测试",
    "硬件测试",
    "质量管理",
    "技术支持",
    "项目经理",
    "项目招投标",
    "知识产权",
    "专利代理",
    "前端开发",
    "前端",
    "测试开发",
    "测试",
    "开发",
    "实施",
    "运维",
    "数据",
    "算法",
    "产品",
    "运营",
    "销售",
    "客服",
    "商务",
    "培训",
    "咨询",
    "律师",
    "法务",
    "翻译",
    "档案",
    "资料",
    "质检",
    "风电",
]


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\u00a0", " ").replace("\u3000", " ").strip()
    text = re.sub(r"\s+", " ", text)
    if text.lower() in {"", "nan", "none", "null", "n/a", "na", "-"}:
        return ""
    return text


def dedup_keep_order(values: Iterable[Any]) -> List[Any]:
    seen = set()
    result = []
    for value in values:
        text = clean_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def normalize_job_name_key(value: Any, *, drop_level_words: bool = True) -> str:
    """Compact job names for deterministic lookup without over-normalizing roles."""
    text = clean_text(value).lower()
    if not text:
        return ""
    text = text.replace("＋", "+").replace("＃", "#")
    text = WEAK_WORD_PATTERN.sub("", text)
    if drop_level_words:
        text = LEVEL_WORD_PATTERN.sub("", text)
    text = SYMBOL_PATTERN.sub("", text)
    return text


def _build_normalized_index(standard_job_names: List[str]) -> Dict[str, List[str]]:
    index: Dict[str, List[str]] = {}
    for name in standard_job_names:
        key = normalize_job_name_key(name)
        if not key:
            continue
        index.setdefault(key, []).append(name)
    return index


def _build_alias_index(
    aliases: Optional[Dict[str, Any]],
    standard_job_names: List[str],
) -> Dict[str, List[Tuple[str, str]]]:
    standard_set = set(standard_job_names)
    alias_index: Dict[str, List[Tuple[str, str]]] = {}
    if not isinstance(aliases, dict):
        return alias_index

    for standard_name, alias_values in aliases.items():
        standard = clean_text(standard_name)
        if standard not in standard_set:
            continue
        values = alias_values if isinstance(alias_values, list) else [alias_values]
        for alias in values:
            alias_text = clean_text(alias)
            alias_key = normalize_job_name_key(alias_text)
            if not alias_key:
                continue
            alias_index.setdefault(alias_key, []).append((standard, alias_text))
    return alias_index


def extract_job_tokens(value: Any) -> List[str]:
    """Extract conservative role tokens for unique-candidate matching."""
    text = clean_text(value).lower()
    if not text:
        return []

    tokens: List[str] = []
    for token in re.findall(r"[a-zA-Z][a-zA-Z0-9+#/]*", text):
        normalized = normalize_job_name_key(token, drop_level_words=False)
        if normalized:
            tokens.append(normalized)

    compact = normalize_job_name_key(text, drop_level_words=False)
    for keyword in JOB_KEYWORDS:
        key = normalize_job_name_key(keyword, drop_level_words=False)
        if key and key in compact:
            tokens.append(key)

    # Keep specific longer tokens first so "软件测试" wins over a broad "测试".
    return sorted(dedup_keep_order(tokens), key=len, reverse=True)


def _token_candidate_names(requested: str, standard_job_names: List[str]) -> List[str]:
    tokens = extract_job_tokens(requested)
    if not tokens:
        return []

    candidates: List[str] = []
    for name in standard_job_names:
        key = normalize_job_name_key(name, drop_level_words=False)
        if not key:
            continue
        specific_tokens = [token for token in tokens if len(token) >= 3 or re.search(r"[a-zA-Z+#/]", token)]
        check_tokens = specific_tokens or tokens
        if all(token in key for token in check_tokens):
            candidates.append(name)
    return dedup_keep_order(candidates)


def _similarity_candidates(requested: str, standard_job_names: List[str]) -> List[Tuple[str, float]]:
    requested_key = normalize_job_name_key(requested)
    if len(requested_key) < 4:
        return []

    scored: List[Tuple[str, float]] = []
    for name in standard_job_names:
        candidate_key = normalize_job_name_key(name)
        if not candidate_key:
            continue
        score = difflib.SequenceMatcher(None, requested_key, candidate_key).ratio()
        if requested_key in candidate_key or candidate_key in requested_key:
            score = max(score, 0.86)
        scored.append((name, score))
    scored.sort(key=lambda item: item[1], reverse=True)
    return scored


def _result(
    requested: str,
    resolved: str,
    asset_found: bool,
    method: str,
    confidence: float,
    candidates: Optional[List[str]] = None,
    matched_alias: str = "",
) -> Dict[str, Any]:
    return {
        "requested_job_name": requested,
        "resolved_standard_job_name": resolved,
        "asset_found": bool(asset_found),
        "resolution_method": method,
        "resolution_confidence": round(float(confidence), 4),
        "candidate_jobs": candidates or ([resolved] if asset_found and resolved else []),
        "matched_alias": matched_alias,
    }


def resolve_standard_job_name(
    raw_job_name: Any,
    standard_job_names: Iterable[Any],
    aliases: Optional[Dict[str, Any]] = None,
    similarity_threshold: float = 0.82,
    similarity_margin: float = 0.08,
) -> Dict[str, Any]:
    """Resolve a raw job name to a standard job name with conservative fallbacks."""
    requested = clean_text(raw_job_name)
    standards = dedup_keep_order(clean_text(name) for name in standard_job_names if clean_text(name))
    if not requested or not standards:
        return _result(requested, requested, False, "empty", 0.0, [])

    if requested in standards:
        return _result(requested, requested, True, "exact", 1.0)

    normalized_index = _build_normalized_index(standards)
    requested_key = normalize_job_name_key(requested)
    normalized_candidates = normalized_index.get(requested_key, [])
    if len(normalized_candidates) == 1:
        return _result(requested, normalized_candidates[0], True, "normalized_exact", 0.96)
    if len(normalized_candidates) > 1:
        return _result(requested, requested, False, "ambiguous_normalized", 0.0, normalized_candidates)

    alias_index = _build_alias_index(aliases, standards)
    alias_candidates = alias_index.get(requested_key, [])
    alias_standards = dedup_keep_order(item[0] for item in alias_candidates)
    if len(alias_standards) == 1:
        matched_alias = alias_candidates[0][1] if alias_candidates else ""
        return _result(requested, alias_standards[0], True, "alias", 0.98, alias_standards, matched_alias)
    if len(alias_standards) > 1:
        return _result(requested, requested, False, "ambiguous_alias", 0.0, alias_standards)

    token_candidates = _token_candidate_names(requested, standards)
    if len(token_candidates) == 1:
        return _result(requested, token_candidates[0], True, "token_unique", 0.9, token_candidates)
    if len(token_candidates) > 1:
        return _result(requested, requested, False, "ambiguous_token", 0.0, token_candidates[:5])

    scored = _similarity_candidates(requested, standards)
    if scored:
        top_name, top_score = scored[0]
        second_score = scored[1][1] if len(scored) > 1 else 0.0
        if top_score >= similarity_threshold and top_score - second_score >= similarity_margin:
            return _result(requested, top_name, True, "similarity", top_score, [top_name])
        likely_candidates = [name for name, score in scored[:5] if score >= max(0.72, similarity_threshold - 0.1)]
        if likely_candidates:
            return _result(requested, requested, False, "unresolved", 0.0, likely_candidates)

    return _result(requested, requested, False, "unresolved", 0.0, [])
