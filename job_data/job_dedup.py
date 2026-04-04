"""
job_dedup.py

岗位重复识别与岗位名称归一。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pandas as pd

from llm_interface_layer.llm_service import call_llm


DOMAIN_KEYWORDS = [
    "java", "python", "golang", "go", "c++", "c#", "php", "vue", "react", "angular",
    "node", "node.js", "html", "css", "javascript", "typescript", "sql", "mysql",
    "oracle", "redis", "mongodb", "postgresql", "linux", "docker", "kubernetes",
    "前端", "后端", "全栈", "开发", "工程师", "测试", "算法", "数据", "分析", "产品",
    "实施", "运维", "安全", "架构", "设计", "视觉", "ui", "交互", "销售", "技术支持",
]

STOP_TOKENS = {
    "工程师", "开发", "专员", "顾问", "助理", "岗位", "高级", "资深", "中级", "初级",
    "经理", "主管", "负责人", "实习", "全职", "兼职",
}


@dataclass
class PairDecision:
    title_id_1: str
    title_id_2: str
    raw_job_name_1: str
    raw_job_name_2: str
    normalized_job_name_1: str
    normalized_job_name_2: str
    is_same_standard_job: bool
    standard_job_name: str
    confidence: float
    merge_reason: str
    llm_raw_response: str


class UnionFind:
    def __init__(self, items: Iterable[str]) -> None:
        self.parent = {item: item for item in items}

    def find(self, item: str) -> str:
        if self.parent[item] != item:
            self.parent[item] = self.find(self.parent[item])
        return self.parent[item]

    def union(self, a: str, b: str) -> None:
        root_a = self.find(a)
        root_b = self.find(b)
        if root_a != root_b:
            self.parent[root_b] = root_a


def clean_text(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null"}:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def stable_hash(text: str, length: int = 24) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()[:length]


def get_preferred_column(df: pd.DataFrame, candidates: Sequence[str], default: str = "") -> str:
    for col in candidates:
        if col in df.columns:
            return col
    return default


def most_common_nonempty(values: Sequence[object]) -> str:
    cleaned = [clean_text(v) for v in values if clean_text(v)]
    return Counter(cleaned).most_common(1)[0][0] if cleaned else ""


def pick_first_nonempty(values: Sequence[object]) -> str:
    for value in values:
        text = clean_text(value)
        if text:
            return text
    return ""


def normalize_job_name_rule(job_name: object) -> str:
    text = clean_text(job_name).lower()
    if not text:
        return ""
    text = re.sub(r"[()（）【】\[\]]", "", text)
    text = re.sub(r"(急聘|诚聘|直招|校招|社招|双休|五险一金|可转正|接受小白|包吃住)", "", text)
    text = re.sub(r"[|｜/_\s]+", "", text)
    replace_rules = [
        ("java开发", "java开发工程师"),
        ("python开发", "python开发工程师"),
        ("前端开发", "前端开发工程师"),
        ("后端开发", "后端开发工程师"),
        ("测试开发", "测试开发工程师"),
        ("实施工程", "实施工程师"),
        ("技术支持工程", "技术支持工程师"),
        ("运维工程", "运维工程师"),
        ("数据分析员", "数据分析师"),
    ]
    for source, target in replace_rules:
        if source in text and target not in text:
            text = text.replace(source, target)
    return text


def infer_job_family(job_name: str) -> str:
    lowered = clean_text(job_name).lower()
    rules = [
        ("前端", "前端开发"), ("后端", "后端开发"), ("全栈", "全栈开发"),
        ("java", "Java开发"), ("python", "Python开发"), ("测试", "测试"),
        ("算法", "算法"), ("数据分析", "数据分析"), ("数据开发", "数据开发"),
        ("数据", "数据类"), ("产品", "产品"), ("实施", "实施交付"),
        ("运维", "运维"), ("安全", "安全"), ("架构", "架构"),
        ("ui", "UI设计"), ("设计", "设计"), ("销售", "销售"), ("技术支持", "技术支持"),
    ]
    for keyword, family in rules:
        if keyword in lowered:
            return family
    return "其他"


def tokenize_job_name(job_name: object) -> List[str]:
    text = clean_text(job_name).lower()
    if not text:
        return []
    tokens: Set[str] = set(re.findall(r"[a-zA-Z][a-zA-Z0-9.+#-]*", text))
    for keyword in DOMAIN_KEYWORDS:
        if keyword in text:
            tokens.add(keyword.lower())
    for part in re.findall(r"[\u4e00-\u9fa5]{2,}", text):
        if part not in STOP_TOKENS:
            tokens.add(part)
    return sorted(token for token in tokens if token and token not in STOP_TOKENS)


def choose_primary_tokens(tokens: Sequence[str], max_tokens: int = 3) -> List[str]:
    return sorted(tokens, key=lambda x: (-len(x), x))[:max_tokens]


def build_title_profile_table(df: pd.DataFrame) -> pd.DataFrame:
    if "job_title" not in df.columns:
        raise ValueError("Input DataFrame must contain column: job_title")

    result = df.copy()
    company_col = get_preferred_column(result, ["company_name_norm", "company_name"])
    city_col = get_preferred_column(result, ["city", "job_address_norm", "job_address"])
    desc_col = get_preferred_column(result, ["job_description_clean", "job_description_text", "job_description"])

    if "job_title_rule_normalized" not in result.columns:
        source_col = "job_title_norm" if "job_title_norm" in result.columns else "job_title"
        result["job_title_rule_normalized"] = result[source_col].apply(normalize_job_name_rule)

    rows = []
    for raw_job_name, group in result.groupby("job_title", dropna=False):
        raw_job_name = clean_text(raw_job_name)
        normalized_job_name = most_common_nonempty(group["job_title_rule_normalized"].tolist())
        normalized_job_name = normalized_job_name or normalize_job_name_rule(raw_job_name)
        sample_companies = []
        sample_cities = []
        sample_description = ""
        if company_col:
            sample_companies = [clean_text(x) for x in group[company_col].dropna().astype(str).unique().tolist() if clean_text(x)][:5]
        if city_col:
            sample_cities = [clean_text(x) for x in group[city_col].dropna().astype(str).unique().tolist() if clean_text(x)][:5]
        if desc_col:
            sample_description = pick_first_nonempty(group[desc_col].tolist())
        tokens = tokenize_job_name(normalized_job_name or raw_job_name)
        rows.append(
            {
                "title_id": stable_hash(raw_job_name or normalized_job_name),
                "raw_job_name": raw_job_name,
                "normalized_job_name": normalized_job_name,
                "job_family": infer_job_family(normalized_job_name or raw_job_name),
                "title_tokens": tokens,
                "primary_tokens": choose_primary_tokens(tokens),
                "occurrence_count": int(len(group)),
                "sample_companies": sample_companies,
                "sample_cities": sample_cities,
                "sample_description": sample_description,
            }
        )
    return pd.DataFrame(rows).sort_values(
        by=["occurrence_count", "normalized_job_name", "raw_job_name"],
        ascending=[False, True, True],
    ).reset_index(drop=True)


def compute_title_pair_features(left: pd.Series, right: pd.Series) -> Dict[str, Any]:
    title_1 = clean_text(left["normalized_job_name"])
    title_2 = clean_text(right["normalized_job_name"])
    tokens_1 = set(left["title_tokens"] or [])
    tokens_2 = set(right["title_tokens"] or [])
    overlap = tokens_1 & tokens_2
    union = tokens_1 | tokens_2
    token_jaccard = len(overlap) / len(union) if union else 0.0
    edit_similarity = SequenceMatcher(None, title_1.lower(), title_2.lower()).ratio()
    contains_relation = bool(title_1 and title_2 and (title_1 in title_2 or title_2 in title_1))
    same_family = left["job_family"] == right["job_family"]
    same_normalized = title_1 == title_2 and bool(title_1)
    company_overlap = len(set(left["sample_companies"] or []) & set(right["sample_companies"] or []))
    city_overlap = len(set(left["sample_cities"] or []) & set(right["sample_cities"] or []))
    rule_score = 0.0
    if same_normalized:
        rule_score += 0.45
    if same_family:
        rule_score += 0.15
    if contains_relation:
        rule_score += 0.15
    rule_score += 0.15 * edit_similarity
    rule_score += 0.20 * token_jaccard
    if company_overlap > 0:
        rule_score += 0.08
    if city_overlap > 0:
        rule_score += 0.05
    return {
        "edit_similarity": round(edit_similarity, 4),
        "token_jaccard": round(token_jaccard, 4),
        "token_overlap_count": len(overlap),
        "token_overlap_terms": sorted(overlap),
        "same_family": same_family,
        "same_normalized": same_normalized,
        "contains_relation": contains_relation,
        "company_overlap_count": company_overlap,
        "city_overlap_count": city_overlap,
        "rule_recall_score": min(1.0, round(rule_score, 4)),
    }


def should_recall_pair(features: Dict[str, Any]) -> bool:
    if features["same_normalized"]:
        return True
    if features["rule_recall_score"] >= 0.45:
        return True
    if features["same_family"] and features["edit_similarity"] >= 0.70:
        return True
    if features["contains_relation"] and features["same_family"]:
        return True
    if features["token_jaccard"] >= 0.34 and features["same_family"]:
        return True
    if features["company_overlap_count"] > 0 and features["token_overlap_count"] > 0:
        return True
    if features["city_overlap_count"] > 0 and features["edit_similarity"] >= 0.72:
        return True
    return False


def build_block_keys(row: pd.Series) -> List[Tuple[str, str]]:
    keys: Set[Tuple[str, str]] = set()
    family = clean_text(row["job_family"])
    normalized_job_name = clean_text(row["normalized_job_name"])
    if family:
        keys.add(("family", family))
    if normalized_job_name:
        keys.add(("prefix2", normalized_job_name[:2]))
        keys.add(("prefix3", normalized_job_name[:3]))
    for token in row["primary_tokens"] or []:
        keys.add(("token", token))
    return sorted(keys)


def recall_candidate_pairs(title_profiles: pd.DataFrame, max_block_size: int = 200) -> pd.DataFrame:
    if title_profiles.empty:
        return pd.DataFrame()

    block_map: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    lookup = {row["title_id"]: row for _, row in title_profiles.iterrows()}

    for _, row in title_profiles.iterrows():
        for key in build_block_keys(row):
            block_map[key].add(row["title_id"])

    pair_ids: Set[Tuple[str, str]] = set()
    for _, ids in block_map.items():
        sorted_ids = sorted(ids)
        if len(sorted_ids) <= 1 or len(sorted_ids) > max_block_size:
            continue
        for i in range(len(sorted_ids)):
            for j in range(i + 1, len(sorted_ids)):
                pair_ids.add((sorted_ids[i], sorted_ids[j]))

    rows = []
    for title_id_1, title_id_2 in sorted(pair_ids):
        left = lookup[title_id_1]
        right = lookup[title_id_2]
        features = compute_title_pair_features(left, right)
        if not should_recall_pair(features):
            continue
        rows.append(
            {
                "title_id_1": left["title_id"],
                "title_id_2": right["title_id"],
                "raw_job_name_1": left["raw_job_name"],
                "raw_job_name_2": right["raw_job_name"],
                "normalized_job_name_1": left["normalized_job_name"],
                "normalized_job_name_2": right["normalized_job_name"],
                "job_family_1": left["job_family"],
                "job_family_2": right["job_family"],
                "sample_companies_1": json.dumps(left["sample_companies"], ensure_ascii=False),
                "sample_companies_2": json.dumps(right["sample_companies"], ensure_ascii=False),
                "sample_cities_1": json.dumps(left["sample_cities"], ensure_ascii=False),
                "sample_cities_2": json.dumps(right["sample_cities"], ensure_ascii=False),
                "sample_description_1": left["sample_description"],
                "sample_description_2": right["sample_description"],
                **features,
            }
        )

    candidate_pairs = pd.DataFrame(rows)
    if candidate_pairs.empty:
        return candidate_pairs
    return candidate_pairs.sort_values(
        by=["rule_recall_score", "edit_similarity", "token_jaccard"],
        ascending=[False, False, False],
    ).reset_index(drop=True)


def _first_json_item(value: object) -> str:
    if value is None:
        return ""
    try:
        data = json.loads(str(value))
        if isinstance(data, list) and data:
            return clean_text(data[0])
    except Exception:
        pass
    return ""


def build_llm_input_for_pair(candidate_row: pd.Series) -> Dict[str, Any]:
    return {
        "titles": [candidate_row["raw_job_name_1"], candidate_row["raw_job_name_2"]],
        "records": [
            {
                "record_id": candidate_row["title_id_1"],
                "job_title": candidate_row["raw_job_name_1"],
                "normalized_job_title": candidate_row["normalized_job_name_1"],
                "company_name": _first_json_item(candidate_row["sample_companies_1"]),
                "city": _first_json_item(candidate_row["sample_cities_1"]),
                "job_description_text": candidate_row["sample_description_1"],
            },
            {
                "record_id": candidate_row["title_id_2"],
                "job_title": candidate_row["raw_job_name_2"],
                "normalized_job_title": candidate_row["normalized_job_name_2"],
                "company_name": _first_json_item(candidate_row["sample_companies_2"]),
                "city": _first_json_item(candidate_row["sample_cities_2"]),
                "job_description_text": candidate_row["sample_description_2"],
            },
        ],
    }


def choose_pair_standard_job_name(candidate_row: pd.Series, llm_title_map: Dict[str, str]) -> str:
    raw_1 = candidate_row["raw_job_name_1"]
    raw_2 = candidate_row["raw_job_name_2"]
    norm_1 = candidate_row["normalized_job_name_1"]
    norm_2 = candidate_row["normalized_job_name_2"]
    llm_norm_1 = llm_title_map.get(raw_1, "")
    llm_norm_2 = llm_title_map.get(raw_2, "")

    if llm_norm_1 and llm_norm_2 and llm_norm_1 == llm_norm_2:
        return llm_norm_1
    if norm_1 and norm_2 and norm_1 == norm_2:
        return norm_1

    candidates = [x for x in [llm_norm_1, llm_norm_2, norm_1, norm_2, raw_1, raw_2] if clean_text(x)]
    if not candidates:
        return ""

    def rank(name: str) -> Tuple[int, int, str]:
        score = 0
        lowered = name.lower()
        if "工程师" in name:
            score += 3
        if "开发" in name:
            score += 2
        if "经理" in name or "架构师" in name or "分析师" in name:
            score += 2
        if any(keyword in lowered for keyword in ["java", "python", "前端", "后端", "数据", "测试", "运维"]):
            score += 1
        return (score, len(name), name)

    return sorted(candidates, key=rank, reverse=True)[0]


def parse_llm_judgement(candidate_row: pd.Series, llm_response: Dict[str, Any]) -> PairDecision:
    raw_1 = candidate_row["raw_job_name_1"]
    raw_2 = candidate_row["raw_job_name_2"]
    norm_1 = candidate_row["normalized_job_name_1"]
    norm_2 = candidate_row["normalized_job_name_2"]
    title_id_1 = candidate_row["title_id_1"]
    title_id_2 = candidate_row["title_id_2"]

    if isinstance(llm_response.get("is_same_standard_job"), bool):
        is_same = llm_response.get("is_same_standard_job", False)
        standard_job_name = clean_text(llm_response.get("standard_job_name", ""))
        confidence = float(llm_response.get("confidence", candidate_row["rule_recall_score"]))
        merge_reason = clean_text(llm_response.get("merge_reason", "llm_direct_judgement"))
        return PairDecision(title_id_1, title_id_2, raw_1, raw_2, norm_1, norm_2, is_same, standard_job_name if is_same else "", round(confidence, 4), merge_reason, json.dumps(llm_response, ensure_ascii=False))

    normalized_items = llm_response.get("normalized_titles") or llm_response.get("mappings") or []
    llm_title_map: Dict[str, str] = {}
    llm_confidences: List[float] = []
    for item in normalized_items:
        raw_title = clean_text(item.get("raw_title", ""))
        normalized_title = clean_text(item.get("normalized_title", ""))
        if raw_title:
            llm_title_map[raw_title] = normalized_title
        try:
            llm_confidences.append(float(item.get("confidence", 0.0)))
        except Exception:
            pass

    same_by_title_normalize = bool(llm_title_map.get(raw_1) and llm_title_map.get(raw_2) and llm_title_map[raw_1] == llm_title_map[raw_2])
    same_by_duplicate_group = False
    for group in llm_response.get("duplicate_groups", []):
        related_ids = [clean_text(group.get("master_record_id", ""))]
        related_ids.extend(clean_text(x) for x in group.get("duplicate_record_ids", []))
        if title_id_1 in related_ids and title_id_2 in related_ids:
            same_by_duplicate_group = True
            break

    is_same = same_by_duplicate_group or same_by_title_normalize
    if not is_same and candidate_row["same_normalized"] and candidate_row["rule_recall_score"] >= 0.80:
        is_same = True
    if not is_same and candidate_row["contains_relation"] and candidate_row["same_family"] and candidate_row["rule_recall_score"] >= 0.72:
        is_same = True

    standard_job_name = choose_pair_standard_job_name(candidate_row, llm_title_map) if is_same else ""
    base_confidence = sum(llm_confidences) / len(llm_confidences) if llm_confidences else float(candidate_row["rule_recall_score"])
    confidence = base_confidence
    if same_by_duplicate_group:
        confidence = max(confidence, 0.92)
    elif same_by_title_normalize:
        confidence = max(confidence, 0.86)
    elif is_same:
        confidence = max(confidence, 0.75)
    else:
        confidence = min(confidence, 0.49)

    reasons = []
    if same_by_duplicate_group:
        reasons.append("llm_duplicate_group")
    if same_by_title_normalize:
        reasons.append("llm_same_normalized_title")
    if candidate_row["same_normalized"]:
        reasons.append("rule_same_normalized")
    if candidate_row["contains_relation"]:
        reasons.append("rule_title_contains")
    if candidate_row["token_overlap_count"] > 0:
        reasons.append("rule_token_overlap")
    if not reasons:
        reasons.append("rule_not_same")

    return PairDecision(title_id_1, title_id_2, raw_1, raw_2, norm_1, norm_2, is_same, standard_job_name, round(float(confidence), 4), "|".join(reasons), json.dumps(llm_response, ensure_ascii=False))


def judge_candidate_pair_with_llm(candidate_row: pd.Series) -> PairDecision:
    input_data = build_llm_input_for_pair(candidate_row)
    extra_context = {
        "scenario": "pairwise_job_standardization",
        "goal": "判断两个岗位名称是否属于同一标准岗位，并给出标准岗位名称",
        "rule_features": {
            "same_family": bool(candidate_row["same_family"]),
            "same_normalized": bool(candidate_row["same_normalized"]),
            "contains_relation": bool(candidate_row["contains_relation"]),
            "edit_similarity": float(candidate_row["edit_similarity"]),
            "token_jaccard": float(candidate_row["token_jaccard"]),
            "rule_recall_score": float(candidate_row["rule_recall_score"]),
        },
    }
    llm_response = call_llm(
        "job_dedup",
        input_data=input_data,
        context_data=None,
        student_state=None,
        extra_context=extra_context,
    )
    return parse_llm_judgement(candidate_row, llm_response)


def judge_candidate_pairs(candidate_pairs: pd.DataFrame) -> pd.DataFrame:
    """逐对调用大模型判断岗位是否同类，并打印实时进度日志。"""
    if candidate_pairs.empty:
        print("[job_dedup] No candidate pairs recalled, skip LLM pair judgement.")
        return pd.DataFrame(
            columns=[
                "title_id_1", "title_id_2", "raw_job_name_1", "raw_job_name_2",
                "normalized_job_name_1", "normalized_job_name_2", "is_same_standard_job",
                "standard_job_name", "confidence", "merge_reason", "llm_raw_response",
            ]
        )

    total_pairs = len(candidate_pairs)
    print(f"[job_dedup] Start LLM pair judgement, candidate pairs: {total_pairs}")

    decisions = []
    for idx, (_, row) in enumerate(candidate_pairs.iterrows(), start=1):
        pair_label = f"{clean_text(row.get('raw_job_name_1'))} <-> {clean_text(row.get('raw_job_name_2'))}"
        print(
            f"[job_dedup] Judging pair {idx}/{total_pairs}: {pair_label} | "
            f"rule_recall_score={row.get('rule_recall_score')}"
        )

        start_time = time.time()
        try:
            decision = judge_candidate_pair_with_llm(row)
        except Exception as exc:
            elapsed = time.time() - start_time
            print(
                f"[job_dedup] Pair {idx}/{total_pairs} failed after {elapsed:.2f}s: "
                f"{pair_label} | error={exc}"
            )
            raise

        elapsed = time.time() - start_time
        print(
            f"[job_dedup] Pair {idx}/{total_pairs} done in {elapsed:.2f}s: "
            f"is_same={decision.is_same_standard_job}, "
            f"standard_job_name={decision.standard_job_name}, "
            f"confidence={decision.confidence}"
        )
        decisions.append(decision.__dict__)

    print(f"[job_dedup] Finished LLM pair judgement, processed pairs: {len(decisions)}")
    return pd.DataFrame(decisions)


def choose_cluster_standard_name(cluster_profiles: pd.DataFrame) -> str:
    weighted_candidates: Dict[str, float] = defaultdict(float)
    for _, row in cluster_profiles.iterrows():
        normalized_name = clean_text(row["normalized_job_name"])
        raw_name = clean_text(row["raw_job_name"])
        count_weight = max(1, int(row["occurrence_count"]))
        if normalized_name:
            weighted_candidates[normalized_name] += count_weight + 0.2 * len(normalized_name)
        if raw_name:
            weighted_candidates[raw_name] += 0.5 * count_weight
    if not weighted_candidates:
        return ""

    def rank(item: Tuple[str, float]) -> Tuple[float, int, str]:
        name, score = item
        bonus = 0
        if "工程师" in name:
            bonus += 3
        if "开发" in name:
            bonus += 2
        if "经理" in name or "分析师" in name or "架构师" in name:
            bonus += 2
        return (score + bonus, len(name), name)

    return sorted(weighted_candidates.items(), key=rank, reverse=True)[0][0]


def merge_pair_results(
    title_profiles: pd.DataFrame,
    pair_results: pd.DataFrame,
    positive_confidence_threshold: float = 0.60,
) -> pd.DataFrame:
    if title_profiles.empty:
        return pd.DataFrame(
            columns=[
                "title_id", "raw_job_name", "normalized_job_name", "standard_job_name",
                "is_same_standard_job", "confidence", "merge_reason", "occurrence_count",
            ]
        )

    uf = UnionFind(title_profiles["title_id"].tolist())
    positive_edges = pd.DataFrame()
    if not pair_results.empty:
        positive_edges = pair_results[
            (pair_results["is_same_standard_job"] == True)
            & (pair_results["confidence"] >= positive_confidence_threshold)
        ].copy()
        for _, row in positive_edges.iterrows():
            uf.union(row["title_id_1"], row["title_id_2"])

    mapping_df = title_profiles.copy()
    mapping_df["cluster_id"] = mapping_df["title_id"].apply(uf.find)
    cluster_standard_map = {
        cluster_id: choose_cluster_standard_name(cluster_rows)
        for cluster_id, cluster_rows in mapping_df.groupby("cluster_id", dropna=False)
    }
    mapping_df["standard_job_name"] = mapping_df["cluster_id"].map(cluster_standard_map)
    mapping_df["cluster_size"] = mapping_df.groupby("cluster_id")["title_id"].transform("count")
    mapping_df["is_same_standard_job"] = mapping_df["cluster_size"] > 1

    confidence_map: Dict[str, float] = defaultdict(lambda: 0.80)
    reason_map: Dict[str, Set[str]] = defaultdict(set)
    if not positive_edges.empty:
        for _, row in positive_edges.iterrows():
            confidence_value = float(row["confidence"])
            confidence_map[row["title_id_1"]] = max(confidence_map[row["title_id_1"]], confidence_value)
            confidence_map[row["title_id_2"]] = max(confidence_map[row["title_id_2"]], confidence_value)
            reason_map[row["title_id_1"]].add(clean_text(row["merge_reason"]))
            reason_map[row["title_id_2"]].add(clean_text(row["merge_reason"]))

    mapping_df["confidence"] = mapping_df["title_id"].apply(lambda x: round(confidence_map[x], 4))
    mapping_df["merge_reason"] = mapping_df["title_id"].apply(
        lambda x: "|".join(sorted(reason_map[x])) if reason_map[x] else "self_standardized"
    )
    return mapping_df[
        [
            "title_id", "raw_job_name", "normalized_job_name", "standard_job_name",
            "is_same_standard_job", "confidence", "merge_reason", "occurrence_count",
            "job_family", "cluster_size",
        ]
    ].sort_values(
        by=["standard_job_name", "occurrence_count", "raw_job_name"],
        ascending=[True, False, True],
    ).reset_index(drop=True)


def apply_mapping_to_dataframe(df: pd.DataFrame, mapping_df: pd.DataFrame) -> pd.DataFrame:
    if "job_title" not in df.columns:
        raise ValueError("Input DataFrame must contain column: job_title")
    result = df.copy()
    result = result.merge(
        mapping_df[
            [
                "raw_job_name", "normalized_job_name", "standard_job_name",
                "is_same_standard_job", "confidence", "merge_reason",
            ]
        ],
        left_on="job_title",
        right_on="raw_job_name",
        how="left",
    )
    return result.drop(columns=["raw_job_name"], errors="ignore")


def save_dedup_results(
    result_df: pd.DataFrame,
    mapping_df: pd.DataFrame,
    pair_results_df: Optional[pd.DataFrame],
    output_data_csv: str,
    output_mapping_csv: str,
    output_pair_csv: Optional[str] = None,
) -> None:
    Path(output_data_csv).parent.mkdir(parents=True, exist_ok=True)
    Path(output_mapping_csv).parent.mkdir(parents=True, exist_ok=True)
    result_df.to_csv(output_data_csv, index=False, encoding="utf-8-sig")
    mapping_df.to_csv(output_mapping_csv, index=False, encoding="utf-8-sig")
    if output_pair_csv and pair_results_df is not None:
        Path(output_pair_csv).parent.mkdir(parents=True, exist_ok=True)
        pair_results_df.to_csv(output_pair_csv, index=False, encoding="utf-8-sig")


def process_job_dedup(
    df: pd.DataFrame,
    output_data_csv: Optional[str] = None,
    output_mapping_csv: Optional[str] = None,
    output_pair_csv: Optional[str] = None,
    positive_confidence_threshold: float = 0.60,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    working_df = df.copy()
    if "job_title_rule_normalized" not in working_df.columns:
        source_col = "job_title_norm" if "job_title_norm" in working_df.columns else "job_title"
        working_df["job_title_rule_normalized"] = working_df[source_col].apply(normalize_job_name_rule)
    title_profiles = build_title_profile_table(working_df)
    candidate_pairs = recall_candidate_pairs(title_profiles)
    pair_results = judge_candidate_pairs(candidate_pairs)
    mapping_df = merge_pair_results(title_profiles, pair_results, positive_confidence_threshold)
    result_df = apply_mapping_to_dataframe(working_df, mapping_df)
    if output_data_csv and output_mapping_csv:
        save_dedup_results(result_df, mapping_df, pair_results, output_data_csv, output_mapping_csv, output_pair_csv)
    return result_df, mapping_df, pair_results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="岗位重复识别与岗位名称归一")
    parser.add_argument("--input", default="outputs/jobs_cleaned.csv", help="输入清洗后 CSV 文件路径")
    parser.add_argument("--output-data", default="outputs/jobs_dedup_result.csv", help="输出岗位结果 CSV 文件路径")
    parser.add_argument("--output-mapping", default="outputs/job_name_mapping.csv", help="输出岗位名称映射表 CSV 文件路径")
    parser.add_argument("--output-pairs", default="outputs/job_dedup_pairs.csv", help="输出候选岗位对判断结果 CSV 文件路径")
    parser.add_argument("--threshold", type=float, default=0.60, help="判定合并为同一标准岗位的置信度阈值")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {args.input}")
    df = pd.read_csv(input_path, dtype=str).fillna("")
    result_df, mapping_df, pair_results_df = process_job_dedup(
        df=df,
        output_data_csv=args.output_data,
        output_mapping_csv=args.output_mapping,
        output_pair_csv=args.output_pairs,
        positive_confidence_threshold=args.threshold,
    )
    print("Job dedup finished.")
    print(f"Input rows: {len(df)}")
    print(f"Title mapping rows: {len(mapping_df)}")
    print(f"Candidate pair rows: {len(pair_results_df)}")
    print(f"Output data: {args.output_data}")
    print(f"Output mapping: {args.output_mapping}")
    print(f"Output pairs: {args.output_pairs}")


if __name__ == "__main__":
    main()



