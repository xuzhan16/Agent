"""
build_match_assets.py

人岗匹配前端展示与赛题评测资产后处理脚本。

本脚本不重跑岗位底库主流水线，只基于已经生成的
outputs/intermediate/jobs_extracted_full.csv 构建后续 job_match
和前端展示需要的匹配资产。
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_CSV = PROJECT_ROOT / "outputs" / "intermediate" / "jobs_extracted_full.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "match_assets"
DEFAULT_SKILL_KNOWLEDGE_MAP = PROJECT_ROOT / "configs" / "skill_knowledge_map.json"
DEFAULT_CORE_JOB_RULES = PROJECT_ROOT / "configs" / "core_job_rules.json"

CHINA_TZ = timezone(timedelta(hours=8))

DEGREE_RANK: Dict[str, int] = {
    "未明确": -1,
    "学历不限": 0,
    "高中/中专": 1,
    "大专": 2,
    "本科": 3,
    "硕士": 4,
    "博士": 5,
}

DEGREE_PATTERNS: List[Tuple[str, str]] = [
    (r"(学历不限|不限学历|无学历要求)", "学历不限"),
    (r"(博士|博士研究生|PhD|PHD|phd)", "博士"),
    (r"(硕士|研究生|master|MBA|mba)", "硕士"),
    (r"(本科|学士|bachelor)", "本科"),
    (r"(大专|专科|高职|college)", "大专"),
    (r"(高中|中专)", "高中/中专"),
]

MAJOR_ALIAS_MAP: Dict[str, List[str]] = {
    "计算机科学与技术": ["计算机科学与技术", "计算机科学", "计算机技术", "计算机", "网络工程"],
    "软件工程": ["软件工程", "软件开发", "软件技术"],
    "数据科学与大数据技术": ["数据科学与大数据技术", "数据科学", "大数据", "数据技术"],
    "人工智能": ["人工智能", "机器学习", "智能科学与技术", "模式识别"],
    "统计学": ["统计学", "应用统计", "数理统计", "经济统计"],
    "数学": ["数学", "应用数学", "数学与应用数学", "计算数学"],
    "电子信息工程": ["电子信息", "电子信息工程", "通信工程", "自动化", "电子科学与技术"],
    "信息管理与信息系统": ["信息管理与信息系统", "信息管理", "信息系统", "信管"],
    "地理信息科学": ["地理信息", "GIS", "gis", "测绘工程", "遥感科学与技术"],
    "网络空间安全": ["网络空间安全", "信息安全", "网络安全", "安全工程"],
    "物联网工程": ["物联网工程", "物联网"],
    "金融学": ["金融学", "金融工程", "金融科技"],
    "市场营销": ["市场营销", "电子商务", "工商管理"],
}

CERTIFICATE_ALIAS_MAP: Dict[str, List[str]] = {
    "CET-4": ["CET-4", "cet-4", "cet4", "英语四级", "大学英语四级", "四级"],
    "CET-6": ["CET-6", "cet-6", "cet6", "英语六级", "大学英语六级", "六级"],
    "PMP": ["PMP", "pmp", "项目管理专业人士"],
    "CPA": ["CPA", "cpa", "注册会计师"],
    "软考": ["软考", "软件设计师", "系统架构设计师", "信息系统项目管理师", "系统集成项目管理工程师"],
    "软件评测师": ["软件评测师", "软件测试工程师证书"],
    "ISTQB": ["ISTQB", "istqb", "国际软件测试认证"],
    "计算机二级": ["计算机二级", "全国计算机等级考试二级", "NCRE二级", "ncre二级"],
    "教师资格证": ["教师资格证", "教资"],
    "日语N1": ["日语N1", "日语一级", "JLPT N1", "N1"],
    "日语N2": ["日语N2", "日语二级", "JLPT N2", "N2"],
    "日语N3": ["日语N3", "日语三级", "JLPT N3", "N3"],
    "CCNA": ["CCNA", "ccna"],
    "HCIA": ["HCIA", "hcia", "华为HCIA"],
    "HCIP": ["HCIP", "hcip", "华为HCIP"],
    "AWS认证": ["AWS认证", "AWS Certified", "aws认证"],
    "阿里云认证": ["阿里云认证", "ACP认证", "ACA认证"],
    "Oracle认证": ["Oracle认证", "OCP", "ocp"],
}

MANDATORY_CERT_TERMS = ["必须", "必备", "需持有", "需要持有", "持有", "具备", "取得", "通过", "要求"]
PREFERRED_CERT_TERMS = ["优先", "加分", "更佳", "最好", "有者优先", "可优先"]
EMPTY_CERT_LABEL = "无明确要求"


@dataclass
class SampleRequirement:
    record_id: str = ""
    standard_job_name: str = ""
    job_title: str = ""
    company_name: str = ""
    city: str = ""
    industry: str = ""
    is_cs_related: str = ""
    portrait_degree_requirement: str = ""
    portrait_major_requirement: str = ""
    portrait_certificate_requirement_json: str = "[]"
    sample_degree_requirement: str = "未明确"
    sample_degree_rank: int = -1
    sample_major_requirements_json: str = "[]"
    sample_certificate_requirements_json: str = "[]"
    sample_certificate_force_flags_json: str = "{}"
    degree_extraction_method: str = "not_found"
    major_extraction_method: str = "not_found"
    certificate_extraction_method: str = "not_found"
    extraction_confidence: float = 0.0
    degree_evidence_text: str = ""
    major_evidence_text: str = ""
    certificate_evidence_text: str = ""


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = text.replace("\u00a0", " ").replace("\u3000", " ")
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\t", " ")
    text = re.sub(r"[ ]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = "\n".join(line.strip() for line in text.splitlines())
    text = text.strip()
    if text.lower() in {"", "nan", "none", "null", "n/a", "na", "-"}:
        return ""
    return text


def compact_token(text: str) -> str:
    return re.sub(r"[()（）\[\]【】\-_/|｜·,，;；:+.#\s]", "", clean_text(text).lower())


def dedup_keep_order(values: Iterable[str]) -> List[str]:
    seen = set()
    result = []
    for value in values:
        text = clean_text(value)
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return result


def parse_list_like_value(value: Any) -> List[str]:
    if isinstance(value, list):
        return dedup_keep_order(clean_text(item) for item in value if clean_text(item))
    text = clean_text(value)
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            loaded = json.loads(text)
            if isinstance(loaded, list):
                return dedup_keep_order(clean_text(item) for item in loaded if clean_text(item))
        except json.JSONDecodeError:
            pass
    parts = re.split(r"[、,，;；/|｜\n]+", text)
    return dedup_keep_order(part for part in parts if clean_text(part))


def normalize_by_alias(text: str, alias_map: Dict[str, List[str]]) -> str:
    raw_text = clean_text(text)
    if not raw_text:
        return ""
    raw_compact = compact_token(raw_text)
    for standard_value, aliases in alias_map.items():
        if raw_text == standard_value or raw_compact == compact_token(standard_value):
            return standard_value
        for alias in aliases:
            alias_compact = compact_token(alias)
            if not alias_compact:
                continue
            if raw_compact == alias_compact:
                return standard_value
            if len(alias_compact) >= 2 and alias_compact in raw_compact:
                return standard_value
            if len(raw_compact) >= 2 and raw_compact in alias_compact:
                return standard_value
    return raw_text


def normalize_major(text: str) -> str:
    value = normalize_by_alias(text, MAJOR_ALIAS_MAP)
    if value in {"相关", "以上", "及以上", "专业", "学历", "不限", "若干"}:
        return ""
    if any(marker in value for marker in ["学历", "工作", "经验", "岗位", "职位", "年龄"]):
        return ""
    return value


def normalize_certificate(text: str) -> str:
    value = normalize_by_alias(text, CERTIFICATE_ALIAS_MAP)
    if value in {"证书", "认证", "资格证", "毕业证书", "学历证书"}:
        return ""
    if "毕业证" in value or "学历证" in value:
        return ""
    return value


def load_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def get_first_value(row: Dict[str, Any], candidates: Sequence[str]) -> str:
    for candidate in candidates:
        value = clean_text(row.get(candidate, ""))
        if value:
            return value
    return ""


def is_truthy(value: Any) -> bool:
    text = clean_text(value).lower()
    return text in {"true", "1", "yes", "y", "是", "相关", "计算机相关"}


def split_sentences(text: str) -> List[str]:
    cleaned = clean_text(text)
    if not cleaned:
        return []
    parts = re.split(r"[\n。；;.!！?？]+", cleaned)
    return [part.strip() for part in parts if 2 <= len(part.strip()) <= 240]


def find_evidence(text: str, keywords: Sequence[str]) -> str:
    compact_keywords = [compact_token(keyword) for keyword in keywords if compact_token(keyword)]
    for sentence in split_sentences(text):
        compact_sentence = compact_token(sentence)
        if any(keyword in compact_sentence for keyword in compact_keywords):
            return sentence[:220]
    return ""


def read_csv_rows(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def generated_at() -> str:
    return datetime.now(CHINA_TZ).isoformat(timespec="seconds")


def normalize_degree_value(value: str) -> str:
    text = clean_text(value)
    if not text:
        return ""
    matched = []
    for pattern, degree_name in DEGREE_PATTERNS:
        if re.search(pattern, text, flags=re.IGNORECASE):
            matched.append(degree_name)
    if not matched:
        return ""
    if "学历不限" in matched and len(matched) == 1:
        return "学历不限"
    valid = [item for item in matched if item != "学历不限"]
    return min(valid, key=lambda item: DEGREE_RANK.get(item, 99)) if valid else matched[0]


def extract_degree_requirement(job_desc: str, fallback_degree: str = "") -> Tuple[str, int, str, str]:
    text = clean_text(job_desc)
    matched = []
    for pattern, degree_name in DEGREE_PATTERNS:
        if re.search(pattern, text, flags=re.IGNORECASE):
            matched.append(degree_name)
    matched = dedup_keep_order(matched)
    if matched:
        if "学历不限" in matched and len(matched) == 1:
            selected = "学历不限"
        else:
            valid = [item for item in matched if item != "学历不限"]
            selected = min(valid, key=lambda item: DEGREE_RANK.get(item, 99)) if valid else "学历不限"
        evidence = find_evidence(text, matched) or text[:220]
        return selected, DEGREE_RANK.get(selected, -1), "regex_job_desc", evidence

    fallback_values = parse_list_like_value(fallback_degree)
    fallback_values = [normalize_degree_value(item) for item in fallback_values]
    fallback_values = [item for item in fallback_values if item]
    if fallback_values:
        valid = [item for item in fallback_values if item != "学历不限"]
        selected = min(valid, key=lambda item: DEGREE_RANK.get(item, 99)) if valid else fallback_values[0]
        return selected, DEGREE_RANK.get(selected, -1), "fallback_portrait", ""
    return "未明确", -1, "not_found", ""


def extract_major_requirements(job_desc: str, fallback_major: str = "") -> Tuple[List[str], str, str]:
    text = clean_text(job_desc)
    majors = []
    for standard_major, aliases in MAJOR_ALIAS_MAP.items():
        if standard_major in text or any(alias and alias in text for alias in aliases):
            majors.append(standard_major)

    major_pattern = re.compile(r"([\u4e00-\u9fa5A-Za-z0-9/&、,，及和与或（）()]{2,80})(?:相关)?专业")
    for match in major_pattern.finditer(text):
        parts = re.split(r"[、,，/及和与或（）()\s]+", match.group(1))
        for part in parts:
            normalized = normalize_major(part)
            if normalized and normalized in MAJOR_ALIAS_MAP:
                majors.append(normalized)

    majors = dedup_keep_order(majors)
    if majors:
        evidence = find_evidence(text, majors) or text[:220]
        return majors, "rule_job_desc", evidence

    fallback_items = []
    for item in parse_list_like_value(fallback_major):
        normalized = normalize_major(item)
        if normalized and normalized in MAJOR_ALIAS_MAP:
            fallback_items.append(normalized)
    fallback_items = dedup_keep_order(fallback_items)
    if fallback_items:
        return fallback_items, "fallback_portrait", ""
    return [], "not_found", ""


def classify_certificate_force(evidence: str) -> str:
    text = clean_text(evidence)
    if not text:
        return "unknown"
    if any(term in text for term in PREFERRED_CERT_TERMS):
        return "preferred"
    if any(term in text for term in MANDATORY_CERT_TERMS):
        return "must"
    return "mentioned"


def extract_certificate_requirements(
    job_desc: str,
    fallback_certificate: str = "",
) -> Tuple[List[str], Dict[str, str], str, str]:
    text = clean_text(job_desc)
    certificates = []
    force_flags: Dict[str, str] = {}
    for standard_cert, aliases in CERTIFICATE_ALIAS_MAP.items():
        all_names = [standard_cert] + aliases
        if any(name and name.lower() in text.lower() for name in all_names):
            certificates.append(standard_cert)
            evidence = find_evidence(text, all_names)
            force_flags[standard_cert] = classify_certificate_force(evidence)

    cert_pattern = re.compile(r"([\u4e00-\u9fa5A-Za-z0-9+-]{2,30}(?:证书|认证|资格证))")
    for match in cert_pattern.finditer(text):
        normalized = normalize_certificate(match.group(1))
        if normalized:
            certificates.append(normalized)
            evidence = find_evidence(text, [match.group(1), normalized])
            force_flags[normalized] = classify_certificate_force(evidence)

    certificates = dedup_keep_order(certificates)
    if certificates:
        evidence = find_evidence(text, certificates) or text[:220]
        return certificates, force_flags, "rule_job_desc", evidence

    fallback_items = []
    for item in parse_list_like_value(fallback_certificate):
        normalized = normalize_certificate(item)
        if normalized:
            fallback_items.append(normalized)
    fallback_items = dedup_keep_order(fallback_items)
    if fallback_items:
        return fallback_items, {item: "unknown" for item in fallback_items}, "fallback_portrait", ""
    return [], {}, "not_found", ""


def build_sample_requirements(rows: List[Dict[str, Any]]) -> List[SampleRequirement]:
    sample_requirements = []
    for row in rows:
        standard_job_name = get_first_value(row, ["standard_job_name_y", "standard_job_name_x", "standard_job_name"])
        job_desc = get_first_value(row, ["job_description_clean", "job_description", "job_desc_clean", "job_desc"])
        portrait_degree = clean_text(row.get("degree_requirement", ""))
        portrait_major = clean_text(row.get("major_requirement", ""))
        portrait_certificate = clean_text(row.get("certificate_requirement", ""))

        degree, degree_rank, degree_method, degree_evidence = extract_degree_requirement(job_desc, portrait_degree)
        majors, major_method, major_evidence = extract_major_requirements(job_desc, portrait_major)
        certs, cert_flags, cert_method, cert_evidence = extract_certificate_requirements(job_desc, portrait_certificate)

        confidence_parts = [
            1.0 if degree_method == "regex_job_desc" else 0.65 if degree_method == "fallback_portrait" else 0.25,
            1.0 if major_method == "rule_job_desc" else 0.65 if major_method == "fallback_portrait" else 0.25,
            1.0 if cert_method == "rule_job_desc" else 0.65 if cert_method == "fallback_portrait" else 0.6,
        ]
        confidence = round(sum(confidence_parts) / len(confidence_parts), 4)

        sample_requirements.append(
            SampleRequirement(
                record_id=clean_text(row.get("record_id", "")),
                standard_job_name=standard_job_name,
                job_title=get_first_value(row, ["job_title", "job_title_raw"]),
                company_name=get_first_value(row, ["company_name", "company_name_raw"]),
                city=clean_text(row.get("city", "")),
                industry=clean_text(row.get("industry", "")),
                is_cs_related=clean_text(row.get("is_cs_related", "")),
                portrait_degree_requirement=portrait_degree,
                portrait_major_requirement=portrait_major,
                portrait_certificate_requirement_json=json.dumps(parse_list_like_value(portrait_certificate), ensure_ascii=False),
                sample_degree_requirement=degree,
                sample_degree_rank=degree_rank,
                sample_major_requirements_json=json.dumps(majors, ensure_ascii=False),
                sample_certificate_requirements_json=json.dumps(certs, ensure_ascii=False),
                sample_certificate_force_flags_json=json.dumps(cert_flags, ensure_ascii=False),
                degree_extraction_method=degree_method,
                major_extraction_method=major_method,
                certificate_extraction_method=cert_method,
                extraction_confidence=confidence,
                degree_evidence_text=degree_evidence,
                major_evidence_text=major_evidence,
                certificate_evidence_text=cert_evidence,
            )
        )
    return sample_requirements


def distribution_from_counter(counter: Counter, total: int, top_n: int = 30) -> List[Dict[str, Any]]:
    result = []
    for rank, (name, count) in enumerate(counter.most_common(top_n), start=1):
        result.append(
            {
                "rank": rank,
                "name": name,
                "count": int(count),
                "ratio": round(count / total, 4) if total else 0.0,
            }
        )
    return result


def mode_text(values: Iterable[str], default: str = "") -> str:
    counter = Counter(clean_text(value) for value in values if clean_text(value))
    if not counter:
        return default
    return counter.most_common(1)[0][0]


def summarize_level(values: Iterable[str]) -> str:
    counter = Counter(clean_text(value) for value in values if clean_text(value))
    if not counter:
        return ""
    return " / ".join(item for item, _ in counter.most_common(2))


def choose_degree_gate(degree_distribution: List[Dict[str, Any]]) -> str:
    valid_items = [item for item in degree_distribution if item.get("name") != "未明确"]
    if not valid_items:
        return "未明确"
    max_count = max(int(item.get("count", 0)) for item in valid_items)
    tied = [item for item in valid_items if int(item.get("count", 0)) == max_count]
    return max(tied, key=lambda item: DEGREE_RANK.get(str(item.get("name")), -1)).get("name", "未明确")


def select_major_gate_set(
    major_distribution: List[Dict[str, Any]],
    min_ratio: float = 0.1,
    max_count: int = 5,
) -> List[str]:
    majors = []
    for item in major_distribution:
        name = clean_text(item.get("name", ""))
        ratio = float(item.get("ratio", 0.0) or 0.0)
        if name and name != "未明确" and ratio >= min_ratio:
            majors.append(name)
        if len(majors) >= max_count:
            break
    if not majors:
        majors = [clean_text(item.get("name", "")) for item in major_distribution[:3] if clean_text(item.get("name", "")) != "未明确"]
    return dedup_keep_order(majors[:max_count])


def split_certificates_by_force(
    certificate_distribution: List[Dict[str, Any]],
    cert_force_counter: Dict[str, Counter],
    min_preferred_ratio: float = 0.05,
    min_must_ratio: float = 0.35,
) -> Tuple[List[str], List[str]]:
    must = []
    preferred = []
    for item in certificate_distribution:
        name = clean_text(item.get("name", ""))
        ratio = float(item.get("ratio", 0.0) or 0.0)
        if not name or name == EMPTY_CERT_LABEL:
            continue
        force_counter = cert_force_counter.get(name, Counter())
        force_total = sum(force_counter.values())
        must_ratio = force_counter.get("must", 0) / force_total if force_total else 0.0
        if ratio >= min_must_ratio and must_ratio >= 0.5:
            must.append(name)
        elif ratio >= min_preferred_ratio:
            preferred.append(name)
    preferred = [item for item in preferred if item not in must]
    return dedup_keep_order(must), dedup_keep_order(preferred)


def build_requirement_stats(
    rows: List[Dict[str, Any]],
    sample_requirements: List[SampleRequirement],
) -> Dict[str, Any]:
    original_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        standard_job_name = get_first_value(row, ["standard_job_name_y", "standard_job_name_x", "standard_job_name"])
        if standard_job_name:
            original_groups[standard_job_name].append(row)

    sample_groups: Dict[str, List[SampleRequirement]] = defaultdict(list)
    for item in sample_requirements:
        if item.standard_job_name:
            sample_groups[item.standard_job_name].append(item)

    jobs = {}
    for standard_job_name, group_items in sorted(sample_groups.items()):
        total = len(group_items)
        degree_counter = Counter()
        major_counter = Counter()
        cert_counter = Counter()
        cert_force_counter: Dict[str, Counter] = defaultdict(Counter)
        degree_known = major_known = cert_known = 0

        for item in group_items:
            degree = clean_text(item.sample_degree_requirement) or "未明确"
            degree_counter[degree] += 1
            if degree != "未明确":
                degree_known += 1

            majors = parse_list_like_value(item.sample_major_requirements_json)
            if majors:
                major_known += 1
                major_counter.update(set(majors))
            else:
                major_counter["未明确"] += 1

            certs = parse_list_like_value(item.sample_certificate_requirements_json)
            try:
                cert_flags = json.loads(item.sample_certificate_force_flags_json)
            except json.JSONDecodeError:
                cert_flags = {}
            if certs:
                cert_known += 1
                for cert in set(certs):
                    cert_counter[cert] += 1
                    cert_force_counter[cert][clean_text(cert_flags.get(cert, "mentioned")) or "mentioned"] += 1
            else:
                cert_counter[EMPTY_CERT_LABEL] += 1

        degree_distribution = distribution_from_counter(degree_counter, total, top_n=20)
        major_distribution = distribution_from_counter(major_counter, total, top_n=30)
        certificate_distribution = distribution_from_counter(cert_counter, total, top_n=30)
        degree_gate = choose_degree_gate(degree_distribution)
        major_gate_set = select_major_gate_set(major_distribution)
        must_have, preferred = split_certificates_by_force(certificate_distribution, cert_force_counter)

        original_group = original_groups.get(standard_job_name, [])
        category = mode_text(row.get("job_category", "") for row in original_group)
        job_level_summary = summarize_level(row.get("job_level", "") for row in original_group)
        no_cert_count = cert_counter.get(EMPTY_CERT_LABEL, 0)
        mainstream_degree = degree_distribution[0]["name"] if degree_distribution else "未明确"
        mainstream_degree_ratio = degree_distribution[0]["ratio"] if degree_distribution else 0.0

        jobs[standard_job_name] = {
            "standard_job_name": standard_job_name,
            "sample_count": int(total),
            "job_category": category,
            "job_level_summary": job_level_summary,
            "degree_distribution": degree_distribution,
            "major_distribution": major_distribution,
            "certificate_distribution": certificate_distribution,
            "no_certificate_requirement_ratio": round(no_cert_count / total, 4) if total else 0.0,
            "mainstream_degree": mainstream_degree,
            "mainstream_degree_ratio": mainstream_degree_ratio,
            "mainstream_majors": [item["name"] for item in major_distribution if item["name"] != "未明确"][:5],
            "mainstream_certificates": [item["name"] for item in certificate_distribution if item["name"] != EMPTY_CERT_LABEL][:5],
            "degree_gate": degree_gate,
            "major_gate_set": major_gate_set,
            "must_have_certificates": must_have,
            "preferred_certificates": preferred,
            "source_quality": {
                "degree_coverage": round(degree_known / total, 4) if total else 0.0,
                "major_coverage": round(major_known / total, 4) if total else 0.0,
                "certificate_coverage": round(cert_known / total, 4) if total else 0.0,
            },
        }
    return jobs


def aggregate_skill_counts(rows: List[Dict[str, Any]]) -> Counter:
    counter = Counter()
    for row in rows:
        values = []
        values.extend(parse_list_like_value(row.get("hard_skills", "")))
        values.extend(parse_list_like_value(row.get("tools_or_tech_stack", "")))
        counter.update(dedup_keep_order(values))
    return counter


def map_skill_to_knowledge(
    skill_text: str,
    skill_knowledge_map: Dict[str, Dict[str, List[str]]],
) -> Tuple[List[str], List[str]]:
    skill_compact = compact_token(skill_text)
    required = []
    preferred = []
    for key, payload in skill_knowledge_map.items():
        key_compact = compact_token(key)
        if not key_compact or not skill_compact:
            continue
        if key_compact in skill_compact or skill_compact in key_compact:
            required.extend(payload.get("required", []))
            preferred.extend(payload.get("preferred", []))
    return dedup_keep_order(required), dedup_keep_order(preferred)


def build_skill_knowledge_assets(
    rows: List[Dict[str, Any]],
    skill_knowledge_map: Dict[str, Dict[str, List[str]]],
) -> Dict[str, Any]:
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        standard_job_name = get_first_value(row, ["standard_job_name_y", "standard_job_name_x", "standard_job_name"])
        if standard_job_name:
            groups[standard_job_name].append(row)

    jobs = {}
    for standard_job_name, group_rows in sorted(groups.items()):
        skill_counter = aggregate_skill_counts(group_rows)
        top_skills = [skill for skill, _ in skill_counter.most_common(20)]
        required_points = []
        preferred_points = []
        for skill in top_skills + [standard_job_name]:
            required, preferred = map_skill_to_knowledge(skill, skill_knowledge_map)
            required_points.extend(required)
            preferred_points.extend(preferred)

        if not required_points:
            required_points = [f"{skill}基础" for skill in top_skills[:5]]
        preferred_points = [item for item in preferred_points if item not in set(required_points)]

        jobs[standard_job_name] = {
            "standard_job_name": standard_job_name,
            "hard_skills": [skill for skill, _ in skill_counter.most_common(12)],
            "tools_or_tech_stack": [skill for skill, _ in skill_counter.most_common(12)],
            "required_knowledge_points": dedup_keep_order(required_points)[:20],
            "preferred_knowledge_points": dedup_keep_order(preferred_points)[:20],
            "knowledge_source": "skill_rule_map_v1",
        }
    return jobs


def load_core_rules(path: Path) -> Dict[str, Any]:
    return load_json_file(
        path,
        {
            "core_job_count": 10,
            "min_sample_count": 20,
            "include_only_cs_related": True,
            "max_jobs_per_category": 3,
            "blacklist_keywords": [],
            "category_bonus": {},
        },
    )


def calc_completeness(group_rows: List[Dict[str, Any]], stats: Dict[str, Any]) -> float:
    total = len(group_rows) or 1
    degree_score = stats.get("source_quality", {}).get("degree_coverage", 0.0)
    major_score = stats.get("source_quality", {}).get("major_coverage", 0.0)
    skill_score = sum(1 for row in group_rows if parse_list_like_value(row.get("hard_skills", ""))) / total
    path_score = sum(
        1
        for row in group_rows
        if parse_list_like_value(row.get("vertical_paths", "")) or parse_list_like_value(row.get("transfer_paths", ""))
    ) / total
    level_score = sum(1 for row in group_rows if clean_text(row.get("job_level", ""))) / total
    category_score = sum(1 for row in group_rows if clean_text(row.get("job_category", ""))) / total
    return round(
        degree_score * 0.18
        + major_score * 0.18
        + skill_score * 0.24
        + path_score * 0.2
        + level_score * 0.1
        + category_score * 0.1,
        4,
    )


def build_selection_reason(candidate: Dict[str, Any]) -> str:
    reasons = [f"样本数 {candidate.get('sample_count', 0)}"]
    if candidate.get("job_category"):
        reasons.append(f"代表{candidate['job_category']}方向")
    if candidate.get("completeness", 0) >= 0.7:
        reasons.append("画像完整度较高")
    else:
        reasons.append("具备基础画像数据")
    return "，".join(reasons)


def build_core_jobs(
    rows: List[Dict[str, Any]],
    requirement_stats: Dict[str, Any],
    skill_assets: Dict[str, Any],
    core_rules: Dict[str, Any],
) -> List[Dict[str, Any]]:
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        standard_job_name = get_first_value(row, ["standard_job_name_y", "standard_job_name_x", "standard_job_name"])
        if standard_job_name:
            groups[standard_job_name].append(row)

    blacklist_keywords = core_rules.get("blacklist_keywords", [])
    title_whitelist_keywords = core_rules.get("title_whitelist_keywords", [])
    include_only_cs = bool(core_rules.get("include_only_cs_related", True))
    min_sample_count = int(core_rules.get("min_sample_count", 20))
    category_bonus = core_rules.get("category_bonus", {})

    candidates = []
    for standard_job_name, group_rows in groups.items():
        if len(group_rows) < min_sample_count:
            continue
        if any(keyword and keyword in standard_job_name for keyword in blacklist_keywords):
            continue
        if title_whitelist_keywords and not any(keyword and keyword in standard_job_name for keyword in title_whitelist_keywords):
            continue
        if include_only_cs and not any(is_truthy(row.get("is_cs_related", "")) for row in group_rows):
            continue
        stats = requirement_stats.get(standard_job_name, {})
        category = stats.get("job_category") or mode_text(row.get("job_category", "") for row in group_rows)
        completeness = calc_completeness(group_rows, stats)
        bonus = float(category_bonus.get(category, 1.0) or 1.0)
        score = len(group_rows) * bonus + completeness * 80
        candidates.append(
            {
                "standard_job_name": standard_job_name,
                "sample_count": len(group_rows),
                "job_category": category,
                "job_level_summary": stats.get("job_level_summary", ""),
                "completeness": completeness,
                "score": round(score, 4),
            }
        )

    candidates.sort(key=lambda item: (item["score"], item["sample_count"]), reverse=True)
    target_count = int(core_rules.get("core_job_count", 10))
    max_per_category = int(core_rules.get("max_jobs_per_category", 3))
    selected = []
    category_counter = Counter()
    for candidate in candidates:
        category = candidate.get("job_category", "") or "未分类"
        if category_counter[category] >= max_per_category:
            continue
        selected.append(candidate)
        category_counter[category] += 1
        if len(selected) >= target_count:
            break

    if len(selected) < target_count:
        selected_names = {item["standard_job_name"] for item in selected}
        for candidate in candidates:
            if candidate["standard_job_name"] not in selected_names:
                selected.append(candidate)
                selected_names.add(candidate["standard_job_name"])
            if len(selected) >= target_count:
                break

    core_jobs = []
    for idx, candidate in enumerate(selected[:target_count], start=1):
        name = candidate["standard_job_name"]
        stats = requirement_stats.get(name, {})
        skills = skill_assets.get(name, {}).get("hard_skills", [])[:8]
        mainstream_majors = stats.get("mainstream_majors", [])[:3]
        mainstream_certs = stats.get("mainstream_certificates", [])[:3]
        cert_summary = "多数岗位无明确证书要求" if stats.get("no_certificate_requirement_ratio", 0) >= 0.5 else "、".join(mainstream_certs)
        core_jobs.append(
            {
                "display_order": idx,
                "standard_job_name": name,
                "sample_count": int(candidate["sample_count"]),
                "job_category": candidate.get("job_category", ""),
                "job_level_summary": candidate.get("job_level_summary", ""),
                "selection_score": candidate.get("score", 0),
                "selection_reason": build_selection_reason(candidate),
                "mainstream_degree": stats.get("mainstream_degree", "未明确"),
                "mainstream_majors_summary": "、".join(mainstream_majors),
                "mainstream_cert_summary": cert_summary,
                "top_skills": skills,
            }
        )
    return core_jobs


def write_sample_requirements_csv(path: Path, sample_requirements: List[SampleRequirement]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(SampleRequirement().__dict__.keys())
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for item in sample_requirements:
            row = item.__dict__.copy()
            row["extraction_confidence"] = f"{item.extraction_confidence:.4f}"
            writer.writerow(row)


def build_manifest(
    input_csv: Path,
    output_dir: Path,
    sample_count: int,
    job_count: int,
    core_job_count: int,
) -> Dict[str, Any]:
    return {
        "version": "v1",
        "generated_at": generated_at(),
        "source_file": str(input_csv),
        "output_dir": str(output_dir),
        "sample_count": int(sample_count),
        "job_count": int(job_count),
        "core_job_count": int(core_job_count),
        "assets": {
            "job_sample_requirements_csv": str(output_dir / "job_sample_requirements.csv"),
            "job_requirement_stats_json": str(output_dir / "job_requirement_stats.json"),
            "core_jobs_json": str(output_dir / "core_jobs.json"),
            "job_skill_knowledge_assets_json": str(output_dir / "job_skill_knowledge_assets.json"),
        },
    }


def process_build_match_assets(
    input_csv: str | Path = DEFAULT_INPUT_CSV,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    skill_knowledge_map_path: str | Path = DEFAULT_SKILL_KNOWLEDGE_MAP,
    core_job_rules_path: str | Path = DEFAULT_CORE_JOB_RULES,
) -> Dict[str, Any]:
    input_csv = Path(input_csv)
    output_dir = Path(output_dir)
    skill_knowledge_map_path = Path(skill_knowledge_map_path)
    core_job_rules_path = Path(core_job_rules_path)

    if not input_csv.exists():
        raise FileNotFoundError(f"输入文件不存在: {input_csv}")

    rows = read_csv_rows(input_csv)
    usable_rows = []
    for row in rows:
        standard_job_name = get_first_value(row, ["standard_job_name_y", "standard_job_name_x", "standard_job_name"])
        if standard_job_name:
            row["standard_job_name"] = standard_job_name
            usable_rows.append(row)

    skill_knowledge_map = load_json_file(skill_knowledge_map_path, {})
    core_rules = load_core_rules(core_job_rules_path)

    sample_requirements = build_sample_requirements(usable_rows)
    requirement_stats = build_requirement_stats(usable_rows, sample_requirements)
    skill_assets = build_skill_knowledge_assets(usable_rows, skill_knowledge_map)
    core_jobs = build_core_jobs(usable_rows, requirement_stats, skill_assets, core_rules)

    output_dir.mkdir(parents=True, exist_ok=True)
    write_sample_requirements_csv(output_dir / "job_sample_requirements.csv", sample_requirements)
    write_json_file(
        output_dir / "job_requirement_stats.json",
        {
            "version": "v1",
            "generated_at": generated_at(),
            "source_file": str(input_csv),
            "jobs": requirement_stats,
        },
    )
    write_json_file(
        output_dir / "job_skill_knowledge_assets.json",
        {
            "version": "v1",
            "generated_at": generated_at(),
            "source_file": str(input_csv),
            "knowledge_map_file": str(skill_knowledge_map_path),
            "jobs": skill_assets,
        },
    )
    write_json_file(
        output_dir / "core_jobs.json",
        {
            "version": "v1",
            "generated_at": generated_at(),
            "source_file": str(input_csv),
            "jobs": core_jobs,
        },
    )
    manifest = build_manifest(
        input_csv=input_csv,
        output_dir=output_dir,
        sample_count=len(sample_requirements),
        job_count=len(requirement_stats),
        core_job_count=len(core_jobs),
    )
    write_json_file(output_dir / "match_assets_manifest.json", manifest)
    return manifest


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="构建人岗匹配前端展示与赛题评测后处理资产")
    parser.add_argument("--input-csv", default=str(DEFAULT_INPUT_CSV), help="jobs_extracted_full.csv 路径")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="match_assets 输出目录")
    parser.add_argument("--skill-map", default=str(DEFAULT_SKILL_KNOWLEDGE_MAP), help="技能到知识点映射配置")
    parser.add_argument("--core-rules", default=str(DEFAULT_CORE_JOB_RULES), help="核心岗位筛选配置")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    summary = process_build_match_assets(
        input_csv=args.input_csv,
        output_dir=args.output_dir,
        skill_knowledge_map_path=args.skill_map,
        core_job_rules_path=args.core_rules,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
