"""
build_job_ability_assets.py

Post-process job samples into contest-oriented seven-dimension job ability assets.

This script is intentionally independent from the full job data pipeline. It reads the
already generated jobs_extracted_full.csv and existing match_assets, then writes a
lightweight job ability layer for job_profile, job_match, reports, and frontend display.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SOURCE = PROJECT_ROOT / "outputs" / "intermediate" / "jobs_extracted_full.csv"
DEFAULT_ASSET_DIR = PROJECT_ROOT / "outputs" / "match_assets"
DEFAULT_REQUIREMENT_STATS = DEFAULT_ASSET_DIR / "job_requirement_stats.json"
DEFAULT_SKILL_ASSETS = DEFAULT_ASSET_DIR / "job_skill_knowledge_assets.json"
CHINA_TZ = timezone(timedelta(hours=8))


ABILITY_DIMENSIONS: List[Dict[str, Any]] = [
    {
        "key": "professional_skill",
        "label": "专业技能",
        "keywords": ["技能", "开发", "测试", "设计", "数据库", "算法", "编程", "架构", "技术", "工具"],
    },
    {
        "key": "certificate",
        "label": "证书要求",
        "keywords": ["证书", "资格证", "认证", "英语", "CET", "软考", "日语", "等级证"],
    },
    {
        "key": "innovation",
        "label": "创新能力",
        "keywords": [
            "创新",
            "技术攻关",
            "方案设计",
            "优化",
            "研发",
            "算法",
            "建模",
            "改进",
            "新技术",
            "产品创新",
            "解决方案",
            "架构设计",
        ],
    },
    {
        "key": "learning",
        "label": "学习能力",
        "keywords": [
            "学习能力",
            "快速学习",
            "自驱",
            "成长",
            "培训",
            "适应能力",
            "主动学习",
            "学习新技术",
            "善于总结",
            "勤恳好学",
        ],
    },
    {
        "key": "pressure_resistance",
        "label": "抗压能力",
        "keywords": [
            "抗压",
            "压力",
            "承受压力",
            "多任务",
            "紧急响应",
            "责任心",
            "驻场",
            "加班",
            "高强度",
            "结果导向",
            "心理素质",
        ],
    },
    {
        "key": "communication",
        "label": "沟通能力",
        "keywords": [
            "沟通",
            "协调",
            "表达",
            "客户对接",
            "跨部门",
            "团队协作",
            "汇报",
            "需求沟通",
            "文档编写",
            "培训客户",
            "协作",
        ],
    },
    {
        "key": "internship_practice",
        "label": "实习能力",
        "keywords": [
            "实习",
            "项目经验",
            "实践经验",
            "交付",
            "上线",
            "驻场",
            "项目实施",
            "业务实践",
            "工作经验",
            "项目经历",
            "实际项目",
        ],
    },
]

DIMENSION_LABELS = {item["key"]: item["label"] for item in ABILITY_DIMENSIONS}


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\u00a0", " ").replace("\u3000", " ").strip()
    text = re.sub(r"\s+", " ", text)
    if text.lower() in {"", "nan", "none", "null", "n/a", "na", "-", "[]", "{}"}:
        return ""
    return text


def dedup_keep_order(values: Iterable[Any]) -> List[Any]:
    seen = set()
    result: List[Any] = []
    for value in values:
        if value is None or value == "":
            continue
        key = json.dumps(value, ensure_ascii=False, sort_keys=True) if isinstance(value, (dict, list)) else str(value)
        if key not in seen:
            seen.add(key)
            result.append(value)
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
                return dedup_keep_order(clean_text(item) for item in loaded if clean_text(item))
        except json.JSONDecodeError:
            pass
    return dedup_keep_order(clean_text(item) for item in re.split(r"[、,，;；/|｜\n]+", text) if clean_text(item))


def safe_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def safe_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, dict) else {}


def save_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def normalize_standard_job_name(row: Dict[str, Any]) -> str:
    return (
        clean_text(row.get("standard_job_name_y"))
        or clean_text(row.get("standard_job_name_x"))
        or clean_text(row.get("standard_job_name"))
        or clean_text(row.get("job_name"))
        or clean_text(row.get("job_title"))
    )


def row_blob(row: Dict[str, Any]) -> str:
    values = [
        row.get("soft_skills"),
        row.get("practice_requirement"),
        row.get("job_description_clean"),
        row.get("raw_requirement_summary"),
        row.get("hard_skills"),
        row.get("tools_or_tech_stack"),
    ]
    return "\n".join(clean_text(item) for item in values if clean_text(item))


def find_keywords(text: str, keywords: Iterable[str]) -> List[str]:
    return dedup_keep_order(keyword for keyword in keywords if keyword and keyword.lower() in text.lower())


def first_evidence_sentence(text: str, matched_keywords: List[str], max_chars: int = 120) -> str:
    if not text:
        return ""
    sentences = [clean_text(item) for item in re.split(r"[。；;！!？?\n]", text) if clean_text(item)]
    for sentence in sentences:
        if any(keyword in sentence for keyword in matched_keywords):
            return sentence[:max_chars]
    return clean_text(text)[:max_chars]


def score_level(score: float) -> str:
    if score >= 75:
        return "high"
    if score >= 55:
        return "medium_high"
    if score >= 35:
        return "medium"
    return "low"


def level_text(level: str) -> str:
    return {
        "high": "要求较高",
        "medium_high": "要求中高",
        "medium": "要求中等",
        "low": "要求较低",
    }.get(level, "要求未明确")


def clamp_score(value: float) -> int:
    return int(round(max(0.0, min(100.0, value))))


@dataclass
class SampleEvidence:
    sample_id: str
    standard_job_name: str
    job_title: str
    company_name: str
    ability_dimension: str
    ability_label: str
    matched_keywords: List[str]
    evidence_text: str
    evidence_source: str
    evidence_score: float
    extraction_method: str


def build_professional_skill_dimension(
    job_name: str,
    rows: List[Dict[str, Any]],
    skill_assets: Dict[str, Any],
) -> Tuple[Dict[str, Any], List[SampleEvidence]]:
    sample_count = len(rows)
    evidence_rows: List[SampleEvidence] = []
    skill_counter: Counter[str] = Counter()
    evidence_count = 0
    for index, row in enumerate(rows, start=1):
        skills = parse_list_like(row.get("hard_skills")) + parse_list_like(row.get("tools_or_tech_stack"))
        if skills:
            evidence_count += 1
            skill_counter.update(skills)
            evidence_rows.append(
                SampleEvidence(
                    sample_id=clean_text(row.get("source_row_id") or row.get("job_id") or index),
                    standard_job_name=job_name,
                    job_title=clean_text(row.get("job_title") or row.get("position_name")),
                    company_name=clean_text(row.get("company_name")),
                    ability_dimension="professional_skill",
                    ability_label=DIMENSION_LABELS["professional_skill"],
                    matched_keywords=dedup_keep_order(skills)[:12],
                    evidence_text="、".join(dedup_keep_order(skills)[:12]),
                    evidence_source="hard_skills/tools_or_tech_stack",
                    evidence_score=1.0,
                    extraction_method="structured_skill_fields",
                )
            )

    required_points = safe_list(skill_assets.get("required_knowledge_points"))
    preferred_points = safe_list(skill_assets.get("preferred_knowledge_points"))
    evidence_ratio = evidence_count / sample_count if sample_count else 0.0
    point_bonus = min((len(required_points) * 1.8 + len(preferred_points) * 0.8), 24.0)
    score = clamp_score(evidence_ratio * 70.0 + point_bonus + min(math.log1p(sample_count) * 2.5, 10.0))
    keywords = [skill for skill, _ in skill_counter.most_common(12)]
    if not keywords:
        keywords = [clean_text(item) for item in required_points[:8] if clean_text(item)]
    level = score_level(score)
    return (
        {
            "dimension": "professional_skill",
            "label": DIMENSION_LABELS["professional_skill"],
            "score": score,
            "level": level,
            "keywords": keywords,
            "evidence_ratio": round(evidence_ratio, 4),
            "evidence_count": evidence_count,
            "description": f"该岗位专业技能{level_text(level)}，高频技能包括{'、'.join(keywords[:5]) if keywords else '暂无明确记录'}。",
        },
        evidence_rows,
    )


def build_certificate_dimension(
    job_name: str,
    rows: List[Dict[str, Any]],
    stats: Dict[str, Any],
) -> Tuple[Dict[str, Any], List[SampleEvidence]]:
    sample_count = len(rows)
    evidence_rows: List[SampleEvidence] = []
    cert_counter: Counter[str] = Counter()
    evidence_count = 0
    for index, row in enumerate(rows, start=1):
        certs = parse_list_like(row.get("certificate_requirement"))
        certs = [item for item in certs if item and item != "无明确要求"]
        if certs:
            evidence_count += 1
            cert_counter.update(certs)
            evidence_rows.append(
                SampleEvidence(
                    sample_id=clean_text(row.get("source_row_id") or row.get("job_id") or index),
                    standard_job_name=job_name,
                    job_title=clean_text(row.get("job_title") or row.get("position_name")),
                    company_name=clean_text(row.get("company_name")),
                    ability_dimension="certificate",
                    ability_label=DIMENSION_LABELS["certificate"],
                    matched_keywords=dedup_keep_order(certs),
                    evidence_text="、".join(dedup_keep_order(certs)),
                    evidence_source="certificate_requirement",
                    evidence_score=1.0,
                    extraction_method="structured_certificate_fields",
                )
            )

    must_have = safe_list(stats.get("must_have_certificates"))
    preferred = safe_list(stats.get("preferred_certificates"))
    non_empty_ratio = 1.0 - float(stats.get("no_certificate_requirement_ratio") or 0.0)
    evidence_ratio = max(evidence_count / sample_count if sample_count else 0.0, non_empty_ratio)
    score = clamp_score(evidence_ratio * 70.0 + min(len(must_have) * 10.0 + len(preferred) * 4.0, 24.0))
    keywords = [cert for cert, _ in cert_counter.most_common(8)] or [clean_text(item) for item in (must_have + preferred)[:8]]
    level = score_level(score)
    return (
        {
            "dimension": "certificate",
            "label": DIMENSION_LABELS["certificate"],
            "score": score,
            "level": level,
            "keywords": keywords,
            "evidence_ratio": round(evidence_ratio, 4),
            "evidence_count": evidence_count,
            "description": f"该岗位证书要求{level_text(level)}，常见证书为{'、'.join(keywords[:5]) if keywords else '多数样本无明确证书要求'}。",
        },
        evidence_rows,
    )


def build_keyword_dimension(
    job_name: str,
    rows: List[Dict[str, Any]],
    dimension: Dict[str, Any],
) -> Tuple[Dict[str, Any], List[SampleEvidence]]:
    sample_count = len(rows)
    dimension_key = dimension["key"]
    label = dimension["label"]
    keywords = dimension["keywords"]
    keyword_counter: Counter[str] = Counter()
    evidence_rows: List[SampleEvidence] = []
    evidence_count = 0

    for index, row in enumerate(rows, start=1):
        blob = row_blob(row)
        matched = find_keywords(blob, keywords)
        if not matched:
            continue
        evidence_count += 1
        keyword_counter.update(matched)
        source = "soft_skills/practice_requirement/job_description_clean"
        evidence_rows.append(
            SampleEvidence(
                sample_id=clean_text(row.get("source_row_id") or row.get("job_id") or index),
                standard_job_name=job_name,
                job_title=clean_text(row.get("job_title") or row.get("position_name")),
                company_name=clean_text(row.get("company_name")),
                ability_dimension=dimension_key,
                ability_label=label,
                matched_keywords=matched,
                evidence_text=first_evidence_sentence(blob, matched),
                evidence_source=source,
                evidence_score=round(len(matched) / max(len(keywords), 1), 4),
                extraction_method="keyword_stat_rule",
            )
        )

    evidence_ratio = evidence_count / sample_count if sample_count else 0.0
    keyword_diversity = len(keyword_counter)
    raw_score = evidence_ratio * 68.0 + min(keyword_diversity * 2.5, 16.0) + min(math.log1p(sample_count), 5.0)
    # Soft/general ability signals can be repeated from unified portraits, so keep
    # a conservative ceiling unless future sample-level extraction is stronger.
    score = clamp_score(min(raw_score, 92.0))
    top_keywords = [keyword for keyword, _ in keyword_counter.most_common(8)]
    level = score_level(score)
    return (
        {
            "dimension": dimension_key,
            "label": label,
            "score": score,
            "level": level,
            "keywords": top_keywords,
            "evidence_ratio": round(evidence_ratio, 4),
            "evidence_count": evidence_count,
            "description": f"该岗位{label}{level_text(level)}，证据关键词包括{'、'.join(top_keywords[:5]) if top_keywords else '暂无明确记录'}。",
        },
        evidence_rows,
    )


def build_source_quality(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    sample_count = len(rows)
    if not sample_count:
        return {"soft_skill_coverage": 0.0, "practice_coverage": 0.0, "hard_skill_coverage": 0.0, "confidence": 0.0}
    soft_count = sum(1 for row in rows if parse_list_like(row.get("soft_skills")))
    practice_count = sum(1 for row in rows if clean_text(row.get("practice_requirement")))
    hard_count = sum(1 for row in rows if parse_list_like(row.get("hard_skills")) or parse_list_like(row.get("tools_or_tech_stack")))
    confidence = (
        min(sample_count / 50.0, 1.0) * 0.35
        + (hard_count / sample_count) * 0.25
        + (soft_count / sample_count) * 0.25
        + (practice_count / sample_count) * 0.15
    )
    return {
        "soft_skill_coverage": round(soft_count / sample_count, 4),
        "practice_coverage": round(practice_count / sample_count, 4),
        "hard_skill_coverage": round(hard_count / sample_count, 4),
        "confidence": round(confidence, 4),
    }


def read_rows(source_file: Path) -> List[Dict[str, Any]]:
    with source_file.open("r", encoding="utf-8-sig", errors="ignore", newline="") as f:
        return [dict(row) for row in csv.DictReader(f)]


def group_rows_by_job(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        job_name = normalize_standard_job_name(row)
        if job_name:
            grouped[job_name].append(row)
    return dict(grouped)


def build_assets(
    source_file: Path = DEFAULT_SOURCE,
    output_dir: Path = DEFAULT_ASSET_DIR,
    requirement_stats_path: Path = DEFAULT_REQUIREMENT_STATS,
    skill_assets_path: Path = DEFAULT_SKILL_ASSETS,
) -> Dict[str, Any]:
    rows = read_rows(source_file)
    grouped = group_rows_by_job(rows)
    requirement_jobs = safe_dict(load_json(requirement_stats_path).get("jobs"))
    skill_jobs = safe_dict(load_json(skill_assets_path).get("jobs"))
    generated_at = datetime.now(CHINA_TZ).isoformat(timespec="seconds")

    jobs: Dict[str, Any] = {}
    evidence_rows: List[SampleEvidence] = []
    for job_name, job_rows in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0])):
        stats = safe_dict(requirement_jobs.get(job_name))
        skill_assets = safe_dict(skill_jobs.get(job_name))
        ability_requirements: Dict[str, Any] = {}

        professional_asset, professional_evidence = build_professional_skill_dimension(job_name, job_rows, skill_assets)
        certificate_asset, certificate_evidence = build_certificate_dimension(job_name, job_rows, stats)
        ability_requirements["professional_skill"] = professional_asset
        ability_requirements["certificate"] = certificate_asset
        evidence_rows.extend(professional_evidence + certificate_evidence)

        for dimension in ABILITY_DIMENSIONS[2:]:
            dimension_asset, dimension_evidence = build_keyword_dimension(job_name, job_rows, dimension)
            ability_requirements[dimension["key"]] = dimension_asset
            evidence_rows.extend(dimension_evidence)

        ability_radar = [
            {
                "dimension": DIMENSION_LABELS[key],
                "key": key,
                "score": safe_dict(ability_requirements.get(key)).get("score", 0),
            }
            for key in [item["key"] for item in ABILITY_DIMENSIONS]
        ]
        jobs[job_name] = {
            "standard_job_name": job_name,
            "sample_count": len(job_rows),
            "ability_requirements": ability_requirements,
            "ability_radar": ability_radar,
            "source_quality": build_source_quality(job_rows),
        }

    output_dir.mkdir(parents=True, exist_ok=True)
    assets_path = output_dir / "job_ability_assets.json"
    evidence_path = output_dir / "job_sample_ability_evidence.csv"
    manifest_path = output_dir / "job_ability_assets_manifest.json"

    assets = {
        "version": "v1",
        "generated_at": generated_at,
        "source_file": str(source_file),
        "jobs": jobs,
    }
    save_json(assets_path, assets)

    with evidence_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "sample_id",
                "standard_job_name",
                "job_title",
                "company_name",
                "ability_dimension",
                "ability_label",
                "matched_keywords_json",
                "evidence_text",
                "evidence_source",
                "evidence_score",
                "extraction_method",
            ],
        )
        writer.writeheader()
        for item in evidence_rows:
            writer.writerow(
                {
                    "sample_id": item.sample_id,
                    "standard_job_name": item.standard_job_name,
                    "job_title": item.job_title,
                    "company_name": item.company_name,
                    "ability_dimension": item.ability_dimension,
                    "ability_label": item.ability_label,
                    "matched_keywords_json": json.dumps(item.matched_keywords, ensure_ascii=False),
                    "evidence_text": item.evidence_text,
                    "evidence_source": item.evidence_source,
                    "evidence_score": item.evidence_score,
                    "extraction_method": item.extraction_method,
                }
            )

    manifest = {
        "version": "v1",
        "generated_at": generated_at,
        "source_file": str(source_file),
        "job_count": len(jobs),
        "sample_count": len(rows),
        "evidence_row_count": len(evidence_rows),
        "output_files": {
            "job_ability_assets_json": str(assets_path),
            "job_sample_ability_evidence_csv": str(evidence_path),
            "job_ability_assets_manifest_json": str(manifest_path),
        },
        "ability_dimensions": [
            {"key": item["key"], "label": item["label"], "keyword_count": len(item["keywords"])}
            for item in ABILITY_DIMENSIONS
        ],
        "notes": [
            "本资产由已处理岗位样本后处理生成，不依赖运行时 LLM。",
            "能力分数基于关键词覆盖、结构化技能/证书字段、样本量和资产完整度估算。",
        ],
    }
    save_json(manifest_path, manifest)
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build seven-dimension job ability assets.")
    parser.add_argument("--source-file", default=str(DEFAULT_SOURCE), help="Path to jobs_extracted_full.csv")
    parser.add_argument("--output-dir", default=str(DEFAULT_ASSET_DIR), help="Output match_assets directory")
    parser.add_argument("--requirement-stats", default=str(DEFAULT_REQUIREMENT_STATS), help="job_requirement_stats.json")
    parser.add_argument("--skill-assets", default=str(DEFAULT_SKILL_ASSETS), help="job_skill_knowledge_assets.json")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest = build_assets(
        source_file=Path(args.source_file),
        output_dir=Path(args.output_dir),
        requirement_stats_path=Path(args.requirement_stats),
        skill_assets_path=Path(args.skill_assets),
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
