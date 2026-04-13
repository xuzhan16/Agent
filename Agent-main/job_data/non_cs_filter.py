"""
non_cs_filter.py

在岗位数据主流水线中真正执行“非计算机岗位过滤”。

设计目标：
1. 先用规则快速过滤掉明显非计算机岗位；
2. 对边界模糊岗位再调用 LLM 做补充判断；
3. 保留过滤审计信息，方便后续答辩和对账；
4. 不让 LLM 失败阻断整条数据处理链路。
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from llm_interface_layer.llm_service import call_llm


logger = logging.getLogger(__name__)


DEFAULT_OUTPUT_FILTERED_CSV = "outputs/intermediate/jobs_cs_filtered.csv"
DEFAULT_OUTPUT_AUDIT_CSV = "outputs/intermediate/job_cs_filter_audit.csv"
DEFAULT_DESC_MAX_CHARS = 1400


STRONG_CS_TITLE_KEYWORDS = {
    "java": 3.0,
    "python": 3.0,
    "golang": 3.0,
    "go开发": 3.0,
    "c++": 3.0,
    "c#": 3.0,
    "php": 3.0,
    "前端": 3.0,
    "后端": 3.0,
    "全栈": 3.0,
    "算法": 3.0,
    "软件": 2.8,
    "开发工程师": 2.6,
    "测试开发": 2.8,
    "运维": 2.6,
    "网络安全": 2.8,
    "数据开发": 2.6,
    "数据分析": 2.3,
    "数据库": 2.4,
    "机器学习": 3.0,
    "人工智能": 3.0,
    "大数据": 2.8,
    "产品经理": 1.4,
    "实施工程师": 1.2,
    "技术支持工程师": 1.0,
    "ui": 1.3,
    "交互": 1.3,
}

STRONG_CS_DESC_KEYWORDS = {
    "java": 2.2,
    "python": 2.2,
    "golang": 2.2,
    "c++": 2.0,
    "c#": 2.0,
    "sql": 2.2,
    "mysql": 2.0,
    "oracle": 1.8,
    "redis": 1.8,
    "mongodb": 1.8,
    "postgresql": 1.8,
    "html": 1.6,
    "css": 1.6,
    "javascript": 2.0,
    "typescript": 2.0,
    "vue": 1.8,
    "react": 1.8,
    "spring": 1.8,
    "linux": 1.6,
    "docker": 1.6,
    "kubernetes": 1.6,
    "软件开发": 2.0,
    "系统开发": 1.8,
    "系统设计": 1.8,
    "编程": 1.8,
    "代码": 1.8,
    "数据库": 1.8,
    "前端": 1.6,
    "后端": 1.6,
    "算法": 2.0,
    "数据分析": 1.8,
    "数据建模": 1.8,
    "产品需求": 1.0,
    "软件实施": 1.2,
    "信息化": 1.4,
}

CS_INDUSTRY_KEYWORDS = {
    "信息技术": 1.8,
    "互联网": 1.8,
    "软件": 1.8,
    "计算机": 1.8,
    "it": 1.4,
    "人工智能": 1.8,
    "电子商务": 1.2,
    "大数据": 1.6,
    "云计算": 1.6,
    "网络安全": 1.8,
}

STRONG_NON_CS_TITLE_KEYWORDS = {
    "化学": 3.2,
    "化工": 3.2,
    "食品": 3.2,
    "微生物": 3.2,
    "生物": 3.0,
    "制药": 3.0,
    "药学": 3.0,
    "医学": 3.0,
    "护士": 3.2,
    "医生": 3.2,
    "机械": 3.0,
    "电气": 3.0,
    "土木": 3.0,
    "建筑": 3.0,
    "暖通": 3.0,
    "施工": 3.0,
    "财务": 2.8,
    "会计": 2.8,
    "出纳": 2.8,
    "销售": 2.8,
    "招商主管": 2.8,
    "招商主管": 2.8,
    "招商主管": 2.8,
    "导购": 2.8,
    "客服": 2.6,
    "人事": 2.8,
    "行政": 2.8,
    "教师": 2.8,
    "普工": 3.0,
    "仓管": 2.8,
    "物流": 2.6,
}

STRONG_NON_CS_DESC_KEYWORDS = {
    "化学工程": 2.8,
    "化工工艺": 2.8,
    "食品": 2.8,
    "微生物": 2.8,
    "生物实验": 2.6,
    "药品": 2.6,
    "药学": 2.6,
    "临床": 2.8,
    "病房": 2.8,
    "护理": 2.8,
    "机械设计": 2.8,
    "机械制图": 2.8,
    "cad": 2.0,
    "土建": 2.8,
    "施工现场": 2.8,
    "工艺流程": 2.4,
    "生产车间": 2.8,
    "质量检验": 2.6,
    "销售客户": 2.4,
    "渠道拓展": 2.4,
    "会计核算": 2.6,
    "财税": 2.6,
    "招聘培训": 2.4,
    "行政后勤": 2.6,
}

NON_CS_INDUSTRY_KEYWORDS = {
    "食品": 2.4,
    "化工": 2.4,
    "生物": 2.4,
    "医药": 2.4,
    "医疗": 2.4,
    "建筑": 2.4,
    "土木": 2.4,
    "机械": 2.4,
    "制造": 2.0,
    "餐饮": 2.4,
    "零售": 1.8,
    "物流": 1.8,
    "教育": 1.8,
    "房地产": 1.8,
}

AMBIGUOUS_TITLE_KEYWORDS = {
    "实施工程师",
    "技术支持工程师",
    "产品经理",
    "数据分析师",
    "ui设计师",
    "交互设计师",
    "运营",
    "项目经理",
}


def clean_text(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null"}:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def get_first_existing_value(record: pd.Series, candidates: Sequence[str]) -> str:
    for field in candidates:
        if field in record.index:
            text = clean_text(record.get(field, ""))
            if text:
                return text
    return ""


def keyword_matches(text: str, weights: Dict[str, float]) -> Tuple[float, List[str]]:
    lowered = clean_text(text).lower()
    if not lowered:
        return 0.0, []
    score = 0.0
    matches: List[str] = []
    for keyword, weight in weights.items():
        keyword_lower = keyword.lower()
        if keyword_lower in lowered:
            score += float(weight)
            matches.append(keyword)
    return round(score, 4), matches


def build_row_text_payload(record: pd.Series) -> Dict[str, str]:
    title = get_first_existing_value(record, ["job_title_norm", "job_title"])
    industry = get_first_existing_value(record, ["industry"])
    job_description = get_first_existing_value(
        record,
        ["job_description_clean", "job_description"],
    )
    company_description = get_first_existing_value(
        record,
        ["company_description_clean", "company_description"],
    )
    combined_description = " ".join(
        part for part in [job_description, company_description] if part
    ).strip()
    return {
        "job_title": title,
        "industry": industry,
        "job_description": combined_description[:DEFAULT_DESC_MAX_CHARS],
    }


def build_rule_features(record: pd.Series) -> Dict[str, Any]:
    payload = build_row_text_payload(record)
    title = payload["job_title"]
    industry = payload["industry"]
    description = payload["job_description"]

    title_cs_score, title_cs_matches = keyword_matches(title, STRONG_CS_TITLE_KEYWORDS)
    desc_cs_score, desc_cs_matches = keyword_matches(description, STRONG_CS_DESC_KEYWORDS)
    industry_cs_score, industry_cs_matches = keyword_matches(industry, CS_INDUSTRY_KEYWORDS)

    title_non_cs_score, title_non_cs_matches = keyword_matches(title, STRONG_NON_CS_TITLE_KEYWORDS)
    desc_non_cs_score, desc_non_cs_matches = keyword_matches(description, STRONG_NON_CS_DESC_KEYWORDS)
    industry_non_cs_score, industry_non_cs_matches = keyword_matches(industry, NON_CS_INDUSTRY_KEYWORDS)

    cs_score = round(title_cs_score * 1.2 + desc_cs_score + industry_cs_score, 4)
    non_cs_score = round(title_non_cs_score * 1.2 + desc_non_cs_score + industry_non_cs_score, 4)
    matched_cs_keywords = sorted(set(title_cs_matches + desc_cs_matches + industry_cs_matches))
    matched_non_cs_keywords = sorted(
        set(title_non_cs_matches + desc_non_cs_matches + industry_non_cs_matches)
    )

    title_lower = title.lower()
    ambiguous = any(keyword.lower() in title_lower for keyword in AMBIGUOUS_TITLE_KEYWORDS)

    return {
        "job_title": title,
        "industry": industry,
        "job_description": description,
        "cs_score": cs_score,
        "non_cs_score": non_cs_score,
        "matched_cs_keywords": matched_cs_keywords,
        "matched_non_cs_keywords": matched_non_cs_keywords,
        "is_ambiguous_title": ambiguous,
    }


def rule_decide_is_cs_related(record: pd.Series) -> Dict[str, Any]:
    features = build_rule_features(record)
    cs_score = float(features["cs_score"])
    non_cs_score = float(features["non_cs_score"])
    matched_cs = features["matched_cs_keywords"]
    matched_non_cs = features["matched_non_cs_keywords"]
    ambiguous = bool(features["is_ambiguous_title"])

    if non_cs_score >= 3.0 and (non_cs_score - cs_score) >= 1.5:
        return {
            "decision": False,
            "source": "rule_drop",
            "confidence": min(0.99, round(0.65 + non_cs_score * 0.06, 4)),
            "reason": f"命中明显非计算机信号: {', '.join(matched_non_cs[:6])}",
            "features": features,
        }

    if cs_score >= 3.0 and (cs_score - non_cs_score) >= 1.0 and not (
        ambiguous and non_cs_score > 0
    ):
        return {
            "decision": True,
            "source": "rule_keep",
            "confidence": min(0.99, round(0.62 + cs_score * 0.05, 4)),
            "reason": f"命中明显计算机信号: {', '.join(matched_cs[:6])}",
            "features": features,
        }

    if ambiguous and cs_score >= 2.4 and non_cs_score <= 1.2:
        return {
            "decision": True,
            "source": "rule_keep",
            "confidence": 0.72,
            "reason": f"岗位名有歧义，但描述更偏计算机方向: {', '.join(matched_cs[:6])}",
            "features": features,
        }

    return {
        "decision": None,
        "source": "needs_llm",
        "confidence": 0.0,
        "reason": "规则层无法稳定判断，交给 LLM 二次判定",
        "features": features,
    }


def fallback_decide_is_cs_related(features: Dict[str, Any]) -> Dict[str, Any]:
    cs_score = float(features.get("cs_score") or 0.0)
    non_cs_score = float(features.get("non_cs_score") or 0.0)
    matched_cs = features.get("matched_cs_keywords") or []
    matched_non_cs = features.get("matched_non_cs_keywords") or []

    if cs_score >= non_cs_score:
        return {
            "decision": True,
            "source": "fallback_keep",
            "confidence": 0.51,
            "reason": (
                f"LLM 不可用，按保守回退保留该岗位；计算机信号: {', '.join(matched_cs[:6]) or '无明显关键词'}"
            ),
        }
    return {
        "decision": False,
        "source": "fallback_drop",
        "confidence": 0.51,
        "reason": (
            f"LLM 不可用，按保守回退过滤该岗位；非计算机信号: {', '.join(matched_non_cs[:6]) or '无明显关键词'}"
        ),
    }


def call_non_cs_filter_llm(record: pd.Series) -> Dict[str, Any]:
    payload = build_row_text_payload(record)
    llm_result = call_llm(
        "non_cs_filter",
        input_data=payload,
        context_data=None,
        student_state=None,
        extra_context=None,
    )
    return {
        "decision": bool(llm_result.get("is_cs_related")),
        "source": "llm_keep" if bool(llm_result.get("is_cs_related")) else "llm_drop",
        "confidence": float(llm_result.get("confidence") or 0.0),
        "reason": clean_text(llm_result.get("reason")) or "LLM 未返回明确原因",
    }


def enrich_filter_columns(result_df: pd.DataFrame) -> pd.DataFrame:
    enriched = result_df.copy()
    if "cs_filter_confidence" in enriched.columns:
        enriched["cs_filter_confidence"] = (
            pd.to_numeric(enriched["cs_filter_confidence"], errors="coerce")
            .fillna(0.0)
            .round(4)
        )
    for column in [
        "cs_filter_source",
        "cs_filter_reason",
        "cs_filter_matched_cs_keywords",
        "cs_filter_matched_non_cs_keywords",
    ]:
        if column in enriched.columns:
            enriched[column] = enriched[column].apply(clean_text)
    return enriched


def process_non_cs_filter(
    df: pd.DataFrame,
    output_filtered_csv: Optional[str] = None,
    output_audit_csv: Optional[str] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """
    主流程：在清洗结果上执行“计算机岗位过滤”。

    返回：
    - filtered_df：保留的计算机岗位
    - audit_df：全量岗位及过滤判定信息
    - stats：过滤统计摘要
    """
    if "job_title" not in df.columns:
        raise ValueError("Input DataFrame must contain column: job_title")

    audit_rows: List[Dict[str, Any]] = []
    llm_reviewed_rows = 0

    for _, row in df.iterrows():
        base_record = row.to_dict()
        rule_result = rule_decide_is_cs_related(row)
        features = rule_result["features"]
        final_result = rule_result

        if final_result["decision"] is None:
            llm_reviewed_rows += 1
            try:
                llm_result = call_non_cs_filter_llm(row)
                final_result = {
                    **llm_result,
                    "features": features,
                }
            except Exception as exc:  # noqa: BLE001 - 过滤模块必须容错，不能拖垮整条流水线
                logger.warning("non_cs_filter llm fallback triggered: %s", exc)
                fallback_result = fallback_decide_is_cs_related(features)
                final_result = {
                    **fallback_result,
                    "features": features,
                }

        base_record["is_cs_related"] = bool(final_result["decision"])
        base_record["cs_filter_source"] = clean_text(final_result["source"])
        base_record["cs_filter_confidence"] = float(final_result["confidence"] or 0.0)
        base_record["cs_filter_reason"] = clean_text(final_result["reason"])
        base_record["cs_filter_matched_cs_keywords"] = ", ".join(features["matched_cs_keywords"])
        base_record["cs_filter_matched_non_cs_keywords"] = ", ".join(
            features["matched_non_cs_keywords"]
        )
        base_record["cs_filter_cs_score"] = float(features["cs_score"])
        base_record["cs_filter_non_cs_score"] = float(features["non_cs_score"])
        base_record["cs_filter_is_ambiguous_title"] = bool(features["is_ambiguous_title"])
        audit_rows.append(base_record)

    audit_df = enrich_filter_columns(pd.DataFrame(audit_rows))
    filtered_df = audit_df[audit_df["is_cs_related"] == True].copy()  # noqa: E712

    if output_filtered_csv:
        output_path = Path(output_filtered_csv)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        filtered_df.to_csv(output_path, index=False, encoding="utf-8-sig")

    if output_audit_csv:
        output_path = Path(output_audit_csv)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        audit_df.to_csv(output_path, index=False, encoding="utf-8-sig")

    source_counts = {
        str(key): int(value)
        for key, value in audit_df["cs_filter_source"].value_counts(dropna=False).to_dict().items()
    }
    stats = {
        "cleaned_rows": int(len(df)),
        "filtered_rows": int(len(filtered_df)),
        "filtered_out_rows": int(len(df) - len(filtered_df)),
        "llm_reviewed_rows": int(llm_reviewed_rows),
        "source_counts": source_counts,
    }
    return filtered_df.reset_index(drop=True), audit_df.reset_index(drop=True), stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="执行计算机岗位过滤")
    parser.add_argument("--input", required=True, help="清洗后的岗位 CSV 路径")
    parser.add_argument(
        "--output-filtered",
        default=DEFAULT_OUTPUT_FILTERED_CSV,
        help="过滤后仅保留计算机岗位的 CSV 输出路径",
    )
    parser.add_argument(
        "--output-audit",
        default=DEFAULT_OUTPUT_AUDIT_CSV,
        help="全量过滤审计 CSV 输出路径",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"input csv not found: {input_path}")
    df = pd.read_csv(input_path, dtype=str).fillna("")
    filtered_df, audit_df, stats = process_non_cs_filter(
        df=df,
        output_filtered_csv=args.output_filtered,
        output_audit_csv=args.output_audit,
    )
    print(
        json.dumps(
            {
                "stats": stats,
                "filtered_columns": list(filtered_df.columns),
                "audit_columns": list(audit_df.columns),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
