"""
job_profile_aggregator.py

岗位画像模块的聚合层。

职责：
1. 对同一个 standard_job_name 对应的岗位组数据做聚合统计；
2. 输出技能频次、学历要求分布、行业分布、城市分布、薪资统计；
3. 返回结构化聚合结果，供 job_profile_input_payload 和最终 job_profile_result 使用；
4. 不直接调用大模型。

设计说明：
- 优先消费已结构化字段，例如 hard_skills / tools_or_tech_stack / degree_requirement；
- 若结构化字段不存在，则从 job_desc 做轻量规则兜底抽取；
- 使用 pandas + collections 实现，保持逻辑清晰、可解释、易维护。
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd


DEFAULT_OUTPUT_PATH = Path("outputs/state/job_profile_aggregation_result.json")


SKILL_ALIAS_MAP: Dict[str, List[str]] = {
    "Python": ["python", "python3", "py"],
    "Java": ["java", "spring", "springboot"],
    "C++": ["c++", "cpp"],
    "SQL": ["sql", "mysql", "postgresql", "oracle", "hive sql"],
    "机器学习": ["机器学习", "machine learning", "ml", "sklearn", "scikit-learn"],
    "深度学习": ["深度学习", "deep learning", "pytorch", "tensorflow"],
    "NLP": ["nlp", "自然语言处理", "文本挖掘", "大语言模型", "llm"],
    "数据分析": ["数据分析", "数据处理", "数据清洗", "业务分析"],
    "数据可视化": ["数据可视化", "可视化", "tableau", "power bi", "powerbi"],
    "Excel": ["excel", "vlookup", "数据透视表"],
    "Spark": ["spark", "pyspark"],
    "Hadoop": ["hadoop", "hdfs", "mapreduce"],
    "Flink": ["flink"],
    "Docker": ["docker"],
    "Linux": ["linux", "shell", "bash"],
    "Git": ["git", "github", "gitlab"],
}


DEGREE_PATTERNS: List[Tuple[str, str]] = [
    (r"(博士|博士研究生|phd)", "博士"),
    (r"(硕士|研究生|master|mba)", "硕士"),
    (r"(本科|学士|bachelor)", "本科"),
    (r"(大专|专科|高职|college)", "大专"),
    (r"(高中|中专)", "高中/中专"),
    (r"(学历不限|不限学历|无学历要求)", "学历不限"),
]


@dataclass
class SalaryAggregationResult:
    """岗位组薪资统计结果。"""

    salary_min_month_avg: Optional[float] = None
    salary_min_month_median: Optional[float] = None
    salary_min_month_p25: Optional[float] = None
    salary_min_month_p75: Optional[float] = None
    salary_max_month_avg: Optional[float] = None
    salary_max_month_median: Optional[float] = None
    salary_max_month_p25: Optional[float] = None
    salary_max_month_p75: Optional[float] = None
    salary_mid_month_avg: Optional[float] = None
    salary_mid_month_median: Optional[float] = None
    valid_salary_count: int = 0


@dataclass
class JobProfileAggregationResult:
    """岗位组聚合统计结果。"""

    standard_job_name: str = ""
    job_count: int = 0
    skill_frequency: List[Dict[str, Any]] = field(default_factory=list)
    degree_requirement_distribution: List[Dict[str, Any]] = field(default_factory=list)
    industry_distribution: List[Dict[str, Any]] = field(default_factory=list)
    city_distribution: List[Dict[str, Any]] = field(default_factory=list)
    salary_stats: Dict[str, Any] = field(default_factory=dict)
    top_company_types: List[Dict[str, Any]] = field(default_factory=list)
    top_company_sizes: List[Dict[str, Any]] = field(default_factory=list)
    source_columns: List[str] = field(default_factory=list)
    aggregation_warnings: List[str] = field(default_factory=list)


def clean_text(value: Any) -> str:
    """基础文本清洗。"""
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = str(value)
    text = text.replace("\u00a0", " ").replace("\u3000", " ")
    text = re.sub(r"\s+", " ", text).strip()
    if text.lower() in {"", "nan", "none", "null", "n/a", "na", "-"}:
        return ""
    return text


def safe_float(value: Any) -> Optional[float]:
    """安全转 float。"""
    text = clean_text(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def dedup_keep_order(values: Iterable[str]) -> List[str]:
    """稳定去重。"""
    seen = set()
    result = []
    for value in values:
        text = clean_text(value)
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return result


def parse_list_like_value(value: Any) -> List[str]:
    """
    将 list / JSON字符串 / 普通分隔符字符串 统一转成 list[str]。

    示例：
    - ["Python", "SQL"]
    - '["Python", "SQL"]'
    - "Python、SQL、Excel"
    """
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


def compact_token(text: str) -> str:
    """压缩 token，便于别名匹配。"""
    return re.sub(r"[()（）\[\]【】\-_/|·,，;；:+.#\s]", "", clean_text(text).lower())


def normalize_skill(skill_text: str) -> str:
    """用本地别名字典标准化技能标签。"""
    raw_text = clean_text(skill_text)
    if not raw_text:
        return ""

    raw_compact = compact_token(raw_text)
    for standard_skill, aliases in SKILL_ALIAS_MAP.items():
        if raw_text == standard_skill or raw_compact == compact_token(standard_skill):
            return standard_skill
        for alias in aliases:
            alias_compact = compact_token(alias)
            if not alias_compact:
                continue
            if raw_compact == alias_compact:
                return standard_skill
            if len(alias_compact) >= 2 and alias_compact in raw_compact:
                return standard_skill
            if len(raw_compact) >= 2 and raw_compact in alias_compact:
                return standard_skill
    return raw_text


def get_first_existing_column(df: pd.DataFrame, candidates: Sequence[str]) -> str:
    """从候选字段中返回第一个存在的列名。"""
    for column_name in candidates:
        if column_name in df.columns:
            return column_name
    return ""


def filter_standard_job_group(df: pd.DataFrame, standard_job_name: str) -> pd.DataFrame:
    """根据 standard_job_name 过滤岗位组。"""
    if df is None or df.empty:
        return pd.DataFrame()

    target_name = clean_text(standard_job_name)
    if not target_name:
        raise ValueError("standard_job_name cannot be empty")

    if "standard_job_name" in df.columns:
        group_df = df[df["standard_job_name"].apply(clean_text) == target_name].copy()
    elif "job_name" in df.columns:
        group_df = df[df["job_name"].apply(clean_text) == target_name].copy()
    else:
        raise ValueError("DataFrame must contain 'standard_job_name' or 'job_name'")

    if group_df.empty and "job_name" in df.columns:
        group_df = df[df["job_name"].apply(clean_text) == target_name].copy()
    return group_df.reset_index(drop=True)


def extract_skills_from_text(job_desc: Any) -> List[str]:
    """从岗位描述中做轻量技能召回。"""
    text = clean_text(job_desc)
    if not text:
        return []

    matched_skills = []
    lowered_text = text.lower()
    for standard_skill, aliases in SKILL_ALIAS_MAP.items():
        if standard_skill.lower() in lowered_text:
            matched_skills.append(standard_skill)
            continue
        if any(alias.lower() in lowered_text for alias in aliases):
            matched_skills.append(standard_skill)

    return dedup_keep_order(matched_skills)


def extract_degree_from_text(job_desc: Any) -> List[str]:
    """从岗位描述中做轻量学历要求召回。"""
    text = clean_text(job_desc)
    if not text:
        return []

    degrees = []
    for pattern, degree_name in DEGREE_PATTERNS:
        if re.search(pattern, text, flags=re.IGNORECASE):
            degrees.append(degree_name)
    return dedup_keep_order(degrees)


def collect_row_skills(row: pd.Series) -> List[str]:
    """
    汇总单条岗位记录中的技能标签。

    优先读取结构化字段：
    - hard_skills
    - tools_or_tech_stack
    - skill_requirements
    - tool_requirements

    如果这些字段都没有，再从 job_desc 做规则兜底召回。
    """
    skill_columns = [
        "hard_skills",
        "tools_or_tech_stack",
        "skill_requirements",
        "tool_requirements",
        "hard_skill_tags",
        "tool_skill_tags",
    ]

    collected = []
    for column_name in skill_columns:
        if column_name in row.index:
            collected.extend(parse_list_like_value(row.get(column_name)))

    if not collected and "job_desc" in row.index:
        collected.extend(extract_skills_from_text(row.get("job_desc")))

    return dedup_keep_order(normalize_skill(item) for item in collected if clean_text(item))


def collect_row_degree_requirements(row: pd.Series) -> List[str]:
    """
    汇总单条岗位记录中的学历要求。

    优先读取结构化字段：
    - degree_requirement
    - degree_requirements
    - degree_tags

    若不存在则从 job_desc 兜底抽取。
    """
    degree_columns = ["degree_requirement", "degree_requirements", "degree_tags"]

    collected = []
    for column_name in degree_columns:
        if column_name in row.index:
            collected.extend(parse_list_like_value(row.get(column_name)))

    if not collected and "job_desc" in row.index:
        collected.extend(extract_degree_from_text(row.get("job_desc")))

    return dedup_keep_order(collected)


def counter_to_ranked_list(counter: Counter, total_count: int, top_n: int = 50) -> List[Dict[str, Any]]:
    """将 Counter 转成带 count/ratio/rank 的有序列表。"""
    ranked_items = []
    for rank, (name, count) in enumerate(counter.most_common(top_n), start=1):
        ranked_items.append(
            {
                "rank": rank,
                "name": name,
                "count": int(count),
                "ratio": round(count / total_count, 4) if total_count > 0 else 0.0,
            }
        )
    return ranked_items


def aggregate_skill_frequency(group_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """聚合岗位组技能频次。"""
    skill_counter = Counter()
    for _, row in group_df.iterrows():
        row_skills = set(collect_row_skills(row))
        skill_counter.update(row_skills)
    return counter_to_ranked_list(skill_counter, total_count=len(group_df), top_n=100)


def aggregate_degree_distribution(group_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """聚合岗位组学历要求分布。"""
    degree_counter = Counter()
    for _, row in group_df.iterrows():
        row_degrees = set(collect_row_degree_requirements(row))
        if row_degrees:
            degree_counter.update(row_degrees)
        else:
            degree_counter.update(["未明确"])
    return counter_to_ranked_list(degree_counter, total_count=len(group_df), top_n=20)


def aggregate_column_distribution(
    group_df: pd.DataFrame,
    column_name: str,
    top_n: int = 50,
    empty_label: str = "未明确",
) -> List[Dict[str, Any]]:
    """聚合单个离散字段的分布。"""
    if column_name not in group_df.columns:
        return []

    counter = Counter()
    for value in group_df[column_name].tolist():
        text = clean_text(value) or empty_label
        counter[text] += 1
    return counter_to_ranked_list(counter, total_count=len(group_df), top_n=top_n)


def build_salary_stats(group_df: pd.DataFrame) -> Dict[str, Any]:
    """计算岗位组薪资统计。"""
    if group_df.empty:
        return asdict(SalaryAggregationResult())

    salary_min_col = get_first_existing_column(
        group_df,
        ["salary_min_month", "salary_month_min", "salary_min"],
    )
    salary_max_col = get_first_existing_column(
        group_df,
        ["salary_max_month", "salary_month_max", "salary_max"],
    )

    salary_min_series = (
        pd.to_numeric(group_df[salary_min_col], errors="coerce").dropna()
        if salary_min_col
        else pd.Series(dtype="float64")
    )
    salary_max_series = (
        pd.to_numeric(group_df[salary_max_col], errors="coerce").dropna()
        if salary_max_col
        else pd.Series(dtype="float64")
    )

    salary_mid_values = []
    if salary_min_col and salary_max_col:
        for _, row in group_df.iterrows():
            low = safe_float(row.get(salary_min_col))
            high = safe_float(row.get(salary_max_col))
            if low is not None and high is not None:
                salary_mid_values.append((low + high) / 2)
            elif low is not None:
                salary_mid_values.append(low)
            elif high is not None:
                salary_mid_values.append(high)
    salary_mid_series = pd.Series(salary_mid_values, dtype="float64").dropna()

    result = SalaryAggregationResult(
        salary_min_month_avg=round(float(salary_min_series.mean()), 2) if not salary_min_series.empty else None,
        salary_min_month_median=round(float(salary_min_series.median()), 2) if not salary_min_series.empty else None,
        salary_min_month_p25=round(float(salary_min_series.quantile(0.25)), 2) if not salary_min_series.empty else None,
        salary_min_month_p75=round(float(salary_min_series.quantile(0.75)), 2) if not salary_min_series.empty else None,
        salary_max_month_avg=round(float(salary_max_series.mean()), 2) if not salary_max_series.empty else None,
        salary_max_month_median=round(float(salary_max_series.median()), 2) if not salary_max_series.empty else None,
        salary_max_month_p25=round(float(salary_max_series.quantile(0.25)), 2) if not salary_max_series.empty else None,
        salary_max_month_p75=round(float(salary_max_series.quantile(0.75)), 2) if not salary_max_series.empty else None,
        salary_mid_month_avg=round(float(salary_mid_series.mean()), 2) if not salary_mid_series.empty else None,
        salary_mid_month_median=round(float(salary_mid_series.median()), 2) if not salary_mid_series.empty else None,
        valid_salary_count=int(max(len(salary_min_series), len(salary_max_series), len(salary_mid_series))),
    )
    return asdict(result)


def build_aggregation_warnings(
    standard_job_name: str,
    group_df: pd.DataFrame,
    aggregation_result: Dict[str, Any],
) -> List[str]:
    """根据聚合结果生成 warning。"""
    warnings = []
    if group_df.empty:
        warnings.append(f"未找到 standard_job_name={standard_job_name} 的岗位组数据")
    if not aggregation_result.get("skill_frequency"):
        warnings.append("技能频次为空，可能缺少结构化技能字段且 job_desc 也未抽取到有效技能")
    if not aggregation_result.get("degree_requirement_distribution"):
        warnings.append("学历要求分布为空，可能缺少学历字段且 job_desc 未包含明确学历要求")
    salary_stats = aggregation_result.get("salary_stats", {})
    if not salary_stats or salary_stats.get("valid_salary_count", 0) == 0:
        warnings.append("薪资统计为空，当前岗位组缺少可解析的月薪字段")
    return dedup_keep_order(warnings)


def aggregate_job_profile_group(
    df: pd.DataFrame,
    standard_job_name: str,
) -> Dict[str, Any]:
    """
    主聚合函数。

    输入：
    - df: 清洗后的岗位 DataFrame
    - standard_job_name: 目标标准岗位名称

    输出：
    - 结构化聚合结果 dict
    """
    group_df = filter_standard_job_group(df, standard_job_name)

    aggregation_result = JobProfileAggregationResult(
        standard_job_name=clean_text(standard_job_name),
        job_count=int(len(group_df)),
        skill_frequency=aggregate_skill_frequency(group_df),
        degree_requirement_distribution=aggregate_degree_distribution(group_df),
        industry_distribution=aggregate_column_distribution(group_df, "industry", top_n=50),
        city_distribution=aggregate_column_distribution(group_df, "city", top_n=100),
        salary_stats=build_salary_stats(group_df),
        top_company_types=aggregate_column_distribution(group_df, "company_type", top_n=30),
        top_company_sizes=aggregate_column_distribution(group_df, "company_size", top_n=30),
        source_columns=list(group_df.columns),
        aggregation_warnings=[],
    )

    result_dict = asdict(aggregation_result)
    result_dict["aggregation_warnings"] = build_aggregation_warnings(
        standard_job_name=standard_job_name,
        group_df=group_df,
        aggregation_result=result_dict,
    )
    return result_dict


def aggregate_and_save_job_profile_group(
    df: pd.DataFrame,
    standard_job_name: str,
    output_path: Optional[str | Path] = DEFAULT_OUTPUT_PATH,
) -> Dict[str, Any]:
    """文件级封装：聚合岗位组并按需保存 JSON。"""
    result = aggregate_job_profile_group(df, standard_job_name)
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
    return result


def build_demo_dataframe() -> pd.DataFrame:
    """构造最小可运行 demo 数据。"""
    return pd.DataFrame(
        [
            {
                "job_name": "数据分析师",
                "standard_job_name": "数据分析师",
                "city": "杭州",
                "province": "浙江",
                "salary_min_month": 12000,
                "salary_max_month": 18000,
                "company_name": "A科技公司",
                "company_type": "民营",
                "company_size": "1000-9999人",
                "industry": "互联网",
                "job_desc": "本科及以上，熟悉Python、SQL、Excel，具备数据分析和可视化能力。",
                "company_desc": "互联网数据产品公司",
                "update_date": "2026-03-01",
                "hard_skills": ["Python", "SQL", "数据分析"],
                "tools_or_tech_stack": ["Excel", "Tableau"],
                "degree_requirement": "本科",
            },
            {
                "job_name": "商业数据分析",
                "standard_job_name": "数据分析师",
                "city": "上海",
                "province": "上海",
                "salary_min_month": 15000,
                "salary_max_month": 22000,
                "company_name": "B电商公司",
                "company_type": "上市公司",
                "company_size": "10000人以上",
                "industry": "电子商务",
                "job_desc": "本科以上学历，掌握SQL、Python、Power BI，有AB实验和业务分析经验。",
                "company_desc": "大型电商公司",
                "update_date": "2026-03-10",
                "hard_skills": '["SQL", "Python", "A/B测试"]',
                "tools_or_tech_stack": '["Power BI"]',
                "degree_requirement": "本科",
            },
            {
                "job_name": "BI分析师",
                "standard_job_name": "数据分析师",
                "city": "杭州",
                "province": "浙江",
                "salary_min_month": 10000,
                "salary_max_month": 16000,
                "company_name": "C软件公司",
                "company_type": "民营",
                "company_size": "100-499人",
                "industry": "软件服务",
                "job_desc": "大专及以上，熟练使用SQL、Excel、Tableau，具备良好数据分析能力。",
                "company_desc": "企业数字化软件服务商",
                "update_date": "2026-03-15",
                "degree_requirement": "大专",
            },
        ]
    )


def parse_args() -> argparse.Namespace:
    """命令行参数解析。"""
    parser = argparse.ArgumentParser(description="Aggregate job profile group statistics")
    parser.add_argument(
        "--input-csv",
        default="",
        help="可选：输入岗位 CSV 文件路径；不传则使用内置 demo 数据",
    )
    parser.add_argument(
        "--standard-job-name",
        default="数据分析师",
        help="目标标准岗位名称",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_PATH),
        help="聚合结果 JSON 输出路径",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.input_csv:
        input_df = pd.read_csv(args.input_csv)
    else:
        input_df = build_demo_dataframe()

    result = aggregate_and_save_job_profile_group(
        df=input_df,
        standard_job_name=args.standard_job_name,
        output_path=args.output,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
