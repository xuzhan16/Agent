"""
job_profile_builder.py

岗位要求画像模块的 builder 层。

职责边界：
1. 基于 standard_job_name 从清洗后的岗位 DataFrame 中筛选岗位组；
2. 从岗位描述中用规则抽取显式要求；
3. 使用本地词表对技能、工具、专业、证书做标准化映射；
4. 构造适合送给大模型 job_profile 任务的 job_profile_input_payload；
5. 不在本文件中直接调用大模型。

设计思路：
- Skill extraction：先从岗位描述召回 skill phrase，再做别名归一；
- Taxonomy mapping：用本地 alias map 映射到标准标签；
- 弱监督抽取：规则先行，模型后续补充。
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pandas as pd


DEFAULT_OUTPUT_PATH = Path("outputs/state/job_profile_input_payload.json")


@dataclass
class JobGroupSummary:
    """岗位组统计摘要。"""

    standard_job_name: str = ""
    job_count: int = 0
    city_distribution: Dict[str, int] = field(default_factory=dict)
    province_distribution: Dict[str, int] = field(default_factory=dict)
    industry_distribution: Dict[str, int] = field(default_factory=dict)
    company_type_distribution: Dict[str, int] = field(default_factory=dict)
    company_size_distribution: Dict[str, int] = field(default_factory=dict)
    salary_min_month_median: Optional[float] = None
    salary_max_month_median: Optional[float] = None
    salary_min_month_avg: Optional[float] = None
    salary_max_month_avg: Optional[float] = None
    latest_update_date: str = ""


@dataclass
class ExplicitRequirementProfile:
    """规则抽取到的岗位显式要求。"""

    degree_requirements: List[str] = field(default_factory=list)
    major_requirements: List[str] = field(default_factory=list)
    experience_requirements: List[str] = field(default_factory=list)
    skill_requirements: List[str] = field(default_factory=list)
    tool_requirements: List[str] = field(default_factory=list)
    certificate_requirements: List[str] = field(default_factory=list)
    practice_requirements: List[str] = field(default_factory=list)
    soft_skill_hints: List[str] = field(default_factory=list)
    requirement_sentences: List[str] = field(default_factory=list)


@dataclass
class NormalizedRequirementProfile:
    """归一化后的岗位要求标签。"""

    degree_tags: List[str] = field(default_factory=list)
    major_tags: List[str] = field(default_factory=list)
    hard_skill_tags: List[str] = field(default_factory=list)
    tool_skill_tags: List[str] = field(default_factory=list)
    certificate_tags: List[str] = field(default_factory=list)
    practice_tags: List[str] = field(default_factory=list)
    soft_skill_tags: List[str] = field(default_factory=list)
    experience_tags: List[str] = field(default_factory=list)
    domain_tags: List[str] = field(default_factory=list)


@dataclass
class JobProfileInputPayload:
    """传给 job_profile 大模型任务的输入 payload。"""

    standard_job_name: str = ""
    group_summary: Dict[str, Any] = field(default_factory=dict)
    explicit_requirements: Dict[str, Any] = field(default_factory=dict)
    normalized_requirements: Dict[str, Any] = field(default_factory=dict)
    representative_samples: List[Dict[str, Any]] = field(default_factory=list)
    source_columns: List[str] = field(default_factory=list)
    build_warnings: List[str] = field(default_factory=list)


DEGREE_PATTERNS: List[Tuple[str, str]] = [
    (r"(博士|博士研究生|phd)", "博士"),
    (r"(硕士|研究生|master|mba)", "硕士"),
    (r"(本科|学士|bachelor)", "本科"),
    (r"(大专|专科|高职|college)", "大专"),
    (r"(学历不限|不限学历|无学历要求)", "学历不限"),
    (r"(高中|中专)", "高中/中专"),
]


EXPERIENCE_PATTERNS: List[Tuple[str, str]] = [
    (r"(经验不限|无需经验|不限经验|接受应届|应届生|应届毕业生|可实习)", "经验不限/应届可投"),
    (r"([1-2]\s*年|一到两年|1年以上|2年以上)", "1-2年经验"),
    (r"([3-5]\s*年|三到五年|3年以上|5年以上)", "3-5年经验"),
    (r"(5\s*年以上|五年以上|8\s*年以上|10\s*年以上)", "5年以上经验"),
    (r"(实习经验|有实习经历|校内外实践)", "有实习/实践经验"),
    (r"(项目经验|有项目经历|项目落地)", "有项目经验"),
]


PRACTICE_REQUIREMENT_PATTERNS: List[Tuple[str, str]] = [
    (r"(实习经验|实习经历|可实习|在校实习)", "实习要求"),
    (r"(项目经验|项目经历|独立负责项目|有完整项目)", "项目要求"),
    (r"(出差|驻场|外派)", "可接受出差/驻场"),
    (r"(英语读写|英文文档|口语流利|CET-6|英语六级)", "英语能力要求"),
    (r"(抗压|高强度|加班)", "抗压/强执行要求"),
    (r"(跨部门|协同|沟通业务|客户沟通)", "跨部门协作要求"),
]


SOFT_SKILL_PATTERNS: List[Tuple[str, str]] = [
    (r"(沟通|表达|协调|跨部门)", "沟通协作"),
    (r"(学习能力|自驱|主动学习|快速学习)", "学习能力"),
    (r"(责任心|认真负责|owner意识|主人翁)", "责任心"),
    (r"(逻辑思维|结构化思考|分析能力|问题拆解)", "逻辑分析"),
    (r"(抗压|执行力|结果导向|推动落地)", "执行抗压"),
    (r"(团队合作|团队协作|合作意识)", "团队合作"),
    (r"(创新|探索|好奇心)", "创新意识"),
]


MAJOR_ALIAS_MAP: Dict[str, List[str]] = {
    "计算机科学与技术": ["计算机科学与技术", "计算机科学", "计算机技术", "计算机", "软件工程", "网络工程"],
    "数据科学与大数据技术": ["数据科学与大数据技术", "数据科学", "大数据", "数据技术", "人工智能与大数据"],
    "人工智能": ["人工智能", "机器学习", "智能科学与技术", "模式识别"],
    "统计学": ["统计学", "应用统计", "数理统计", "经济统计"],
    "数学": ["数学", "应用数学", "数学与应用数学", "计算数学"],
    "电子信息工程": ["电子信息", "电子信息工程", "通信工程", "自动化"],
    "信息管理与信息系统": ["信息管理与信息系统", "信息管理", "信息系统", "信管"],
    "金融学": ["金融学", "金融工程", "金融科技"],
    "市场营销": ["市场营销", "电子商务", "工商管理"],
}


SKILL_ALIAS_MAP: Dict[str, List[str]] = {
    "Python": ["python", "python3", "py"],
    "Java": ["java", "spring", "springboot", "jvm"],
    "C++": ["c++", "cpp", "stl"],
    "SQL": ["sql", "mysql", "postgresql", "hive sql", "oracle"],
    "数据分析": ["数据分析", "数据处理", "数据清洗", "业务分析", "分析建模"],
    "数据挖掘": ["数据挖掘", "特征工程", "特征分析"],
    "机器学习": ["机器学习", "machine learning", "ml", "sklearn", "xgboost", "lightgbm"],
    "深度学习": ["深度学习", "deep learning", "pytorch", "tensorflow", "神经网络"],
    "NLP": ["nlp", "自然语言处理", "文本挖掘", "llm", "大语言模型"],
    "数据可视化": ["数据可视化", "可视化", "图表分析", "dashboard"],
    "爬虫": ["爬虫", "scrapy", "requests", "beautifulsoup", "selenium"],
    "A/B测试": ["ab测试", "a/b测试", "实验分析", "ab实验"],
    "推荐系统": ["推荐系统", "推荐算法", "召回", "排序"],
}


TOOL_ALIAS_MAP: Dict[str, List[str]] = {
    "Excel": ["excel", "vlookup", "数据透视表", "pivot"],
    "Power BI": ["powerbi", "power bi"],
    "Tableau": ["tableau"],
    "Git": ["git", "github", "gitlab"],
    "Linux": ["linux", "shell", "bash"],
    "Spark": ["spark", "pyspark"],
    "Hadoop": ["hadoop", "hdfs", "mapreduce"],
    "Flink": ["flink"],
    "Hive": ["hive"],
    "Docker": ["docker"],
    "Kubernetes": ["kubernetes", "k8s"],
    "Redis": ["redis"],
    "Kafka": ["kafka"],
    "Airflow": ["airflow"],
    "Pandas": ["pandas"],
    "NumPy": ["numpy"],
    "Scikit-learn": ["scikit-learn", "sklearn"],
    "PyTorch": ["pytorch", "torch"],
    "TensorFlow": ["tensorflow"],
}


CERTIFICATE_ALIAS_MAP: Dict[str, List[str]] = {
    "CET-4": ["cet-4", "cet4", "英语四级", "大学英语四级", "四级"],
    "CET-6": ["cet-6", "cet6", "英语六级", "大学英语六级", "六级"],
    "PMP": ["pmp", "项目管理专业人士"],
    "CPA": ["cpa", "注册会计师"],
    "软考": ["软考", "软件设计师", "系统架构设计师", "信息系统项目管理师"],
    "计算机二级": ["计算机二级", "全国计算机等级考试二级", "ncre二级"],
    "教师资格证": ["教师资格证", "教资"],
}


DOMAIN_RULES: Dict[str, List[str]] = {
    "数据分析": ["数据分析", "SQL", "Excel", "Tableau", "Power BI", "A/B测试"],
    "人工智能": ["机器学习", "深度学习", "NLP", "推荐系统", "Python", "PyTorch", "TensorFlow"],
    "大数据工程": ["Spark", "Hadoop", "Hive", "Flink", "Kafka", "SQL"],
    "软件开发": ["Java", "Python", "C++", "Git", "Docker", "Kubernetes", "Redis"],
    "商业分析": ["数据分析", "A/B测试", "Excel", "Power BI", "沟通协作"],
}


def clean_text(value: Any) -> str:
    """统一文本清洗。"""
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = str(value)
    text = text.replace("\u00a0", " ").replace("\u3000", " ")
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\t", " ")
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"[ ]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = "\n".join(line.strip() for line in text.splitlines())
    text = text.strip()
    if text.lower() in {"", "nan", "none", "null", "n/a", "na", "-"}:
        return ""
    return text


def safe_float(value: Any) -> Optional[float]:
    """安全转 float。"""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = clean_text(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def safe_list(value: Any) -> List[Any]:
    """将任意输入安全转 list。"""
    if isinstance(value, list):
        return value
    if value is None or (isinstance(value, float) and pd.isna(value)) or value == "":
        return []
    return [value]


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


def compact_token(text: str) -> str:
    """将 token 压缩成更适合匹配的形式。"""
    lowered = clean_text(text).lower().replace(" ", "")
    return re.sub(r"[()（）\[\]【】\-_/|·,，;；:+.#]", "", lowered)


def normalize_by_alias(text: str, alias_map: Dict[str, List[str]]) -> str:
    """用本地别名字典做标准化映射。"""
    raw_text = clean_text(text)
    if not raw_text:
        return ""

    raw_compact = compact_token(raw_text)
    for std_name, aliases in alias_map.items():
        if raw_text == std_name or raw_compact == compact_token(std_name):
            return std_name
        for alias in aliases:
            alias_compact = compact_token(alias)
            if not alias_compact:
                continue
            if raw_compact == alias_compact:
                return std_name
            if len(alias_compact) >= 2 and alias_compact in raw_compact:
                return std_name
            if len(raw_compact) >= 2 and raw_compact in alias_compact:
                return std_name
    return raw_text


def normalize_major(text: str) -> str:
    return normalize_by_alias(text, MAJOR_ALIAS_MAP)


def normalize_skill(text: str) -> str:
    return normalize_by_alias(text, SKILL_ALIAS_MAP)


def normalize_tool(text: str) -> str:
    return normalize_by_alias(text, TOOL_ALIAS_MAP)


def normalize_certificate(text: str) -> str:
    return normalize_by_alias(text, CERTIFICATE_ALIAS_MAP)


def get_first_existing_column(df: pd.DataFrame, candidates: Sequence[str]) -> str:
    """从候选字段名中返回第一个存在的字段。"""
    for name in candidates:
        if name in df.columns:
            return name
    return ""


def get_standard_job_group(df: pd.DataFrame, standard_job_name: str) -> pd.DataFrame:
    """
    根据 standard_job_name 读取岗位组数据。

    如果 standard_job_name 字段不存在，则回退到 job_name 字段精确匹配。
    """
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


def collect_job_text(row: pd.Series) -> str:
    """拼接单条岗位记录的职位名、JD、公司描述。"""
    chunks = []
    for col in ["standard_job_name", "job_name", "job_desc", "company_desc", "industry", "company_type"]:
        if col in row.index:
            text = clean_text(row.get(col, ""))
            if text:
                chunks.append(text)
    return "\n".join(chunks)


def split_requirement_sentences(text: str) -> List[str]:
    """将岗位描述拆成较短的候选要求句。"""
    cleaned = clean_text(text)
    if not cleaned:
        return []

    parts = re.split(r"[\n。；;.!！?？]+", cleaned)
    return dedup_keep_order(part for part in parts if 2 <= len(clean_text(part)) <= 200)


def extract_degree_requirements(text: str) -> List[str]:
    """规则抽取学历要求。"""
    result = []
    for pattern, degree_name in DEGREE_PATTERNS:
        if re.search(pattern, text, flags=re.IGNORECASE):
            result.append(degree_name)
    return dedup_keep_order(result)


def extract_major_requirements(text: str) -> List[str]:
    """规则抽取专业要求。"""
    result = []
    for std_major, aliases in MAJOR_ALIAS_MAP.items():
        if std_major in text or any(alias in text for alias in aliases):
            result.append(std_major)

    major_pattern = re.compile(r"([\u4e00-\u9fa5A-Za-z0-9/&]{2,20})(?:相关)?专业")
    for match in major_pattern.finditer(text):
        major_text = normalize_major(match.group(1))
        if major_text and major_text not in {"相关", "以上"}:
            result.append(major_text)
    return dedup_keep_order(result)


def extract_experience_requirements(text: str) -> List[str]:
    """规则抽取经验要求。"""
    result = []
    for pattern, tag in EXPERIENCE_PATTERNS:
        if re.search(pattern, text, flags=re.IGNORECASE):
            result.append(tag)

    year_matches = re.findall(r"(\d+)\s*[-~至到]?\s*(\d+)?\s*年(?:以上)?(?:工作)?经验", text)
    for low, high in year_matches:
        if high:
            result.append(f"{low}-{high}年经验")
        else:
            result.append(f"{low}年以上经验")
    return dedup_keep_order(result)


def extract_skill_requirements(text: str) -> List[str]:
    """规则抽取技能要求，并做初步归一。"""
    result = []
    for std_skill, aliases in SKILL_ALIAS_MAP.items():
        if std_skill.lower() in text.lower():
            result.append(std_skill)
            continue
        if any(alias.lower() in text.lower() for alias in aliases):
            result.append(std_skill)

    # 额外召回“XX能力 / XX经验 / 熟悉XX”短语中的技能候选，再交给 normalize_skill。
    phrase_patterns = [
        r"熟悉([A-Za-z0-9+#\u4e00-\u9fa5]{2,30})",
        r"掌握([A-Za-z0-9+#\u4e00-\u9fa5]{2,30})",
        r"具备([A-Za-z0-9+#\u4e00-\u9fa5]{2,30})能力",
    ]
    for pattern in phrase_patterns:
        for phrase in re.findall(pattern, text):
            normalized = normalize_skill(phrase)
            if normalized and len(normalized) <= 30:
                result.append(normalized)
    return dedup_keep_order(result)


def extract_tool_requirements(text: str) -> List[str]:
    """规则抽取工具、框架、数据库要求。"""
    result = []
    for std_tool, aliases in TOOL_ALIAS_MAP.items():
        if std_tool.lower() in text.lower():
            result.append(std_tool)
            continue
        if any(alias.lower() in text.lower() for alias in aliases):
            result.append(std_tool)
    return dedup_keep_order(result)


def extract_certificate_requirements(text: str) -> List[str]:
    """规则抽取证书要求。"""
    result = []
    for std_cert, aliases in CERTIFICATE_ALIAS_MAP.items():
        if std_cert.lower() in text.lower() or any(alias.lower() in text.lower() for alias in aliases):
            result.append(std_cert)

    cert_pattern = re.compile(r"([\u4e00-\u9fa5A-Za-z0-9+-]{2,30}(?:证书|认证|资格证))")
    for match in cert_pattern.finditer(text):
        result.append(normalize_certificate(match.group(1)))
    return dedup_keep_order(result)


def extract_practice_requirements(text: str) -> List[str]:
    """规则抽取实习/项目/出差等实践类要求。"""
    result = []
    for pattern, tag in PRACTICE_REQUIREMENT_PATTERNS:
        if re.search(pattern, text, flags=re.IGNORECASE):
            result.append(tag)
    return dedup_keep_order(result)


def extract_soft_skill_hints(text: str) -> List[str]:
    """规则抽取软技能提示。"""
    result = []
    for pattern, tag in SOFT_SKILL_PATTERNS:
        if re.search(pattern, text, flags=re.IGNORECASE):
            result.append(tag)
    return dedup_keep_order(result)


def build_job_group_summary(group_df: pd.DataFrame, standard_job_name: str) -> Dict[str, Any]:
    """构造岗位组统计摘要。"""
    if group_df.empty:
        return asdict(JobGroupSummary(standard_job_name=clean_text(standard_job_name)))

    def _count_distribution(col_name: str) -> Dict[str, int]:
        if col_name not in group_df.columns:
            return {}
        values = [clean_text(value) for value in group_df[col_name].tolist() if clean_text(value)]
        return dict(Counter(values).most_common(30))

    salary_min_col = get_first_existing_column(group_df, ["salary_min_month", "salary_month_min", "salary_min"])
    salary_max_col = get_first_existing_column(group_df, ["salary_max_month", "salary_month_max", "salary_max"])

    salary_min_series = pd.Series(dtype="float64")
    salary_max_series = pd.Series(dtype="float64")
    if salary_min_col:
        salary_min_series = pd.to_numeric(group_df[salary_min_col], errors="coerce").dropna()
    if salary_max_col:
        salary_max_series = pd.to_numeric(group_df[salary_max_col], errors="coerce").dropna()

    latest_update_date = ""
    if "update_date" in group_df.columns:
        update_values = [clean_text(value) for value in group_df["update_date"].tolist() if clean_text(value)]
        latest_update_date = max(update_values) if update_values else ""

    summary = JobGroupSummary(
        standard_job_name=clean_text(standard_job_name),
        job_count=int(len(group_df)),
        city_distribution=_count_distribution("city"),
        province_distribution=_count_distribution("province"),
        industry_distribution=_count_distribution("industry"),
        company_type_distribution=_count_distribution("company_type"),
        company_size_distribution=_count_distribution("company_size"),
        salary_min_month_median=round(float(salary_min_series.median()), 2) if not salary_min_series.empty else None,
        salary_max_month_median=round(float(salary_max_series.median()), 2) if not salary_max_series.empty else None,
        salary_min_month_avg=round(float(salary_min_series.mean()), 2) if not salary_min_series.empty else None,
        salary_max_month_avg=round(float(salary_max_series.mean()), 2) if not salary_max_series.empty else None,
        latest_update_date=latest_update_date,
    )
    return asdict(summary)


def build_explicit_requirements(group_df: pd.DataFrame) -> Dict[str, Any]:
    """从岗位组文本中抽取显式要求。"""
    degree_requirements = []
    major_requirements = []
    experience_requirements = []
    skill_requirements = []
    tool_requirements = []
    certificate_requirements = []
    practice_requirements = []
    soft_skill_hints = []
    requirement_sentences = []

    for _, row in group_df.iterrows():
        job_text = collect_job_text(row)
        sentences = split_requirement_sentences(job_text)
        requirement_sentences.extend(sentences)

        for sentence in sentences:
            degree_requirements.extend(extract_degree_requirements(sentence))
            major_requirements.extend(extract_major_requirements(sentence))
            experience_requirements.extend(extract_experience_requirements(sentence))
            skill_requirements.extend(extract_skill_requirements(sentence))
            tool_requirements.extend(extract_tool_requirements(sentence))
            certificate_requirements.extend(extract_certificate_requirements(sentence))
            practice_requirements.extend(extract_practice_requirements(sentence))
            soft_skill_hints.extend(extract_soft_skill_hints(sentence))

    explicit_requirements = ExplicitRequirementProfile(
        degree_requirements=dedup_keep_order(degree_requirements),
        major_requirements=dedup_keep_order(major_requirements),
        experience_requirements=dedup_keep_order(experience_requirements),
        skill_requirements=dedup_keep_order(skill_requirements),
        tool_requirements=dedup_keep_order(tool_requirements),
        certificate_requirements=dedup_keep_order(certificate_requirements),
        practice_requirements=dedup_keep_order(practice_requirements),
        soft_skill_hints=dedup_keep_order(soft_skill_hints),
        requirement_sentences=dedup_keep_order(requirement_sentences)[:300],
    )
    return asdict(explicit_requirements)


def infer_domain_tags(
    standard_job_name: str,
    major_tags: List[str],
    hard_skill_tags: List[str],
    tool_skill_tags: List[str],
    soft_skill_tags: List[str],
) -> List[str]:
    """根据岗位名、专业、技能、工具、软技能标签推断领域标签。"""
    merged_text = " ".join(
        [clean_text(standard_job_name)]
        + [clean_text(item) for item in major_tags]
        + [clean_text(item) for item in hard_skill_tags]
        + [clean_text(item) for item in tool_skill_tags]
        + [clean_text(item) for item in soft_skill_tags]
    ).lower()

    domain_tags = []
    for domain_name, keywords in DOMAIN_RULES.items():
        if any(keyword.lower() in merged_text for keyword in keywords):
            domain_tags.append(domain_name)
    return dedup_keep_order(domain_tags)


def build_normalized_requirements(
    standard_job_name: str,
    explicit_requirements: Dict[str, Any],
) -> Dict[str, Any]:
    """对显式要求做标准化映射，得到统一标签。"""
    major_tags = dedup_keep_order(
        normalize_major(item) for item in safe_list(explicit_requirements.get("major_requirements"))
    )
    hard_skill_tags = dedup_keep_order(
        normalize_skill(item) for item in safe_list(explicit_requirements.get("skill_requirements"))
    )
    tool_skill_tags = dedup_keep_order(
        normalize_tool(item) for item in safe_list(explicit_requirements.get("tool_requirements"))
    )
    certificate_tags = dedup_keep_order(
        normalize_certificate(item)
        for item in safe_list(explicit_requirements.get("certificate_requirements"))
    )
    soft_skill_tags = dedup_keep_order(
        clean_text(item) for item in safe_list(explicit_requirements.get("soft_skill_hints"))
    )
    experience_tags = dedup_keep_order(
        clean_text(item)
        for item in safe_list(explicit_requirements.get("experience_requirements"))
        if clean_text(item)
    )
    practice_tags = dedup_keep_order(
        clean_text(item)
        for item in safe_list(explicit_requirements.get("practice_requirements"))
        if clean_text(item)
    )
    degree_tags = dedup_keep_order(
        clean_text(item)
        for item in safe_list(explicit_requirements.get("degree_requirements"))
        if clean_text(item)
    )

    normalized_profile = NormalizedRequirementProfile(
        degree_tags=degree_tags,
        major_tags=major_tags,
        hard_skill_tags=hard_skill_tags,
        tool_skill_tags=tool_skill_tags,
        certificate_tags=certificate_tags,
        practice_tags=practice_tags,
        soft_skill_tags=soft_skill_tags,
        experience_tags=experience_tags,
        domain_tags=infer_domain_tags(
            standard_job_name=standard_job_name,
            major_tags=major_tags,
            hard_skill_tags=hard_skill_tags,
            tool_skill_tags=tool_skill_tags,
            soft_skill_tags=soft_skill_tags,
        ),
    )
    return asdict(normalized_profile)


def pick_representative_samples(group_df: pd.DataFrame, max_samples: int = 5) -> List[Dict[str, Any]]:
    """选取少量代表性 JD 样本，供后续大模型参考。"""
    if group_df.empty:
        return []

    sample_df = group_df.copy()
    if "job_desc" in sample_df.columns:
        sample_df["_desc_len"] = sample_df["job_desc"].apply(lambda x: len(clean_text(x)))
        sample_df = sample_df.sort_values("_desc_len", ascending=False)

    sample_df = sample_df.head(max_samples)
    samples = []
    for _, row in sample_df.iterrows():
        samples.append(
            {
                "job_name": clean_text(row.get("job_name", "")),
                "standard_job_name": clean_text(row.get("standard_job_name", "")),
                "city": clean_text(row.get("city", "")),
                "province": clean_text(row.get("province", "")),
                "salary_min_month": safe_float(
                    row.get("salary_min_month", row.get("salary_month_min", row.get("salary_min")))
                ),
                "salary_max_month": safe_float(
                    row.get("salary_max_month", row.get("salary_month_max", row.get("salary_max")))
                ),
                "company_name": clean_text(row.get("company_name", "")),
                "company_type": clean_text(row.get("company_type", "")),
                "company_size": clean_text(row.get("company_size", "")),
                "industry": clean_text(row.get("industry", "")),
                "job_desc": clean_text(row.get("job_desc", "")),
                "company_desc": clean_text(row.get("company_desc", "")),
                "update_date": clean_text(row.get("update_date", "")),
            }
        )
    return samples


def build_warnings(
    standard_job_name: str,
    group_df: pd.DataFrame,
    explicit_requirements: Dict[str, Any],
) -> List[str]:
    """根据抽取结果生成构建期 warning。"""
    warnings = []
    if group_df.empty:
        warnings.append(f"未找到 standard_job_name={standard_job_name} 的岗位组数据")

    if not safe_list(explicit_requirements.get("degree_requirements")):
        warnings.append("未从岗位描述中抽取到明确学历要求")
    if not safe_list(explicit_requirements.get("skill_requirements")):
        warnings.append("未从岗位描述中抽取到明确技能要求")
    if not safe_list(explicit_requirements.get("experience_requirements")):
        warnings.append("未从岗位描述中抽取到明确经验要求")
    if not safe_list(explicit_requirements.get("major_requirements")):
        warnings.append("未从岗位描述中抽取到明确专业要求")
    return dedup_keep_order(warnings)


def build_job_profile_input_payload_from_group(
    group_df: pd.DataFrame,
    standard_job_name: str,
) -> Dict[str, Any]:
    """基于单个岗位组 DataFrame 构造 job_profile_input_payload。"""
    target_name = clean_text(standard_job_name)
    explicit_requirements = build_explicit_requirements(group_df)
    normalized_requirements = build_normalized_requirements(target_name, explicit_requirements)

    payload = JobProfileInputPayload(
        standard_job_name=target_name,
        group_summary=build_job_group_summary(group_df, target_name),
        explicit_requirements=explicit_requirements,
        normalized_requirements=normalized_requirements,
        representative_samples=pick_representative_samples(group_df),
        source_columns=list(group_df.columns),
        build_warnings=build_warnings(target_name, group_df, explicit_requirements),
    )
    return asdict(payload)


def build_job_profile_input_payload(
    df: pd.DataFrame,
    standard_job_name: str,
    output_path: Optional[str | Path] = None,
) -> Dict[str, Any]:
    """
    主入口：从完整岗位 DataFrame 中筛出某个 standard_job_name 岗位组，
    并构造 job_profile_input_payload。
    """
    group_df = get_standard_job_group(df, standard_job_name)
    payload = build_job_profile_input_payload_from_group(group_df, standard_job_name)

    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    return payload


def build_demo_dataframe() -> pd.DataFrame:
    """构造可直接运行的 demo 岗位数据。"""
    return pd.DataFrame(
        [
            {
                "job_name": "数据分析师",
                "standard_job_name": "数据分析师",
                "city": "杭州",
                "province": "浙江",
                "salary_min_month": 12000,
                "salary_max_month": 20000,
                "company_name": "某互联网公司",
                "company_type": "民营",
                "company_size": "1000-9999人",
                "industry": "互联网",
                "job_desc": (
                    "本科及以上学历，统计学、计算机、数据科学相关专业优先；"
                    "熟悉Python、SQL、Excel、Tableau，具备数据分析和A/B测试经验；"
                    "有项目经验或互联网实习经验，沟通能力强，能跨部门协作。"
                ),
                "company_desc": "专注互联网数据产品与业务增长分析。",
                "update_date": "2026-03-01",
            },
            {
                "job_name": "商业数据分析",
                "standard_job_name": "数据分析师",
                "city": "上海",
                "province": "上海",
                "salary_min_month": 15000,
                "salary_max_month": 23000,
                "company_name": "某电商平台",
                "company_type": "上市公司",
                "company_size": "10000人以上",
                "industry": "电子商务",
                "job_desc": (
                    "本科以上学历，数学、统计、计算机等相关专业；"
                    "掌握SQL、Python、Power BI，熟悉用户分析、指标分析和可视化报表；"
                    "具备良好的逻辑思维、沟通表达和推动落地能力，英语六级优先。"
                ),
                "company_desc": "大型综合电商与零售科技企业。",
                "update_date": "2026-03-15",
            },
        ]
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build job profile input payload")
    parser.add_argument(
        "--input-csv",
        default="",
        help="可选：岗位明细 CSV 路径；不传则使用内置 demo 数据",
    )
    parser.add_argument(
        "--standard-job-name",
        default="数据分析师",
        help="目标标准岗位名称",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_PATH),
        help="job_profile_input_payload JSON 输出路径",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.input_csv:
        source_df = pd.read_csv(args.input_csv)
    else:
        source_df = build_demo_dataframe()

    result_payload = build_job_profile_input_payload(
        df=source_df,
        standard_job_name=args.standard_job_name,
        output_path=args.output,
    )
    print(json.dumps(result_payload, ensure_ascii=False, indent=2))
