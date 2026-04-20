"""
student_profile_builder.py

学生就业能力画像模块的 builder 层。

职责边界：
1. 只读取 student_api_state.json 里已有的 basic_info 和 resume_parse_result；
2. 不重写 resume_parse，不调用 LLM；
3. 使用本地字典/规则完成字段整理和标准化映射；
4. 构造适合后续传给大模型 student_profile 任务的 profile_input_payload。

设计参考：
- pyresparser：基础字段整理思路
- SkillNER：技能别名归一思路
- ESCO taxonomy：标准标签映射思路
- Tabiya：多实体特征拼装思路

注意：
这里不直接集成上述项目/标准库，只保留轻量本地映射实现，方便先跑通、后扩展。
"""

from __future__ import annotations

import argparse
import json
import re
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


DEFAULT_STATE_PATH = Path("student_api_state.json")


@dataclass
class PracticeProfile:
    """实践经历概览特征。"""

    project_count: int = 0
    internship_count: int = 0
    award_count: int = 0
    has_project_experience: bool = False
    has_internship_experience: bool = False
    has_competition_or_award: bool = False
    has_target_job_intention: bool = False
    total_practice_items: int = 0
    project_keywords: List[str] = field(default_factory=list)
    internship_keywords: List[str] = field(default_factory=list)
    practice_tags: List[str] = field(default_factory=list)


@dataclass
class ProfileInputPayload:
    """传给 student_profile 大模型任务的输入载荷。"""

    basic_info: Dict[str, Any] = field(default_factory=dict)
    normalized_education: Dict[str, Any] = field(default_factory=dict)
    explicit_profile: Dict[str, Any] = field(default_factory=dict)
    normalized_profile: Dict[str, Any] = field(default_factory=dict)
    practice_profile: Dict[str, Any] = field(default_factory=dict)
    evidence_summary: Dict[str, Any] = field(default_factory=dict)
    source_snapshot: Dict[str, Any] = field(default_factory=dict)


# 专业归一：左侧为标准专业标签，右侧为可能出现的别名/关键词
MAJOR_NORMALIZATION_MAP: Dict[str, List[str]] = {
    "计算机科学与技术": ["计算机科学与技术", "计算机科学", "计算机技术", "计算机", "软件工程", "网络工程"],
    "数据科学与大数据技术": ["数据科学与大数据技术", "数据科学", "大数据", "数据技术", "人工智能与大数据"],
    "人工智能": ["人工智能", "智能科学与技术", "机器学习"],
    "统计学": ["统计学", "应用统计", "数理统计", "经济统计"],
    "信息管理与信息系统": ["信息管理与信息系统", "信管", "信息管理", "信息系统"],
    "电子信息工程": ["电子信息工程", "电子信息", "通信工程", "自动化"],
    "数学": ["数学与应用数学", "应用数学", "数学"],
    "金融学": ["金融学", "金融工程", "金融科技", "投资学"],
    "市场营销": ["市场营销", "营销管理", "电子商务", "工商管理"],
}


# 技能归一：标准技能 -> 别名
SKILL_ALIAS_MAP: Dict[str, List[str]] = {
    "Python": ["python", "py", "python3"],
    "Java": ["java", "java8", "java11", "jdk"],
    "C++": ["c++", "cpp"],
    "SQL": ["sql", "mysql", "postgresql", "oracle sql", "hive sql"],
    "机器学习": ["机器学习", "machine learning", "ml", "sklearn", "scikit-learn"],
    "深度学习": ["深度学习", "deep learning", "dl", "pytorch", "tensorflow"],
    "数据分析": ["数据分析", "数据处理", "数据清洗", "分析建模", "业务分析"],
    "数据可视化": ["可视化", "数据可视化", "图表分析", "dashboard"],
    "NLP": ["nlp", "自然语言处理", "文本挖掘"],
    "爬虫": ["爬虫", "web scraping", "scrapy", "requests", "beautifulsoup"],
    "Linux": ["linux", "shell", "bash"],
    "Git": ["git", "github", "gitlab"],
    "Excel": ["excel", "vlookup", "pivot", "数据透视表"],
    "Power BI": ["powerbi", "power bi", "bi 工具", "bi报表"],
    "Tableau": ["tableau"],
    "Spark": ["spark", "pyspark"],
    "Hadoop": ["hadoop", "hdfs", "mapreduce"],
    "Docker": ["docker", "容器化"],
    "Kubernetes": ["kubernetes", "k8s"],
    "HTML/CSS": ["html", "css", "html5", "css3"],
    "JavaScript": ["javascript", "js", "typescript", "ts"],
}


TOOL_SKILL_SET: Set[str] = {
    "Excel",
    "Power BI",
    "Tableau",
    "Git",
    "Docker",
    "Kubernetes",
    "Linux",
    "Spark",
    "Hadoop",
}


CERTIFICATE_ALIAS_MAP: Dict[str, List[str]] = {
    "CET-4": ["cet-4", "cet4", "英语四级", "大学英语四级", "四级"],
    "CET-6": ["cet-6", "cet6", "英语六级", "大学英语六级", "六级"],
    "计算机二级": ["计算机二级", "全国计算机等级考试二级", "ncre二级"],
    "软考": ["软考", "软件设计师", "系统架构设计师", "信息系统项目管理师"],
    "教师资格证": ["教师资格证", "教资"],
    "CPA": ["cpa", "注册会计师"],
    "PMP": ["pmp", "项目管理专业人士"],
}


OCCUPATION_HINT_MAP: Dict[str, List[str]] = {
    "数据分析": ["数据分析", "数据分析师", "bi分析", "商业分析", "数据运营"],
    "算法工程": ["算法工程师", "机器学习工程师", "nlp算法", "推荐算法", "算法"],
    "后端开发": ["后端开发", "java开发", "python后端", "go后端", "服务端开发"],
    "前端开发": ["前端开发", "web前端", "前端工程师", "vue", "react"],
    "测试开发": ["测试开发", "软件测试", "自动化测试", "测试工程师"],
    "产品经理": ["产品经理", "数据产品", "ai产品", "产品助理"],
    "运维/DevOps": ["运维", "devops", "sre", "云平台运维"],
    "网络安全": ["网络安全", "安全工程师", "渗透测试", "安全运营"],
}


DOMAIN_TAG_RULES: Dict[str, List[str]] = {
    "数据智能": ["数据分析", "机器学习", "深度学习", "NLP", "Power BI", "Tableau", "Spark", "Hadoop"],
    "软件研发": ["Java", "Python", "C++", "HTML/CSS", "JavaScript", "Git", "Docker", "Kubernetes"],
    "商业分析": ["数据分析", "Excel", "Power BI", "Tableau", "SQL"],
    "人工智能": ["机器学习", "深度学习", "NLP", "Python"],
    "云计算/大数据": ["Spark", "Hadoop", "Docker", "Kubernetes", "Linux", "SQL"],
}


EXPERIENCE_KEYWORD_RULES: Dict[str, List[str]] = {
    "数据处理": ["数据清洗", "数据处理", "etl", "报表", "sql", "分析"],
    "建模算法": ["模型", "机器学习", "深度学习", "预测", "分类", "推荐"],
    "工程开发": ["开发", "接口", "系统", "平台", "后端", "前端", "工程"],
    "科研实践": ["科研", "论文", "实验", "竞赛", "专利"],
    "业务协作": ["业务", "沟通", "需求", "协作", "运营"],
}


def load_student_state(state_path: str | Path = DEFAULT_STATE_PATH) -> Dict[str, Any]:
    """读取 student_api_state.json。"""
    path = Path(state_path)
    if not path.exists():
        raise FileNotFoundError(f"student_api_state.json not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, dict) else {}


def _clean_text(value: Any) -> str:
    """基础文本清洗。"""
    if value is None:
        return ""
    text = str(value)
    text = text.replace("\u00a0", " ").replace("\u3000", " ")
    text = re.sub(r"\s+", " ", text).strip()
    if text.lower() in {"", "none", "null", "nan", "n/a", "na", "-"}:
        return ""
    return text


def _safe_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if value is None or value == "":
        return []
    return [value]


def _dedup_keep_order(values: Iterable[str]) -> List[str]:
    """按原始顺序去重。"""
    seen = set()
    result = []
    for value in values:
        cleaned = _clean_text(value)
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            result.append(cleaned)
    return result


def _flatten_experience_text(items: List[Dict[str, Any]], fields: Iterable[str]) -> str:
    """将项目/实习经历中的若干字段拼成一个检索文本。"""
    chunks = []
    for item in items:
        row = _safe_dict(item)
        for field_name in fields:
            text = _clean_text(row.get(field_name, ""))
            if text:
                chunks.append(text)
    return " ".join(chunks)


def _match_alias(text: str, alias_map: Dict[str, List[str]]) -> str:
    """根据别名字典返回标准标签，未命中则返回清洗后的原值。"""
    cleaned_text = _clean_text(text)
    if not cleaned_text:
        return ""

    lowered_text = cleaned_text.lower().replace(" ", "")
    compact_text = re.sub(r"[()（）\[\]【】\-_/|·,，;；:+]", "", lowered_text)

    for standard_name, aliases in alias_map.items():
        if cleaned_text == standard_name:
            return standard_name
        for alias in aliases:
            lowered_alias = alias.lower().replace(" ", "")
            compact_alias = re.sub(r"[()（）\[\]【】\-_/|·,，;；:+]", "", lowered_alias)
            if not compact_alias:
                continue
            if compact_text == compact_alias:
                return standard_name

            # 只对长度>=2的别名做子串匹配，避免 "c++" -> "c" 这类短 token 误伤 "excel"。
            if len(compact_alias) >= 2 and compact_alias in compact_text:
                return standard_name
            if len(compact_text) >= 2 and compact_text in compact_alias:
                return standard_name
    return cleaned_text


def normalize_major(major_text: str) -> str:
    """专业标准化。"""
    return _match_alias(major_text, MAJOR_NORMALIZATION_MAP)


def normalize_skill(skill_text: str) -> str:
    """技能标准化。"""
    return _match_alias(skill_text, SKILL_ALIAS_MAP)


def normalize_certificate(certificate_text: str) -> str:
    """证书标准化。"""
    return _match_alias(certificate_text, CERTIFICATE_ALIAS_MAP)


def normalize_occupation_hint(text: str) -> str:
    """职业方向 hints 归一。"""
    return _match_alias(text, OCCUPATION_HINT_MAP)


def split_skill_tokens(value: Any) -> List[str]:
    """将技能字段按中英文分隔符拆分成候选 token。"""
    if isinstance(value, list):
        tokens: List[str] = []
        for item in value:
            tokens.extend(split_skill_tokens(item))
        return _dedup_keep_order(tokens)

    text = _clean_text(value)
    if not text:
        return []
    parts = re.split(r"[、,，;；/|｜\n\t]+", text)
    return _dedup_keep_order(part for part in parts if _clean_text(part))


def extract_basic_and_resume(student_state: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    从 student_api_state.json 提取 basic_info 和 resume_parse_result。

    兼容两种情况：
    1. 顶层 basic_info 已同步；
    2. basic_info 只存在 resume_parse_result 内部。
    """
    top_basic_info = _safe_dict(student_state.get("basic_info"))
    resume_parse_result = _safe_dict(student_state.get("resume_parse_result"))
    nested_basic_info = _safe_dict(resume_parse_result.get("basic_info"))

    merged_basic_info = {
        "name": _clean_text(top_basic_info.get("name") or nested_basic_info.get("name")),
        "gender": _clean_text(top_basic_info.get("gender") or nested_basic_info.get("gender")),
        "phone": _clean_text(top_basic_info.get("phone") or nested_basic_info.get("phone")),
        "email": _clean_text(top_basic_info.get("email") or nested_basic_info.get("email")),
        "school": _clean_text(top_basic_info.get("school") or nested_basic_info.get("school")),
        "school_level": _clean_text(
            top_basic_info.get("school_level") or nested_basic_info.get("school_level")
        ),
        "major": _clean_text(top_basic_info.get("major") or nested_basic_info.get("major")),
        "degree": _clean_text(top_basic_info.get("degree") or nested_basic_info.get("degree")),
        "graduation_year": _clean_text(
            top_basic_info.get("graduation_year") or nested_basic_info.get("graduation_year")
        ),
    }
    return merged_basic_info, resume_parse_result


def collect_explicit_fields(
    basic_info: Dict[str, Any],
    resume_parse_result: Dict[str, Any],
) -> Dict[str, Any]:
    """整理简历解析结果中的显式字段。"""
    education_experience = [
        _safe_dict(item) for item in _safe_list(resume_parse_result.get("education_experience"))
    ]
    project_experience = [
        _safe_dict(item) for item in _safe_list(resume_parse_result.get("project_experience"))
    ]
    internship_experience = [
        _safe_dict(item) for item in _safe_list(resume_parse_result.get("internship_experience"))
    ]

    skills = _dedup_keep_order(
        normalize_skill(token)
        for token in split_skill_tokens(resume_parse_result.get("skills"))
    )
    certificates = _dedup_keep_order(
        normalize_certificate(token)
        for token in _safe_list(resume_parse_result.get("certificates"))
    )
    awards = _dedup_keep_order(_clean_text(item) for item in _safe_list(resume_parse_result.get("awards")))

    # 工具字段不一定显式存在，所以从技能列表和经历文本里二次召回。
    tool_candidates = [skill for skill in skills if skill in TOOL_SKILL_SET]
    experience_text = " ".join(
        [
            _flatten_experience_text(project_experience, ["project_name", "role", "description"]),
            _flatten_experience_text(
                internship_experience,
                ["company_name", "position", "description"],
            ),
            _clean_text(resume_parse_result.get("raw_resume_text", "")),
        ]
    )
    for standard_tool, aliases in SKILL_ALIAS_MAP.items():
        if standard_tool not in TOOL_SKILL_SET:
            continue
        if any(alias.lower() in experience_text.lower() for alias in aliases):
            tool_candidates.append(standard_tool)

    target_job_intention = _clean_text(resume_parse_result.get("target_job_intention", ""))

    return {
        "degree": _clean_text(basic_info.get("degree")),
        "school": _clean_text(basic_info.get("school")),
        "school_level": _clean_text(basic_info.get("school_level")),
        "major": _clean_text(basic_info.get("major")),
        "skills": skills,
        "tools": _dedup_keep_order(tool_candidates),
        "certificates": certificates,
        "project_experience": project_experience,
        "internship_experience": internship_experience,
        "awards": awards,
        "target_job_intention": target_job_intention,
        "self_evaluation": _clean_text(resume_parse_result.get("self_evaluation", "")),
        "raw_resume_text": _clean_text(resume_parse_result.get("raw_resume_text", "")),
        "parse_warnings": [
            _clean_text(item)
            for item in _safe_list(resume_parse_result.get("parse_warnings"))
            if _clean_text(item)
        ],
    }


def build_hard_and_tool_skills(skills: List[str], tools: List[str]) -> Tuple[List[str], List[str]]:
    """将技能拆成 hard_skills 与 tool_skills。"""
    normalized_skills = _dedup_keep_order(normalize_skill(skill) for skill in skills)
    normalized_tools = _dedup_keep_order(normalize_skill(tool) for tool in tools)

    tool_skill_set = set(TOOL_SKILL_SET)
    tool_skills = _dedup_keep_order(
        [skill for skill in normalized_skills if skill in tool_skill_set] + normalized_tools
    )
    hard_skills = _dedup_keep_order(
        [skill for skill in normalized_skills if skill and skill not in set(tool_skills)]
    )
    return hard_skills, tool_skills


def build_qualification_tags(
    degree: str,
    school: str,
    major: str,
    certificates: List[str],
    school_level: str = "",
) -> List[str]:
    """构造资格标签。"""
    tags = []
    degree_text = _clean_text(degree)
    school_text = _clean_text(school)
    school_level_text = _clean_text(school_level)
    major_text = normalize_major(major)

    if degree_text:
        tags.append(f"学历:{degree_text}")
    if school_text:
        tags.append(f"学校:{school_text}")
    if school_level_text:
        tags.append(f"学校层次:{school_level_text}")
    if major_text:
        tags.append(f"专业:{major_text}")
    for certificate in certificates:
        cert = normalize_certificate(certificate)
        if cert:
            tags.append(f"证书:{cert}")
    return _dedup_keep_order(tags)


def infer_experience_tags(
    project_experience: List[Dict[str, Any]],
    internship_experience: List[Dict[str, Any]],
    awards: List[str],
) -> Tuple[List[str], List[str], List[str]]:
    """根据项目/实习/奖项文本构造经验标签，并返回项目/实习关键词。"""
    project_text = _flatten_experience_text(
        project_experience,
        ["project_name", "role", "description"],
    )
    internship_text = _flatten_experience_text(
        internship_experience,
        ["company_name", "position", "description"],
    )
    award_text = " ".join(_clean_text(item) for item in awards)

    experience_tags: List[str] = []
    project_keywords: List[str] = []
    internship_keywords: List[str] = []

    for tag_name, keywords in EXPERIENCE_KEYWORD_RULES.items():
        if any(keyword.lower() in project_text.lower() for keyword in keywords):
            experience_tags.append(f"项目:{tag_name}")
            project_keywords.extend(keyword for keyword in keywords if keyword.lower() in project_text.lower())
        if any(keyword.lower() in internship_text.lower() for keyword in keywords):
            experience_tags.append(f"实习:{tag_name}")
            internship_keywords.extend(keyword for keyword in keywords if keyword.lower() in internship_text.lower())
        if any(keyword.lower() in award_text.lower() for keyword in keywords):
            experience_tags.append(f"奖项:{tag_name}")

    if project_experience:
        experience_tags.append("有项目经历")
    if internship_experience:
        experience_tags.append("有实习经历")
    if awards:
        experience_tags.append("有竞赛/获奖经历")

    return (
        _dedup_keep_order(experience_tags),
        _dedup_keep_order(project_keywords),
        _dedup_keep_order(internship_keywords),
    )


def infer_occupation_hints(
    target_job_intention: str,
    skills: List[str],
    project_experience: List[Dict[str, Any]],
    internship_experience: List[Dict[str, Any]],
    self_evaluation: str,
) -> List[str]:
    """根据求职意向、技能和经历文本推断职业方向 hints。"""
    hints: List[str] = []

    if _clean_text(target_job_intention):
        hints.append(normalize_occupation_hint(target_job_intention))

    merged_text = " ".join(
        [
            _clean_text(target_job_intention),
            " ".join(_clean_text(skill) for skill in skills),
            _flatten_experience_text(project_experience, ["project_name", "role", "description"]),
            _flatten_experience_text(
                internship_experience,
                ["company_name", "position", "description"],
            ),
            _clean_text(self_evaluation),
        ]
    )

    for standard_hint, keywords in OCCUPATION_HINT_MAP.items():
        if any(keyword.lower() in merged_text.lower() for keyword in keywords):
            hints.append(standard_hint)

    return _dedup_keep_order(hints)


def infer_domain_tags(
    normalized_major: str,
    hard_skills: List[str],
    tool_skills: List[str],
    occupation_hints: List[str],
    project_experience: List[Dict[str, Any]],
    internship_experience: List[Dict[str, Any]],
) -> List[str]:
    """综合专业、技能、职业方向和经历文本推断领域标签。"""
    domain_tags: List[str] = []

    merged_signals = set(hard_skills) | set(tool_skills) | set(occupation_hints)
    if normalized_major:
        merged_signals.add(normalized_major)

    merged_text = " ".join(
        list(merged_signals)
        + [
            _flatten_experience_text(project_experience, ["project_name", "role", "description"]),
            _flatten_experience_text(
                internship_experience,
                ["company_name", "position", "description"],
            ),
        ]
    ).lower()

    for domain_name, keywords in DOMAIN_TAG_RULES.items():
        if any(keyword.lower() in merged_text for keyword in keywords):
            domain_tags.append(domain_name)

    if normalized_major in {"计算机科学与技术", "软件工程", "人工智能", "数据科学与大数据技术"}:
        domain_tags.append("信息技术")
    if normalized_major in {"统计学", "数学"}:
        domain_tags.append("数理分析")
    if normalized_major in {"金融学", "市场营销"}:
        domain_tags.append("商业管理")

    return _dedup_keep_order(domain_tags)


def build_practice_profile(
    project_experience: List[Dict[str, Any]],
    internship_experience: List[Dict[str, Any]],
    awards: List[str],
    target_job_intention: str,
    project_keywords: List[str],
    internship_keywords: List[str],
    experience_tags: List[str],
) -> Dict[str, Any]:
    """构造实践画像特征。"""
    profile = PracticeProfile(
        project_count=len(project_experience),
        internship_count=len(internship_experience),
        award_count=len(awards),
        has_project_experience=bool(project_experience),
        has_internship_experience=bool(internship_experience),
        has_competition_or_award=bool(awards),
        has_target_job_intention=bool(_clean_text(target_job_intention)),
        total_practice_items=len(project_experience) + len(internship_experience) + len(awards),
        project_keywords=_dedup_keep_order(project_keywords),
        internship_keywords=_dedup_keep_order(internship_keywords),
        practice_tags=_dedup_keep_order(experience_tags),
    )
    return asdict(profile)


def build_normalized_profile(explicit_fields: Dict[str, Any]) -> Dict[str, Any]:
    """基于显式字段构造归一化画像特征。"""
    normalized_major = normalize_major(explicit_fields.get("major", ""))
    hard_skills, tool_skills = build_hard_and_tool_skills(
        skills=_safe_list(explicit_fields.get("skills")),
        tools=_safe_list(explicit_fields.get("tools")),
    )
    certificates = _dedup_keep_order(
        normalize_certificate(cert) for cert in _safe_list(explicit_fields.get("certificates"))
    )
    project_experience = [
        _safe_dict(item) for item in _safe_list(explicit_fields.get("project_experience"))
    ]
    internship_experience = [
        _safe_dict(item) for item in _safe_list(explicit_fields.get("internship_experience"))
    ]
    awards = _dedup_keep_order(_clean_text(item) for item in _safe_list(explicit_fields.get("awards")))

    experience_tags, project_keywords, internship_keywords = infer_experience_tags(
        project_experience=project_experience,
        internship_experience=internship_experience,
        awards=awards,
    )
    occupation_hints = infer_occupation_hints(
        target_job_intention=_clean_text(explicit_fields.get("target_job_intention", "")),
        skills=hard_skills + tool_skills,
        project_experience=project_experience,
        internship_experience=internship_experience,
        self_evaluation=_clean_text(explicit_fields.get("self_evaluation", "")),
    )
    domain_tags = infer_domain_tags(
        normalized_major=normalized_major,
        hard_skills=hard_skills,
        tool_skills=tool_skills,
        occupation_hints=occupation_hints,
        project_experience=project_experience,
        internship_experience=internship_experience,
    )

    practice_profile = build_practice_profile(
        project_experience=project_experience,
        internship_experience=internship_experience,
        awards=awards,
        target_job_intention=_clean_text(explicit_fields.get("target_job_intention", "")),
        project_keywords=project_keywords,
        internship_keywords=internship_keywords,
        experience_tags=experience_tags,
    )

    return {
        "major_std": normalized_major,
        "hard_skills": hard_skills,
        "tool_skills": tool_skills,
        "qualification_tags": build_qualification_tags(
            degree=_clean_text(explicit_fields.get("degree", "")),
            school=_clean_text(explicit_fields.get("school", "")),
            school_level=_clean_text(explicit_fields.get("school_level", "")),
            major=normalized_major,
            certificates=certificates,
        ),
        "experience_tags": experience_tags,
        "occupation_hints": occupation_hints,
        "domain_tags": domain_tags,
        "practice_profile": practice_profile,
    }


def build_profile_input_payload_from_state(
    student_state: Dict[str, Any],
) -> Dict[str, Any]:
    """从已加载的 student_state 构造 profile_input_payload。"""
    basic_info, resume_parse_result = extract_basic_and_resume(student_state)
    explicit_fields = collect_explicit_fields(basic_info, resume_parse_result)
    normalized_profile = build_normalized_profile(explicit_fields)

    payload = ProfileInputPayload(
        basic_info=deepcopy(basic_info),
        normalized_education={
            "degree": _clean_text(explicit_fields.get("degree", "")),
            "school": _clean_text(explicit_fields.get("school", "")),
            "school_level": _clean_text(explicit_fields.get("school_level", "")),
            "major_raw": _clean_text(explicit_fields.get("major", "")),
            "major_std": normalized_profile.get("major_std", ""),
            "graduation_year": _clean_text(basic_info.get("graduation_year", "")),
        },
        explicit_profile={
            "skills": deepcopy(explicit_fields.get("skills", [])),
            "tools": deepcopy(explicit_fields.get("tools", [])),
            "certificates": deepcopy(explicit_fields.get("certificates", [])),
            "project_experience": deepcopy(explicit_fields.get("project_experience", [])),
            "internship_experience": deepcopy(explicit_fields.get("internship_experience", [])),
            "awards": deepcopy(explicit_fields.get("awards", [])),
            "target_job_intention": _clean_text(explicit_fields.get("target_job_intention", "")),
            "self_evaluation": _clean_text(explicit_fields.get("self_evaluation", "")),
        },
        normalized_profile={
            "major_std": normalized_profile.get("major_std", ""),
            "hard_skills": deepcopy(normalized_profile.get("hard_skills", [])),
            "tool_skills": deepcopy(normalized_profile.get("tool_skills", [])),
            "qualification_tags": deepcopy(normalized_profile.get("qualification_tags", [])),
            "experience_tags": deepcopy(normalized_profile.get("experience_tags", [])),
            "occupation_hints": deepcopy(normalized_profile.get("occupation_hints", [])),
            "domain_tags": deepcopy(normalized_profile.get("domain_tags", [])),
        },
        practice_profile=deepcopy(normalized_profile.get("practice_profile", {})),
        evidence_summary={
            "project_count": len(_safe_list(explicit_fields.get("project_experience"))),
            "internship_count": len(_safe_list(explicit_fields.get("internship_experience"))),
            "award_count": len(_safe_list(explicit_fields.get("awards"))),
            "skill_count": len(_safe_list(explicit_fields.get("skills"))),
            "certificate_count": len(_safe_list(explicit_fields.get("certificates"))),
            "parse_warnings": deepcopy(_safe_list(explicit_fields.get("parse_warnings"))),
        },
        source_snapshot={
            "resume_parse_result": deepcopy(resume_parse_result),
        },
    )
    return asdict(payload)


def build_profile_input_payload(
    state_path: str | Path = DEFAULT_STATE_PATH,
    output_path: Optional[str | Path] = None,
) -> Dict[str, Any]:
    """
    主入口：读取 student_api_state.json 并输出 profile_input_payload。

    如果传入 output_path，则额外把 payload 保存成 JSON 文件，方便后续调试。
    """
    student_state = load_student_state(state_path)
    profile_input_payload = build_profile_input_payload_from_state(student_state)

    if output_path:
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with output_file.open("w", encoding="utf-8") as f:
            json.dump(profile_input_payload, f, ensure_ascii=False, indent=2)

    return profile_input_payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build student profile input payload from student_api_state.json"
    )
    parser.add_argument(
        "--state-path",
        default=str(DEFAULT_STATE_PATH),
        help="student_api_state.json 文件路径",
    )
    parser.add_argument(
        "--output",
        default="outputs/state/student_profile_input_payload.json",
        help="可选：输出 profile_input_payload JSON 路径",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    payload = build_profile_input_payload(
        state_path=args.state_path,
        output_path=args.output,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
