"""
resume_parser.py — 简历解析主流程

职责划分：
    【文件读取】根据后缀选择 txt / docx / pdf 的纯文本抽取策略。
    【文本清洗】去 HTML、统一换行与空白，便于模型稳定消费。
    【LLM 输入】组装 resume_text、file_meta、section_hints、output_requirements。
    【模型调用】经 llm_service.call_llm("resume_parse", ...) 走统一网关。
    【结果归一】validate_resume_parse_result 兼容嵌套与扁平两种 JSON，补默认字段。
    【状态持久化】StateManager 写入 student_api_state.json，并同步顶层 basic_info。

典型入口：
    - process_resume_file：完整流水线（读文件 → 解析 → 写状态）。
    - parse_resume_with_llm：仅「文本 + 可选 meta」→ 解析 dict，不写盘。

依赖：llm_interface_layer（call_llm、StateManager）、resume_schema（默认结构）。
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import unicodedata
import zipfile
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from xml.etree import ElementTree

from llm_interface_layer.llm_service import call_llm
from llm_interface_layer.state_manager import StateManager

try:
    from .resume_schema import default_resume_parse_result_dict
except ImportError:
    from resume_parse_module.resume_schema import default_resume_parse_result_dict


LOGGER = logging.getLogger(__name__)
# 与 load_resume_file 分支一一对应；新增格式需同步扩展抽取函数与 file_meta
SUPPORTED_SUFFIXES = {".txt", ".docx", ".pdf"}
DATE_RANGE_PATTERN = re.compile(
    r"(?P<start>\d{4}[./-]\d{1,2})\s*(?:~|-|—|–|至|到)\s*(?P<end>(?:\d{4}[./-]\d{1,2})|(?:\d{1,2}[./-]\d{1,2})|至今|现在)"
)
SECTION_KEYWORDS = {
    "education": ["教育经历", "教育背景", "学历背景", "学习经历"],
    "internship": ["实习经历", "工作经历", "实践经历", "校园经历"],
    "project": ["项目经历", "项目经验", "科研项目", "课程项目"],
    "skills": ["专业技能", "技能特长", "技能证书", "掌握技能", "语言能力", "计算机能力"],
    "awards": ["获奖情况", "荣誉奖项", "奖励荣誉"],
    "self_evaluation": ["自我评价", "个人总结", "个人优势"],
    "target_job": ["求职意向", "目标岗位", "应聘岗位", "目标职位", "意向岗位"],
}
DEGREE_KEYWORDS = ("博士研究生", "硕士研究生", "硕士", "本科", "大专", "专科", "中专")
COMPANY_SUFFIX_KEYWORDS = (
    "有限责任公司",
    "股份有限公司",
    "有限公司",
    "集团",
    "科技",
    "网络",
    "软件",
    "信息技术",
    "研究院",
    "工作室",
    "银行",
    "中心",
)
POSITION_TITLE_KEYWORDS = (
    "高级软件工程师",
    "软件研发工程师",
    "后端开发工程师",
    "前端开发工程师",
    "Java工程师",
    "Java开发工程师",
    "Python工程师",
    "数据分析师",
    "产品经理",
    "测试工程师",
    "开发工程师",
    "软件工程师",
    "前端工程师",
    "后端工程师",
    "算法工程师",
    "运维工程师",
    "实施工程师",
    "销售工程师",
    "运营专员",
    "运营助理",
    "产品助理",
    "技术助理",
    "研究助理",
    "工程师",
    "实习生",
    "助理",
    "专员",
)
SKILL_PATTERNS = [
    ("C++", re.compile(r"c\+\+", re.IGNORECASE)),
    ("Java", re.compile(r"java", re.IGNORECASE)),
    ("JavaScript", re.compile(r"javascript|\bjs\b", re.IGNORECASE)),
    ("Oracle", re.compile(r"oracle", re.IGNORECASE)),
    ("Mysql", re.compile(r"mysql", re.IGNORECASE)),
    ("SQL", re.compile(r"(?:\bsql\b|sql语句|sql语言)", re.IGNORECASE)),
    ("eclipse", re.compile(r"eclipse", re.IGNORECASE)),
    ("Linux", re.compile(r"linux", re.IGNORECASE)),
    ("Netty", re.compile(r"netty", re.IGNORECASE)),
    ("Dubbo", re.compile(r"dubbo", re.IGNORECASE)),
    ("Jquery", re.compile(r"jquery", re.IGNORECASE)),
    ("Bootstrap", re.compile(r"bootstrap", re.IGNORECASE)),
    ("SSH", re.compile(r"ssh", re.IGNORECASE)),
    ("C", re.compile(r"(?:\bc\b|c语言)", re.IGNORECASE)),
]
CERTIFICATE_PATTERNS = [
    ("大学英语六级", re.compile(r"(?:大学英语六级|英语六级|cet-?6)", re.IGNORECASE)),
    ("普通话二级甲等", re.compile(r"普通话二级甲等", re.IGNORECASE)),
    ("C1驾照", re.compile(r"(?:c1驾照|c1驾驶证)", re.IGNORECASE)),
]
AWARD_HINT_KEYWORDS = ("奖学金", "励志奖", "优秀", "一等奖", "二等奖", "三等奖", "国家励志")
DESCRIPTION_LABELS = (
    "开发环境：",
    "项目描述：",
    "项目介绍：",
    "责任描述：",
    "职责描述：",
    "语言能力：",
    "计算机能力：",
    "其他能力：",
    "毕业设计",
)

DEGREE_DURATION_YEARS = {
    "本科": 4,
    "硕士": 3,
    "博士": 4,
    "专科": 3,
}

PROJECT_SECTION_HEADING_SET = {
    "项目经历",
    "项目经验",
    "科研项目",
    "课程项目",
}

RADICAL_NORMALIZATION_MAP = {
    "\u2ee9": "黄",
}

# ---------------------------------------------------------------------------
# 日志
# ---------------------------------------------------------------------------


def setup_logging() -> None:
    """初始化简易日志配置。"""
    if LOGGER.handlers:
        return
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
    )


# ---------------------------------------------------------------------------
# 多格式简历 → 纯文本
# ---------------------------------------------------------------------------


def extract_text_from_txt(file_path: str | Path) -> str:
    """
    读取 txt 简历文本。

    依次尝试 utf-8-sig（带 BOM）、utf-8、gbk，兼容常见中文 Windows 导出文件；
    若均失败则 utf-8 忽略非法字节，避免整文件读崩。
    """
    path = Path(file_path)
    for encoding in ("utf-8-sig", "utf-8", "gbk"):
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
    return path.read_text(encoding="utf-8", errors="ignore")


def extract_text_from_docx(file_path: str | Path) -> str:
    """
    从 docx 提取文本。

    这里直接用 zip + XML 解析 document.xml，避免强依赖 python-docx。
    """
    path = Path(file_path)
    texts: List[str] = []
    namespace = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}

    with zipfile.ZipFile(path) as docx_zip:
        xml_bytes = docx_zip.read("word/document.xml")
    root = ElementTree.fromstring(xml_bytes)

    # WordprocessingML：w:p 为段落，w:t 为文本 run；跨 run 拼接还原一行
    for paragraph in root.findall(".//w:p", namespace):
        run_texts = [
            node.text
            for node in paragraph.findall(".//w:t", namespace)
            if node.text
        ]
        paragraph_text = "".join(run_texts).strip()
        if paragraph_text:
            texts.append(paragraph_text)

    return "\n".join(_dedup_nearby_lines(texts))


def _load_pdf_reader(file_path: str | Path):
    """优先使用 pypdf，其次兼容 PyPDF2。"""
    try:
        from pypdf import PdfReader

        return PdfReader(str(file_path))
    except ImportError:
        try:
            from PyPDF2 import PdfReader

            return PdfReader(str(file_path))
        except ImportError as exc:
            raise RuntimeError(
                "PDF 文本提取需要安装 pypdf 或 PyPDF2，例如：pip install pypdf"
            ) from exc


def extract_text_from_pdf(file_path: str | Path) -> str:
    """
    从 PDF 简历提取文本。

    如果是扫描件 PDF，这里可能提取不到正文，OCR 逻辑先只预留在 parse_warnings 中提示。
    """
    reader = _load_pdf_reader(file_path)
    page_texts: List[str] = []
    for page in reader.pages:
        page_text = page.extract_text() or ""
        if page_text.strip():
            page_texts.append(page_text)
    return "\n".join(page_texts)


def load_resume_file(file_path: str | Path) -> Tuple[str, Dict[str, Any]]:
    """
    读取简历文件并返回：
    - resume_text
    - file_meta
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Resume file not found: {path}")

    suffix = path.suffix.lower()
    if suffix not in SUPPORTED_SUFFIXES:
        raise ValueError(
            f"Unsupported resume file type: {suffix}. Supported types: {sorted(SUPPORTED_SUFFIXES)}"
        )

    if suffix == ".txt":
        resume_text = extract_text_from_txt(path)
        extraction_method = "txt_reader"
    elif suffix == ".docx":
        resume_text = extract_text_from_docx(path)
        extraction_method = "docx_xml_reader"
    else:
        resume_text = extract_text_from_pdf(path)
        extraction_method = "pdf_text_reader"

    file_meta = {
        "file_name": path.name,
        "file_path": str(path.resolve()),
        "file_suffix": suffix,
        "file_size": path.stat().st_size,
        "extraction_method": extraction_method,
        "text_length": len(resume_text or ""),
        "maybe_scanned_pdf": suffix == ".pdf" and len((resume_text or "").strip()) < 30,
    }
    return resume_text, file_meta


def clean_resume_text(text: str) -> str:
    """
    清理简历文本：HTML 标签、不间断空格、换行归一、连续空行压缩。

    目的：减少噪声 token，避免模型被 HTML/异常空白干扰。
    """
    if text is None:
        return ""

    cleaned = str(text)
    for source_char, target_char in RADICAL_NORMALIZATION_MAP.items():
        cleaned = cleaned.replace(source_char, target_char)
    # 归一化兼容字形（如“项⽬”“⾄今”）并清理控制字符，避免规则匹配失败。
    cleaned = unicodedata.normalize("NFKC", cleaned)
    cleaned = cleaned.replace("\x00", "")
    cleaned = re.sub(r"[\x01-\x08\x0b-\x1f\x7f]", " ", cleaned)
    # NBSP、全角空格 → 普通空格
    cleaned = cleaned.replace("\u00a0", " ").replace("\u3000", " ")
    cleaned = cleaned.replace("\r\n", "\n").replace("\r", "\n")
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    cleaned = re.sub(r"[ \t\f\v]+", " ", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    cleaned = "\n".join(line.strip() for line in cleaned.splitlines())
    return cleaned.strip()


def _normalize_compare_text(text: Any) -> str:
    normalized = str(text or "")
    normalized = normalized.replace("\u00a0", " ").replace("\u3000", " ")
    normalized = re.sub(r"\s+", "", normalized)
    normalized = re.sub(r"[：:，,；;。、“”\"'·\-_~—–|/\\()（）\[\]【】]+", "", normalized)
    return normalized.lower()


def _dedup_nearby_lines(lines: Iterable[str]) -> List[str]:
    """去掉相邻或近邻重复段落，降低 docx 复杂排版导致的重复噪声。"""
    result: List[str] = []
    recent_keys: List[str] = []
    for line in lines:
        text = str(line or "").strip()
        if not text:
            continue
        key = _normalize_compare_text(text)
        if not key:
            continue
        if key in recent_keys[-4:]:
            continue
        result.append(text)
        recent_keys.append(key)
    return result


def _dedup_keep_order(values: Iterable[str]) -> List[str]:
    seen = set()
    result = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _dedup_object_list(
    rows: Iterable[Dict[str, Any]],
    key_fields: Tuple[str, ...],
) -> List[Dict[str, Any]]:
    seen = set()
    result = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = tuple(str(row.get(field, "") or "").strip() for field in key_fields)
        if not any(key):
            key = (str(row.get("description", "") or "").strip(),)
        if not any(key) or key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result


def _match_section_heading(line: str) -> str:
    compact = _normalize_compare_text(line)
    if not compact:
        return ""
    for section_name, keywords in SECTION_KEYWORDS.items():
        for keyword in keywords:
            keyword_compact = _normalize_compare_text(keyword)
            if compact == keyword_compact or compact == keyword_compact * 2:
                return section_name
            if compact.startswith(keyword_compact) and len(compact) <= len(keyword_compact) * 3:
                return section_name
    return ""


def _match_section_heading_with_content(line: str) -> Tuple[str, str]:
    stripped = str(line or "").strip()
    matched_section = _match_section_heading(stripped)
    if matched_section:
        return matched_section, ""

    for section_name, keywords in SECTION_KEYWORDS.items():
        for keyword in keywords:
            pattern = rf"^\s*{re.escape(keyword)}\s*[:：]?\s*(.*)$"
            match = re.match(pattern, stripped)
            if match:
                return section_name, match.group(1).strip()
    return "", ""


def _extract_resume_sections(resume_text: str) -> Dict[str, List[str]]:
    sections: Dict[str, List[str]] = {"header": []}
    current_section = "header"
    for raw_line in resume_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        matched_section, remaining_text = _match_section_heading_with_content(line)
        if matched_section:
            current_section = matched_section
            sections.setdefault(current_section, [])
            if remaining_text:
                sections[current_section].append(remaining_text)
            continue
        sections.setdefault(current_section, []).append(line)

    return {
        section_name: _dedup_nearby_lines(section_lines)
        for section_name, section_lines in sections.items()
    }


def _extract_phone(text: str) -> str:
    match = re.search(r"1[3-9]\d[- ]?\d{4}[- ]?\d{4}", text)
    return match.group(0).replace(" ", "") if match else ""


def _extract_email(text: str) -> str:
    match = re.search(r"[A-Za-z0-9_.+-]+@[A-Za-z0-9-]+\.[A-Za-z0-9-.]+", text)
    return match.group(0) if match else ""


def _extract_gender(text: str) -> str:
    match = re.search(r"(?:性别[:：]?\s*)(男|女)", text)
    return match.group(1) if match else ""


def _extract_name(header_lines: List[str]) -> str:
    for line in header_lines[:8]:
        candidate = line.strip()
        if not candidate:
            continue
        if any(keyword in candidate for keywords in SECTION_KEYWORDS.values() for keyword in keywords):
            continue
        if re.search(r"\d|@|大学|学院|公司|项目|经历", candidate):
            continue
        normalized_candidate = candidate
        for source_char, target_char in RADICAL_NORMALIZATION_MAP.items():
            normalized_candidate = normalized_candidate.replace(source_char, target_char)
        normalized_candidate = re.sub(r"[\u2e80-\u2eff\u2f00-\u2fdf]", "", normalized_candidate)
        if re.fullmatch(r"[\u3400-\u9fff]{2,4}", normalized_candidate):
            return normalized_candidate
    return ""


def _extract_target_job_by_rule(section_lines: List[str], resume_text: str) -> str:
    search_text = "\n".join(section_lines) + "\n" + resume_text
    match = re.search(
        r"(?:目标岗位|求职意向|应聘岗位|目标职位|意向岗位)\s*[:：]?\s*([^\n\r]{2,40})",
        search_text,
    )
    if not match:
        return ""
    candidate = match.group(1).strip()
    candidate = re.split(
        r"(?:语言能力|计算机能力|其他能力|教育背景|工作经历|实习经历|项目经历|技能证书|专业技能|自我评价|获奖情况|校园经历|个人总结|个人优势|$)",
        candidate,
        maxsplit=1,
    )[0].strip()
    candidate = re.split(r"[，,；;。\n]", candidate)[0].strip()
    return candidate


def _extract_date_range(text: str) -> Tuple[str, str]:
    match = DATE_RANGE_PATTERN.search(text)
    if not match:
        return "", ""
    start_date = match.group("start")
    end_date = match.group("end")
    if re.fullmatch(r"\d{1,2}[./-]\d{1,2}", end_date):
        end_date = f"{start_date[:4]}.{end_date.replace('-', '.')}"
    return start_date.replace("-", "."), end_date.replace("-", ".")


def _remove_date_range_prefix(text: str) -> str:
    return DATE_RANGE_PATTERN.sub("", text, count=1).strip(" ：:，,；;")


def _split_lines_by_date_ranges(section_lines: List[str]) -> List[str]:
    normalized_lines: List[str] = []
    for raw_line in section_lines:
        line = str(raw_line or "").strip()
        if not line:
            continue
        matches = list(DATE_RANGE_PATTERN.finditer(line))
        if len(matches) <= 1:
            normalized_lines.append(line)
            continue
        for index, match in enumerate(matches):
            chunk_start = match.start()
            chunk_end = matches[index + 1].start() if index + 1 < len(matches) else len(line)
            chunk = line[chunk_start:chunk_end].strip(" ，,；;")
            if chunk:
                normalized_lines.append(chunk)
    return _dedup_nearby_lines(normalized_lines)


def _infer_degree_from_text(text: str) -> str:
    normalized = str(text or "")
    if not normalized:
        return ""
    if re.search(r"(?:博士|博[一二三四]|博士后)", normalized):
        return "博士"
    if re.search(r"(?:硕士|研究生|研[一二三])", normalized):
        return "硕士"
    if re.search(r"(?:本科|学士|大[一二三四五])", normalized):
        return "本科"
    if re.search(r"(?:专科|大专)", normalized):
        return "专科"
    return ""


def _infer_graduation_year_from_education(education_experience: List[Dict[str, str]]) -> str:
    for item in education_experience:
        end_date = str(item.get("end_date") or "").strip()
        match = re.match(r"^(\d{4})", end_date)
        if match:
            return match.group(1)

    if not education_experience:
        return ""

    first_item = education_experience[0]
    start_date = str(first_item.get("start_date") or "").strip()
    start_match = re.match(r"^(\d{4})[./-]\d{1,2}$", start_date)
    if not start_match:
        return ""

    start_year = int(start_match.group(1))
    degree = str(first_item.get("degree") or "").strip()
    for degree_name, duration in DEGREE_DURATION_YEARS.items():
        if degree_name in degree:
            return str(start_year + duration)
    return ""


def _parse_school_major_degree(text: str) -> Tuple[str, str, str, str]:
    body = str(text or "").strip()
    school = ""
    major = ""
    degree = ""
    description = ""

    school_match = re.search(r"(.+?(?:大学|学院|学校))", body)
    if school_match:
        school = school_match.group(1).strip()
        body = body[school_match.end():].strip()

    body = re.sub(r"^[（(\[]?\s*(?:985|211|双一流|一本|二本)\s*[）)\]]?", "", body).strip()

    after_degree = ""
    for degree_keyword in DEGREE_KEYWORDS:
        if degree_keyword in body:
            degree = degree_keyword
            degree_pattern = re.escape(degree_keyword) + r"(?:学位)?"
            split_parts = re.split(degree_pattern, body, maxsplit=1)
            before_degree = split_parts[0]
            after_degree = split_parts[1] if len(split_parts) > 1 else ""
            body = before_degree
            break

    if not degree:
        degree = _infer_degree_from_text(f"{text} {body}")

    body = re.sub(r"[，,；;。]+", " ", body)
    body = re.sub(r"\s+", " ", body).strip()
    split_match = re.search(
        r"(毕业设计|项目经历|工作经历|实习经历|技能证书|语言能力|计算机能力|其他能力|奖学金|获奖|优秀等级)",
        body,
    )
    if split_match:
        major = body[: split_match.start()].strip()
        description = body[split_match.start():].strip()
    else:
        major = body

    major = re.sub(r"^[（(\[]?\s*(?:985|211|双一流|一本|二本)\s*[）)\]]?", "", major).strip()
    major = re.sub(r"(?:本科|硕士|博士)?在读$", "", major).strip()
    major = re.sub(r"(?:大[一二三四五六七八九十]|研[一二三]|博[一二三四])$", "", major).strip()
    major = major.strip(" ，,；;。")

    after_degree = re.sub(r"[，,；;。]+", " ", after_degree)
    after_degree = re.sub(r"\s+", " ", after_degree).strip()
    description = f"{description} {after_degree}".strip()
    return school, major, degree, description


def _append_description(current: Dict[str, Any], line: str) -> None:
    if not line or not isinstance(current, dict):
        return
    existing = _clean_description_text(current.get("description", ""))
    incoming = _clean_description_text(line)
    if not incoming or incoming in existing:
        return
    current["description"] = _clean_description_text(f"{existing} {incoming}".strip())


def _clean_description_text(text: Any) -> str:
    description = str(text or "").strip()
    if not description:
        return ""
    description = description.replace("\u00a0", " ").replace("\u3000", " ")
    description = re.sub(r"\s+", " ", description)
    for label in DESCRIPTION_LABELS:
        description = re.sub(
            rf"(?<!^)\s*{re.escape(label)}",
            f" {label}",
            description,
        )
    description = re.sub(r"([；;。])(?=[^\s])", r"\1 ", description)
    description = re.sub(r"\s+", " ", description)
    return description.strip(" ，,；;")


def _parse_education_entries(section_lines: List[str]) -> List[Dict[str, str]]:
    entries: List[Dict[str, str]] = []
    current: Optional[Dict[str, str]] = None

    for line in _split_lines_by_date_ranges(section_lines):
        start_date, end_date = _extract_date_range(line)
        line_has_school = any(token in line for token in ("大学", "学院", "学校"))
        if start_date and (line_has_school or any(keyword in line for keyword in DEGREE_KEYWORDS)):
            if current:
                entries.append(current)
            school, major, degree, inline_description = _parse_school_major_degree(_remove_date_range_prefix(line))
            current = {
                "school": school,
                "major": major,
                "degree": degree,
                "start_date": start_date,
                "end_date": end_date,
                "description": _clean_description_text(inline_description),
            }
            continue
        if current:
            _append_description(current, line)

    if current:
        entries.append(current)

    return _dedup_object_list(entries, ("school", "major", "start_date", "end_date"))


def _split_company_and_position(text: str) -> Tuple[str, str, str]:
    body = str(text or "").strip()
    for suffix in COMPANY_SUFFIX_KEYWORDS:
        index = body.find(suffix)
        if index != -1:
            company = body[: index + len(suffix)].strip()
            remaining = body[index + len(suffix):].strip(" ：:，,；;")
            for keyword in POSITION_TITLE_KEYWORDS:
                keyword_index = remaining.find(keyword)
                if keyword_index != -1:
                    position = remaining[: keyword_index + len(keyword)].strip(" ：:，,；;")
                    description = remaining[keyword_index + len(keyword):].strip(" ：:，,；;")
                    return company, position, description
            return company, remaining, ""

    parts = body.split()
    if len(parts) >= 2:
        return parts[0].strip(), " ".join(parts[1:]).strip(), ""
    return "", body, ""


def _parse_internship_entries(section_lines: List[str]) -> List[Dict[str, str]]:
    entries: List[Dict[str, str]] = []
    current: Optional[Dict[str, str]] = None

    for line in _split_lines_by_date_ranges(section_lines):
        start_date, end_date = _extract_date_range(line)
        if start_date:
            if current:
                entries.append(current)
            body = _remove_date_range_prefix(line)
            company_name, position, inline_description = _split_company_and_position(body)
            current = {
                "company_name": company_name,
                "position": position,
                "start_date": start_date,
                "end_date": end_date,
                "description": _clean_description_text(inline_description),
            }
            continue
        if current:
            _append_description(current, line)

    if current:
        entries.append(current)

    filtered_entries = [
        item
        for item in entries
        if item.get("company_name") or item.get("position") or item.get("description")
    ]
    return _dedup_object_list(
        filtered_entries,
        ("company_name", "position", "start_date", "end_date"),
    )


def _normalize_project_name(text: str) -> str:
    name = _clean_description_text(text)
    if not name:
        return ""
    name = re.sub(r"https?://\S+", "", name, flags=re.IGNORECASE).strip()
    name = re.split(
        r"(?:开发环境|项目描述|项目介绍|责任描述|职责描述|技术栈)[:：]",
        name,
        maxsplit=1,
    )[0].strip()
    name = re.sub(r"[\-—–~]+", " ", name)
    name = re.sub(r"\s+", " ", name).strip(" ：:，,；;。")
    return name


def _extract_project_role(line: str) -> str:
    role_match = re.search(r"(?:角色|岗位|担任)[:：]\s*([^\n，,；;。]{2,30})", line)
    if role_match:
        return role_match.group(1).strip()

    line_without_url = re.sub(r"https?://\S+", "", _clean_description_text(line), flags=re.IGNORECASE).strip()
    for keyword in POSITION_TITLE_KEYWORDS:
        if keyword in line_without_url:
            return line_without_url

    if re.fullmatch(r".{2,24}(?:工程师|实习生|助理|专员|经理)", line_without_url):
        return line_without_url
    return ""


def _looks_like_project_title(line: str) -> bool:
    cleaned = _clean_description_text(line)
    if not cleaned:
        return False
    if cleaned in PROJECT_SECTION_HEADING_SET:
        return False
    if len(cleaned) > 48:
        return False
    if any(punc in cleaned for punc in ("，", ",", "。", ";", "；")):
        return False
    if re.search(r"(?:项目描述|项目介绍|职责描述|责任描述|开发环境|技术栈)[:：]", cleaned):
        return False
    if cleaned.startswith(("项目描述", "项目介绍", "职责描述", "责任描述", "技术栈", "开发环境")):
        return False
    if re.match(r"^(?:https?://|www\.)", cleaned, flags=re.IGNORECASE):
        return False

    if not re.search(r"(?:~|至今|现在|20\d{2}|项目|平台|系统|智能体|小程序|App|APP)", cleaned):
        return False

    role_like = any(keyword in cleaned for keyword in POSITION_TITLE_KEYWORDS)
    if role_like and (
        "http" in cleaned.lower()
        or re.fullmatch(r".{2,24}(?:工程师|实习生|助理|专员|经理)", cleaned)
    ):
        return False
    return True


def _parse_project_entries(section_lines: List[str]) -> List[Dict[str, str]]:
    entries: List[Dict[str, str]] = []
    current: Optional[Dict[str, str]] = None

    for line in _split_lines_by_date_ranges(section_lines):
        start_date, end_date = _extract_date_range(line)
        if start_date:
            if current:
                entries.append(current)
            body = _remove_date_range_prefix(line)
            project_name = _normalize_project_name(body)
            description = ""
            split_index = re.search(r"(?:开发环境|项目描述|项目介绍|责任描述|职责描述|技术栈)[:：]", body)
            if split_index:
                description = body[split_index.start():].strip(" ：:，,；;")
            current = {
                "project_name": project_name.strip(),
                "role": "",
                "start_date": start_date,
                "end_date": end_date,
                "description": _clean_description_text(description),
            }
            continue

        if _looks_like_project_title(line):
            fallback_project_name = _normalize_project_name(line)
            has_period_hint = bool(re.search(r"(?:~|至今|现在|20\d{2}|\d{4}[./-]\d{1,2})", line))
            if fallback_project_name and (not current or has_period_hint):
                if current and (current.get("project_name") or current.get("description")):
                    entries.append(current)
                current = {
                    "project_name": fallback_project_name,
                    "role": "",
                    "start_date": "",
                    "end_date": "",
                    "description": "",
                }
                continue

        if current:
            if not current.get("role"):
                role = _extract_project_role(line)
                if role:
                    current["role"] = role
                    remaining = _clean_description_text(
                        re.sub(r"https?://\S+", "", line, flags=re.IGNORECASE)
                    )
                    if remaining and remaining != role:
                        _append_description(current, remaining)
                    continue
            _append_description(current, line)

    if current:
        entries.append(current)

    filtered_entries = [
        item
        for item in entries
        if item.get("project_name") or item.get("description")
    ]
    return _dedup_object_list(filtered_entries, ("project_name", "start_date", "end_date"))


def _extract_skills_by_rule(section_lines: List[str], resume_text: str) -> List[str]:
    explicit_text = "\n".join(section_lines).strip()
    explicit_matches = []
    if explicit_text:
        for skill_name, pattern in SKILL_PATTERNS:
            if pattern.search(explicit_text):
                explicit_matches.append(skill_name)
    if explicit_matches:
        return _dedup_keep_order(explicit_matches)

    fallback_text = resume_text
    fallback_matches = []
    for skill_name, pattern in SKILL_PATTERNS:
        if pattern.search(fallback_text):
            fallback_matches.append(skill_name)
    return _dedup_keep_order(fallback_matches)


def _extract_certificates_by_rule(section_lines: List[str], resume_text: str) -> List[str]:
    text = "\n".join(section_lines) if section_lines else resume_text
    extracted = []
    for cert_name, pattern in CERTIFICATE_PATTERNS:
        if pattern.search(text):
            extracted.append(cert_name)
    return _dedup_keep_order(extracted)


def _extract_awards_by_rule(section_lines: List[str], resume_text: str) -> List[str]:
    text = "\n".join(section_lines) if section_lines else resume_text
    extracted = []
    for fragment in re.split(r"[。；;\n]", text):
        candidate = fragment.strip(" ，,")
        if candidate and any(keyword in candidate for keyword in AWARD_HINT_KEYWORDS):
            extracted.append(candidate)
    return _dedup_keep_order(extracted)


def _build_rule_resume_parse_result(resume_text: str) -> Tuple[Dict[str, Any], Dict[str, List[str]]]:
    sections = _extract_resume_sections(resume_text)
    all_lines = _dedup_nearby_lines(resume_text.splitlines())
    header_lines = sections.get("header", [])
    education_lines = sections.get("education", [])
    internship_lines = sections.get("internship", [])
    project_lines = sections.get("project", [])
    skill_lines = sections.get("skills", [])
    award_lines = sections.get("awards", [])
    self_eval_lines = sections.get("self_evaluation", [])
    target_job_lines = sections.get("target_job", [])

    education_candidate_lines = _dedup_nearby_lines(
        education_lines
        + [
            line
            for line in all_lines
            if DATE_RANGE_PATTERN.search(line)
            and (
                any(token in line for token in ("大学", "学院", "学校"))
                or any(keyword in line for keyword in DEGREE_KEYWORDS)
                or bool(re.search(r"(?:大[一二三四五]|研[一二三]|博[一二三四])", line))
            )
        ]
    )
    education_experience = _parse_education_entries(education_candidate_lines)
    internship_experience = _parse_internship_entries(internship_lines)
    project_experience = _parse_project_entries(project_lines)
    target_job_intention = _extract_target_job_by_rule(target_job_lines, resume_text)
    skills = _extract_skills_by_rule(skill_lines, resume_text)
    certificates = _extract_certificates_by_rule(skill_lines, resume_text)
    awards = _extract_awards_by_rule(award_lines + education_lines, resume_text)

    fallback_school = education_experience[0]["school"] if education_experience else ""
    fallback_major = education_experience[0]["major"] if education_experience else ""
    fallback_degree = education_experience[0]["degree"] if education_experience else ""
    if not fallback_degree:
        fallback_degree = _infer_degree_from_text(resume_text)
    fallback_graduation_year = _infer_graduation_year_from_education(education_experience)

    rule_result = default_resume_parse_result_dict()
    rule_result["basic_info"] = {
        "name": _extract_name(header_lines),
        "gender": _extract_gender(resume_text),
        "phone": _extract_phone(resume_text),
        "email": _extract_email(resume_text),
        "school": fallback_school,
        "major": fallback_major,
        "degree": fallback_degree,
        "graduation_year": fallback_graduation_year,
    }
    rule_result["education_experience"] = education_experience
    rule_result["internship_experience"] = internship_experience
    rule_result["project_experience"] = project_experience
    rule_result["skills"] = skills
    rule_result["certificates"] = certificates
    rule_result["awards"] = awards
    rule_result["self_evaluation"] = " ".join(self_eval_lines[:8]).strip()
    rule_result["target_job_intention"] = target_job_intention
    rule_result["raw_resume_text"] = resume_text

    parse_warnings = []
    if education_lines and not education_experience:
        parse_warnings.append("教育背景正文存在，但未解析到结构化教育经历")
    if internship_lines and not internship_experience:
        parse_warnings.append("工作/实习经历正文存在，但未解析到结构化实习经历")
    if project_lines and not project_experience:
        parse_warnings.append("项目经历正文存在，但未解析到结构化项目经历")
    if target_job_lines and not target_job_intention:
        parse_warnings.append("目标岗位正文存在，但未解析到明确目标岗位")
    if skill_lines and not skills:
        parse_warnings.append("技能相关正文存在，但未解析到稳定技能列表")
    rule_result["parse_warnings"] = _dedup_keep_order(parse_warnings)
    return rule_result, sections


def _merge_basic_info(
    llm_basic_info: Dict[str, Any],
    rule_basic_info: Dict[str, Any],
) -> Dict[str, str]:
    merged = {}
    for field_name in default_resume_parse_result_dict()["basic_info"].keys():
        llm_value = _ensure_str(llm_basic_info.get(field_name, ""))
        rule_value = _ensure_str(rule_basic_info.get(field_name, ""))
        merged[field_name] = llm_value or rule_value
    return merged


def _merge_object_rows(
    rule_rows: List[Dict[str, Any]],
    llm_rows: List[Dict[str, Any]],
    key_fields: Tuple[str, ...],
    description_priority: str = "llm",
) -> List[Dict[str, Any]]:
    if not rule_rows:
        return _dedup_object_list(llm_rows, key_fields)
    if not llm_rows:
        return _dedup_object_list(rule_rows, key_fields)

    remaining_llm_rows = []
    llm_index = {}
    for row in llm_rows:
        key = tuple(_ensure_str(row.get(field, "")) for field in key_fields)
        llm_index.setdefault(key, row)
        remaining_llm_rows.append(row)

    merged_rows = []
    for rule_row in rule_rows:
        key = tuple(_ensure_str(rule_row.get(field, "")) for field in key_fields)
        llm_row = llm_index.get(key, {})
        merged_row = {}
        for field_name in rule_row.keys():
            llm_value = _ensure_str(llm_row.get(field_name, ""))
            rule_value = _ensure_str(rule_row.get(field_name, ""))
            if field_name == "description":
                merged_row[field_name] = llm_value if description_priority == "llm" and llm_value else rule_value or llm_value
            else:
                merged_row[field_name] = rule_value or llm_value
        merged_rows.append(merged_row)

    existing_keys = {
        tuple(_ensure_str(row.get(field, "")) for field in key_fields)
        for row in merged_rows
    }
    for llm_row in llm_rows:
        llm_key = tuple(_ensure_str(llm_row.get(field, "")) for field in key_fields)
        if llm_key not in existing_keys:
            merged_rows.append(llm_row)

    return _dedup_object_list(merged_rows, key_fields)


def _merge_resume_parse_results(
    llm_result: Dict[str, Any],
    rule_result: Dict[str, Any],
    rule_sections: Dict[str, List[str]],
    raw_resume_text: str,
) -> Dict[str, Any]:
    normalized_llm = validate_resume_parse_result(llm_result, raw_resume_text=raw_resume_text)
    normalized_rule = validate_resume_parse_result(rule_result, raw_resume_text=raw_resume_text)

    merged = default_resume_parse_result_dict()
    merged["basic_info"] = _merge_basic_info(
        normalized_llm.get("basic_info", {}),
        normalized_rule.get("basic_info", {}),
    )
    merged["education_experience"] = _merge_object_rows(
        normalized_rule.get("education_experience", []),
        normalized_llm.get("education_experience", []),
        ("school", "major", "start_date", "end_date"),
    )
    merged["internship_experience"] = _merge_object_rows(
        normalized_rule.get("internship_experience", []),
        normalized_llm.get("internship_experience", []),
        ("company_name", "position", "start_date", "end_date"),
    )
    merged["project_experience"] = _merge_object_rows(
        normalized_rule.get("project_experience", []),
        normalized_llm.get("project_experience", []),
        ("project_name", "start_date", "end_date"),
    )
    merged["skills"] = _dedup_keep_order(
        normalized_rule.get("skills", []) + normalized_llm.get("skills", [])
    )
    merged["certificates"] = _dedup_keep_order(
        normalized_rule.get("certificates", []) + normalized_llm.get("certificates", [])
    )
    merged["awards"] = _dedup_keep_order(
        normalized_rule.get("awards", []) + normalized_llm.get("awards", [])
    )
    merged["self_evaluation"] = _ensure_str(
        normalized_llm.get("self_evaluation") or normalized_rule.get("self_evaluation")
    )
    merged["target_job_intention"] = _ensure_str(
        normalized_rule.get("target_job_intention")
        or normalized_llm.get("target_job_intention")
    )
    merged["raw_resume_text"] = _ensure_str(raw_resume_text)
    merged["parse_warnings"] = _dedup_keep_order(
        normalized_llm.get("parse_warnings", [])
        + normalized_rule.get("parse_warnings", [])
    )

    if rule_sections.get("education") and not merged["education_experience"]:
        merged["parse_warnings"].append("教育背景正文存在，但最终结构化教育经历仍为空")
    if rule_sections.get("internship") and not merged["internship_experience"]:
        merged["parse_warnings"].append("工作/实习经历正文存在，但最终结构化实习经历仍为空")
    if rule_sections.get("project") and not merged["project_experience"]:
        merged["parse_warnings"].append("项目经历正文存在，但最终结构化项目经历仍为空")
    if rule_sections.get("target_job") and not merged["target_job_intention"]:
        merged["parse_warnings"].append("目标岗位正文存在，但最终目标岗位字段仍为空")
    if rule_sections.get("skills") and not merged["skills"]:
        merged["parse_warnings"].append("技能相关正文存在，但最终技能列表仍为空")

    return validate_resume_parse_result(merged, raw_resume_text=raw_resume_text)


# ---------------------------------------------------------------------------
# LLM 输入构造（section_hints + output_requirements）
# ---------------------------------------------------------------------------


def _extract_resume_section_hints(resume_text: str) -> Dict[str, str]:
    """
    轻量提取分块提示，不做复杂业务解析，只给 LLM 额外上下文。
    """
    hints: Dict[str, str] = {}
    lines = [line.strip() for line in resume_text.splitlines() if line.strip()]
    for section_name, keywords in SECTION_KEYWORDS.items():
        matched_line = next(
            (line for line in lines if any(keyword in line for keyword in keywords)),
            "",
        )
        if matched_line:
            hints[section_name] = matched_line
    return hints


def build_resume_parse_input(
    resume_text: str,
    file_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    构造 resume_parse 任务的统一输入 payload。

    resume_text：已清洗全文；file_meta：文件侧元信息（长度、是否疑似扫描 PDF 等）；
    section_hints：按关键词命中到的「章节标题行」，辅助模型对齐板块；
    output_requirements：明确期望的基础字段与列表字段名，利于结构化输出。
    """
    cleaned_text = clean_resume_text(resume_text)
    return {
        "resume_text": cleaned_text,
        "file_meta": deepcopy(file_meta or {}),
        "section_hints": _extract_resume_section_hints(cleaned_text),
        "output_requirements": {
            "basic_info": [
                "name",
                "gender",
                "phone",
                "email",
                "school",
                "major",
                "degree",
                "graduation_year",
            ],
            "list_fields": [
                "education_experience",
                "internship_experience",
                "project_experience",
                "skills",
                "certificates",
                "awards",
                "parse_warnings",
            ],
        },
    }


# ---------------------------------------------------------------------------
# 解析结果归一化（类型安全 + 扁平结构兼容）
# ---------------------------------------------------------------------------


def _ensure_dict(value: Any) -> Dict[str, Any]:
    """非 dict 时退回空 dict，避免 .get 链式调用报错。"""
    return value if isinstance(value, dict) else {}


def _ensure_str(value: Any) -> str:
    """标量转可展示字符串；dict/list 用 JSON 保留结构，其余 str() 后 strip。"""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    # 模型偶发返回嵌套对象当「描述」，序列化为字符串保留信息
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value).strip()


def _ensure_str_list(value: Any) -> List[str]:
    """统一为字符串列表：支持 list、空、单字符串（按常见分隔符拆分）、单标量。"""
    if isinstance(value, list):
        return [_ensure_str(item) for item in value if _ensure_str(item)]
    if value is None or value == "":
        return []
    if isinstance(value, str):
        # 模型用顿号/逗号/分号等拼成一条字符串时，拆成多条技能或奖项
        return [
            item.strip()
            for item in re.split(r"[、,，;；|/\n]+", value)
            if item.strip()
        ]
    return [_ensure_str(value)]


def _normalize_object_list(value: Any, template: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    将「经历」类列表统一为 dict 列表：每项键集与 template 一致，缺省填空串。

    列表元素若非 dict（如纯字符串），则写入 template 中的 description 语义字段。
    """
    if not isinstance(value, list):
        return []

    normalized_rows: List[Dict[str, Any]] = []
    for item in value:
        source = item if isinstance(item, dict) else {"description": _ensure_str(item)}
        normalized_rows.append(
            {
                field_name: _ensure_str(source.get(field_name, default_value))
                for field_name, default_value in template.items()
            }
        )
    return normalized_rows


def validate_resume_parse_result(
    result: Any,
    raw_resume_text: str = "",
) -> Dict[str, Any]:
    """
    校验和补全 resume_parse 输出。

    兼容两类返回：
    1. 你最终想要的嵌套结构；
    2. 当前 mock 接口层返回的扁平结构。
    """
    default_result = default_resume_parse_result_dict()
    source = _ensure_dict(result)

    basic_info_source = _ensure_dict(source.get("basic_info"))
    # 扁平返回：姓名电话等可能在顶层，合并进 basic_info
    for field_name in default_result["basic_info"]:
        if not basic_info_source.get(field_name):
            basic_info_source[field_name] = source.get(field_name, "")

    normalized = {
        "basic_info": {
            field_name: _ensure_str(basic_info_source.get(field_name, ""))
            for field_name in default_result["basic_info"]
        },
        "education_experience": _normalize_object_list(
            source.get("education_experience", []),
            {
                "school": "",
                "major": "",
                "degree": "",
                "start_date": "",
                "end_date": "",
                "description": "",
            },
        ),
        "internship_experience": _normalize_object_list(
            source.get("internship_experience", []),
            {
                "company_name": "",
                "position": "",
                "start_date": "",
                "end_date": "",
                "description": "",
            },
        ),
        "project_experience": _normalize_object_list(
            source.get("project_experience", []),
            {
                "project_name": "",
                "role": "",
                "start_date": "",
                "end_date": "",
                "description": "",
            },
        ),
        "skills": _ensure_str_list(source.get("skills", [])),
        "certificates": _ensure_str_list(source.get("certificates", [])),
        "awards": _ensure_str_list(source.get("awards", [])),
        "self_evaluation": _ensure_str(
            source.get("self_evaluation", source.get("raw_summary", ""))
        ),
        "target_job_intention": _ensure_str(source.get("target_job_intention", "")),
        "raw_resume_text": _ensure_str(source.get("raw_resume_text", raw_resume_text)),
        "parse_warnings": _ensure_str_list(source.get("parse_warnings", [])),
    }

    if not normalized["raw_resume_text"]:
        normalized["raw_resume_text"] = _ensure_str(raw_resume_text)

    # 业务级质量提示：不抛错，仅追加 parse_warnings 供上游展示
    if not normalized["basic_info"]["name"]:
        normalized["parse_warnings"].append("未解析到姓名字段")
    if not normalized["basic_info"]["phone"] and not normalized["basic_info"]["email"]:
        normalized["parse_warnings"].append("未解析到有效联系方式")
    if not normalized["education_experience"]:
        normalized["parse_warnings"].append("未解析到结构化教育经历")
    if not normalized["skills"]:
        normalized["parse_warnings"].append("未解析到技能列表")

    seen = set()
    dedup_warnings = []
    for warning in normalized["parse_warnings"]:
        warning = _ensure_str(warning)
        if warning and warning not in seen:
            seen.add(warning)
            dedup_warnings.append(warning)
    normalized["parse_warnings"] = dedup_warnings
    return normalized


# ---------------------------------------------------------------------------
# 对外 API：调模型、写状态、命令行入口
# ---------------------------------------------------------------------------


def parse_resume_with_llm(
    resume_text: str,
    file_meta: Optional[Dict[str, Any]] = None,
    student_state: Optional[Dict[str, Any]] = None,
    extra_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    通过统一大模型接口执行 resume_parse。

    student_state：可选，传入当前学生状态供网关做会话/追踪；
    extra_context：调用方扩展字段，会与内置 schema_hint 合并；
    异常时返回带 parse_warnings 的默认结构，不向外抛 LLM 异常。
    """
    cleaned_resume_text = clean_resume_text(resume_text)
    rule_result, rule_sections = _build_rule_resume_parse_result(cleaned_resume_text)
    input_data = build_resume_parse_input(cleaned_resume_text, file_meta=file_meta)

    merged_extra_context = {
        "resume_module": "resume_parse_module",
        "schema_hint": default_resume_parse_result_dict(),
        "rule_extracted_facts": {
            "basic_info": deepcopy(rule_result.get("basic_info", {})),
            "target_job_intention": rule_result.get("target_job_intention", ""),
            "education_experience_count": len(rule_result.get("education_experience", [])),
            "internship_experience_count": len(rule_result.get("internship_experience", [])),
            "project_experience_count": len(rule_result.get("project_experience", [])),
            "skills": deepcopy(rule_result.get("skills", [])),
        },
    }
    if extra_context:
        merged_extra_context.update(deepcopy(extra_context))

    try:
        raw_result = call_llm(
            "resume_parse",
            input_data,
            context_data=None,
            student_state=student_state,
            extra_context=merged_extra_context,
        )
    except Exception as exc:
        LOGGER.exception("call_llm('resume_parse', ...) failed")
        rule_result["parse_warnings"] = _dedup_keep_order(
            rule_result.get("parse_warnings", []) + [f"LLM 调用失败: {exc}"]
        )
        return validate_resume_parse_result(
            rule_result,
            raw_resume_text=cleaned_resume_text,
        )

    merged_result = _merge_resume_parse_results(
        llm_result=raw_result,
        rule_result=rule_result,
        rule_sections=rule_sections,
        raw_resume_text=cleaned_resume_text,
    )

    if file_meta and file_meta.get("maybe_scanned_pdf"):
        merged_result["parse_warnings"].append(
            "PDF 文本提取内容过少，疑似扫描件；OCR 接口已预留但当前未启用"
        )
    # 二次校验：合并扫描件警告后再次去重/补全
    return validate_resume_parse_result(
        merged_result,
        raw_resume_text=cleaned_resume_text,
    )


def update_student_state_with_resume_result(
    resume_parse_result: Dict[str, Any],
    state_path: Optional[str | Path] = None,
    student_state: Optional[Dict[str, Any]] = None,
    state_manager: Optional[StateManager] = None,
) -> Dict[str, Any]:
    """
    将简历解析结果写回 student_api_state.json。

    除了写 resume_parse_result，也同步刷新顶层 basic_info，方便后续链路直接读取。
    """
    manager = state_manager or StateManager()
    updated_state = manager.update_state(
        task_type="resume_parse",
        task_result=resume_parse_result,
        state_path=state_path,
        student_state=student_state,
    )
    updated_state["basic_info"] = deepcopy(resume_parse_result.get("basic_info", {}))
    manager.save_state(updated_state, state_path)
    return updated_state


def process_resume_file(
    file_path: str | Path,
    state_path: Optional[str | Path] = None,
    student_state: Optional[Dict[str, Any]] = None,
    extra_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    主流程：读本地简历 → extract + clean → parse_resume_with_llm → 更新 student_api_state.json。

    返回 dict 含 resume_parse_result、student_state（更新后）、file_meta，便于测试与编排。
    """
    setup_logging()
    LOGGER.info("Start loading resume file: %s", file_path)

    resume_text, file_meta = load_resume_file(file_path)
    LOGGER.info(
        "Resume text extracted. method=%s, text_length=%s",
        file_meta.get("extraction_method"),
        file_meta.get("text_length"),
    )

    resume_parse_result = parse_resume_with_llm(
        resume_text=resume_text,
        file_meta=file_meta,
        student_state=student_state,
        extra_context=extra_context,
    )
    LOGGER.info(
        "Resume parse finished. name=%s, skills=%s, warnings=%s",
        resume_parse_result.get("basic_info", {}).get("name", ""),
        len(resume_parse_result.get("skills", [])),
        len(resume_parse_result.get("parse_warnings", [])),
    )

    updated_state = update_student_state_with_resume_result(
        resume_parse_result=resume_parse_result,
        state_path=state_path,
        student_state=student_state,
    )
    LOGGER.info("student_api_state.json updated: %s", state_path or "default state path")

    return {
        "resume_parse_result": resume_parse_result,
        "student_state": updated_state,
        "file_meta": file_meta,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resume parse module demo")
    parser.add_argument(
        "--input",
        required=True,
        help="简历文件路径，支持 txt/docx/pdf",
    )
    parser.add_argument(
        "--state-path",
        default="student_api_state.json",
        help="student_api_state.json 输出路径",
    )
    return parser.parse_args()


if __name__ == "__main__":
    setup_logging()
    args = parse_args()
    result_bundle = process_resume_file(
        file_path=args.input,
        state_path=args.state_path,
    )
    print(json.dumps(result_bundle["resume_parse_result"], ensure_ascii=False, indent=2))




