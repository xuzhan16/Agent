"""
resume_parser.py — 简历解析主流程

职责划分：
    【文件读取】根据后缀选择 txt / docx / pdf 的纯文本抽取策略。
    【文本清洗】去 HTML、统一换行与空白，便于模型稳定消费。
    【LLM 输入】组装 resume_text、file_meta、section_hints、output_requirements。
    【模型调用】经 llm_service.call_llm("resume_parse", ...) 走统一网关。
    【结果归一】validate_resume_parse_result 兼容嵌套与扁平两种 JSON，补默认字段。
    【状态持久化】StateManager 写入 student.json，并同步顶层 basic_info。

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
import zipfile
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
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

    return "\n".join(texts)


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
    # NBSP、全角空格 → 普通空格
    cleaned = cleaned.replace("\u00a0", " ").replace("\u3000", " ")
    cleaned = cleaned.replace("\r\n", "\n").replace("\r", "\n")
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    cleaned = re.sub(r"[ \t\f\v]+", " ", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    cleaned = "\n".join(line.strip() for line in cleaned.splitlines())
    return cleaned.strip()


# ---------------------------------------------------------------------------
# LLM 输入构造（section_hints + output_requirements）
# ---------------------------------------------------------------------------


def _extract_resume_section_hints(resume_text: str) -> Dict[str, str]:
    """
    轻量提取分块提示，不做复杂业务解析，只给 LLM 额外上下文。
    """
    section_keywords = {
        "education": ["教育经历", "教育背景", "学历背景", "学习经历"],
        "internship": ["实习经历", "工作经历", "实践经历", "校园经历"],
        "project": ["项目经历", "项目经验", "科研项目", "课程项目"],
        "skills": ["专业技能", "技能特长", "技能证书", "掌握技能"],
        "awards": ["获奖情况", "荣誉奖项", "奖励荣誉"],
        "self_evaluation": ["自我评价", "个人总结", "个人优势"],
        "target_job": ["求职意向", "目标岗位", "应聘岗位"],
    }

    hints: Dict[str, str] = {}
    lines = [line.strip() for line in resume_text.splitlines() if line.strip()]
    for section_name, keywords in section_keywords.items():
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
    input_data = build_resume_parse_input(cleaned_resume_text, file_meta=file_meta)

    merged_extra_context = {
        "resume_module": "resume_parse_module",
        "schema_hint": default_resume_parse_result_dict(),
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
        fallback = default_resume_parse_result_dict()
        fallback["raw_resume_text"] = cleaned_resume_text
        fallback["parse_warnings"] = [f"LLM 调用失败: {exc}"]
        return fallback

    normalized_result = validate_resume_parse_result(
        raw_result,
        raw_resume_text=cleaned_resume_text,
    )

    if file_meta and file_meta.get("maybe_scanned_pdf"):
        normalized_result["parse_warnings"].append(
            "PDF 文本提取内容过少，疑似扫描件；OCR 接口已预留但当前未启用"
        )
    # 二次校验：合并扫描件警告后再次去重/补全
    return validate_resume_parse_result(
        normalized_result,
        raw_resume_text=cleaned_resume_text,
    )


def update_student_state_with_resume_result(
    resume_parse_result: Dict[str, Any],
    state_path: Optional[str | Path] = None,
    student_state: Optional[Dict[str, Any]] = None,
    state_manager: Optional[StateManager] = None,
) -> Dict[str, Any]:
    """
    将简历解析结果写回 student.json。

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
    主流程：读本地简历 → extract + clean → parse_resume_with_llm → 更新 student.json。

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
    LOGGER.info("student.json updated: %s", state_path or "default state path")

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
        default="outputs/state/student.json",
        help="student.json 输出路径",
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




