"""
career_report_formatter.py

career_report 模块的章节草稿格式化层。

职责：
1. 生成固定报告章节骨架；
2. 使用 report_input_payload 中的结构化结果填充章节草稿；
3. 输出结构化 report_sections 草稿；
4. 负责将章节草稿渲染为 Markdown 报告初稿。

设计约束：
- 不让模型从零自由生成整篇报告；
- 先由程序固定章节骨架并填入结构化内容，再交给 career_report service 调用 LLM 做最终润色。
"""

from __future__ import annotations

import argparse
import json
import re
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from .career_report_builder import (
    DEFAULT_OUTPUT_PATH as DEFAULT_BUILDER_OUTPUT_PATH,
    build_demo_career_path_plan_result,
    build_demo_job_match_result,
    build_demo_job_profile_result,
    build_demo_student_profile_result,
    build_report_input_payload,
    load_json_file,
)


DEFAULT_OUTPUT_PATH = Path("outputs/state/career_report_sections_draft.json")

REPORT_SECTION_TITLES = [
    "学生基本情况与能力画像",
    "目标岗位画像与职业探索",
    "人岗匹配分析",
    "职业目标设定与职业路径规划",
    "分阶段行动计划",
    "风险分析与动态调整建议",
    "总结与建议",
]


@dataclass
class ReportSectionDraft:
    """单个报告章节草稿。"""

    section_title: str = ""
    section_content: str = ""
    section_data: Dict[str, Any] = field(default_factory=dict)


def clean_text(value: Any) -> str:
    """基础文本清洗。"""
    if value is None:
        return ""
    text = str(value).replace("\u00a0", " ").replace("\u3000", " ").strip()
    text = re.sub(r"\s+", " ", text)
    if text.lower() in {"", "nan", "none", "null", "n/a", "na", "-"}:
        return ""
    return text


def safe_dict(value: Any) -> Dict[str, Any]:
    """安全转 dict。"""
    return value if isinstance(value, dict) else {}


def safe_float(value: Any, default: float = 0.0) -> float:
    """安全转 float。"""
    text = clean_text(value)
    if not text:
        return default
    try:
        return float(text)
    except (TypeError, ValueError):
        return default


def dedup_keep_order(values: Iterable[Any]) -> List[Any]:
    """稳定去重。"""
    seen = set()
    result = []
    for value in values:
        if value is None or value == "":
            continue
        key = json.dumps(value, ensure_ascii=False, sort_keys=True) if isinstance(value, (dict, list)) else str(value)
        if key not in seen:
            seen.add(key)
            result.append(value)
    return result


def parse_list_like(value: Any) -> List[Any]:
    """将 list / JSON 字符串 / 分隔字符串统一转 list。"""
    if isinstance(value, list):
        return dedup_keep_order(value)
    text = clean_text(value)
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            loaded = json.loads(text)
            if isinstance(loaded, list):
                return dedup_keep_order(loaded)
        except json.JSONDecodeError:
            pass
    return dedup_keep_order(
        clean_text(part)
        for part in re.split(r"[、,，;；/|｜\n]+", text)
        if clean_text(part)
    )


def normalize_text_list(value: Any) -> List[str]:
    """统一文本列表格式。"""
    return dedup_keep_order(clean_text(item) for item in parse_list_like(value) if clean_text(item))


def normalize_dict_list(value: Any) -> List[Dict[str, Any]]:
    """统一 dict 列表格式。"""
    result = []
    for item in parse_list_like(value):
        if isinstance(item, dict):
            result.append(deepcopy(item))
    return dedup_keep_order(result)


def save_json(data: Any, output_path: str | Path) -> None:
    """保存 JSON 文件。"""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def join_text_items(items: Any, default_text: str = "暂无明确记录") -> str:
    """将列表/字符串压成自然语言片段。"""
    values = normalize_text_list(items)
    return "、".join(values) if values else default_text


def format_named_distribution(items: Any, top_n: int = 5) -> str:
    """将 city/industry 分布列表格式化为短文本。"""
    parts = []
    for item in normalize_dict_list(items)[:top_n]:
        item_dict = safe_dict(item)
        name = clean_text(item_dict.get("name"))
        ratio = item_dict.get("ratio")
        count = item_dict.get("count")
        if name and ratio not in {None, ""}:
            parts.append(f"{name}（占比{ratio}，样本{count}）")
        elif name:
            parts.append(name)
    return "、".join(parts) if parts else "暂无明确分布信息"


def format_experience_list(items: Any, item_type: str) -> str:
    """将项目/实习经历格式化为简要文本。"""
    rows = []
    for item in normalize_dict_list(items)[:5]:
        item_dict = safe_dict(item)
        if item_type == "project":
            name = clean_text(item_dict.get("project_name") or item_dict.get("name")) or "未命名项目"
            desc = clean_text(item_dict.get("description"))
            rows.append(f"{name}：{desc}" if desc else name)
        else:
            company = clean_text(item_dict.get("company_name") or item_dict.get("company")) or "未注明公司"
            position = clean_text(item_dict.get("position") or item_dict.get("role"))
            desc = clean_text(item_dict.get("description"))
            text = f"{company}-{position}" if position else company
            rows.append(f"{text}：{desc}" if desc else text)
    return "；".join(rows) if rows else "暂无明确记录"


def build_student_section(report_input_payload: Dict[str, Any]) -> Dict[str, Any]:
    """生成“学生基本情况与能力画像”章节草稿。"""
    student = safe_dict(report_input_payload.get("student_snapshot"))
    practice_profile = safe_dict(student.get("practice_profile"))
    potential_profile = safe_dict(student.get("potential_profile"))

    content_lines = [
        f"学生姓名：{clean_text(student.get('name')) or '未提供'}",
        f"教育背景：{clean_text(student.get('school')) or '未提供学校'}，{clean_text(student.get('degree')) or '未提供学历'}，专业为{clean_text(student.get('major')) or '未提供专业'}，预计毕业年份{clean_text(student.get('graduation_year')) or '未提供'}。",
        f"核心硬技能：{join_text_items(student.get('hard_skills'))}。",
        f"工具技能：{join_text_items(student.get('tool_skills'))}。",
        f"证书与资质：{join_text_items(student.get('certificates'))}。",
        f"软技能标签：{join_text_items(student.get('soft_skills'))}。",
        f"项目经历摘要：{format_experience_list(practice_profile.get('project_experience'), 'project')}。",
        f"实习经历摘要：{format_experience_list(practice_profile.get('internship_experience'), 'internship')}。",
        f"画像完整度得分：{safe_float(student.get('profile_completeness_score')):.2f}；竞争力得分：{safe_float(student.get('competitiveness_score')):.2f}。",
        f"潜力画像：{clean_text(potential_profile.get('growth_level')) or '未明确'}；优势方向：{join_text_items(potential_profile.get('preferred_directions') or potential_profile.get('domain_tags'))}。",
        f"学生画像总结：{clean_text(student.get('summary')) or '暂无总结'}",
    ]
    return asdict(
        ReportSectionDraft(
            section_title="学生基本情况与能力画像",
            section_content="\n".join(content_lines),
            section_data=deepcopy(student),
        )
    )


def build_job_section(report_input_payload: Dict[str, Any]) -> Dict[str, Any]:
    """生成“目标岗位画像与职业探索”章节草稿。"""
    job_snapshot = safe_dict(report_input_payload.get("job_snapshot"))
    salary_summary = safe_dict(job_snapshot.get("salary_summary"))

    salary_text = "暂无明确薪资统计"
    if salary_summary:
        salary_min = salary_summary.get("salary_min_month_avg")
        salary_max = salary_summary.get("salary_max_month_avg")
        salary_mid = salary_summary.get("salary_mid_month_avg")
        if salary_min is not None or salary_max is not None or salary_mid is not None:
            salary_text = f"月薪下界均值{salary_min}，上界均值{salary_max}，中位参考{salary_mid}"

    content_lines = [
        f"目标岗位：{clean_text(job_snapshot.get('standard_job_name')) or '未明确'}",
        f"岗位类别与层级：{clean_text(job_snapshot.get('job_category')) or '未明确类别'} / {clean_text(job_snapshot.get('job_level')) or '未明确层级'}。",
        f"学历要求：{clean_text(job_snapshot.get('degree_requirement')) or '未明确'}；专业偏好：{join_text_items(job_snapshot.get('major_requirement'))}。",
        f"核心技能要求：{join_text_items(job_snapshot.get('hard_skills'))}。",
        f"工具/技术栈要求：{join_text_items(job_snapshot.get('tool_skills'))}。",
        f"软技能与实践要求：软技能包括{join_text_items(job_snapshot.get('soft_skills'))}；实践要求包括{join_text_items(job_snapshot.get('practice_requirement'))}。",
        f"薪资概况：{salary_text}。",
        f"城市分布：{format_named_distribution(job_snapshot.get('city_distribution'))}。",
        f"行业分布：{format_named_distribution(job_snapshot.get('industry_distribution'))}。",
        f"岗位画像总结：{clean_text(job_snapshot.get('summary')) or '暂无总结'}",
    ]
    return asdict(
        ReportSectionDraft(
            section_title="目标岗位画像与职业探索",
            section_content="\n".join(content_lines),
            section_data=deepcopy(job_snapshot),
        )
    )


def build_match_section(report_input_payload: Dict[str, Any]) -> Dict[str, Any]:
    """生成“人岗匹配分析”章节草稿。"""
    match_snapshot = safe_dict(report_input_payload.get("job_match_snapshot"))
    dimension_scores = safe_dict(match_snapshot.get("dimension_scores"))

    content_lines = [
        f"目标岗位：{clean_text(match_snapshot.get('target_job_name')) or '未明确'}",
        f"综合匹配分数：{safe_float(match_snapshot.get('overall_match_score')):.2f}；匹配等级：{clean_text(match_snapshot.get('match_level')) or '未明确'}。",
        "四维度分数："
        f"基础要求 {safe_float(dimension_scores.get('basic_requirement_score')):.2f}，"
        f"职业技能 {safe_float(dimension_scores.get('vocational_skill_score')):.2f}，"
        f"职业素质 {safe_float(dimension_scores.get('professional_quality_score')):.2f}，"
        f"发展潜力 {safe_float(dimension_scores.get('development_potential_score')):.2f}。",
        f"匹配优势：{join_text_items(match_snapshot.get('strengths'))}。",
        f"当前短板：{join_text_items(match_snapshot.get('weaknesses'))}。",
        f"补强建议：{join_text_items(match_snapshot.get('improvement_suggestions'))}。",
        f"投递建议：{clean_text(match_snapshot.get('recommendation')) or '暂无明确建议'}",
        f"分析摘要：{clean_text(match_snapshot.get('analysis_summary')) or '暂无摘要'}",
    ]
    return asdict(
        ReportSectionDraft(
            section_title="人岗匹配分析",
            section_content="\n".join(content_lines),
            section_data=deepcopy(match_snapshot),
        )
    )


def build_goal_and_path_section(report_input_payload: Dict[str, Any]) -> Dict[str, Any]:
    """生成“职业目标设定与职业路径规划”章节草稿。"""
    plan_snapshot = safe_dict(report_input_payload.get("career_path_plan_snapshot"))
    career_goal = safe_dict(plan_snapshot.get("career_goal"))
    career_path = safe_dict(plan_snapshot.get("career_path"))

    content_lines = [
        f"首选目标岗位：{clean_text(career_goal.get('primary_target_job')) or '未明确'}",
        f"备选目标岗位：{join_text_items(career_goal.get('secondary_target_jobs'))}。",
        f"目标定位：{clean_text(career_goal.get('goal_positioning')) or '暂无明确定位'}",
        f"选择原因：{clean_text(career_goal.get('goal_reason')) or '暂无明确说明'}",
        f"直接路径：{join_text_items(career_path.get('direct_path'))}。",
        f"过渡路径：{join_text_items(career_path.get('transition_path'))}。",
        f"长期路径：{join_text_items(career_path.get('long_term_path'))}。",
        f"路径策略：{clean_text(career_path.get('path_strategy')) or '未明确'}。",
        f"路径决策摘要：{clean_text(plan_snapshot.get('decision_summary')) or '暂无摘要'}",
    ]
    return asdict(
        ReportSectionDraft(
            section_title="职业目标设定与职业路径规划",
            section_content="\n".join(content_lines),
            section_data={"career_goal": deepcopy(career_goal), "career_path": deepcopy(career_path)},
        )
    )


def build_phase_plan_section(report_input_payload: Dict[str, Any]) -> Dict[str, Any]:
    """生成“分阶段行动计划”章节草稿。"""
    plan_snapshot = safe_dict(report_input_payload.get("career_path_plan_snapshot"))
    phase_plan = safe_dict(plan_snapshot.get("phase_plan"))
    short_term_plan = normalize_text_list(phase_plan.get("short_term_plan"))
    mid_term_plan = normalize_text_list(phase_plan.get("mid_term_plan"))

    short_lines = [f"{idx + 1}. {item}" for idx, item in enumerate(short_term_plan)] if short_term_plan else ["暂无明确短期计划"]
    mid_lines = [f"{idx + 1}. {item}" for idx, item in enumerate(mid_term_plan)] if mid_term_plan else ["暂无明确中期计划"]

    content_lines = [
        "短期计划（建议 3-6 个月）：",
        *short_lines,
        "中期计划（建议 6-18 个月）：",
        *mid_lines,
    ]
    return asdict(
        ReportSectionDraft(
            section_title="分阶段行动计划",
            section_content="\n".join(content_lines),
            section_data=deepcopy(phase_plan),
        )
    )


def build_risk_section(report_input_payload: Dict[str, Any]) -> Dict[str, Any]:
    """生成“风险分析与动态调整建议”章节草稿。"""
    plan_snapshot = safe_dict(report_input_payload.get("career_path_plan_snapshot"))
    match_snapshot = safe_dict(report_input_payload.get("job_match_snapshot"))

    content_lines = [
        f"主要风险与缺口：{join_text_items(plan_snapshot.get('risk_and_gap'))}。",
        f"匹配短板补充：{join_text_items(match_snapshot.get('weaknesses'))}。",
        f"动态调整建议：{join_text_items(match_snapshot.get('improvement_suggestions'))}。",
        "建议每 1-2 个月根据岗位招聘要求、项目产出、实习反馈和技能掌握情况更新一次职业规划，并同步修订简历与目标岗位优先级。",
    ]
    return asdict(
        ReportSectionDraft(
            section_title="风险分析与动态调整建议",
            section_content="\n".join(content_lines),
            section_data={
                "risk_and_gap": normalize_text_list(plan_snapshot.get("risk_and_gap")),
                "weaknesses": normalize_text_list(match_snapshot.get("weaknesses")),
                "improvement_suggestions": normalize_text_list(match_snapshot.get("improvement_suggestions")),
            },
        )
    )


def build_summary_section(report_input_payload: Dict[str, Any]) -> Dict[str, Any]:
    """生成“总结与建议”章节草稿。"""
    meta = safe_dict(report_input_payload.get("report_meta"))
    context = safe_dict(report_input_payload.get("report_generation_context"))

    content_lines = [
        f"综合结论：{clean_text(context.get('decision_summary')) or clean_text(context.get('match_summary')) or '暂无综合结论'}",
        f"核心建议：{clean_text(context.get('recommendation')) or '建议围绕目标岗位持续补齐核心技能、项目经验与岗位表达能力。'}",
        f"报告对象：{clean_text(meta.get('student_name')) or '学生'}；目标岗位：{clean_text(meta.get('target_job_name')) or '未明确'}。",
    ]
    return asdict(
        ReportSectionDraft(
            section_title="总结与建议",
            section_content="\n".join(content_lines),
            section_data={"report_meta": deepcopy(meta), "report_generation_context": deepcopy(context)},
        )
    )


def build_report_sections_draft(report_input_payload: Dict[str, Any], output_path: Optional[str | Path] = None) -> List[Dict[str, Any]]:
    """主入口：生成固定章节骨架 + 结构化填充草稿。"""
    payload = safe_dict(report_input_payload)
    sections = [
        build_student_section(payload),
        build_job_section(payload),
        build_match_section(payload),
        build_goal_and_path_section(payload),
        build_phase_plan_section(payload),
        build_risk_section(payload),
        build_summary_section(payload),
    ]
    if output_path:
        save_json(sections, output_path)
    return sections


def render_report_sections_markdown(
    report_title: str,
    report_sections: List[Dict[str, Any]],
    report_summary: str = "",
) -> str:
    """将结构化章节草稿渲染为 Markdown 文本。"""
    title = clean_text(report_title) or "大学生职业生涯发展报告"
    lines = [f"# {title}", ""]
    summary = clean_text(report_summary)
    if summary:
        lines.extend(["## 报告摘要", summary, ""])

    for section in report_sections:
        section_dict = safe_dict(section)
        section_title = clean_text(section_dict.get("section_title"))
        section_content = clean_text(section_dict.get("section_content"))
        if not section_title:
            continue
        lines.append(f"## {section_title}")
        lines.append(section_content or "暂无明确内容。")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def parse_args() -> argparse.Namespace:
    """命令行参数解析。"""
    parser = argparse.ArgumentParser(description="Build career_report section drafts")
    parser.add_argument("--input", default="", help="可选：report_input_payload JSON 路径")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="report_sections 草稿输出路径")
    parser.add_argument("--markdown-output", default="", help="可选：Markdown 草稿输出路径")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.input:
        input_payload = load_json_file(args.input)
    else:
        input_payload = build_report_input_payload(
            student_profile_result=build_demo_student_profile_result(),
            job_profile_result=build_demo_job_profile_result(),
            job_match_result=build_demo_job_match_result(),
            career_path_plan_result=build_demo_career_path_plan_result(),
            output_path=DEFAULT_BUILDER_OUTPUT_PATH,
        )

    section_result = build_report_sections_draft(input_payload, output_path=args.output)
    if args.markdown_output:
        meta = safe_dict(input_payload.get("report_meta"))
        report_md = render_report_sections_markdown(
            report_title=clean_text(meta.get("report_title")),
            report_sections=section_result,
            report_summary=clean_text(safe_dict(input_payload.get("report_generation_context")).get("decision_summary")),
        )
        Path(args.markdown_output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.markdown_output).write_text(report_md, encoding="utf-8")

    print(json.dumps(section_result, ensure_ascii=False, indent=2))
