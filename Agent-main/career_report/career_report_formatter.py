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
    if isinstance(value, (list, tuple, set, frozenset, dict)) and not value:
        return ""
    if type(value).__name__ in {"dict_keys", "dict_values", "dict_items"}:
        try:
            if not list(value):
                return ""
        except TypeError:
            return ""
    text = str(value).replace("\u00a0", " ").replace("\u3000", " ").strip()
    text = re.sub(r"\s+", " ", text)
    if text.lower() in {"", "nan", "none", "null", "n/a", "na", "-", "[]", "{}", "set()", "dict_keys([])", "dict_values([])", "dict_items([])"}:
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
    if isinstance(value, (tuple, set, frozenset)):
        return dedup_keep_order(value)
    if isinstance(value, dict):
        return dedup_keep_order(value.keys())
    if type(value).__name__ in {"dict_keys", "dict_values"}:
        return dedup_keep_order(list(value))
    if type(value).__name__ == "dict_items":
        return dedup_keep_order([key for key, _ in list(value)])
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


def format_percent(value: Any, default_text: str = "暂无明确记录") -> str:
    """格式化比例，兼容 0-1 与 0-100 两种输入。"""
    if value in {None, ""}:
        return default_text
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default_text
    if number <= 1:
        number *= 100
    return f"{number:.0f}%"


def format_bool(value: Any) -> str:
    """格式化布尔结论。"""
    if value is True:
        return "通过"
    if value is False:
        return "未通过"
    return "暂无明确记录"


def format_requirement_distribution(items: Any, top_n: int = 5) -> str:
    """格式化学历/专业/证书要求分布。"""
    rows = []
    for item in normalize_dict_list(items)[:top_n]:
        item_dict = safe_dict(item)
        name = clean_text(item_dict.get("name"))
        ratio = format_percent(item_dict.get("ratio"), default_text="")
        count = item_dict.get("count")
        if name and ratio and count not in {None, ""}:
            rows.append(f"{name}（{ratio}，样本{count}）")
        elif name and ratio:
            rows.append(f"{name}（{ratio}）")
        elif name:
            rows.append(name)
    return "、".join(rows) if rows else "暂无明确记录"


def filter_path_dependent_plan(items: Any) -> List[str]:
    """无真实路径时过滤依赖纵向/横向路径的旧计划话术。"""
    banned_terms = ["纵向路径", "横向转岗路径", "晋升路径", "转岗路径", "过渡路径"]
    return [
        item
        for item in normalize_text_list(items)
        if not any(term in item for term in banned_terms)
    ]


def strip_semantic_leakage(text: Any) -> str:
    """从规则摘要中移除容易被误读为目标岗位画像的语义召回句。"""
    value = clean_text(text)
    if not value:
        return ""
    parts = [clean_text(part) for part in re.split(r"(?<=[。！？!?])", value) if clean_text(part)]
    filtered = [part for part in parts if "语义检索" not in part and "相近岗位画像" not in part]
    return "".join(filtered) or value


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
    target_context = safe_dict(
        report_input_payload.get("target_job_profile_context")
        or job_snapshot.get("target_job_profile_context")
    )
    generation_context = safe_dict(report_input_payload.get("report_generation_context"))
    salary_summary = safe_dict(job_snapshot.get("salary_summary"))

    salary_text = "暂无明确薪资统计"
    if salary_summary:
        salary_min = salary_summary.get("salary_min_month_avg")
        salary_max = salary_summary.get("salary_max_month_avg")
        salary_mid = salary_summary.get("salary_mid_month_avg")
        if salary_min is not None or salary_max is not None or salary_mid is not None:
            salary_text = f"月薪下界均值{salary_min}，上界均值{salary_max}，中位参考{salary_mid}"

    requested_job_name = clean_text(
        target_context.get("requested_job_name")
        or job_snapshot.get("requested_job_name")
        or job_snapshot.get("standard_job_name")
    )
    resolved_job_name = clean_text(
        target_context.get("resolved_standard_job_name")
        or target_context.get("standard_job_name")
        or job_snapshot.get("resolved_standard_job_name")
    )
    asset_found = bool(target_context.get("asset_found"))
    asset_status = (
        f"已命中标准岗位画像资产：{resolved_job_name or requested_job_name}。"
        if asset_found
        else "当前目标岗位未完整命中标准岗位画像资产，以下画像仅基于已有岗位结构、SQL事实与规则抽取结果，可信度有限。"
    )
    sample_count = target_context.get("sample_count") or job_snapshot.get("sample_count") or 0
    degree_distribution = target_context.get("degree_distribution")
    major_distribution = target_context.get("major_distribution")
    certificate_distribution = target_context.get("certificate_distribution")
    no_cert_ratio = format_percent(target_context.get("no_certificate_requirement_ratio"))
    required_knowledge = normalize_text_list(target_context.get("required_knowledge_points"))[:12]
    preferred_knowledge = normalize_text_list(target_context.get("preferred_knowledge_points"))[:12]

    semantic_reference_text = ""
    semantic_snapshot = safe_dict(generation_context.get("semantic_fact_snapshot"))
    semantic_hits = []
    for hit in normalize_dict_list(semantic_snapshot.get("hits"))[:3]:
        hit_name = clean_text(hit.get("standard_job_name"))
        if hit_name and hit_name not in {requested_job_name, resolved_job_name}:
            semantic_hits.append(hit_name)
    if semantic_hits:
        semantic_reference_text = (
            f"相似岗位参考：语义检索还召回了{join_text_items(semantic_hits)}等岗位，仅作为补充参考，不作为目标岗位主画像。"
        )

    content_lines = [
        f"目标岗位：{requested_job_name or '未明确'}",
        f"标准化岗位：{resolved_job_name or '暂无明确记录'}；画像资产状态：{asset_status}",
        f"岗位类别与层级：{clean_text(target_context.get('job_category') or job_snapshot.get('job_category')) or '未明确类别'} / {clean_text(target_context.get('job_level_summary') or job_snapshot.get('job_level')) or '未明确层级'}；样本数：{sample_count}。",
        f"学历主流画像：{clean_text(target_context.get('degree_gate') or target_context.get('mainstream_degree') or job_snapshot.get('degree_requirement')) or '未明确'}；学历分布：{format_requirement_distribution(degree_distribution)}。",
        f"专业主流画像：{join_text_items(target_context.get('major_gate_set') or target_context.get('mainstream_majors') or job_snapshot.get('major_requirement'))}；专业分布：{format_requirement_distribution(major_distribution)}。",
        f"证书要求画像：必备证书为{join_text_items(target_context.get('must_have_certificates'))}；偏好证书为{join_text_items(target_context.get('preferred_certificates') or target_context.get('mainstream_certificates'))}；证书分布：{format_requirement_distribution(certificate_distribution)}；无明确证书要求比例：{no_cert_ratio}。",
        f"核心技能要求：{join_text_items(job_snapshot.get('hard_skills'))}。",
        f"工具/技术栈要求：{join_text_items(job_snapshot.get('tool_skills'))}。",
        f"核心知识点要求：{join_text_items(required_knowledge)}。",
        f"加分知识点：{join_text_items(preferred_knowledge)}。",
        f"软技能与实践要求：软技能包括{join_text_items(job_snapshot.get('soft_skills'))}；实践要求包括{join_text_items(job_snapshot.get('practice_requirement'))}。",
        f"薪资概况：{salary_text}。",
        f"城市分布：{format_named_distribution(job_snapshot.get('city_distribution'))}。",
        f"行业分布：{format_named_distribution(job_snapshot.get('industry_distribution'))}。",
        f"岗位画像总结：{clean_text(job_snapshot.get('summary')) or '暂无总结'}",
    ]
    if semantic_reference_text:
        content_lines.append(semantic_reference_text)
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
    job_match_context = safe_dict(report_input_payload.get("job_match_context"))
    target_job_match = safe_dict(
        job_match_context.get("target_job_match")
        or match_snapshot.get("target_job_match")
    )
    dimension_scores = safe_dict(match_snapshot.get("dimension_scores"))
    hard_info_display = safe_dict(target_job_match.get("hard_info_display"))
    hard_info_evaluation = safe_dict(target_job_match.get("hard_info_evaluation"))
    skill_knowledge_match = safe_dict(target_job_match.get("skill_knowledge_match"))
    contest_evaluation = safe_dict(target_job_match.get("contest_evaluation"))
    degree_display = safe_dict(hard_info_display.get("degree"))
    major_display = safe_dict(hard_info_display.get("major"))
    certificate_display = safe_dict(hard_info_display.get("certificate"))
    degree_eval = safe_dict(hard_info_evaluation.get("degree"))
    major_eval = safe_dict(hard_info_evaluation.get("major"))
    certificate_eval = safe_dict(hard_info_evaluation.get("certificate"))
    asset_found = bool(target_job_match.get("asset_found"))
    asset_warning = "" if asset_found else "岗位评测资产不足，学历/专业/证书和知识点硬门槛结论仅供参考。"
    knowledge_accuracy = format_percent(skill_knowledge_match.get("knowledge_point_accuracy"))
    analysis_summary = strip_semantic_leakage(match_snapshot.get("analysis_summary"))

    content_lines = [
        f"目标岗位：{clean_text(match_snapshot.get('target_job_name')) or '未明确'}",
        "第一层：规则匹配结果。",
        f"综合匹配分数：{safe_float(match_snapshot.get('overall_match_score')):.2f}；匹配等级：{clean_text(match_snapshot.get('match_level')) or '未明确'}。",
        "四维度分数："
        f"基础要求 {safe_float(dimension_scores.get('basic_requirement_score')):.2f}，"
        f"职业技能 {safe_float(dimension_scores.get('vocational_skill_score')):.2f}，"
        f"职业素质 {safe_float(dimension_scores.get('professional_quality_score')):.2f}，"
        f"发展潜力 {safe_float(dimension_scores.get('development_potential_score')):.2f}。",
        "说明：旧规则综合分代表规则侧综合匹配程度，不等同于赛题硬门槛全部通过。",
        f"匹配优势：{join_text_items(match_snapshot.get('strengths'))}。",
        f"当前短板：{join_text_items(match_snapshot.get('weaknesses'))}。",
        f"补强建议：{join_text_items(match_snapshot.get('improvement_suggestions'))}。",
        f"投递建议：{clean_text(match_snapshot.get('recommendation')) or '暂无明确建议'}",
        f"分析摘要：{analysis_summary or '暂无摘要'}",
        "第二层：赛题评测结果。",
        f"赛题资产分：{safe_float(target_job_match.get('asset_match_score')):.2f}；主展示分：{safe_float(target_job_match.get('display_match_score')):.2f}；风险等级：{clean_text(target_job_match.get('risk_level')) or '暂无明确记录'}。",
        f"学历匹配：学生学历为{clean_text(degree_display.get('student_value') or degree_eval.get('student_value')) or '暂无明确记录'}；岗位门槛/主流要求为{clean_text(degree_eval.get('job_gate') or degree_display.get('mainstream_requirement')) or '暂无明确记录'}；结论：{format_bool(degree_eval.get('pass'))}；风险：{clean_text(degree_display.get('risk_level')) or '暂无明确记录'}；说明：{clean_text(degree_display.get('message') or degree_eval.get('reason')) or '暂无明确记录'}。",
        f"专业匹配：学生专业为{clean_text(major_display.get('student_value') or major_eval.get('student_value')) or '暂无明确记录'}；岗位专业集合为{join_text_items(major_eval.get('job_gate_set') or major_display.get('mainstream_majors'))}；结论：{format_bool(major_eval.get('pass'))}；风险：{clean_text(major_display.get('risk_level')) or '暂无明确记录'}；说明：{clean_text(major_display.get('message') or major_eval.get('reason')) or '暂无明确记录'}。",
        f"证书匹配：学生证书为{join_text_items(certificate_display.get('student_values') or certificate_eval.get('student_values'))}；必备证书为{join_text_items(certificate_eval.get('must_have_certificates') or certificate_display.get('must_have_certificates'))}；偏好证书为{join_text_items(certificate_eval.get('preferred_certificates') or certificate_display.get('preferred_certificates'))}；结论：{format_bool(certificate_eval.get('pass'))}；风险：{clean_text(certificate_display.get('risk_level')) or '暂无明确记录'}；说明：{clean_text(certificate_display.get('message') or certificate_eval.get('reason')) or '暂无明确记录'}。",
        f"三项硬门槛总体结论：{format_bool(hard_info_evaluation.get('all_pass'))}。",
        f"技能知识点匹配：要求知识点包括{join_text_items(skill_knowledge_match.get('required_knowledge_points'))}；已命中知识点包括{join_text_items(skill_knowledge_match.get('matched_knowledge_points'))}；缺失知识点包括{join_text_items(skill_knowledge_match.get('missing_knowledge_points'))}；知识点覆盖率为{knowledge_accuracy}；是否达到 80%：{format_bool(skill_knowledge_match.get('pass'))}。",
        f"赛题综合评测：硬门槛通过={format_bool(contest_evaluation.get('hard_info_pass'))}；技能准确性达标={format_bool(contest_evaluation.get('skill_accuracy_pass'))}；最终赛题匹配成功={format_bool(contest_evaluation.get('contest_match_success'))}。",
    ]
    if asset_warning:
        content_lines.append(asset_warning)
    score_explanation = clean_text(target_job_match.get("score_explanation"))
    if score_explanation:
        content_lines.append(f"分数解释：{score_explanation}")
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
    path_context = safe_dict(report_input_payload.get("path_context"))
    career_goal = safe_dict(plan_snapshot.get("career_goal"))
    career_path = safe_dict(plan_snapshot.get("career_path"))
    target_path_status = clean_text(
        path_context.get("target_path_data_status")
        or plan_snapshot.get("target_path_data_status")
        or career_path.get("target_path_data_status")
    )
    target_path_message = clean_text(
        path_context.get("target_path_data_message")
        or plan_snapshot.get("target_path_data_message")
        or career_path.get("target_path_data_message")
    )
    path_strategy = clean_text(
        path_context.get("path_strategy")
        or plan_snapshot.get("path_strategy")
        or career_path.get("path_strategy")
    )
    user_target_job = clean_text(career_goal.get("user_target_job"))
    system_recommended_job = clean_text(career_goal.get("system_recommended_job"))
    primary_plan_job = clean_text(career_goal.get("primary_plan_job") or career_goal.get("primary_target_job"))
    goal_decision_reason = normalize_text_list(career_goal.get("goal_decision_reason"))
    llm_goal_explanation = safe_dict(career_goal.get("llm_goal_decision_explanation"))
    explanation_summary = clean_text(llm_goal_explanation.get("decision_reason_summary"))
    why_recommended = clean_text(llm_goal_explanation.get("why_recommended_job"))
    why_target_not_primary = clean_text(llm_goal_explanation.get("why_target_job_not_primary"))
    balance_text = clean_text(llm_goal_explanation.get("how_to_balance_target_and_recommended"))
    no_target_path = target_path_status == "missing" or path_strategy == "no_target_path_data"

    if no_target_path:
        direct_path_text = "暂无真实路径数据"
        transition_path_text = "暂无真实路径数据"
        long_term_path_text = "暂无真实路径数据"
        decision_summary = (
            target_path_message
            or "当前目标岗位暂无真实晋升/转岗路径数据，系统不会强行生成不存在的职业路径。"
        )
    else:
        direct_path_text = join_text_items(career_path.get("direct_path"))
        transition_path_text = join_text_items(career_path.get("transition_path"))
        long_term_path_text = join_text_items(career_path.get("long_term_path"))
        decision_summary = clean_text(plan_snapshot.get("decision_summary")) or "暂无摘要"

    content_lines = [
        f"用户原始目标岗位：{user_target_job or clean_text(career_goal.get('primary_target_job')) or '未明确'}",
        f"系统推荐岗位：{system_recommended_job or '暂无明确记录'}",
        f"系统推荐主路径岗位：{primary_plan_job or '未明确'}",
        f"原目标角色：{clean_text(career_goal.get('target_job_role')) or '暂无明确记录'}；推荐岗位角色：{clean_text(career_goal.get('recommended_job_role')) or '暂无明确记录'}。",
        f"主目标决策来源：{clean_text(career_goal.get('goal_decision_source')) or '暂无明确记录'}；置信度：{clean_text(career_goal.get('goal_decision_confidence')) or '暂无明确记录'}。",
        f"备选目标岗位：{join_text_items(career_goal.get('secondary_target_jobs'))}。",
        f"目标定位：{clean_text(career_goal.get('goal_positioning')) or '暂无明确定位'}",
        f"选择原因：{clean_text(career_goal.get('goal_reason')) or '暂无明确说明'}",
        f"规则决策依据：{join_text_items(goal_decision_reason)}。",
        f"目标岗位路径数据状态：{target_path_status or 'available'}。",
        f"路径数据说明：{target_path_message or '当前目标岗位存在可用路径数据。'}",
        f"直接路径：{direct_path_text}。",
        f"过渡路径：{transition_path_text}。",
        f"长期路径：{long_term_path_text}。",
        f"路径策略：{path_strategy or '未明确'}。",
        f"路径决策摘要：{decision_summary}",
    ]
    if explanation_summary:
        content_lines.insert(9, f"LLM解释摘要：{explanation_summary}")
    if why_recommended:
        content_lines.insert(10, f"为什么推荐该主路径：{why_recommended}")
    if why_target_not_primary and user_target_job and primary_plan_job and user_target_job != primary_plan_job:
        content_lines.insert(11, f"为什么原目标暂不作为主路径：{why_target_not_primary}")
    if balance_text:
        content_lines.insert(12, f"如何平衡两个目标：{balance_text}")
    if no_target_path:
        content_lines.append("可信度说明：当前目标岗位没有真实路径数据，报告不会展示或生成任何伪造的晋升/转岗路径。")
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
    path_context = safe_dict(report_input_payload.get("path_context"))
    match_snapshot = safe_dict(report_input_payload.get("job_match_snapshot"))
    job_match_context = safe_dict(report_input_payload.get("job_match_context"))
    target_job_match = safe_dict(job_match_context.get("target_job_match") or match_snapshot.get("target_job_match"))
    skill_knowledge_match = safe_dict(target_job_match.get("skill_knowledge_match"))
    phase_plan = safe_dict(plan_snapshot.get("phase_plan"))
    career_goal = safe_dict(plan_snapshot.get("career_goal"))
    primary_plan_job = clean_text(career_goal.get("primary_plan_job") or career_goal.get("primary_target_job"))
    user_target_job = clean_text(career_goal.get("user_target_job"))
    llm_goal_explanation = safe_dict(career_goal.get("llm_goal_decision_explanation"))
    target_path_status = clean_text(
        path_context.get("target_path_data_status")
        or plan_snapshot.get("target_path_data_status")
        or safe_dict(plan_snapshot.get("career_path")).get("target_path_data_status")
    )
    no_target_path = target_path_status == "missing" or clean_text(path_context.get("path_strategy")) == "no_target_path_data"
    short_term_plan = normalize_text_list(phase_plan.get("short_term_plan"))
    mid_term_plan = normalize_text_list(phase_plan.get("mid_term_plan"))
    short_term_plan = dedup_keep_order(
        normalize_text_list(llm_goal_explanation.get("short_term_focus")) + short_term_plan
    )
    mid_term_plan = dedup_keep_order(
        normalize_text_list(llm_goal_explanation.get("mid_term_focus")) + mid_term_plan
    )

    if no_target_path:
        missing_points = normalize_text_list(skill_knowledge_match.get("missing_knowledge_points"))[:8]
        short_term_plan = dedup_keep_order(
            short_term_plan
            + [
                "围绕学历、专业、证书三项硬门槛核验结果，补充可证明的简历材料与证书信息。",
                f"优先补齐缺失知识点：{join_text_items(missing_points)}。",
                f"准备 1-2 个与{primary_plan_job or '主路径岗位'}直接相关的项目案例，突出技术选型、问题定位和结果指标。",
            ]
        )
        mid_term_plan = dedup_keep_order(
            filter_path_dependent_plan(mid_term_plan)
            + [
                f"围绕{primary_plan_job or '主路径岗位'}持续积累真实项目或实习经验，并用作品集证明岗位关键能力。",
                f"将{user_target_job}作为中期补强后的冲刺目标，逐步补齐其关键知识点与项目证据。"
                if user_target_job and primary_plan_job and user_target_job != primary_plan_job
                else "",
                "根据投递反馈动态调整目标岗位和备选岗位，不依赖当前缺失的晋升/转岗路径数据。",
            ]
        )

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
    job_match_context = safe_dict(report_input_payload.get("job_match_context"))
    target_job_match = safe_dict(job_match_context.get("target_job_match") or match_snapshot.get("target_job_match"))
    hard_info_display = safe_dict(target_job_match.get("hard_info_display"))
    skill_knowledge_match = safe_dict(target_job_match.get("skill_knowledge_match"))
    contest_evaluation = safe_dict(target_job_match.get("contest_evaluation"))

    content_lines = [
        f"主要风险与缺口：{join_text_items(plan_snapshot.get('risk_and_gap'))}。",
        f"匹配短板补充：{join_text_items(match_snapshot.get('weaknesses'))}。",
        f"学历风险：{clean_text(safe_dict(hard_info_display.get('degree')).get('message')) or '暂无明确记录'}",
        f"专业风险：{clean_text(safe_dict(hard_info_display.get('major')).get('message')) or '暂无明确记录'}",
        f"证书风险：{clean_text(safe_dict(hard_info_display.get('certificate')).get('message')) or '暂无明确记录'}",
        f"知识点缺口：{join_text_items(skill_knowledge_match.get('missing_knowledge_points'))}；知识点覆盖率：{format_percent(skill_knowledge_match.get('knowledge_point_accuracy'))}；是否达到 80%：{format_bool(skill_knowledge_match.get('pass'))}。",
        f"赛题匹配成功结论：{format_bool(contest_evaluation.get('contest_match_success'))}。",
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
    path_context = safe_dict(report_input_payload.get("path_context"))
    plan_snapshot = safe_dict(report_input_payload.get("career_path_plan_snapshot"))
    career_goal = safe_dict(plan_snapshot.get("career_goal"))
    target_job = clean_text(meta.get("target_job_name")) or "目标岗位"
    user_target_job = clean_text(career_goal.get("user_target_job"))
    primary_plan_job = clean_text(career_goal.get("primary_plan_job") or career_goal.get("primary_target_job") or target_job)
    system_recommended_job = clean_text(career_goal.get("system_recommended_job"))
    balance_text = clean_text(
        safe_dict(career_goal.get("llm_goal_decision_explanation")).get("how_to_balance_target_and_recommended")
    )
    no_target_path = clean_text(path_context.get("target_path_data_status")) == "missing" or clean_text(path_context.get("path_strategy")) == "no_target_path_data"
    if no_target_path:
        if user_target_job and primary_plan_job and user_target_job != primary_plan_job:
            conclusion = (
                f"系统建议以{primary_plan_job}作为短期主路径，同时保留{user_target_job}作为中期补强后的冲刺目标。"
                "当前主路径岗位暂无真实晋升/转岗路径数据，因此本报告不生成职业路径，只从岗位匹配、硬门槛评测、知识点缺口和行动计划角度给出建议。"
            )
        else:
            conclusion = (
                f"当前建议以{primary_plan_job or target_job}作为求职目标，但本地岗位图谱和离线岗位画像中暂未沉淀该岗位明确晋升/转岗路径。"
                "因此本报告不生成目标岗位路径，仅从岗位匹配、硬门槛评测、技能知识点缺口和行动计划角度给出建议。"
            )
    else:
        conclusion = clean_text(context.get("decision_summary")) or clean_text(context.get("match_summary")) or "暂无综合结论"

    content_lines = [
        f"综合结论：{conclusion}",
        f"用户原始目标岗位：{user_target_job or '暂无明确记录'}；系统推荐岗位：{system_recommended_job or '暂无明确记录'}；主路径岗位：{primary_plan_job or target_job}。",
        f"目标平衡建议：{balance_text or '建议根据投递反馈和知识点补齐进度动态调整主目标与备选目标。'}",
        f"核心建议：{f'短期优先围绕{primary_plan_job}准备项目证据、知识点补齐和投递材料；{user_target_job}作为中期补强后的冲刺目标持续跟踪。' if user_target_job and primary_plan_job and user_target_job != primary_plan_job else (clean_text(context.get('recommendation')) or '建议围绕目标岗位持续补齐核心技能、项目经验与岗位表达能力。')}",
        f"报告对象：{clean_text(meta.get('student_name')) or '学生'}；报告主路径岗位：{primary_plan_job or target_job or '未明确'}。",
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
