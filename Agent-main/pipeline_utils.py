"""
pipeline_utils.py

最小闭环联调辅助工具。

提供：
1. 统一日志打印；
2. 上游结果存在性校验；
3. 各阶段结果摘要提取；
4. 中间结果快照保存；
5. 统一异常封装。
"""

from __future__ import annotations

import json
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


def safe_dict(value: Any) -> Dict[str, Any]:
    """安全转 dict。"""
    return value if isinstance(value, dict) else {}


def safe_list(value: Any) -> List[Any]:
    """安全转 list。"""
    return value if isinstance(value, list) else []


def truncate_text(text: Any, max_length: int = 300) -> str:
    """将长文本压缩成适合日志打印的短摘要。"""
    normalized_text = str(text or "").strip()
    if len(normalized_text) <= max_length:
        return normalized_text
    return normalized_text[:max_length] + "...[truncated]"


def print_module_log(
    stage_name: str,
    message: str,
    *,
    status: str = "INFO",
    step_index: Optional[int] = None,
    total_steps: Optional[int] = None,
) -> None:
    """统一打印带时间戳、阶段名和步骤序号的日志。"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    step_prefix = ""
    if step_index is not None and total_steps is not None:
        step_prefix = f"[{step_index}/{total_steps}] "
    print(f"[{timestamp}] [{status}] {step_prefix}{stage_name} - {message}")


def ensure_stage_result_exists(
    student_state: Dict[str, Any],
    result_field: str,
    stage_name: str,
) -> Dict[str, Any]:
    """校验上一阶段结果是否已写入 state，并返回对应 dict。"""
    stage_result = safe_dict(student_state.get(result_field))
    if not stage_result:
        raise ValueError(
            f"{stage_name} 缺少上游状态字段 {result_field}，请先检查前置步骤是否成功写回 student_api_state.json"
        )
    return stage_result


def save_stage_snapshot(
    stage_name: str,
    stage_result: Dict[str, Any],
    snapshot_dir: str | Path,
) -> Path:
    """将每个阶段的结果额外保存一份快照，方便排查联调问题。"""
    output_dir = Path(snapshot_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = output_dir / f"{stage_name}.json"
    with snapshot_path.open("w", encoding="utf-8") as f:
        json.dump(stage_result, f, ensure_ascii=False, indent=2)
    return snapshot_path


def build_result_summary(stage_name: str, stage_result: Dict[str, Any]) -> Dict[str, Any]:
    """按模块类型提取适合终端展示的轻量摘要。"""
    result = safe_dict(stage_result)
    if stage_name == "resume_parse":
        basic_info = safe_dict(result.get("basic_info"))
        return {
            "name": basic_info.get("name", ""),
            "school": basic_info.get("school", ""),
            "major": basic_info.get("major", ""),
            "target_job_intention": result.get("target_job_intention", ""),
            "skills_top10": safe_list(result.get("skills"))[:10],
            "project_count": len(safe_list(result.get("project_experience"))),
            "internship_count": len(safe_list(result.get("internship_experience"))),
            "parse_warnings": safe_list(result.get("parse_warnings")),
        }
    if stage_name == "student_profile":
        return {
            "complete_score": result.get("complete_score", 0.0),
            "competitiveness_score": result.get("competitiveness_score", 0.0),
            "score_level": result.get("score_level", ""),
            "soft_skills_top10": safe_list(result.get("soft_skills"))[:10],
            "strengths_top5": safe_list(result.get("strengths"))[:5],
            "weaknesses_top5": safe_list(result.get("weaknesses"))[:5],
            "summary": truncate_text(result.get("summary"), 400),
        }
    if stage_name == "job_profile":
        group_summary = safe_dict(result.get("group_summary"))
        return {
            "standard_job_name": result.get("standard_job_name", ""),
            "job_category": result.get("job_category", ""),
            "job_level": result.get("job_level", ""),
            "job_count": group_summary.get("job_count", 0),
            "hard_skills_top10": safe_list(result.get("hard_skills"))[:10],
            "tools_top10": safe_list(result.get("tools_or_tech_stack"))[:10],
            "summary": truncate_text(result.get("summary"), 400),
        }
    if stage_name == "job_match":
        return {
            "overall_match_score": result.get("overall_match_score", result.get("overall_score", 0.0)),
            "score_level": result.get("score_level", ""),
            "strengths_top5": safe_list(result.get("strengths"))[:5],
            "weaknesses_top5": safe_list(result.get("weaknesses"))[:5],
            "improvement_suggestions_top5": safe_list(result.get("improvement_suggestions"))[:5],
            "recommendation": truncate_text(result.get("recommendation"), 400),
            "analysis_summary": truncate_text(result.get("analysis_summary", result.get("summary", "")), 400),
        }
    if stage_name == "career_path_plan":
        return {
            "primary_target_job": result.get("primary_target_job", ""),
            "secondary_target_jobs": safe_list(
                result.get("secondary_target_jobs", result.get("backup_target_jobs"))
            )[:5],
            "direct_path": safe_list(result.get("direct_path"))[:10],
            "transition_path": safe_list(result.get("transition_path"))[:10],
            "short_term_plan_top5": safe_list(result.get("short_term_plan"))[:5],
            "mid_term_plan_top5": safe_list(result.get("mid_term_plan"))[:5],
            "summary": truncate_text(
                result.get("decision_summary", result.get("summary", "")),
                400,
            ),
        }
    if stage_name == "career_report":
        completeness_check = safe_dict(result.get("completeness_check"))
        report_sections = safe_list(result.get("report_sections"))
        return {
            "report_title": result.get("report_title", ""),
            "report_summary": truncate_text(result.get("report_summary"), 400),
            "section_count": len(report_sections),
            "is_complete": completeness_check.get("is_complete", False),
            "missing_sections": safe_list(completeness_check.get("missing_sections")),
            "edit_suggestions_top5": safe_list(result.get("edit_suggestions"))[:5],
            "report_text_preview": truncate_text(
                result.get("report_text_markdown", result.get("report_text", "")),
                800,
            ),
        }
    return {
        "result_keys": list(result.keys()),
        "raw_preview": truncate_text(json.dumps(result, ensure_ascii=False), 500),
    }


def print_stage_summary(stage_name: str, stage_result: Dict[str, Any]) -> Dict[str, Any]:
    """打印某一阶段的摘要并返回摘要 dict。"""
    summary = build_result_summary(stage_name, stage_result)
    print_module_log(stage_name, "阶段结果摘要如下：", status="SUCCESS")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return summary


def format_pipeline_exception(stage_name: str, exc: Exception) -> str:
    """将异常格式化成便于日志定位的字符串。"""
    return (
        f"{stage_name} 执行失败: {exc.__class__.__name__}: {exc}\n"
        f"{traceback.format_exc()}"
    )
