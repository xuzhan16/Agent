"""
main_career_report_demo.py

career_report 模块最小可运行 demo。

演示内容：
1. 使用 mock 的 student_profile_result、job_profile_result、job_match_result、career_path_plan_result；
2. 演示将上游结构化结果写入 student.json；
3. 演示调用 career_report_builder 构造 report_input_payload；
4. 演示调用 career_report_formatter 生成固定章节草稿；
5. 演示调用 career_report_service，通过统一大模型接口生成最终职业报告；
6. 演示将 career_report_result 写回 student.json；
7. 打印最终 career_report_result 摘要。
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any, Dict, List

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from career_report.career_report_builder import (
    build_demo_career_path_plan_result,
    build_demo_job_match_result,
    build_demo_job_profile_result,
    build_demo_student_profile_result,
    build_report_input_payload,
)
from career_report.career_report_formatter import build_report_sections_draft
from career_report.career_report_service import run_career_report_service
from llm_interface_layer.state_manager import StateManager


LOGGER = logging.getLogger(__name__)

STATE_PATH = Path("outputs/state/main_career_report_demo_student.json")
BUILDER_OUTPUT_PATH = Path("outputs/state/main_career_report_demo_input_payload.json")
FORMATTER_OUTPUT_PATH = Path("outputs/state/main_career_report_demo_sections_draft.json")
SERVICE_OUTPUT_PATH = Path("outputs/state/main_career_report_demo_service_result.json")


def setup_logging() -> None:
    """初始化日志配置。"""
    if LOGGER.handlers:
        return
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
    )


def safe_dict(value: Any) -> Dict[str, Any]:
    """安全转 dict。"""
    return value if isinstance(value, dict) else {}


def safe_list(value: Any) -> List[Any]:
    """安全转 list。"""
    return value if isinstance(value, list) else []


def init_mock_student_state(
    student_profile_result: Dict[str, Any],
    job_profile_result: Dict[str, Any],
    job_match_result: Dict[str, Any],
    career_path_plan_result: Dict[str, Any],
    state_path: str | Path,
) -> Dict[str, Any]:
    """初始化 demo student.json，并写入四个上游模块结果。"""
    state_manager = StateManager()
    student_state = state_manager.init_state(state_path=state_path, overwrite=True)
    student_state = state_manager.update_state(
        task_type="student_profile",
        task_result=student_profile_result,
        state_path=state_path,
        student_state=student_state,
    )
    student_state = state_manager.update_state(
        task_type="job_profile",
        task_result=job_profile_result,
        state_path=state_path,
        student_state=student_state,
    )
    student_state = state_manager.update_state(
        task_type="job_match",
        task_result=job_match_result,
        state_path=state_path,
        student_state=student_state,
    )
    student_state = state_manager.update_state(
        task_type="career_path_plan",
        task_result=career_path_plan_result,
        state_path=state_path,
        student_state=student_state,
    )
    return student_state


def print_input_preview(
    student_profile_result: Dict[str, Any],
    job_profile_result: Dict[str, Any],
    job_match_result: Dict[str, Any],
    career_path_plan_result: Dict[str, Any],
) -> None:
    """打印 demo 输入摘要。"""
    print("\n========== Step 1/4 输入读取预览 ==========")
    print("student summary:", student_profile_result.get("summary", ""))
    print("target job:", job_profile_result.get("standard_job_name", ""))
    print("match score:", job_match_result.get("overall_match_score", 0.0))
    print("match level:", job_match_result.get("score_level", ""))
    print("primary target job:", career_path_plan_result.get("primary_target_job", ""))
    print("secondary target jobs:", safe_list(career_path_plan_result.get("secondary_target_jobs")))
    print("state path:", str(STATE_PATH))


def run_builder_demo(
    student_profile_result: Dict[str, Any],
    job_profile_result: Dict[str, Any],
    job_match_result: Dict[str, Any],
    career_path_plan_result: Dict[str, Any],
) -> Dict[str, Any]:
    """演示调用 builder 构造 report_input_payload。"""
    LOGGER.info("Step 2/4: build report_input_payload")
    payload = build_report_input_payload(
        student_profile_result=student_profile_result,
        job_profile_result=job_profile_result,
        job_match_result=job_match_result,
        career_path_plan_result=career_path_plan_result,
        output_path=BUILDER_OUTPUT_PATH,
    )

    print("\n========== Step 2/4 Builder 输出摘要 ==========")
    print("report_title:", safe_dict(payload.get("report_meta")).get("report_title", ""))
    print("student name:", safe_dict(payload.get("student_snapshot")).get("name", ""))
    print("job name:", safe_dict(payload.get("job_snapshot")).get("standard_job_name", ""))
    print("match level:", safe_dict(payload.get("job_match_snapshot")).get("match_level", ""))
    print("career goal:", safe_dict(safe_dict(payload.get("career_path_plan_snapshot")).get("career_goal")).get("primary_target_job", ""))
    print("build warnings:", safe_list(payload.get("build_warnings")))
    print("builder output:", str(BUILDER_OUTPUT_PATH))
    return payload


def run_formatter_demo(report_input_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """演示调用 formatter 生成固定章节草稿。"""
    LOGGER.info("Step 3/4: build report section drafts")
    section_drafts = build_report_sections_draft(
        report_input_payload=report_input_payload,
        output_path=FORMATTER_OUTPUT_PATH,
    )

    print("\n========== Step 3/4 Formatter 章节草稿摘要 ==========")
    print("section_count:", len(section_drafts))
    print("section_titles:", [safe_dict(item).get("section_title", "") for item in section_drafts])
    print("formatter output:", str(FORMATTER_OUTPUT_PATH))
    return section_drafts


def run_service_demo(
    student_profile_result: Dict[str, Any],
    job_profile_result: Dict[str, Any],
    job_match_result: Dict[str, Any],
    career_path_plan_result: Dict[str, Any],
    student_state: Dict[str, Any],
) -> Dict[str, Any]:
    """演示调用 service + 统一 LLM 接口，并写回 student.json。"""
    LOGGER.info("Step 4/4: run career_report_service and update student.json")
    career_report_result = run_career_report_service(
        student_profile_result=student_profile_result,
        job_profile_result=job_profile_result,
        job_match_result=job_match_result,
        career_path_plan_result=career_path_plan_result,
        student_state=student_state,
        context_data={
            "graph_context": {
                "mock_note": "预留 Neo4j 图谱上下文，当前 career_report demo 使用 mock。",
            },
            "sql_context": {
                "mock_note": "预留 SQL 岗位明细上下文，当前 career_report demo 使用 mock。",
            },
        },
        state_path=STATE_PATH,
        builder_output_path=BUILDER_OUTPUT_PATH,
        formatter_output_path=FORMATTER_OUTPUT_PATH,
        service_output_path=SERVICE_OUTPUT_PATH,
        extra_context={"demo_name": "main_career_report_demo"},
    )

    latest_state = StateManager().load_state(STATE_PATH)
    state_report_result = safe_dict(latest_state.get("career_report_result"))
    completeness_check = safe_dict(career_report_result.get("completeness_check"))

    print("\n========== Step 4/4 Service + LLM 最终报告摘要 ==========")
    print("report_title:", career_report_result.get("report_title", ""))
    print("report_summary:", career_report_result.get("report_summary", ""))
    print("section_count:", len(safe_list(career_report_result.get("report_sections"))))
    print("is_complete:", completeness_check.get("is_complete", False))
    print("missing_sections:", safe_list(completeness_check.get("missing_sections")))
    print("edit_suggestions:", safe_list(career_report_result.get("edit_suggestions")))
    print("state career_report_result exists:", bool(state_report_result))
    print("service output:", str(SERVICE_OUTPUT_PATH))
    print("\n---------- 报告正文预览 ----------")
    print(str(career_report_result.get("report_text_markdown", ""))[:1200])
    return career_report_result


def main() -> None:
    """demo 主入口。"""
    setup_logging()
    LOGGER.info("Start main_career_report_demo")

    student_profile_result = build_demo_student_profile_result()
    job_profile_result = build_demo_job_profile_result()
    job_match_result = build_demo_job_match_result()
    career_path_plan_result = build_demo_career_path_plan_result()

    student_state = init_mock_student_state(
        student_profile_result=student_profile_result,
        job_profile_result=job_profile_result,
        job_match_result=job_match_result,
        career_path_plan_result=career_path_plan_result,
        state_path=STATE_PATH,
    )

    print_input_preview(
        student_profile_result,
        job_profile_result,
        job_match_result,
        career_path_plan_result,
    )
    report_input_payload = run_builder_demo(
        student_profile_result,
        job_profile_result,
        job_match_result,
        career_path_plan_result,
    )
    run_formatter_demo(report_input_payload)
    run_service_demo(
        student_profile_result,
        job_profile_result,
        job_match_result,
        career_path_plan_result,
        student_state,
    )

    LOGGER.info("main_career_report_demo finished")


if __name__ == "__main__":
    main()
