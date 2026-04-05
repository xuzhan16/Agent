"""
main_career_path_plan_demo.py

career_path_plan 模块最小可运行 demo。

演示目标：
1. 使用 mock 的 student_profile_result、job_profile_result、job_match_result 作为输入；
2. 演示将三类上游结果写入并读取 student.json；
3. 演示调用 career_path_plan_builder 构造 career_plan_input_payload；
4. 演示调用 career_path_plan_selector 做规则目标选择和路径筛选；
5. 演示调用 career_path_plan_service，通过统一大模型接口补充自然语言规划解释；
6. 演示将 career_path_plan_result 写回 student.json；
7. 打印最终 career_path_plan_result 摘要。
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from career_path_plan.career_path_plan_builder import (
    build_career_plan_input_payload,
    build_demo_job_match_result,
    build_demo_job_profile_result,
    build_demo_student_profile_result,
)
from career_path_plan.career_path_plan_selector import select_career_path_plan
from career_path_plan.career_path_plan_service import run_career_path_plan_service
from llm_interface_layer.state_manager import StateManager


LOGGER = logging.getLogger(__name__)

STATE_PATH = Path("outputs/state/main_career_path_plan_demo_student.json")
BUILDER_OUTPUT_PATH = Path("outputs/state/main_career_path_plan_demo_input_payload.json")
SELECTOR_OUTPUT_PATH = Path("outputs/state/main_career_path_plan_demo_selection_result.json")
SERVICE_OUTPUT_PATH = Path("outputs/state/main_career_path_plan_demo_service_result.json")


def setup_logging() -> None:
    """初始化 demo 日志格式。"""
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
    state_path: str | Path,
) -> Dict[str, Any]:
    """初始化 demo student.json，并写入三个上游模块结果。"""
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
    return student_state


def print_input_preview(
    student_profile_result: Dict[str, Any],
    job_profile_result: Dict[str, Any],
    job_match_result: Dict[str, Any],
) -> None:
    """打印 mock 输入摘要。"""
    student_payload = safe_dict(student_profile_result.get("profile_input_payload"))
    normalized_education = safe_dict(student_payload.get("normalized_education"))

    print("\n========== Step 1/4 输入读取预览 ==========")
    print("student degree:", normalized_education.get("degree", ""))
    print("student major:", normalized_education.get("major_std", ""))
    print("student occupation hints:", safe_list(safe_dict(student_payload.get("normalized_profile")).get("occupation_hints")))
    print("target job:", job_profile_result.get("standard_job_name", ""))
    print("vertical paths:", safe_list(job_profile_result.get("vertical_paths")))
    print("transfer paths:", safe_list(job_profile_result.get("transfer_paths")))
    print("overall_match_score:", job_match_result.get("overall_match_score", 0.0))
    print("score_level:", job_match_result.get("score_level", ""))
    print("state path:", str(STATE_PATH))


def run_builder_demo(
    student_profile_result: Dict[str, Any],
    job_profile_result: Dict[str, Any],
    job_match_result: Dict[str, Any],
) -> Dict[str, Any]:
    """演示调用 builder 构造 career_plan_input_payload。"""
    LOGGER.info("Step 2/4: build career_plan_input_payload")
    payload = build_career_plan_input_payload(
        student_profile_result=student_profile_result,
        job_profile_result=job_profile_result,
        job_match_result=job_match_result,
        output_path=BUILDER_OUTPUT_PATH,
    )

    print("\n========== Step 2/4 Builder 输出摘要 ==========")
    print("target_job_name:", payload.get("target_job_name", ""))
    print("candidate_goal_jobs:", safe_list(payload.get("candidate_goal_jobs")))
    print("direct_path_options_count:", len(safe_list(payload.get("direct_path_options"))))
    print("transition_path_options_count:", len(safe_list(payload.get("transition_path_options"))))
    print("gap_analysis_count:", len(safe_list(payload.get("gap_analysis"))))
    print("direct_path_feasible:", safe_dict(payload.get("planning_constraints")).get("direct_path_feasible"))
    print("transition_first_recommended:", safe_dict(payload.get("planning_constraints")).get("transition_first_recommended"))
    print("build_warnings:", safe_list(payload.get("build_warnings")))
    print("builder output:", str(BUILDER_OUTPUT_PATH))
    return payload


def run_selector_demo(career_plan_input_payload: Dict[str, Any]) -> Dict[str, Any]:
    """演示调用 selector 做规则目标选择和路径筛选。"""
    LOGGER.info("Step 3/4: select career target and paths")
    selector_result = select_career_path_plan(
        career_plan_input_payload=career_plan_input_payload,
        output_path=SELECTOR_OUTPUT_PATH,
    )

    print("\n========== Step 3/4 Selector 规则决策摘要 ==========")
    print("primary_target_job:", selector_result.get("primary_target_job", ""))
    print("secondary_target_jobs:", safe_list(selector_result.get("secondary_target_jobs")))
    print("goal_positioning:", selector_result.get("goal_positioning", ""))
    print("direct_path:", safe_list(selector_result.get("direct_path")))
    print("transition_path:", safe_list(selector_result.get("transition_path")))
    print("long_term_path:", safe_list(selector_result.get("long_term_path")))
    print("path_strategy:", selector_result.get("path_strategy", ""))
    print("risk_notes:", safe_list(selector_result.get("risk_notes")))
    print("selector output:", str(SELECTOR_OUTPUT_PATH))
    return selector_result


def run_service_demo(
    student_profile_result: Dict[str, Any],
    job_profile_result: Dict[str, Any],
    job_match_result: Dict[str, Any],
    student_state: Dict[str, Any],
) -> Dict[str, Any]:
    """演示调用 service + 统一大模型接口，并写回 student.json。"""
    LOGGER.info("Step 4/4: run career_path_plan_service and update student.json")
    result = run_career_path_plan_service(
        student_profile_result=student_profile_result,
        job_profile_result=job_profile_result,
        job_match_result=job_match_result,
        student_state=student_state,
        context_data={
            "graph_context": {
                "mock_note": "这里预留 Neo4j 图谱路径上下文，当前 demo 使用 mock。",
                "related_jobs": ["数据分析实习生", "BI分析师", "商业分析师"],
            },
            "sql_context": {
                "mock_note": "这里预留 SQL 薪资、城市、岗位统计上下文，当前 demo 使用 mock。",
                "target_city_salary_hint": {"杭州": "10k-18k", "上海": "15k-22k"},
            },
        },
        state_path=STATE_PATH,
        builder_output_path=BUILDER_OUTPUT_PATH,
        selector_output_path=SELECTOR_OUTPUT_PATH,
        service_output_path=SERVICE_OUTPUT_PATH,
        extra_context={"demo_name": "main_career_path_plan_demo"},
    )

    latest_state = StateManager().load_state(STATE_PATH)
    state_plan_result = safe_dict(latest_state.get("career_path_plan_result"))

    print("\n========== Step 4/4 Service + LLM 最终结果摘要 ==========")
    print("primary_target_job:", result.get("primary_target_job", ""))
    print("secondary_target_jobs:", safe_list(result.get("secondary_target_jobs")))
    print("goal_positioning:", result.get("goal_positioning", ""))
    print("goal_reason:", result.get("goal_reason", ""))
    print("direct_path:", safe_list(result.get("direct_path")))
    print("transition_path:", safe_list(result.get("transition_path")))
    print("long_term_path:", safe_list(result.get("long_term_path")))
    print("path_strategy:", result.get("path_strategy", ""))
    print("short_term_plan:", safe_list(result.get("short_term_plan")))
    print("mid_term_plan:", safe_list(result.get("mid_term_plan")))
    print("decision_summary:", result.get("decision_summary", ""))
    print("fallback_strategy:", result.get("fallback_strategy", ""))
    print("risk_and_gap:", safe_list(result.get("risk_and_gap")))
    print("state career_path_plan_result exists:", bool(state_plan_result))
    print("service output:", str(SERVICE_OUTPUT_PATH))
    return result


def main() -> None:
    """demo 主入口。"""
    setup_logging()
    LOGGER.info("Start main_career_path_plan_demo")

    student_profile_result = build_demo_student_profile_result()
    job_profile_result = build_demo_job_profile_result()
    job_match_result = build_demo_job_match_result()
    student_state = init_mock_student_state(
        student_profile_result=student_profile_result,
        job_profile_result=job_profile_result,
        job_match_result=job_match_result,
        state_path=STATE_PATH,
    )

    print_input_preview(student_profile_result, job_profile_result, job_match_result)
    career_plan_input_payload = run_builder_demo(
        student_profile_result,
        job_profile_result,
        job_match_result,
    )
    run_selector_demo(career_plan_input_payload)
    run_service_demo(
        student_profile_result=student_profile_result,
        job_profile_result=job_profile_result,
        job_match_result=job_match_result,
        student_state=student_state,
    )

    LOGGER.info("main_career_path_plan_demo finished")


if __name__ == "__main__":
    main()
