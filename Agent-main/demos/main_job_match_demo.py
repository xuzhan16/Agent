"""
main_job_match_demo.py

job_match 模块最小可运行 demo。

演示目标：
1. 使用 mock 的 student_profile_result 和 job_profile_result 作为输入；
2. 演示将输入写入/读取 student.json；
3. 演示调用 job_match_builder 构造 match_input_payload；
4. 演示调用 job_match_scorer 进行规则匹配与评分；
5. 演示调用 job_match_service，通过统一大模型接口补充解释性结论；
6. 演示将 job_match_result 写回 student.json；
7. 打印最终 job_match_result 摘要。
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

from job_match.job_match_builder import (
    build_demo_job_profile_result,
    build_demo_student_profile_result,
    build_match_input_payload,
)
from job_match.job_match_scorer import score_match_input_payload
from job_match.job_match_service import run_job_match_service
from llm_interface_layer.state_manager import StateManager


LOGGER = logging.getLogger(__name__)

STATE_PATH = Path("outputs/state/main_job_match_demo_student.json")
BUILDER_OUTPUT_PATH = Path("outputs/state/main_job_match_demo_input_payload.json")
SCORER_OUTPUT_PATH = Path("outputs/state/main_job_match_demo_score_result.json")
SERVICE_OUTPUT_PATH = Path("outputs/state/main_job_match_demo_service_result.json")


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


def save_json(data: Dict[str, Any], output_path: str | Path) -> None:
    """保存 JSON 文件。"""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def init_mock_student_state(
    student_profile_result: Dict[str, Any],
    job_profile_result: Dict[str, Any],
    state_path: str | Path,
) -> Dict[str, Any]:
    """
    初始化一份 mock student.json，并写入 student_profile_result / job_profile_result。
    """
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
    return student_state


def print_input_preview(
    student_profile_result: Dict[str, Any],
    job_profile_result: Dict[str, Any],
) -> None:
    """打印 mock 输入摘要。"""
    student_payload = safe_dict(student_profile_result.get("profile_input_payload"))
    normalized_education = safe_dict(student_payload.get("normalized_education"))
    normalized_profile = safe_dict(student_payload.get("normalized_profile"))

    print("\n========== Step 1/4 输入读取预览 ==========")
    print("student degree:", normalized_education.get("degree", ""))
    print("student major:", normalized_education.get("major_std", ""))
    print("student hard skills:", safe_list(normalized_profile.get("hard_skills")))
    print("student tool skills:", safe_list(normalized_profile.get("tool_skills")))
    print("target job:", job_profile_result.get("standard_job_name", ""))
    print("job hard skills:", safe_list(job_profile_result.get("hard_skills")))
    print("job tool skills:", safe_list(job_profile_result.get("tools_or_tech_stack")))
    print("state path:", str(STATE_PATH))


def run_builder_demo(
    student_profile_result: Dict[str, Any],
    job_profile_result: Dict[str, Any],
) -> Dict[str, Any]:
    """演示调用 builder 构造 match_input_payload。"""
    LOGGER.info("Step 2/4: build match_input_payload")
    match_input_payload = build_match_input_payload(
        student_profile_result=student_profile_result,
        job_profile_result=job_profile_result,
        output_path=BUILDER_OUTPUT_PATH,
    )

    comparable_schema = safe_dict(match_input_payload.get("comparable_schema"))
    print("\n========== Step 2/4 Builder 输出摘要 ==========")
    print("education schema:", safe_dict(comparable_schema.get("education")))
    print("major schema:", safe_dict(comparable_schema.get("major")))
    print("hard skill schema:", safe_dict(comparable_schema.get("hard_skills")))
    print("tool skill schema:", safe_dict(comparable_schema.get("tool_skills")))
    print("soft skill schema:", safe_dict(comparable_schema.get("soft_skills")))
    print("certificates schema:", safe_dict(comparable_schema.get("certificates")))
    print("practice schema:", safe_dict(comparable_schema.get("practice_experience")))
    print("career direction schema:", safe_dict(comparable_schema.get("career_direction")))
    print("build warnings:", safe_list(match_input_payload.get("build_warnings")))
    print("builder output:", str(BUILDER_OUTPUT_PATH))
    return match_input_payload


def run_scorer_demo(match_input_payload: Dict[str, Any]) -> Dict[str, Any]:
    """演示调用 scorer 做规则评分。"""
    LOGGER.info("Step 3/4: score match_input_payload")
    score_result = score_match_input_payload(
        match_input_payload=match_input_payload,
        output_path=SCORER_OUTPUT_PATH,
    )

    print("\n========== Step 3/4 Scorer 规则评分摘要 ==========")
    print("basic_requirement_score:", score_result.get("basic_requirement_score", 0.0))
    print("vocational_skill_score:", score_result.get("vocational_skill_score", 0.0))
    print("professional_quality_score:", score_result.get("professional_quality_score", 0.0))
    print("development_potential_score:", score_result.get("development_potential_score", 0.0))
    print("overall_match_score:", score_result.get("overall_match_score", 0.0))
    print("score_level:", score_result.get("score_level", ""))
    print("matched_items_count:", len(safe_list(score_result.get("matched_items"))))
    print("missing_items_count:", len(safe_list(score_result.get("missing_items"))))
    print("rule_summary:", score_result.get("rule_summary", ""))
    print("scorer output:", str(SCORER_OUTPUT_PATH))
    return score_result


def run_service_demo(
    student_profile_result: Dict[str, Any],
    job_profile_result: Dict[str, Any],
    student_state: Dict[str, Any],
) -> Dict[str, Any]:
    """演示调用 service + 统一大模型接口，并写回 student.json。"""
    LOGGER.info("Step 4/4: run job_match_service and update student.json")
    job_match_result = run_job_match_service(
        student_profile_result=student_profile_result,
        job_profile_result=job_profile_result,
        student_state=student_state,
        context_data={
            "graph_context": {
                "mock_note": "预留 Neo4j 图谱上下文，当前 demo 使用 mock。",
                "related_jobs": ["数据分析实习生", "商业分析师", "BI分析师"],
            },
            "sql_context": {
                "mock_note": "预留 SQL 明细上下文，当前 demo 使用 mock。",
                "target_city_salary_hint": {
                    "杭州": "10k-18k",
                    "上海": "15k-22k",
                },
            },
        },
        state_path=STATE_PATH,
        builder_output_path=BUILDER_OUTPUT_PATH,
        scorer_output_path=SCORER_OUTPUT_PATH,
        service_output_path=SERVICE_OUTPUT_PATH,
        extra_context={"demo_name": "main_job_match_demo"},
    )

    latest_state = StateManager().load_state(STATE_PATH)
    state_job_match_result = safe_dict(latest_state.get("job_match_result"))

    print("\n========== Step 4/4 Service + LLM 最终结果摘要 ==========")
    print("overall_match_score:", job_match_result.get("overall_match_score", 0.0))
    print("score_level:", job_match_result.get("score_level", ""))
    print("strengths:", safe_list(job_match_result.get("strengths")))
    print("weaknesses:", safe_list(job_match_result.get("weaknesses")))
    print("improvement_suggestions:", safe_list(job_match_result.get("improvement_suggestions")))
    print("recommendation:", job_match_result.get("recommendation", ""))
    print("analysis_summary:", job_match_result.get("analysis_summary", ""))
    print("state job_match_result exists:", bool(state_job_match_result))
    print("service output:", str(SERVICE_OUTPUT_PATH))
    return job_match_result


def main() -> None:
    """Demo 主入口。"""
    setup_logging()
    LOGGER.info("Start main_job_match_demo")

    student_profile_result = build_demo_student_profile_result()
    job_profile_result = build_demo_job_profile_result()
    student_state = init_mock_student_state(
        student_profile_result=student_profile_result,
        job_profile_result=job_profile_result,
        state_path=STATE_PATH,
    )

    print_input_preview(student_profile_result, job_profile_result)
    match_input_payload = run_builder_demo(student_profile_result, job_profile_result)
    run_scorer_demo(match_input_payload)
    run_service_demo(student_profile_result, job_profile_result, student_state)

    LOGGER.info("main_job_match_demo finished")


if __name__ == "__main__":
    main()
