"""
main_demo.py

使用 mock 数据演示统一大模型接口层的链式调用流程：
resume_parse -> job_profile -> student_profile -> job_match -> career_path_plan -> career_report
"""

from __future__ import annotations

import json
from pathlib import Path

if __package__:
    from .llm_service import LLMService
    from .schemas import TaskType
    from .state_manager import StateManager
else:
    import sys

    current_dir = Path(__file__).resolve().parent
    parent_dir = current_dir.parent
    if str(parent_dir) not in sys.path:
        sys.path.insert(0, str(parent_dir))

    from llm_interface_layer.llm_service import LLMService
    from llm_interface_layer.schemas import TaskType
    from llm_interface_layer.state_manager import StateManager


def print_step(title: str, payload: dict) -> None:
    """简洁打印每一步结果。"""
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def build_mock_context() -> dict:
    """为接口层预留 graph_context / sql_context 接入结构。"""
    return {
        "graph_context": {
            "job_skill_relations": [
                {"job": "数据分析师", "skill": "Python"},
                {"job": "数据分析师", "skill": "SQL"},
            ],
            "promotion_paths": ["数据分析师 -> 高级数据分析师 -> 数据分析负责人"],
            "transfer_paths": ["数据分析师 -> BI分析师", "数据分析师 -> 数据产品经理"],
        },
        "sql_context": {
            "salary_stats": {"数据分析师": {"min": 9000, "max": 18000, "median": 13000}},
            "city_distribution": {"深圳": 120, "广州": 95, "杭州": 80},
            "company_samples": ["某教育科技公司", "某互联网数据平台公司"],
        },
    }


def main() -> None:
    state_path = Path("outputs/state/student.json")
    state_manager = StateManager()
    state_manager.init_state(state_path=state_path, overwrite=True)

    service = LLMService(state_manager=state_manager)
    context_data = build_mock_context()

    # 1. 简历解析
    step_1 = service.run_task_and_update_state(
        task_type=TaskType.RESUME_PARSE,
        input_data={
            "resume_text": (
                "姓名：张三，手机：13800000000，邮箱：student@example.com。"
                "本科，计算机科学与技术专业，2026届。"
                "掌握 Python、SQL，有课程推荐系统项目经历和数据分析实习经历。"
            )
        },
        context_data=context_data,
        state_path=state_path,
    )
    print_step("Step 1 - resume_parse", step_1["result"])

    # 2. 岗位画像生成
    step_2 = service.run_task_and_update_state(
        task_type=TaskType.JOB_PROFILE,
        input_data={
            "target_job_name": "数据分析师",
            "job_context": {
                "industry": "互联网/教育科技",
                "city": "深圳",
            },
        },
        context_data=context_data,
        state_path=state_path,
    )
    print_step("Step 2 - job_profile", step_2["result"])

    # 3. 学生就业能力画像
    step_3 = service.run_task_and_update_state(
        task_type=TaskType.STUDENT_PROFILE,
        input_data={
            "student_json": step_2["student_state"],
        },
        context_data=context_data,
        state_path=state_path,
    )
    print_step("Step 3 - student_profile", step_3["result"])

    # 4. 人岗匹配
    step_4 = service.run_task_and_update_state(
        task_type=TaskType.JOB_MATCH,
        input_data={
            "job_profile": step_3["student_state"].get("job_profile_result", {}),
            "student_profile": step_3["student_state"].get("student_profile_result", {}),
        },
        context_data=context_data,
        state_path=state_path,
    )
    print_step("Step 4 - job_match", step_4["result"])

    # 5. 职业路径规划
    step_5 = service.run_task_and_update_state(
        task_type=TaskType.CAREER_PATH_PLAN,
        input_data={
            "student_profile_result": step_4["student_state"].get("student_profile_result", {}),
            "job_profile_result": step_4["student_state"].get("job_profile_result", {}),
            "job_match_result": step_4["student_state"].get("job_match_result", {}),
            "graph_context": context_data.get("graph_context", {}),
            "sql_context": context_data.get("sql_context", {}),
        },
        context_data=context_data,
        state_path=state_path,
    )
    print_step("Step 5 - career_path_plan", step_5["result"])

    # 6. 职业报告生成
    step_6 = service.run_task_and_update_state(
        task_type=TaskType.CAREER_REPORT,
        input_data={
            "all_task_results": step_5["student_state"],
        },
        context_data=context_data,
        state_path=state_path,
    )
    print_step("Step 6 - career_report", step_6["result"])

    final_state = state_manager.load_state(state_path=state_path)
    print_step("Final student.json state", final_state)
    print(f"\nstudent.json saved to: {state_path.resolve()}")


if __name__ == "__main__":
    main()

