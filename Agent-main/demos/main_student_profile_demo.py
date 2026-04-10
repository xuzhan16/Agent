"""
main_student_profile_demo.py

最小可运行 demo：
演示 student_profile 模块如何从 resume_parse_result 生成 student_profile_result。

流程：
1. 构造 mock student.json；
2. 读取 state；
3. 调用 student_profile_builder 生成中间特征；
4. 调用 student_profile_scorer 生成规则评分；
5. 调用统一大模型接口（当前可走 mock）补充画像；
6. 合并结果并写回 student.json；
7. 打印最终 student_profile_result 摘要。
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from llm_interface_layer.state_manager import StateManager
from student_profile.student_profile_builder import build_profile_input_payload_from_state
from student_profile.student_profile_scorer import score_student_profile_payload
from student_profile.student_profile_service import StudentProfileService, merge_rule_and_llm_result


DEMO_STATE_PATH = Path("outputs/state/main_student_profile_demo_student.json")
DEMO_BUILDER_OUTPUT_PATH = Path("outputs/state/main_student_profile_demo_builder_payload.json")
DEMO_SCORER_OUTPUT_PATH = Path("outputs/state/main_student_profile_demo_score_result.json")


def build_mock_student_state() -> Dict[str, Any]:
    """构造一份 mock student.json 数据。"""
    return {
        "basic_info": {
            "name": "李同学",
            "gender": "女",
            "phone": "13912345678",
            "email": "li.student@example.com",
            "school": "华东某某大学",
            "major": "数据科学与大数据技术",
            "degree": "本科",
            "graduation_year": "2026",
        },
        "resume_parse_result": {
            "basic_info": {
                "name": "李同学",
                "gender": "女",
                "phone": "13912345678",
                "email": "li.student@example.com",
                "school": "华东某某大学",
                "major": "数据科学与大数据技术",
                "degree": "本科",
                "graduation_year": "2026",
            },
            "education_experience": [
                {
                    "school": "华东某某大学",
                    "major": "数据科学与大数据技术",
                    "degree": "本科",
                    "start_date": "2022.09",
                    "end_date": "2026.06",
                    "description": "主修机器学习、数据库系统、数据挖掘、统计建模等课程。",
                }
            ],
            "internship_experience": [
                {
                    "company_name": "某互联网公司",
                    "position": "数据分析实习生",
                    "start_date": "2025.07",
                    "end_date": "2025.10",
                    "description": "使用 SQL 和 Excel 搭建业务看板，参与用户留存分析和周报自动化。",
                }
            ],
            "project_experience": [
                {
                    "project_name": "校园招聘岗位分析系统",
                    "role": "数据开发与分析",
                    "start_date": "2025.03",
                    "end_date": "2025.06",
                    "description": "使用 Python 清洗招聘数据，基于 SQL 做岗位统计分析，并输出可视化报告。",
                },
                {
                    "project_name": "二手商品推荐实验项目",
                    "role": "算法实验",
                    "start_date": "2024.10",
                    "end_date": "2025.01",
                    "description": "基于机器学习模型做特征构造和推荐效果对比实验。",
                },
            ],
            "skills": ["Python", "SQL", "Excel", "机器学习", "Tableau"],
            "certificates": ["CET-6", "计算机二级"],
            "awards": ["大学生数据分析竞赛校级二等奖"],
            "self_evaluation": "对数据分析岗位有明确兴趣，学习能力强，沟通协作较好，喜欢用数据支持业务决策。",
            "target_job_intention": "数据分析师",
            "raw_resume_text": "这是一份 mock 简历文本，用于 student_profile demo。",
            "parse_warnings": [],
        },
        "job_profile_result": {},
        "student_profile_result": {},
        "job_match_result": {},
        "career_path_plan_result": {},
        "career_report_result": {},
    }


def save_json(data: Dict[str, Any], output_path: Path) -> None:
    """保存 JSON 文件。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def print_section(title: str, data: Dict[str, Any]) -> None:
    """统一打印 demo 分段结果。"""
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)
    print(json.dumps(data, ensure_ascii=False, indent=2))


def build_final_summary(student_profile_result: Dict[str, Any]) -> Dict[str, Any]:
    """提取最终 student_profile_result 摘要，便于快速查看结果。"""
    return {
        "complete_score": student_profile_result.get("complete_score", 0.0),
        "competitiveness_score": student_profile_result.get("competitiveness_score", 0.0),
        "score_level": student_profile_result.get("score_level", ""),
        "soft_skills": student_profile_result.get("soft_skills", []),
        "strengths_top5": student_profile_result.get("strengths", [])[:5],
        "weaknesses_top5": student_profile_result.get("weaknesses", [])[:5],
        "missing_dimensions": student_profile_result.get("missing_dimensions", []),
        "summary": student_profile_result.get("summary", ""),
    }


def main() -> None:
    """执行 student_profile 最小可运行 demo。"""
    state_manager = StateManager()
    service = StudentProfileService(state_manager=state_manager)

    # Step 1: 写入 mock student.json
    mock_state = build_mock_student_state()
    save_json(mock_state, DEMO_STATE_PATH)
    print(f"Mock student.json saved to: {DEMO_STATE_PATH.resolve()}")

    # Step 2: 读取 state
    student_state = state_manager.load_state(DEMO_STATE_PATH)
    print_section("Step 2 - Loaded student state", {
        "basic_info": student_state.get("basic_info", {}),
        "resume_parse_result_keys": list(student_state.get("resume_parse_result", {}).keys()),
    })

    # Step 3: 生成中间特征
    profile_input_payload = build_profile_input_payload_from_state(student_state)
    save_json(profile_input_payload, DEMO_BUILDER_OUTPUT_PATH)
    print_section("Step 3 - Builder profile_input_payload", {
        "normalized_education": profile_input_payload.get("normalized_education", {}),
        "normalized_profile": profile_input_payload.get("normalized_profile", {}),
        "practice_profile": profile_input_payload.get("practice_profile", {}),
        "evidence_summary": profile_input_payload.get("evidence_summary", {}),
    })

    # Step 4: 规则评分
    rule_score_result = score_student_profile_payload(profile_input_payload)
    save_json(rule_score_result, DEMO_SCORER_OUTPUT_PATH)
    print_section("Step 4 - Rule score result", rule_score_result)

    # Step 5: 调用统一大模型接口（当前可 mock）
    llm_result = service.call_student_profile_llm(
        profile_input_payload=profile_input_payload,
        rule_score_result=rule_score_result,
        student_state=student_state,
        context_data={
            "graph_context": {
                "mock_note": "这里预留知识图谱上下文，demo 中只做 mock。",
                "related_jobs": ["数据分析师", "BI分析师", "数据运营"],
            },
            "sql_context": {
                "mock_note": "这里预留 SQL 明细上下文，demo 中只做 mock。",
                "city_salary_reference": {"上海": "12k-20k", "杭州": "10k-18k"},
            },
        },
        extra_context={
            "demo_name": "main_student_profile_demo",
        },
    )
    print_section("Step 5 - LLM student_profile result", llm_result)

    # Step 6: 合并规则结果和模型结果，并写回 student.json
    merged_profile_result = merge_rule_and_llm_result(
        profile_input_payload=profile_input_payload,
        rule_score_result=rule_score_result,
        llm_result=llm_result,
        build_warnings=[],
    )
    updated_state = service.update_student_state(
        merged_profile_result=merged_profile_result,
        student_state=student_state,
        state_path=DEMO_STATE_PATH,
    )
    print_section("Step 6 - Final student_profile_result summary", build_final_summary(
        updated_state.get("student_profile_result", {})
    ))

    print(f"\nUpdated student.json saved to: {DEMO_STATE_PATH.resolve()}")
    print(f"Builder payload saved to: {DEMO_BUILDER_OUTPUT_PATH.resolve()}")
    print(f"Rule score result saved to: {DEMO_SCORER_OUTPUT_PATH.resolve()}")


if __name__ == "__main__":
    main()




