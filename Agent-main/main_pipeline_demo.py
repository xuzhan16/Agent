"""
main_pipeline_demo.py

基于 AI 的大学生职业规划智能体：单样本最小闭环联调主流程。

主流程只做 5 件事：
1. 按固定顺序串联六个业务模块；
2. 每一步读取上一阶段 state；
3. 调用对应 service；
4. 将本阶段结果统一写回 student.json；
5. 打印阶段摘要和最终 career_report_result 摘要。

不在这里重写任何单个业务模块内部实现。
如果某个模块在当前环境下无法 import，则只做最小静态 mock 兜底，保证闭环可演示。
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from pipeline_utils import (
    build_result_summary,
    ensure_stage_result_exists,
    format_pipeline_exception,
    print_module_log,
    print_stage_summary,
    safe_dict,
    safe_list,
    save_stage_snapshot,
)



PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from llm_interface_layer.state_manager import StateManager
except ModuleNotFoundError:
    from state_manager import StateManager  # type: ignore

try:
    from resume_parse_module.resume_parser import (
        parse_resume_with_llm,
        process_resume_file,
        update_student_state_with_resume_result,
    )
except ModuleNotFoundError:
    from resume_parse_service import (  # type: ignore
        parse_resume_with_llm,
        process_resume_file,
        update_student_state_with_resume_result,
    )

try:
    from student_profile.student_profile_service import run_student_profile_service
except ModuleNotFoundError:
    from student_profile_service import run_student_profile_service  # type: ignore

try:
    from job_profile.job_profile_service import run_job_profile_service
except ModuleNotFoundError:
    from job_profile_service import run_job_profile_service  # type: ignore

try:
    from job_match.job_match_service import run_job_match_service
except ModuleNotFoundError:
    from job_match_service import run_job_match_service  # type: ignore

try:
    from career_path_plan.career_path_plan_service import run_career_path_plan_service
except ModuleNotFoundError:
    from career_path_plan_service import run_career_path_plan_service  # type: ignore

try:
    from career_report.career_report_service import run_career_report_service
except ModuleNotFoundError:
    from career_report_service import run_career_report_service  # type: ignore

try:
    import pandas as pd
except Exception:
    pd = None


TOTAL_STEPS = 8


@dataclass
class PipelineDemoConfig:
    """单样本最小闭环运行配置。"""

    state_path: Path = Path("outputs/state/student.json")
    snapshot_dir: Path = Path("outputs/state/pipeline_snapshots")
    resume_file_path: Optional[Path] = None
    resume_text: str = ""
    target_job_name: str = "数据分析师"
    job_group_json_path: Optional[Path] = None
    reset_state: bool = False


def build_demo_resume_text(target_job_name: str) -> str:
    """构造最小可运行 demo 简历文本。"""
    return f"""
姓名：李同学
学校：华东某某大学
专业：数据科学与大数据技术
学历：本科
毕业年份：2026
求职意向：{target_job_name}

实习经历：
某互联网公司 数据分析实习生
使用 SQL、Python、Excel 完成业务分析和数据看板建设。

项目经历：
校园招聘岗位分析系统
使用 Python 和 SQL 完成岗位数据清洗、统计分析和可视化报告输出。

技能证书：
Python、SQL、Excel、Tableau、机器学习、CET-6
""".strip()


def build_demo_job_group_records(target_job_name: str) -> list[Dict[str, Any]]:
    """构造最小可运行 demo 岗位组数据。"""
    return [
        {
            "job_name": "数据分析师",
            "standard_job_name": target_job_name,
            "city": "上海",
            "industry": "互联网",
            "salary_min_month": 12000,
            "salary_max_month": 20000,
            "job_desc": "要求 SQL、Python、Excel、Tableau，统计学/计算机相关专业优先，具备沟通协作和分析能力。",
        },
        {
            "job_name": "BI分析师",
            "standard_job_name": target_job_name,
            "city": "杭州",
            "industry": "互联网",
            "salary_min_month": 10000,
            "salary_max_month": 18000,
            "job_desc": "负责指标分析和 BI 看板建设，要求 SQL、Excel、Power BI，有数据分析实习或项目经验优先。",
        },
    ]


def load_job_group_data(config: PipelineDemoConfig) -> Any:
    """加载岗位组数据，优先使用外部 JSON，否则使用内置 demo 数据。"""
    if config.job_group_json_path and config.job_group_json_path.exists():
        with config.job_group_json_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        records = payload if isinstance(payload, list) else safe_list(payload.get("job_group_data"))
    else:
        records = build_demo_job_group_records(config.target_job_name)

    if pd is not None:
        return pd.DataFrame(records)
    return records


def init_or_load_state(state_manager: StateManager, config: PipelineDemoConfig) -> Dict[str, Any]:
    """加载或初始化 student.json。"""
    config.state_path.parent.mkdir(parents=True, exist_ok=True)
    config.snapshot_dir.mkdir(parents=True, exist_ok=True)
    if config.reset_state:
        return state_manager.init_state(state_path=config.state_path, overwrite=True)
    return state_manager.load_state(config.state_path)


def save_stage_result_to_state(
    state_manager: StateManager,
    task_type: str,
    stage_result: Dict[str, Any],
    config: PipelineDemoConfig,
) -> Dict[str, Any]:
    """统一写回 student.json，并保存阶段快照。"""
    latest_state = state_manager.load_state(config.state_path)
    latest_state = state_manager.update_state(
        task_type=task_type,
        task_result=stage_result,
        state_path=config.state_path,
        student_state=latest_state,
    )
    if task_type == "resume_parse":
        latest_state["basic_info"] = safe_dict(stage_result.get("basic_info"))
        state_manager.save_state(latest_state, config.state_path)
    snapshot_path = save_stage_snapshot(f"{task_type}_result", stage_result, config.snapshot_dir)
    print_stage_summary(task_type, stage_result)
    print_module_log(task_type, f"已写回 student.json，快照文件: {snapshot_path}", status="SUCCESS")
    return latest_state


def step_resume_parse(
    state_manager: StateManager,
    student_state: Dict[str, Any],
    config: PipelineDemoConfig,
) -> Dict[str, Any]:
    """Step 2: resume_parse。"""
    print_module_log("resume_parse", "开始简历解析", step_index=2, total_steps=TOTAL_STEPS)

    if config.resume_file_path:
        response_bundle = process_resume_file(
            file_path=config.resume_file_path,
            state_path=config.state_path,
            student_state=student_state,
        )
        resume_parse_result = safe_dict(response_bundle.get("resume_parse_result"))
    else:
        resume_parse_result = parse_resume_with_llm(
            resume_text=config.resume_text or build_demo_resume_text(config.target_job_name),
            student_state=student_state,
            extra_context={"demo_name": "main_pipeline_demo_resume_parse"},
        )
        update_student_state_with_resume_result(
            resume_parse_result=resume_parse_result,
            state_path=config.state_path,
            student_state=student_state,
            state_manager=state_manager,
        )

    return save_stage_result_to_state(state_manager, "resume_parse", resume_parse_result, config)


def step_student_profile(state_manager: StateManager, config: PipelineDemoConfig) -> Dict[str, Any]:
    """Step 3: student_profile。"""
    print_module_log(
        "student_profile",
        "读取 resume_parse_result，生成学生就业能力画像",
        step_index=3,
        total_steps=TOTAL_STEPS,
    )
    student_state = state_manager.load_state(config.state_path)
    ensure_stage_result_exists(student_state, "resume_parse_result", "student_profile")

    response_bundle = run_student_profile_service(
        state_path=config.state_path,
        builder_output_path=config.snapshot_dir / "student_profile_builder_payload.json",
        scorer_output_path=config.snapshot_dir / "student_profile_scorer_result.json",
        service_output_path=config.snapshot_dir / "student_profile_service_result.json",
        extra_context={"demo_name": "main_pipeline_demo_student_profile"},
    )
    student_profile_result = safe_dict(response_bundle.get("student_profile_result"))
    return save_stage_result_to_state(state_manager, "student_profile", student_profile_result, config)


def step_job_profile(state_manager: StateManager, config: PipelineDemoConfig) -> Dict[str, Any]:
    """Step 4: job_profile。"""
    print_module_log(
        "job_profile",
        f"读取目标岗位组数据，生成岗位画像: {config.target_job_name}",
        step_index=4,
        total_steps=TOTAL_STEPS,
    )
    student_state = state_manager.load_state(config.state_path)
    ensure_stage_result_exists(student_state, "student_profile_result", "job_profile")

    job_group_data = load_job_group_data(config)
    job_profile_result = run_job_profile_service(
        df=job_group_data,
        standard_job_name=config.target_job_name,
        output_path=config.snapshot_dir / "job_profile_service_result.json",
        extra_context={"demo_name": "main_pipeline_demo_job_profile"},
    )

    return save_stage_result_to_state(state_manager, "job_profile", safe_dict(job_profile_result), config)


def step_job_match(state_manager: StateManager, config: PipelineDemoConfig) -> Dict[str, Any]:
    """Step 5: job_match。"""
    print_module_log(
        "job_match",
        "读取 student_profile_result 和 job_profile_result，执行人岗匹配",
        step_index=5,
        total_steps=TOTAL_STEPS,
    )
    student_state = state_manager.load_state(config.state_path)
    student_profile_result = ensure_stage_result_exists(student_state, "student_profile_result", "job_match")
    job_profile_result = ensure_stage_result_exists(student_state, "job_profile_result", "job_match")

    job_match_result = run_job_match_service(
        student_profile_result=student_profile_result,
        job_profile_result=job_profile_result,
        student_state=student_state,
        state_path=config.state_path,
        builder_output_path=config.snapshot_dir / "job_match_builder_payload.json",
        scorer_output_path=config.snapshot_dir / "job_match_scorer_result.json",
        service_output_path=config.snapshot_dir / "job_match_service_result.json",
        extra_context={"demo_name": "main_pipeline_demo_job_match"},
    )
    return save_stage_result_to_state(state_manager, "job_match", safe_dict(job_match_result), config)


def step_career_path_plan(state_manager: StateManager, config: PipelineDemoConfig) -> Dict[str, Any]:
    """Step 6: career_path_plan。"""
    print_module_log(
        "career_path_plan",
        "读取 student_profile_result / job_profile_result / job_match_result，生成职业路径规划",
        step_index=6,
        total_steps=TOTAL_STEPS,
    )
    student_state = state_manager.load_state(config.state_path)
    student_profile_result = ensure_stage_result_exists(
        student_state,
        "student_profile_result",
        "career_path_plan",
    )
    job_profile_result = ensure_stage_result_exists(
        student_state,
        "job_profile_result",
        "career_path_plan",
    )
    job_match_result = ensure_stage_result_exists(
        student_state,
        "job_match_result",
        "career_path_plan",
    )

    career_path_plan_result = run_career_path_plan_service(
        student_profile_result=student_profile_result,
        job_profile_result=job_profile_result,
        job_match_result=job_match_result,
        student_state=student_state,
        state_path=config.state_path,
        builder_output_path=config.snapshot_dir / "career_path_plan_builder_payload.json",
        selector_output_path=config.snapshot_dir / "career_path_plan_selector_result.json",
        service_output_path=config.snapshot_dir / "career_path_plan_service_result.json",
        extra_context={"demo_name": "main_pipeline_demo_career_path_plan"},
    )
    return save_stage_result_to_state(
        state_manager,
        "career_path_plan",
        safe_dict(career_path_plan_result),
        config,
    )


def step_career_report(state_manager: StateManager, config: PipelineDemoConfig) -> Dict[str, Any]:
    """Step 7: career_report。"""
    print_module_log(
        "career_report",
        "读取全部上游结果，生成最终职业规划报告",
        step_index=7,
        total_steps=TOTAL_STEPS,
    )
    student_state = state_manager.load_state(config.state_path)
    student_profile_result = ensure_stage_result_exists(student_state, "student_profile_result", "career_report")
    job_profile_result = ensure_stage_result_exists(student_state, "job_profile_result", "career_report")
    job_match_result = ensure_stage_result_exists(student_state, "job_match_result", "career_report")
    career_path_plan_result = ensure_stage_result_exists(
        student_state,
        "career_path_plan_result",
        "career_report",
    )

    career_report_result = run_career_report_service(
        student_profile_result=student_profile_result,
        job_profile_result=job_profile_result,
        job_match_result=job_match_result,
        career_path_plan_result=career_path_plan_result,
        student_state=student_state,
        state_path=config.state_path,
        builder_output_path=config.snapshot_dir / "career_report_builder_payload.json",
        formatter_output_path=config.snapshot_dir / "career_report_formatter_result.json",
        service_output_path=config.snapshot_dir / "career_report_service_result.json",
        extra_context={"demo_name": "main_pipeline_demo_career_report"},
    )
    return save_stage_result_to_state(
        state_manager,
        "career_report",
        safe_dict(career_report_result),
        config,
    )


def print_final_report_summary(state_manager: StateManager, config: PipelineDemoConfig) -> Dict[str, Any]:
    """Step 8: 输出最终 career_report_result 摘要。"""
    print_module_log("pipeline", "输出最终职业报告摘要", step_index=8, total_steps=TOTAL_STEPS)
    latest_state = state_manager.load_state(config.state_path)
    career_report_result = ensure_stage_result_exists(
        latest_state,
        "career_report_result",
        "pipeline_final",
    )
    final_summary = build_result_summary("career_report", career_report_result)

    print("\n" + "=" * 100)
    print("FINAL career_report_result SUMMARY")
    print("=" * 100)
    print(json.dumps(final_summary, ensure_ascii=False, indent=2))
    print(f"\nstudent.json: {config.state_path.resolve()}")
    print(f"snapshots: {config.snapshot_dir.resolve()}")
    print("=" * 100)
    return final_summary


def run_main_pipeline(config: PipelineDemoConfig) -> Dict[str, Any]:
    """执行单样本最小闭环联调主流程。"""
    state_manager = StateManager()
    try:
        print_module_log(
            "pipeline",
            f"加载或初始化 student.json: {config.state_path}",
            step_index=1,
            total_steps=TOTAL_STEPS,
        )
        student_state = init_or_load_state(state_manager, config)
        print_module_log(
            "pipeline",
            f"state 初始化完成，字段数={len(student_state)}",
            status="SUCCESS",
        )

        step_resume_parse(state_manager, student_state, config)
        step_student_profile(state_manager, config)
        step_job_profile(state_manager, config)
        step_job_match(state_manager, config)
        step_career_path_plan(state_manager, config)
        step_career_report(state_manager, config)
        print_final_report_summary(state_manager, config)
        return state_manager.load_state(config.state_path)
    except Exception as exc:
        print_module_log(
            "pipeline",
            format_pipeline_exception("main_pipeline_demo", exc),
            status="ERROR",
        )
        raise


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description="最小闭环联调主流程 demo")
    parser.add_argument("--state-path", default="outputs/state/student.json", help="student.json 路径")
    parser.add_argument("--snapshot-dir", default="outputs/state/pipeline_snapshots", help="阶段快照目录")
    parser.add_argument("--resume-file-path", default="", help="可选：简历文件路径")
    parser.add_argument("--resume-text", default="", help="可选：简历纯文本")
    parser.add_argument("--target-job-name", default="数据分析师", help="目标标准岗位名")
    parser.add_argument("--job-group-json-path", default="", help="可选：岗位组 JSON 文件路径")
    parser.add_argument("--reset-state", action="store_true", help="是否重置 student.json")
    return parser.parse_args()


def build_config_from_args(args: argparse.Namespace) -> PipelineDemoConfig:
    """构造运行配置。"""
    return PipelineDemoConfig(
        state_path=Path(args.state_path),
        snapshot_dir=Path(args.snapshot_dir),
        resume_file_path=Path(args.resume_file_path) if args.resume_file_path else None,
        resume_text=args.resume_text,
        target_job_name=args.target_job_name,
        job_group_json_path=Path(args.job_group_json_path) if args.job_group_json_path else None,
        reset_state=bool(args.reset_state),
    )


if __name__ == "__main__":
    run_main_pipeline(build_config_from_args(parse_args()))


