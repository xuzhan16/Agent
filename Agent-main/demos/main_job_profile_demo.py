"""
main_job_profile_demo.py

job_profile 模块最小可运行 demo。

演示内容：
1. 使用 mock pandas DataFrame 构造岗位组数据；
2. 演示根据 standard_job_name 读取岗位组；
3. 演示调用 job_profile_builder 做显式要求抽取与标准化映射；
4. 演示调用 job_profile_aggregator 做岗位组聚合统计；
5. 演示调用 job_profile_service，通过统一大模型接口补充岗位画像字段；
6. 打印最终 job_profile_result 摘要。
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from job_profile.job_profile_aggregator import aggregate_job_profile_group
from job_profile.job_profile_builder import build_job_profile_input_payload, get_standard_job_group
from job_profile.job_profile_service import run_job_profile_service


LOGGER = logging.getLogger(__name__)
TARGET_STANDARD_JOB_NAME = "数据分析师"
OUTPUT_PATH = Path("outputs/state/main_job_profile_demo_result.json")


def setup_logging() -> None:
    """初始化 demo 日志格式。"""
    if LOGGER.handlers:
        return
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
    )


def build_mock_job_dataframe() -> pd.DataFrame:
    """构造 mock 岗位数据，用于演示 job_profile 模块链路。"""
    records = [
        {
            "job_name": "数据分析师",
            "standard_job_name": "数据分析师",
            "city": "杭州",
            "province": "浙江",
            "salary_min_month": 12000,
            "salary_max_month": 18000,
            "company_name": "星河科技有限公司",
            "company_type": "民营",
            "company_size": "500-999人",
            "industry": "互联网",
            "job_desc": (
                "岗位职责：负责业务数据分析、报表建设和A/B测试分析，使用SQL、Python、Excel、Tableau完成数据处理与可视化。"
                "任职要求：本科及以上学历，统计学、计算机、数据科学相关专业优先；"
                "有互联网数据分析项目经验，具备良好沟通协作能力和逻辑分析能力。"
            ),
            "company_desc": "公司专注互联网数据平台和智能运营产品。",
            "update_date": "2026-03-20",
        },
        {
            "job_name": "BI数据分析",
            "standard_job_name": "数据分析师",
            "city": "杭州",
            "province": "浙江",
            "salary_min_month": 10000,
            "salary_max_month": 15000,
            "company_name": "云帆数据科技有限公司",
            "company_type": "民营",
            "company_size": "100-499人",
            "industry": "互联网",
            "job_desc": (
                "负责经营指标体系搭建、数据看板开发、业务专题分析，熟练使用SQL、Excel、Power BI。"
                "要求本科及以上，数学、统计、信息管理相关专业优先，有数据分析实习经历优先，"
                "具备责任心、跨部门沟通能力和快速学习能力。"
            ),
            "company_desc": "提供企业级BI与数据治理服务。",
            "update_date": "2026-03-18",
        },
        {
            "job_name": "商业数据分析师",
            "standard_job_name": "数据分析师",
            "city": "上海",
            "province": "上海",
            "salary_min_month": 15000,
            "salary_max_month": 22000,
            "company_name": "明德商业科技有限公司",
            "company_type": "合资",
            "company_size": "1000-9999人",
            "industry": "咨询服务",
            "job_desc": (
                "参与用户增长分析、实验分析、业务诊断和策略复盘，要求掌握SQL、Python、Pandas、数据可视化。"
                "本科或硕士学历，统计学、数学、计算机相关专业优先；"
                "具备项目经验、良好的结构化思考能力和结果导向意识，英语六级优先。"
            ),
            "company_desc": "聚焦商业咨询、营销科技与数据洞察。",
            "update_date": "2026-03-22",
        },
        {
            "job_name": "数据运营分析",
            "standard_job_name": "数据分析师",
            "city": "深圳",
            "province": "广东",
            "salary_min_month": 9000,
            "salary_max_month": 14000,
            "company_name": "南辰智能科技有限公司",
            "company_type": "民营",
            "company_size": "100-499人",
            "industry": "电子商务",
            "job_desc": (
                "负责运营数据分析、日报周报输出、用户分层分析和活动效果评估，"
                "熟悉Excel、SQL，了解Python更佳。大专及以上学历，"
                "可接受应届毕业生，有实习经验优先，具备执行力和团队合作意识。"
            ),
            "company_desc": "主营电商增长运营和用户数据分析服务。",
            "update_date": "2026-03-16",
        },
        {
            "job_name": "产品经理",
            "standard_job_name": "产品经理",
            "city": "杭州",
            "province": "浙江",
            "salary_min_month": 13000,
            "salary_max_month": 20000,
            "company_name": "星河科技有限公司",
            "company_type": "民营",
            "company_size": "500-999人",
            "industry": "互联网",
            "job_desc": "负责产品需求分析、竞品研究、原型设计和项目推进，要求沟通协作和产品sense。",
            "company_desc": "公司专注互联网数据平台和智能运营产品。",
            "update_date": "2026-03-21",
        },
    ]
    return pd.DataFrame(records)


def safe_dict(value: Any) -> Dict[str, Any]:
    """安全转 dict。"""
    return value if isinstance(value, dict) else {}


def safe_list(value: Any) -> List[Any]:
    """安全转 list。"""
    return value if isinstance(value, list) else []


def format_top_items(items: List[Dict[str, Any]], top_n: int = 5) -> str:
    """将聚合频次结果压缩成便于打印的一行摘要。"""
    if not items:
        return "[]"
    parts = []
    for item in items[:top_n]:
        item_dict = safe_dict(item)
        name = item_dict.get("name", "")
        count = item_dict.get("count", 0)
        ratio = item_dict.get("ratio", 0)
        parts.append(f"{name}:{count}({ratio})")
    return "[" + ", ".join(parts) + "]"


def print_group_preview(df: pd.DataFrame, standard_job_name: str) -> pd.DataFrame:
    """演示岗位组读取。"""
    group_df = get_standard_job_group(df, standard_job_name)
    LOGGER.info("Step 1/4 岗位组读取完成：standard_job_name=%s, rows=%s", standard_job_name, len(group_df))

    preview_columns = [
        "job_name",
        "standard_job_name",
        "city",
        "salary_min_month",
        "salary_max_month",
        "company_name",
        "industry",
    ]
    preview_columns = [column for column in preview_columns if column in group_df.columns]
    print("\n========== Step 1/4 岗位组读取预览 ==========")
    print(group_df[preview_columns].head(10).to_string(index=False))
    return group_df


def print_builder_demo(df: pd.DataFrame, standard_job_name: str) -> Dict[str, Any]:
    """演示显式抽取和标准化映射。"""
    builder_payload = build_job_profile_input_payload(
        df=df,
        standard_job_name=standard_job_name,
        output_path=None,
    )
    normalized_req = safe_dict(builder_payload.get("normalized_requirements"))
    explicit_req = safe_dict(builder_payload.get("explicit_requirements"))

    LOGGER.info(
        "Step 2/4 builder 完成：hard_skills=%s, tools=%s, majors=%s",
        len(safe_list(normalized_req.get("hard_skill_tags"))),
        len(safe_list(normalized_req.get("tool_skill_tags"))),
        len(safe_list(normalized_req.get("major_tags"))),
    )

    print("\n========== Step 2/4 Builder 显式抽取与标准化 ==========")
    print("standard_job_name:", builder_payload.get("standard_job_name", ""))
    print("degree_tags:", safe_list(normalized_req.get("degree_tags")))
    print("major_tags:", safe_list(normalized_req.get("major_tags")))
    print("hard_skill_tags:", safe_list(normalized_req.get("hard_skill_tags")))
    print("tool_skill_tags:", safe_list(normalized_req.get("tool_skill_tags")))
    print("certificate_tags:", safe_list(normalized_req.get("certificate_tags")))
    print("practice_tags:", safe_list(normalized_req.get("practice_tags")))
    print("soft_skill_tags:", safe_list(normalized_req.get("soft_skill_tags")))
    print("experience_tags:", safe_list(normalized_req.get("experience_tags")))
    print("requirement_sentences_count:", len(safe_list(explicit_req.get("requirement_sentences"))))
    print("representative_samples_count:", len(safe_list(builder_payload.get("representative_samples"))))
    print("build_warnings:", safe_list(builder_payload.get("build_warnings")))
    return builder_payload


def print_aggregator_demo(df: pd.DataFrame, standard_job_name: str) -> Dict[str, Any]:
    """演示聚合统计。"""
    aggregation_result = aggregate_job_profile_group(
        df=df,
        standard_job_name=standard_job_name,
    )
    salary_stats = safe_dict(aggregation_result.get("salary_stats"))

    LOGGER.info(
        "Step 3/4 aggregator 完成：skills=%s, degree_dist=%s, cities=%s",
        len(safe_list(aggregation_result.get("skill_frequency"))),
        len(safe_list(aggregation_result.get("degree_requirement_distribution"))),
        len(safe_list(aggregation_result.get("city_distribution"))),
    )

    print("\n========== Step 3/4 Aggregator 聚合统计 ==========")
    print("job_count:", aggregation_result.get("job_count", 0))
    print("skill_frequency_top5:", format_top_items(safe_list(aggregation_result.get("skill_frequency")), top_n=5))
    print(
        "degree_requirement_distribution:",
        format_top_items(safe_list(aggregation_result.get("degree_requirement_distribution")), top_n=5),
    )
    print("industry_distribution:", format_top_items(safe_list(aggregation_result.get("industry_distribution")), top_n=5))
    print("city_distribution:", format_top_items(safe_list(aggregation_result.get("city_distribution")), top_n=5))
    print(
        "salary_stats:",
        {
            "salary_min_month_avg": salary_stats.get("salary_min_month_avg"),
            "salary_max_month_avg": salary_stats.get("salary_max_month_avg"),
            "salary_mid_month_avg": salary_stats.get("salary_mid_month_avg"),
            "valid_salary_count": salary_stats.get("valid_salary_count"),
        },
    )
    print("aggregation_warnings:", safe_list(aggregation_result.get("aggregation_warnings")))
    return aggregation_result


def run_service_demo(df: pd.DataFrame, standard_job_name: str) -> Dict[str, Any]:
    """演示调用统一大模型接口生成最终 job_profile_result。"""
    LOGGER.info("Step 4/4 调用 job_profile_service，内部通过统一 call_llm 接口补充岗位画像字段")

    result = run_job_profile_service(
        df=df,
        standard_job_name=standard_job_name,
        context_data={
            "graph_context": {
                "mock_note": "这里是预留的 Neo4j 图谱上下文接入位，当前 demo 使用 mock 数据。",
                "upstream_jobs": ["数据分析实习生"],
                "downstream_jobs": ["高级数据分析师", "数据分析负责人"],
                "transfer_jobs": ["商业分析师", "数据运营", "BI分析师"],
            },
            "sql_context": {
                "mock_note": "这里是预留的 SQL 明细统计上下文接入位，当前 demo 使用 mock 数据。",
                "sample_city_salary_hint": {
                    "杭州": "10k-18k",
                    "上海": "15k-22k",
                    "深圳": "9k-14k",
                },
            },
        },
        extra_context={
            "demo_name": "main_job_profile_demo",
            "expected_usage": "演示 job_profile 模块 builder + aggregator + 统一 LLM 接口的串联方式",
        },
        output_path=OUTPUT_PATH,
    )

    print("\n========== Step 4/4 Service + LLM 最终结果摘要 ==========")
    print("standard_job_name:", result.get("standard_job_name", ""))
    print("job_category:", result.get("job_category", ""))
    print("job_level:", result.get("job_level", ""))
    print("degree_requirement:", result.get("degree_requirement", ""))
    print("major_requirement:", safe_list(result.get("major_requirement")))
    print("hard_skills:", safe_list(result.get("hard_skills")))
    print("tools_or_tech_stack:", safe_list(result.get("tools_or_tech_stack")))
    print("certificate_requirement:", safe_list(result.get("certificate_requirement")))
    print("practice_requirement:", safe_list(result.get("practice_requirement")))
    print("soft_skills:", safe_list(result.get("soft_skills")))
    print("suitable_student_profile:", result.get("suitable_student_profile", ""))
    print("summary:", result.get("summary", ""))
    print("vertical_paths:", safe_list(result.get("vertical_paths")))
    print("transfer_paths:", safe_list(result.get("transfer_paths")))
    print("result_saved_to:", str(OUTPUT_PATH))
    print("build_warnings:", safe_list(result.get("build_warnings")))
    return result


def main() -> None:
    """demo 主入口。"""
    setup_logging()
    LOGGER.info("Start main_job_profile_demo, target standard_job_name=%s", TARGET_STANDARD_JOB_NAME)

    df = build_mock_job_dataframe()
    print(f"\nMock DataFrame shape: {df.shape}")

    print_group_preview(df, TARGET_STANDARD_JOB_NAME)
    print_builder_demo(df, TARGET_STANDARD_JOB_NAME)
    print_aggregator_demo(df, TARGET_STANDARD_JOB_NAME)
    run_service_demo(df, TARGET_STANDARD_JOB_NAME)

    LOGGER.info("main_job_profile_demo finished.")


if __name__ == "__main__":
    main()
