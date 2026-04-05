"""
resume_demo.py — 简历解析模块可运行示例

流程：
    1. 若未传 --input，在 outputs/resume_demo/ 下生成内置 DEMO_RESUME_TEXT 的 txt；
    2. 调用 process_resume_file（内部会 call_llm + 写 student.json）；
    3. 打印 resume_parse_result 与完整 student_state。

用于本地联调 llm_interface_layer 与 StateManager，无需自备简历文件即可冒烟测试。
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

try:
    from .resume_parser import process_resume_file, setup_logging
except ImportError:
    from resume_parse_module.resume_parser import process_resume_file, setup_logging


# 结构化程度较高的虚构简历，覆盖教育/实习/项目/技能/证书/意向/自我评价等段落
DEMO_RESUME_TEXT = """张三
电话：13800000000
邮箱：student@example.com
学校：某某大学
专业：计算机科学与技术
学历：本科
毕业时间：2026

教育经历
2022.09-2026.06 某某大学 计算机科学与技术 本科

实习经历
2025.07-2025.10 某科技公司 数据分析实习生
负责 SQL 报表、Python 数据清洗和业务分析支持

项目经历
课程推荐系统
角色：开发
内容：负责数据处理、特征分析和模型实验

专业技能
Python、SQL、机器学习、Excel

证书
CET-6

求职意向
数据分析师

自我评价
学习能力强，具备良好的沟通协作能力，对数据分析岗位有明确兴趣。
"""


def build_demo_resume_file(demo_file_path: str | Path) -> Path:
    """生成一份 txt 简历样例，便于直接跑通 demo。"""
    path = Path(demo_file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(DEMO_RESUME_TEXT, encoding="utf-8")
    return path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resume parse demo")
    parser.add_argument(
        "--input",
        default="",
        help="可选：自定义简历文件路径；不传则自动生成 demo txt 简历",
    )
    parser.add_argument(
        "--state-path",
        default="outputs/state/student.json",
        help="student.json 输出路径",
    )
    return parser.parse_args()


def main() -> None:
    """解析命令行参数，执行单条简历流水线并打印 JSON 结果。"""
    setup_logging()
    args = parse_args()

    if args.input:
        resume_file = Path(args.input)
    else:
        resume_file = build_demo_resume_file("outputs/resume_demo/demo_resume.txt")

    result_bundle = process_resume_file(
        file_path=resume_file,
        state_path=args.state_path,
    )

    print("\n" + "=" * 80)
    print("resume_parse_result")
    print("=" * 80)
    print(json.dumps(result_bundle["resume_parse_result"], ensure_ascii=False, indent=2))

    print("\n" + "=" * 80)
    print("student_state")
    print("=" * 80)
    print(json.dumps(result_bundle["student_state"], ensure_ascii=False, indent=2))

    print(f"\nstudent.json saved to: {Path(args.state_path).resolve()}")


if __name__ == "__main__":
    main()


