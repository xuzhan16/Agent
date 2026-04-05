"""
main_pipeline.py - 全流程统一调度入口（基于文档要求的标准链式调度体系）

1. 读取 student.json 状态
2. 按 6 个阶段顺序调度大模型请求，并从 SQLite、Neo4j 补充上下文喂给大模型
3. 每步结果即时落盘
"""
from __future__ import annotations

import argparse
import json
import os
import logging
from pathlib import Path
from typing import Any, Dict

from llm_interface_layer.state_manager import StateManager
from llm_interface_layer.llm_service import call_llm

from db_helper import query_sqlite, query_neo4j

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# 配置数据库连接 (按项目默认路径)
SQLITE_DB_PATH = "outputs/sql/jobs.db"
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "ChangeThisPassword_123!") # 这与 docker-compose.neo4j.yml 里的默认密码对应

class StudentState:
    """对 student.json 的存取管理"""
    def __init__(self, filepath: str | Path):
        self.filepath = Path(filepath)
        self.state: Dict[str, Any] = {
            "basic_info": {},
            "resume_parse_result": {},
            "job_profile_result": {},
            "student_profile_result": {},
            "job_match_result": {},
            "career_path_plan_result": {},
            "career_report_result": {}
        }
        self.load()

    def load(self) -> None:
        if self.filepath.exists():
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    self.state.update(json.load(f))
                logger.info(f"成功加载状态: {self.filepath}")
            except Exception as e:
                logger.warning(f"读取失败，使用空状态: {e}")

    def update(self, module_name: str, result: Dict[str, Any]) -> None:
        self.state[module_name] = result
        self.save()
        logger.info(f">> 【{module_name}】阶段处理完成并写盘。")

    def save(self) -> None:
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(self.filepath, "w", encoding="utf-8") as f:
            json.dump(self.state, f, ensure_ascii=False, indent=2)

def run_pipeline(resume_path: str, initial_target_job: str, state_path: str = "student.json") -> None:
    state = StudentState(state_path)
    
    # 【1】resume_parse_module：读取简历，抽取结构化信息
    logger.info("启动第 1 步: resume_parse")
    # 这里直接走统一的大模型接口 task_type="resume_parse" 
    # 实际应用中可结合 pypdf / python-docx 提取文本作为 input_data
    # 为演示，这里模拟读取到了全文
    resume_text = f"读取了简历文件: {resume_path} 里的内容..." 
    try:
        from resume_parse_module.resume_parser import parse_resume_to_json
        resume_result = parse_resume_to_json(resume_text, state.state)
    except Exception:
        # Fallback to direct raw call_llm
        resume_result = call_llm(
            task_type="resume_parse",
            input_data={"resume_text": resume_text},
            student_state=state.state
        )
    state.update("resume_parse_result", resume_result)


    # 【2】student_profile：基于解析字典生成学生能力画像
    logger.info("启动第 2 步: student_profile")
    # 可结合图谱补齐一些技能和专业的上下文分类
    # 图谱查询：获取学生技能是否属于某个分类
    student_skills = resume_result.get("skills", [])
    skill_context = ""
    # 若有neo4j驱动，可查: query_neo4j(...) 
    
    profile_result = call_llm(
        task_type="student_profile",
        input_data=resume_result, # 用解析后的简历结构去生成多维画像
        context_data={"KG_SkillContext": skill_context}, 
        student_state=state.state
    )
    state.update("student_profile_result", profile_result)


    # 【3】job_profile：获取目标岗位的画像 (结合SQLite和Neo4j)
    logger.info("启动第 3 步: job_profile，拉取后端数据库知识")
    # 从 SQLite 中拉取同名词岗位分布明细 (薪资/城市)
    sql_summary = query_sqlite(
        db_path=SQLITE_DB_PATH,
        query="SELECT job_name_raw, city, salary_month_min, salary_month_max, company_name_clean FROM job_detail WHERE standard_job_name = ? LIMIT 50",
        parameters=(initial_target_job,)
    )
    
    # 从 Neo4j 图谱中拉取知识（如它依赖什么技能，向上晋升的路径）
    neo4j_summary = query_neo4j(
        uri=NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD,
        query="""
        MATCH (j:Job {name: $job})-[r:REQUIRES_SKILL]->(s:Skill) 
        RETURN s.name AS skill 
        LIMIT 10
        """,
        parameters={"job": initial_target_job}
    )
    
    db_context = {
        "SQL_Stats": sql_summary,
        "KG_Stats": neo4j_summary
    }
    
    job_profile_result = call_llm(
        task_type="job_profile",
        input_data={"target_job": initial_target_job},
        context_data=db_context,
        student_state=state.state
    )
    state.update("job_profile_result", job_profile_result)


    # 【4】job_match：计算学生能力与刚才提炼出的该岗位画像的匹配度
    logger.info("启动第 4 步: job_match")
    match_result = call_llm(
        task_type="job_match",
        input_data={
            "student_profile": profile_result,
            "job_profile": job_profile_result
        },
        student_state=state.state
    )
    state.update("job_match_result", match_result)


    # 【5】career_path_plan：根据差距规划路径 (结合Neo4j中的路径)
    logger.info("启动第 5 步: career_path_plan，引入图谱晋升换岗数据")
    # 去图谱里问：这个岗位可以往哪晋升？可以平调去哪？
    transfer_paths = query_neo4j(
        NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD,
        query="MATCH (j:Job {name: $job})-[:TRANSFER_TO]->(target:Job) RETURN target.name AS next_job LIMIT 5",
        parameters={"job": initial_target_job}
    )
    promote_paths = query_neo4j(
        NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD,
        query="MATCH (j:Job {name: $job})-[:PROMOTE_TO]->(target:Job) RETURN target.name AS next_job LIMIT 5",
        parameters={"job": initial_target_job}
    )
    
    plan_context = {
        "neo4j_transfer_paths": transfer_paths,
        "neo4j_promote_paths": promote_paths
    }
    
    plan_result = call_llm(
        task_type="career_path_plan",
        input_data={
            "match_result": match_result,
            "job_profile": job_profile_result,
            "student_profile": profile_result
        },
        context_data=plan_context,
        student_state=state.state
    )
    state.update("career_path_plan_result", plan_result)


    # 【6】career_report：长文集成生成
    logger.info("启动第 6 步: career_report")
    report_result = call_llm(
        task_type="career_report",
        input_data={}, # 所有必要资料都在 state 里了
        student_state=state.state
    )
    state.update("career_report_result", report_result)
    
    # 额外写出一份 Markdown
    if isinstance(report_result, dict) and "report_text_markdown" in report_result:
        with open("final_report.md", "w", encoding="utf-8") as f:
            f.write(report_result["report_text_markdown"])
        logger.info("最终报告已导出为 final_report.md !")
    
    logger.info("=========== 流水线圆满结束 ===========")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--resume", type=str, required=True, help="简历文件路径")
    parser.add_argument("--job", type=str, required=True, help="目标应聘岗位")
    parser.add_argument("--out", type=str, default="student.json")
    args = parser.parse_args()
    
    run_pipeline(args.resume, args.job, args.out)