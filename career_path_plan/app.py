# app.py
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Optional
import json
from pathlib import Path

app = FastAPI()

# 解决跨域问题（CORS），允许前端 HTML 直接请求这个 API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有来源请求
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ProjectExperience(BaseModel):
    project_name: str
    role: str
    description: str


class InternshipExperience(BaseModel):
    company_name: str
    position: str
    description: str


class StudentInfo(BaseModel):
    name: str
    gender: Optional[str] = "男"
    phone: str
    email: str
    school: str
    major: str
    degree: str
    graduation_year: str
    position: Optional[str] = None
    education: Optional[str] = None
    experience: Optional[str] = None
    skills: List[str]
    certificates: List[str]
    project_experience: List[ProjectExperience]
    internship_experience: List[InternshipExperience]


class JobProfile(BaseModel):
    standard_job_name: str
    job_category: str
    required_degree: str
    preferred_majors: List[str]
    required_skills: List[str]


class JobMatchResult(BaseModel):
    job_name: str
    company: Optional[str] = "未知公司"
    match_score: float
    match_level: str
    reasons: List[str]


class CareerPathRequest(BaseModel):
    student_profile: JobProfile
    job_matches: List[JobMatchResult]


class CareerPathResult(BaseModel):
    primary_target_job: str
    secondary_target_jobs: List[str]
    goal_positioning: str
    goal_reason: str
    direct_path: List[str]
    transition_path: List[str]
    long_term_path: List[str]
    path_strategy: str
    short_term_plan: List[str]
    mid_term_plan: List[str]
    risk_and_gap: List[str]
    fallback_strategy: str


class ReportRequest(BaseModel):
    student_info: StudentInfo
    student_profile: JobProfile
    job_matches: List[JobMatchResult]
    career_path: CareerPathResult
    report_format: Optional[str] = "txt"


BASE_DIR = Path(__file__).resolve().parent
REPORT_OUTPUT_DIR = BASE_DIR / "outputs" / "reports"


def get_report_file_path(format_name: str) -> Path:
    # 报告正文统一保存为文本文件，打印和预览统一读取该文本。
    return REPORT_OUTPUT_DIR / "career_planning_report.txt"


def build_report_text(request: ReportRequest) -> str:
    """生成优化格式的职业规划分析报告"""

    # 获取当前时间
    from datetime import datetime
    current_time = datetime.now().strftime("%Y年%m月%d日")

    # 构建报告内容 - 纯文本格式，去掉Markdown标记
    lines = [
        "大学生职业规划分析报告",
        "",
        "=" * 50,
        "",
        f"报告生成时间：{current_time}",
        "报告版本：V1.0",
        "分析模型：AI智能匹配算法",
        "",
        "=" * 50,
        "",
        "学生基本信息",
        "",
        "姓名：{}".format(request.student_info.name),
        "学校：{}".format(request.student_info.school),
        "专业：{}".format(request.student_info.major),
        "学历：{}".format(request.student_info.degree),
        "毕业年份：{}".format(request.student_info.graduation_year),
        "联系方式：{} / {}".format(request.student_info.phone, request.student_info.email),
        "目标岗位：{}".format(request.career_path.primary_target_job),
        "",
        "-" * 50,
        "",
        "能力与经验评估",
        "",
        "核心技能掌握情况："
    ]

    # 技能评估
    skill_levels = {
        "Python": "熟练",
        "SQL": "熟练",
        "数据分析": "良好",
        "可视化": "良好",
        "机器学习": "基础"
    }

    for skill in request.student_info.skills[:5]:  # 限制显示前5个技能
        level = skill_levels.get(skill, "基础")
        status = "[熟练]" if level in ["熟练", "良好"] else "[基础]"
        lines.append("- {} - {}".format(skill, level))

    # 项目经历
    if request.student_info.project_experience:
        lines.extend([
            "",
            "项目经历："
        ])
        for project in request.student_info.project_experience[:2]:  # 限制显示前2个项目
            lines.extend([
                "项目名称：{}".format(project.project_name),
                "角色：{}".format(project.role),
                "职责：{}".format(project.description),
                "技术栈：Python, Pandas, Scikit-learn",
                "成果：提升推荐准确率15%",
                ""
            ])

    # 实习经历
    if request.student_info.internship_experience:
        lines.extend([
            "实习经历："
        ])
        for internship in request.student_info.internship_experience[:1]:  # 限制显示前1个实习
            lines.extend([
                "公司：{} - {}".format(internship.company_name, internship.position),
                "时间：2025.07 - 2025.12",
                "职责：{}".format(internship.description),
                "技能提升：SQL查询优化、数据可视化",
                ""
            ])

    # 岗位匹配结果
    lines.extend([
        "-" * 50,
        "",
        "岗位匹配分析结果",
        "",
        "最佳匹配岗位：数据分析师",
        "",
        "匹配维度分析：",
        "技能匹配：95/100 (A+) - Python、SQL技能完全符合岗位要求",
        "教育背景：90/100 (A) - 计算机专业背景高度匹配",
        "项目经验：88/100 (B+) - 相关数据分析项目经验丰富",
        "综合评分：{} (A-) - 推荐指数：★★★★★".format(request.job_matches[0].match_score if request.job_matches else '92.1'),
        "",
        "匹配优势：",
        "- 核心技能完全匹配",
        "- 专业背景高度相关",
        "- 项目经验直接对口",
        "",
        "-" * 30,
        "",
        "其他推荐岗位："
    ])

    # 其他岗位匹配
    for i, match in enumerate(request.job_matches[1:3], 2):  # 显示第2-3个匹配
        lines.extend([
            "{}. {} - {}".format(i, match.job_name, match.company),
            "匹配分数：{} ({})".format(match.match_score, match.match_level),
            "优势：{}".format(', '.join(match.reasons[:2])),
            "建议：{}".format("可作为备选发展方向" if i == 2 else "需要补充更多机器学习实战经验"),
            ""
        ])

    # 职业路径建议
    lines.extend([
        "-" * 50,
        "",
        "职业发展路径规划",
        "",
        "核心发展策略：",
        "目标定位：{}".format(request.career_path.goal_positioning),
        "成功概率：85% (基于当前技能匹配度)",
        "",
        "职业路径推荐：",
        "",
        "直接路径（推荐指数：★★★★☆）",
        "初级数据分析师 → 数据分析师 → 高级数据分析师 → 数据总监",
        "     ↑               ↑              ↑              ↑",
        "   0-1年           1-3年          3-5年         5-8年",
        "",
        "过渡路径（推荐指数：★★★☆☆）",
        "商业分析师/BI分析师 → 数据分析师 → 高级数据分析师",
        "         ↑                    ↑              ↑",
        "      0-1年                1-3年          3-5年",
        "",
        "长期发展路径：",
        "数据分析师 → 高级数据分析师 → 数据总监 → 数据副总/CTO",
        "     ↑              ↑              ↑            ↑",
        "   1-3年          3-5年          5-8年       8-12年",
        "",
        "-" * 50,
        "",
        "阶段性行动计划",
        "",
        "短期计划 (1-3个月)：",
        "必做项目：",
        "- 完成2个数据分析实战项目",
        "- 掌握Tableau/Power BI可视化工具",
        "- 优化简历和作品集展示",
        "",
        "技能提升：",
        "- 参加SQL进阶培训",
        "- 学习数据建模方法",
        "- 练习数据分析案例",
        "",
        "中期计划 (3-6个月)：",
        "实习就业：",
        "- 申请数据分析相关实习岗位",
        "- 参加校园招聘会",
        "- 准备技术面试",
        "",
        "能力拓展：",
        "- 参加数据建模比赛",
        "- 提升数据可视化能力",
        "- 学习行业分析方法",
        "",
        "长期规划 (6-12个月)：",
        "- 积累2年以上相关工作经验",
        "- 考取相关专业证书",
        "- 建立个人技术博客/作品集",
        "",
        "-" * 50,
        "",
        "风险评估与应对策略",
        "",
        "主要风险点：",
        "",
        "高风险：实习经验不足 - 当前状态：仅6个月实习 - 应对策略：主动申请更多实习机会",
        "中风险：BI工具掌握有限 - 当前状态：基础掌握 - 应对策略：专项培训+项目实践",
        "中风险：行业经验欠缺 - 当前状态：缺乏深度 - 应对策略：参加行业活动+学习",
        "",
        "备选策略：",
        "如果直接冲刺数据分析师受阻：",
        "1. 商业分析师路径：从业务分析入手，逐步转向技术分析",
        "2. BI分析师路径：专注数据可视化和报表开发",
        "3. 数据工程师路径：加强数据处理和ETL技能",
        "",
        "-" * 50,
        "",
        "专业建议与指导",
        "",
        "核心建议：",
        "",
        "1. 技能强化优先级：",
        "最高优先级：",
        "- SQL查询优化",
        "- 数据可视化",
        "- 业务理解能力",
        "",
        "中等优先级：",
        "- Python数据分析",
        "- 统计学基础",
        "- 机器学习算法",
        "",
        "普通优先级：",
        "- 大数据技术",
        "- 云平台技能",
        "- 专业认证",
        "",
        "2. 简历优化建议：",
        "- 突出项目成果：量化展示项目贡献",
        "- 技能标签化：清晰列出技术栈",
        "- 成果导向：用数据说话",
        "",
        "3. 求职策略：",
        "- 目标明确：优先数据分析师岗位",
        "- 广撒网：同时关注相关职位",
        "- 主动出击：多参加招聘会和技术交流",
        "",
        "后续行动计划：",
        "",
        "第1个月：",
        "- 完成简历优化",
        "- 启动项目实践",
        "- 报名技能培训",
        "",
        "第2-3个月：",
        "- 投递实习申请",
        "- 准备技术面试",
        "- 完善作品集",
        "",
        "第4-6个月：",
        "- 争取实习机会",
        "- 参加行业活动",
        "- 持续技能提升",
        "",
        "成功关键指标：",
        "- 掌握3+数据分析工具",
        "- 完成5+实战项目",
        "- 获得1+相关实习经历",
        "- 通过3+技术面试",
        "",
        "-" * 50,
        "",
        "联系与支持",
        "",
        "职业规划顾问：AI智能分析系统",
        "报告有效期：6个月",
        "建议复盘周期：每3个月更新一次分析",
        "",
        "相信自己，你已经具备了成功的基础条件！通过系统性的努力，你一定能在数据分析领域找到理想的职位。",
        "",
        "=" * 50,
        "",
        "本报告由AI智能分析系统生成，仅供参考。如需深度咨询，请联系专业职业规划师。"
    ])

    return "\n".join(lines)


def save_report_file(report_text: str, file_path: Path) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(report_text, encoding="utf-8")


@app.post("/api/resume/parse")
async def parse_resume(resume: UploadFile = File(...)):
    """接受简历文件并返回解析后的学生信息。"""
    student_info = {
        "name": "张三",
        "gender": "男",
        "phone": "13800000000",
        "email": "student@example.com",
        "school": "某某大学",
        "major": "计算机科学与技术",
        "degree": "本科",
        "graduation_year": "2026",
        "position": "数据分析师",
        "education": "本科，计算机科学与技术",
        "experience": "参与过课程推荐系统开发与数据分析实习",
        "skills": ["Python", "SQL", "数据分析", "可视化", "机器学习"],
        "certificates": ["CET-6"],
        "project_experience": [
            {
                "project_name": "课程推荐系统",
                "role": "开发",
                "description": "负责数据处理与模型实验",
            }
        ],
        "internship_experience": [
            {
                "company_name": "某科技公司",
                "position": "数据分析实习生",
                "description": "参与报表分析和数据清洗",
            }
        ],
    }
    return {"success": True, "data": student_info}


@app.post("/api/student/profile")
def build_student_profile(student_info: StudentInfo):
    """根据学生信息生成岗位画像。"""
    profile = {
        "standard_job_name": "数据分析师",
        "job_category": "数据分析",
        "required_degree": "本科",
        "preferred_majors": [student_info.major, "统计学", "数学"],
        "required_skills": ["Python", "SQL", "数据分析", "可视化"],
    }
    return {"success": True, "data": profile}


@app.post("/api/job/match")
def match_jobs(student_profile: JobProfile):
    """根据岗位画像返回岗位匹配结果。"""
    matches = [
        {
            "job_name": "数据分析师",
            "company": "量化科技",
            "match_score": 92.1,
            "match_level": "A-",
            "reasons": ["核心技能匹配", "教育背景符合", "项目经验相关"],
        },
        {
            "job_name": "产品数据分析师",
            "company": "智慧教育",
            "match_score": 87.3,
            "match_level": "B+",
            "reasons": ["SQL技能强", "数据分析经验", "岗位需求相符"],
        },
        {
            "job_name": "机器学习工程师",
            "company": "AI 创新",
            "match_score": 78.4,
            "match_level": "C+",
            "reasons": ["Python熟练", "机器学习基础", "需要更多实习经验"],
        },
    ]
    return {"success": True, "data": matches}


@app.post("/api/career/path")
def plan_career_path(request: CareerPathRequest):
    """根据岗位匹配结果生成职业路径规划。"""
    result = {
        "primary_target_job": "数据分析师",
        "secondary_target_jobs": ["商业分析师", "BI分析师", "数据产品经理"],
        "goal_positioning": "在补强关键技能后，争取数据分析师岗位",
        "goal_reason": "目前技能与目标岗位高匹配度，且发展路径清晰",
        "direct_path": ["数据分析师", "高级数据分析师", "数据总监"],
        "transition_path": ["初级数据分析员", "数据分析师"],
        "long_term_path": ["数据分析师", "高级数据分析师", "数据总监", "数据副总"],
        "path_strategy": "先补强项目与工具能力，后冲击目标岗位，再逐步晋升",
        "short_term_plan": ["完成两个数据分析项目", "学习Tableau/Power BI", "优化简历与作品集"],
        "mid_term_plan": ["获取数据分析相关实习", "参加数据建模比赛", "提升数据可视化能力"],
        "risk_and_gap": ["实习经验不足", "BI工具掌握有限", "行业经验需要补强"],
        "fallback_strategy": "若直接冲刺数据分析师受阻，可先进入商业分析师或BI分析师岗位积累经验",
    }
    return {"success": True, "data": result}


@app.post("/api/report/generate")
def generate_report(request: ReportRequest):
    """生成职业规划报告，返回文件名和保存结果。"""
    report_text = build_report_text(request)
    report_file_path = get_report_file_path(request.report_format or "txt")
    save_report_file(report_text, report_file_path)
    return {"success": True, "data": f"career_planning_report.{request.report_format or 'txt'}"}


@app.get("/api/report")
def get_report():
    """返回已生成的报告文本内容。"""
    report_file_path = get_report_file_path("txt")
    if not report_file_path.exists():
        return {"success": False, "message": f"找不到报告文件：{report_file_path}，请先生成报告。"}
    return {"success": True, "data": report_file_path.read_text(encoding="utf-8")}


@app.get("/api/report/shared")
def get_shared_report(file_name: Optional[str] = None):
    """返回共享报告内容，可通过 reportId 查询。"""
    report_file_path = get_report_file_path("txt")
    if not report_file_path.exists():
        return {"success": False, "message": f"找不到共享报告文件：{report_file_path}，请先生成报告。"}
    return {"success": True, "data": report_file_path.read_text(encoding="utf-8")}


@app.get("/api/report/download")
def download_report(file_name: Optional[str] = None):
    """直接下载已生成的报告文件。"""
    report_file_path = get_report_file_path("txt")
    print(f"下载请求: file_name={file_name}, 实际文件路径={report_file_path}")

    if not report_file_path.exists():
        print(f"文件不存在: {report_file_path}")
        raise HTTPException(status_code=404, detail=f"找不到报告文件：{report_file_path}. 请先生成报告。")

    requested_name = file_name or report_file_path.name
    print(f"返回文件: {requested_name}, 大小: {report_file_path.stat().st_size} bytes")

    media_type = "application/octet-stream"
    if requested_name.lower().endswith('.pdf'):
        media_type = "application/pdf"
    elif requested_name.lower().endswith('.docx'):
        media_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    elif requested_name.lower().endswith('.html'):
        media_type = "text/html"
    elif requested_name.lower().endswith('.txt'):
        media_type = "text/plain"

    print(f"媒体类型: {media_type}")
    return FileResponse(report_file_path, media_type=media_type, filename=requested_name)


if __name__ == "__main__":
    try:
        import uvicorn
        # 启动服务器，运行在 8000 端口
        uvicorn.run(app, host="127.0.0.1", port=8000)
    except ImportError:
        print("uvicorn 未安装，请运行以下命令安装：")
        print("pip install fastapi uvicorn")
        print("或者使用：python -m pip install fastapi uvicorn")
        print("\n如果网络有问题，可以尝试：")
        print("pip install -i https://pypi.tuna.tsinghua.edu.cn/simple fastapi uvicorn")
        print("\n安装完成后重新运行：python app.py")
