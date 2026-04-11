import uvicorn
import json
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List

app = FastAPI(title="Fake Mock LLM Server")

class ChatMessage(BaseModel):
    role: str
    content: str
    
class ChatCompletionRequest(BaseModel):
    model: str
    messages: List[ChatMessage]
    temperature: float = 0.7

@app.post("/v1/chat/completions")
async def fake_completions(req: ChatCompletionRequest):
    print(f"收到大模型请求，模型名称: {req.model}")
    
    # 简单的任务探测器：通过 system_prompt 里的关键词来猜测是在跑哪一步
    system_prompt = req.messages[0].content if req.messages else ""
    user_prompt = req.messages[-1].content if req.messages else ""
    combined_prompt = (system_prompt + " " + user_prompt).lower()
    
    # 匹配准确的任务类型
    # 注意：我们的 prompt 开头包含 "任务类型：xxx"，因此可以直接用这个特征来准确匹配，避免被上下文里的其他字符串误导
    content_to_return = "{}"
    if "resume_parse" in combined_prompt:
        content_to_return = '{"name": "张无忌 (假模型自动生成)", "gender": "男", "phone": "13800138000", "email": "wuji@mock.com", "school": "测试大学", "major": "计算机实验班", "degree": "本科", "graduation_year": "2026", "skills": ["C++", "Python", "SQL"], "certificates": ["CET-6", "计算机二级"], "project_experience": [{"name": "假模型引擎", "desc": "开发了用于代替贵价API的占位器"}], "internship_experience": [{"company": "模拟科技公司", "role": "全栈实习生", "desc": "配合AI调试一切"}]}'
    elif "job_extract" in combined_prompt:
        content_to_return = '{"standard_job_name":"数据分析师","job_category":"数据岗","degree_requirement":"本科及以上","major_requirement":"统计学、计算机、数学等相关专业","experience_requirement":"应届或1-3年相关经验","hard_skills":["SQL","Python","Excel"],"tools_or_tech_stack":["SQL","Python","Tableau"],"certificate_requirement":[],"soft_skills":["沟通协作","业务理解","逻辑分析"],"practice_requirement":"具备数据分析项目或实习经历优先","job_level":"初级/中级","suitable_student_profile":"适合具备数据分析基础、项目实践和良好业务理解能力的学生","raw_requirement_summary":"负责数据分析、指标监控和业务洞察。","vertical_paths":["数据分析师 -> 高级数据分析师","高级数据分析师 -> 数据分析负责人"],"transfer_paths":["数据分析师 -> 商业分析师","数据分析师 -> 数据产品经理"],"path_relation_details":[{"source_job":"数据分析师","target_job":"高级数据分析师","relation_type":"PROMOTE_TO","reason":"分析深度和业务复杂度提升后的常见晋升路径","confidence":"0.88"},{"source_job":"数据分析师","target_job":"商业分析师","relation_type":"TRANSFER_TO","reason":"分析能力可迁移到业务分析场景","confidence":"0.81"}]}'
    elif "job_profile" in combined_prompt:
        content_to_return = '{"standard_job_name": "数据分析师", "job_category": "数据岗", "required_degree": "本科", "preferred_majors": ["统计学", "计算机"], "required_skills": ["SQL", "Python", "Excel"], "vertical_paths": ["初级分析师", "高级分析师", "数据总监"], "transfer_paths": ["数据产品经理", "商业分析师"]}'
    elif "student_profile" in combined_prompt:
        content_to_return = '{"skill_profile": {"SQL": "良好", "Python": "熟悉"}, "complete_score": 85, "competitiveness_score": 80, "strengths": ["技术扎实"], "weaknesses": ["缺业务经验"], "summary": "潜力新人"}'
    elif "job_match" in combined_prompt:
        content_to_return = '{"overall_score": 88, "basic_requirement_score": 100, "skill_score": 80, "strengths": ["学历与专业符合"], "gaps": ["缺乏数仓实务操作"], "improvement_suggestions": ["补充大数据组件相关的认知"]}'
    elif "career_path_plan" in combined_prompt:
        content_to_return = '{"primary_target_job": "数据分析师", "backup_target_jobs": ["数据运营"], "direct_path": ["初级分析专家", "高级专家", "总监"], "transition_path": ["数据产品经理"], "short_term_plan": ["补习高级SQL与Python基础", "练习Tableau"], "mid_term_plan": ["参与商业全链路数据挖掘项目", "考取大厂分析师资格证"], "risk_notes": ["内卷严重，需有实战壁垒"]}'
    elif "career_report" in combined_prompt or "report" in combined_prompt:
        content_to_return = '{"report_title": "来自纯本地假模型的万字模拟报告", "target_job": "数据分析师", "action_summary": "请坚持学习，必有回报！", "report_text": "【这是一份由本地假模型服务器(Mock Server)自动吐出的长文报告正文。】\\n\\n如果您在页面上看到了这段完整的长文内容，它代表了：\\n1. 您的前端页面完美承接了后端数据；\\n2. 您的整个Python流水线 (prompt构造、大模型请求收发、JSON解析提取、本地状态回写) 都已绝对畅通无阻！\\n\\n后续您只需换上真实厂家的 API Key，就能生成真实的千言万语，祝您联调顺利！", "report_sections": {}}'
    
    print(f"匹配到对应任务模板，下发模拟假数据中...")
    import time
    time.sleep(1.5) # 模拟一下思考时间，增强逼真度

    return {
        "id": "chatcmpl-mock-fake-12345",
        "object": "chat.completion",
        "created": 1111111111,
        "model": req.model,
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": content_to_return
                },
                "finish_reason": "stop"
            }
        ],
        "usage": {
            "prompt_tokens": 10,
            "completion_tokens": 50,
            "total_tokens": 60
        }
    }

if __name__ == "__main__":
    uvicorn.run("mock_llm_server:app", host="127.0.0.1", port=8001)
