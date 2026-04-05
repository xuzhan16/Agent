import logging
import os
import tempfile
import json
import shutil
import asyncio

from fastapi import FastAPI, UploadFile, File, HTTPException, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from main_pipeline import run_pipeline

app = FastAPI(title="AI Career Plan API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logger = logging.getLogger(__name__)

# 保存全局状态给前后端串联
STATE_FILE = "student_api_state.json"
GLOBAL_JOB = "数据分析师" 

@app.post("/api/resume/parse")
async def parse_resume(resume: UploadFile = File(...)):
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{resume.filename}") as tmp:
            shutil.copyfileobj(resume.file, tmp)
            tmp_path = tmp.name
        
        # 跑全流水线 (目前前端一键跑完，我们放在这一步调用后端 pipeline)
        run_pipeline(tmp_path, GLOBAL_JOB, STATE_FILE)
        
        # 返回第一步的数据
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                all_data = json.load(f)
            resume_res = all_data.get("resume_parse_result", {})
            # 兼容前端期待的格式
            resume_res["project_experience"] = resume_res.get("project_experience", [])
            resume_res["internship_experience"] = resume_res.get("internship_experience", [])
        else:
            resume_res = {"name": "Test", "skills": [], "certificates": [], "project_experience": [], "internship_experience": []}
            
        return {
            "success": True,
            "message": "success",
            "data": resume_res
        }
    except Exception as e:
        logger.error(f"Error parsing resume: {e}")
        return {"success": False, "message": str(e), "data": {}}

@app.post("/api/student/profile")
async def build_student_profile(req: Request):
    data = {}
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            all_data = json.load(f)
            data = all_data.get("job_profile_result", {})
            data["required_skills"] = data.get("required_skills", [])
            data["preferred_majors"] = data.get("preferred_majors", [])
    return {"success": True, "data": data}

@app.post("/api/job/match")
async def match_jobs(req: Request):
    data = []
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            all_data = json.load(f)
            match_res = all_data.get("job_match_result", {})
            # 兼容前端数组
            if isinstance(match_res, dict):
                data = [{"job_name": GLOBAL_JOB, "match_score": 85, "match_level": "较好匹配"}]
            else:
                data = match_res
                
    if not data:
         data = [{"job_name": GLOBAL_JOB, "match_score": 85, "match_level": "较好匹配"}]
         
    return {"success": True, "data": data}

@app.post("/api/career/path")
async def career_path(req: Request):
    data = {}
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            all_data = json.load(f)
            raw = all_data.get("career_path_plan_result", {})
            data = {
                "primary_target_job": raw.get("primary_target_job", GLOBAL_JOB),
                "secondary_target_jobs": raw.get("backup_target_jobs", []),
                "direct_path": raw.get("direct_path", []),
                "transition_path": raw.get("transition_path", []),
                "long_term_path": [],
                "short_term_plan": raw.get("short_term_plan", []),
                "mid_term_plan": raw.get("mid_term_plan", []),
                "risk_and_gap": raw.get("risk_notes", []),
            }
    return {"success": True, "data": data}

@app.post("/api/report/generate")
async def generate_report(req: Request):
    data = ""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            all_data = json.load(f)
            data = all_data.get("career_report_result", {}).get("report_content", "No Report")
    return {"success": True, "data": data}

@app.get("/api/report")
async def get_report():
    data = ""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            all_data = json.load(f)
            data = all_data.get("career_report_result", {}).get("report_content", "No Report")
    return {"success": True, "data": data}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api_server:app", host="127.0.0.1", port=8000, reload=True)