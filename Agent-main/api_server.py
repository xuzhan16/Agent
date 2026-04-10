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


def _clean_text(value):
    if value is None:
        return ""
    return str(value).strip()


def _safe_dict(value):
    return value if isinstance(value, dict) else {}


def _resolve_state_target_job(all_data: dict) -> str:
    candidates = [
        _safe_dict(all_data.get("career_path_plan_result")).get("primary_target_job"),
        _safe_dict(all_data.get("job_profile_result")).get("standard_job_name"),
        _safe_dict(all_data.get("resume_parse_result")).get("target_job_intention"),
        _safe_dict(all_data.get("basic_info")).get("target_job"),
    ]
    for item in candidates:
        cleaned = _clean_text(item)
        if cleaned:
            return cleaned
    return "未明确目标岗位"

@app.post("/api/resume/parse")
async def parse_resume(resume: UploadFile = File(...)):
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{resume.filename}") as tmp:
            shutil.copyfileobj(resume.file, tmp)
            tmp_path = tmp.name
        
        # 跑全流水线 (目前前端一键跑完，我们放在这一步调用后端 pipeline)
        run_pipeline(tmp_path, "", STATE_FILE)
        
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
            current_target_job = _resolve_state_target_job(all_data)
            
            if isinstance(match_res, dict) and match_res:
                score = match_res.get("overall_match_score", match_res.get("overall_score", 0))
                level = "高度匹配" if score >= 80 else ("较好匹配" if score >= 60 else "需转型突破")
                
                reasons = []
                strengths = match_res.get("strengths", [])
                if isinstance(strengths, list):
                    for s in strengths:
                        reasons.append(str(s.get("description", s) if isinstance(s, dict) else s))
                weaknesses = match_res.get("weaknesses") or match_res.get("gaps") or []
                if isinstance(weaknesses, list):
                    for g in weaknesses:
                        reasons.append(str(g.get("description", g) if isinstance(g, dict) else g))
                if not reasons:
                    missing_items = match_res.get("missing_items", [])
                    if isinstance(missing_items, list):
                        for item in missing_items:
                            if isinstance(item, dict):
                                reason = item.get("reason") or item.get("required_item")
                                if reason:
                                    reasons.append(str(reason))
                
                data = [{
                    "job_name": current_target_job,
                    "match_score": score,
                    "match_level": level,
                    "reasons": reasons
                }]
            elif isinstance(match_res, list):
                data = match_res
                
    if not data:
         fallback_job_name = "未明确目标岗位"
         if os.path.exists(STATE_FILE):
             with open(STATE_FILE, 'r', encoding='utf-8') as f:
                 fallback_job_name = _resolve_state_target_job(json.load(f))
         data = [{"job_name": fallback_job_name, "match_score": 85, "match_level": "较好匹配", "reasons": ["无法获得匹配详情"]}]
         
    return {"success": True, "data": data}

@app.post("/api/career/path")
async def career_path(req: Request):
    data = {}
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            all_data = json.load(f)
            raw = all_data.get("career_path_plan_result", {})
            
            def flatten_list(obj_list):
                if not obj_list: return []
                res = []
                for item in obj_list:
                    if isinstance(item, str):
                        res.append(item)
                    elif isinstance(item, dict):
                        # Custom flatten based on keys
                        if "title" in item and "description" in item:
                            res.append(f"{item['title']}: {item['description']}")
                        elif "phase" in item and "content" in item:
                            res.append(f"{item['phase']}: {item['content']}")
                        elif "step" in item and "action" in item:
                            res.append(f"Step {item.get('step', '')}: {item['action']}")
                        elif "phase" in item and "actions" in item:
                            acts = "、".join(item["actions"]) if isinstance(item["actions"], list) else str(item["actions"])
                            res.append(f"{item['phase']}: {acts}")
                        else:
                            values = [str(v) for v in item.values() if isinstance(v, str)]
                            if values:
                                res.append(" - ".join(values))
                            else:
                                res.append(json.dumps(item, ensure_ascii=False))
                    else:
                        res.append(str(item))
                return res

            data = {
                "primary_target_job": raw.get("primary_target_job", _resolve_state_target_job(all_data)),
                "secondary_target_jobs": raw.get("secondary_target_jobs", raw.get("backup_target_jobs", [])),
                "direct_path": flatten_list(raw.get("direct_path", [])),
                "transition_path": flatten_list(raw.get("transition_path", [])),
                "long_term_path": flatten_list(raw.get("long_term_path", [])),
                "short_term_plan": flatten_list(raw.get("short_term_plan", [])),
                "mid_term_plan": flatten_list(raw.get("mid_term_plan", [])),
                "risk_and_gap": flatten_list(raw.get("risk_and_gap", raw.get("risk_notes", []))),
            }
    return {"success": True, "data": data}

@app.post("/api/report/generate")
async def generate_report(req: Request):
    data = ""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            all_data = json.load(f)
            report_res = all_data.get("career_report_result", {})
            data = report_res.get("report_text_markdown") or report_res.get("report_text") or "No Report"
    return {"success": True, "data": data}

@app.get("/api/report")
async def get_report():
    data = ""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            all_data = json.load(f)
            report_res = all_data.get("career_report_result", {})
            data = report_res.get("report_text_markdown") or report_res.get("report_text") or "No Report"
    return {"success": True, "data": data}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api_server:app", host="127.0.0.1", port=8000, reload=True)
