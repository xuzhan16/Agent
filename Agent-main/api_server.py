import logging
import os
import tempfile
import json
import shutil
import asyncio
from pathlib import Path
from typing import Union

from fastapi import FastAPI, UploadFile, File, HTTPException, Request, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel

from job_data_pipeline import (
    DEFAULT_GROUP_SAMPLE_SIZE,
    DEFAULT_INPUT_FILE,
    DEFAULT_INTERMEDIATE_DIR,
    DEFAULT_NEO4J_OUTPUT_DIR,
    DEFAULT_SQL_DB_PATH,
    run_job_data_pipeline,
)
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
REPORT_FILE_NAME = "final_report.md"


class ManualResumeRequest(BaseModel):
    resume_text: str
    file_name: str = "manual_resume.txt"


class ReportUpdateRequest(BaseModel):
    report_text: str


class JobDataProcessRequest(BaseModel):
    input_file: str = DEFAULT_INPUT_FILE
    intermediate_dir: str = DEFAULT_INTERMEDIATE_DIR
    sql_db_path: str = DEFAULT_SQL_DB_PATH
    neo4j_output_dir: str = DEFAULT_NEO4J_OUTPUT_DIR
    sheet_name: Union[str, int] = 0
    log_every: int = 50
    max_workers: int = 4
    group_sample_size: int = DEFAULT_GROUP_SAMPLE_SIZE


def _clean_text(value):
    if value is None:
        return ""
    return str(value).strip()


def _safe_dict(value):
    return value if isinstance(value, dict) else {}


def _safe_list(value):
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    return [value]


def _state_file_path() -> Path:
    return Path(__file__).resolve().with_name(STATE_FILE)


def _report_file_path() -> Path:
    return Path(__file__).resolve().with_name(REPORT_FILE_NAME)


def _project_root() -> Path:
    return Path(__file__).resolve().parent


def _resolve_project_path(path_value: str) -> str:
    path = Path(path_value)
    if path.is_absolute():
        return str(path)
    return str((_project_root() / path).resolve())


def _load_all_data() -> dict:
    state_path = _state_file_path()
    if not state_path.exists():
        return {}
    with state_path.open("r", encoding="utf-8") as f:
        loaded = json.load(f)
    return loaded if isinstance(loaded, dict) else {}


def _write_all_data(all_data: dict) -> None:
    state_path = _state_file_path()
    with state_path.open("w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)


def _write_report_file(report_text: str) -> None:
    if not _clean_text(report_text):
        return
    _report_file_path().write_text(report_text, encoding="utf-8")


def _normalize_section_list(value):
    if isinstance(value, list):
        result = []
        for item in value:
            item_dict = _safe_dict(item)
            title = _clean_text(item_dict.get("section_title") or item_dict.get("title"))
            content = _clean_text(item_dict.get("section_content") or item_dict.get("content"))
            if title:
                result.append({"section_title": title, "section_content": content})
        return result

    if isinstance(value, dict):
        result = []
        for key, item in value.items():
            item_dict = _safe_dict(item)
            title = _clean_text(item_dict.get("section_title") or item_dict.get("title") or key)
            content = _clean_text(item_dict.get("section_content") or item_dict.get("content") or item)
            if title:
                result.append({"section_title": title, "section_content": content})
        return result
    return []


def _build_resume_response(all_data: dict) -> dict:
    resume_res = _safe_dict(all_data.get("resume_parse_result"))
    basic_info = _safe_dict(resume_res.get("basic_info"))
    response = dict(resume_res)

    for field in ("name", "gender", "phone", "email", "school", "major", "degree", "graduation_year"):
        response[field] = _clean_text(response.get(field) or basic_info.get(field))

    response["skills"] = _safe_list(response.get("skills"))
    response["certificates"] = _safe_list(response.get("certificates"))
    response["project_experience"] = _safe_list(response.get("project_experience"))
    response["internship_experience"] = _safe_list(response.get("internship_experience"))

    response["position"] = _clean_text(
        response.get("position")
        or response.get("target_job_intention")
        or _safe_dict(all_data.get("basic_info")).get("target_job")
    )
    response["education"] = " / ".join(
        item
        for item in [
            _clean_text(response.get("school")),
            _clean_text(response.get("major")),
            _clean_text(response.get("degree")),
        ]
        if item
    )
    project_count = len(response["project_experience"])
    internship_count = len(response["internship_experience"])
    if project_count or internship_count:
        response["experience"] = f"{internship_count} 段实习 / {project_count} 个项目"
    else:
        response["experience"] = "暂无明确经历摘要"
    return response


def _format_salary_text(job_profile_result: dict) -> str:
    salary_stats = _safe_dict(job_profile_result.get("salary_stats"))
    salary_mid = salary_stats.get("salary_mid_month_avg")
    salary_min = salary_stats.get("salary_min_month_avg")
    salary_max = salary_stats.get("salary_max_month_avg")
    if salary_mid not in (None, ""):
        return f"月均约 {salary_mid}"
    if salary_min not in (None, "") or salary_max not in (None, ""):
        return f"{salary_min or '?'} - {salary_max or '?'} / 月"
    return "暂无薪资信息"


def _extract_report_text(report_res: dict) -> str:
    return _clean_text(report_res.get("report_text_markdown") or report_res.get("report_text"))


def _build_report_detail(all_data: dict) -> dict:
    report_res = _safe_dict(all_data.get("career_report_result"))
    report_text = _extract_report_text(report_res)
    if report_text:
        _write_report_file(report_text)
    return {
        "file_name": REPORT_FILE_NAME,
        "report_title": _clean_text(report_res.get("report_title")) or "职业规划报告",
        "report_summary": _clean_text(
            report_res.get("report_summary")
            or report_res.get("action_summary")
            or report_res.get("match_summary")
            or report_res.get("path_summary")
        ),
        "report_text": report_text,
        "report_sections": _normalize_section_list(report_res.get("report_sections")),
        "edit_suggestions": _safe_list(report_res.get("edit_suggestions")),
        "completeness_check": _safe_dict(report_res.get("completeness_check")),
    }


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
        run_pipeline(tmp_path, "", str(_state_file_path()))
        
        # 返回第一步的数据
        all_data = _load_all_data()
        if all_data:
            resume_res = _build_resume_response(all_data)
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
    finally:
        try:
            if 'tmp_path' in locals() and os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except OSError:
            pass


@app.post("/api/resume/manual")
async def parse_manual_resume(req: ManualResumeRequest):
    resume_text = _clean_text(req.resume_text)
    if not resume_text:
        return {"success": False, "message": "简历文本不能为空", "data": {}}

    tmp_path = ""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt", mode="w", encoding="utf-8") as tmp:
            tmp.write(resume_text)
            tmp_path = tmp.name

        run_pipeline(tmp_path, "", str(_state_file_path()))
        all_data = _load_all_data()
        resume_res = _build_resume_response(all_data) if all_data else {}
        return {"success": True, "message": "success", "data": resume_res}
    except Exception as e:
        logger.error(f"Error parsing manual resume: {e}")
        return {"success": False, "message": str(e), "data": {}}
    finally:
        try:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except OSError:
            pass


@app.post("/api/data/process")
async def process_job_data(req: JobDataProcessRequest):
    try:
        result = await asyncio.to_thread(
            run_job_data_pipeline,
            input_path=_resolve_project_path(req.input_file),
            intermediate_dir=_resolve_project_path(req.intermediate_dir),
            sql_db_path=_resolve_project_path(req.sql_db_path),
            neo4j_output_dir=_resolve_project_path(req.neo4j_output_dir),
            sheet_name=req.sheet_name,
            log_every=req.log_every,
            max_workers=req.max_workers,
            group_sample_size=req.group_sample_size,
        )
        return {"success": True, "message": "岗位底库数据处理完成", "data": result}
    except Exception as e:
        logger.error(f"Error processing job data pipeline: {e}")
        return {"success": False, "message": str(e), "data": {}}

@app.post("/api/student/profile")
async def build_student_profile(req: Request):
    data = {}
    all_data = _load_all_data()
    if all_data:
        data = _safe_dict(all_data.get("student_profile_result"))
    return {"success": True, "data": data}

@app.post("/api/job/match")
async def match_jobs(req: Request):
    data = []
    all_data = _load_all_data()
    if all_data:
        match_res = _safe_dict(all_data.get("job_match_result"))
        job_profile_result = _safe_dict(all_data.get("job_profile_result"))
        current_target_job = _resolve_state_target_job(all_data)

        if match_res:
            score = match_res.get("overall_match_score", match_res.get("overall_score", 0))
            level = _clean_text(match_res.get("score_level")) or ("高度匹配" if score >= 80 else ("较好匹配" if score >= 60 else "需转型突破"))

            reasons = []
            strengths = _safe_list(match_res.get("strengths"))
            for s in strengths:
                reasons.append(str(_safe_dict(s).get("description", s) if isinstance(s, dict) else s))

            weaknesses = _safe_list(match_res.get("weaknesses") or match_res.get("gaps"))
            for g in weaknesses:
                reasons.append(str(_safe_dict(g).get("description", g) if isinstance(g, dict) else g))

            if not reasons:
                for item in _safe_list(match_res.get("missing_items")):
                    if isinstance(item, dict):
                        reason = item.get("reason") or item.get("required_item")
                        if reason:
                            reasons.append(str(reason))

            representative_samples = _safe_list(job_profile_result.get("representative_samples"))
            company_name = "暂无公司信息"
            if representative_samples:
                company_name = _clean_text(_safe_dict(representative_samples[0]).get("company_name")) or company_name

            data = [{
                "job_name": current_target_job,
                "match_score": score,
                "match_level": level,
                "reasons": reasons,
                "company": company_name,
                "salary": _format_salary_text(job_profile_result),
                "strengths": _safe_list(match_res.get("strengths")),
                "weaknesses": _safe_list(match_res.get("weaknesses") or match_res.get("gaps")),
                "improvement_suggestions": _safe_list(match_res.get("improvement_suggestions")),
                "recommendation": _clean_text(match_res.get("recommendation")),
                "analysis_summary": _clean_text(match_res.get("analysis_summary") or match_res.get("summary")),
                "dimension_scores": {
                    "basic_requirement_score": match_res.get("basic_requirement_score"),
                    "vocational_skill_score": match_res.get("vocational_skill_score"),
                    "professional_quality_score": match_res.get("professional_quality_score"),
                    "development_potential_score": match_res.get("development_potential_score"),
                },
            }]

    if not data:
         fallback_job_name = "未明确目标岗位"
         all_data = _load_all_data()
         if all_data:
             fallback_job_name = _resolve_state_target_job(all_data)
         data = [{"job_name": fallback_job_name, "match_score": 85, "match_level": "较好匹配", "reasons": ["无法获得匹配详情"]}]
         
    return {"success": True, "data": data}

@app.post("/api/career/path")
async def career_path(req: Request):
    data = {}
    all_data = _load_all_data()
    if all_data:
        raw = _safe_dict(all_data.get("career_path_plan_result"))

        def flatten_list(obj_list):
            if not obj_list:
                return []
            res = []
            for item in obj_list:
                if isinstance(item, str):
                    res.append(item)
                elif isinstance(item, dict):
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
            "goal_positioning": _clean_text(raw.get("goal_positioning")),
            "goal_reason": _clean_text(raw.get("goal_reason")),
            "path_strategy": _clean_text(raw.get("path_strategy")),
            "direct_path": flatten_list(raw.get("direct_path", [])),
            "transition_path": flatten_list(raw.get("transition_path", [])),
            "long_term_path": flatten_list(raw.get("long_term_path", [])),
            "short_term_plan": flatten_list(raw.get("short_term_plan", [])),
            "mid_term_plan": flatten_list(raw.get("mid_term_plan", [])),
            "risk_and_gap": flatten_list(raw.get("risk_and_gap", raw.get("risk_notes", []))),
            "fallback_strategy": _clean_text(raw.get("fallback_strategy")),
            "target_selection_reason": flatten_list(raw.get("target_selection_reason", [])),
            "path_selection_reason": flatten_list(raw.get("path_selection_reason", [])),
        }
    return {"success": True, "data": data}

@app.post("/api/report/generate")
async def generate_report(req: Request):
    all_data = _load_all_data()
    report_detail = _build_report_detail(all_data)
    if not _clean_text(report_detail.get("report_text")):
        return {"success": False, "message": "当前没有可用报告，请先完成主流程生成报告。", "data": ""}
    return {"success": True, "data": report_detail.get("file_name", REPORT_FILE_NAME)}

@app.get("/api/report")
async def get_report():
    all_data = _load_all_data()
    data = _build_report_detail(all_data).get("report_text", "")
    return {"success": True, "data": data}


@app.get("/api/report/detail")
async def get_report_detail():
    all_data = _load_all_data()
    return {"success": True, "data": _build_report_detail(all_data)}


@app.get("/api/report/shared")
async def get_shared_report(file_name: str = Query(default="")):
    del file_name
    all_data = _load_all_data()
    data = _build_report_detail(all_data).get("report_text", "")
    return {"success": True, "data": data}


@app.get("/api/report/download")
async def download_report(file_name: str = Query(default=REPORT_FILE_NAME)):
    all_data = _load_all_data()
    report_text = _build_report_detail(all_data).get("report_text", "")
    if not report_text:
        raise HTTPException(status_code=404, detail="报告不存在")
    return Response(
        content=report_text,
        media_type="text/markdown; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{file_name or REPORT_FILE_NAME}"'},
    )


@app.post("/api/report/update")
async def update_report(req: ReportUpdateRequest):
    report_text = _clean_text(req.report_text)
    if not report_text:
        return {"success": False, "message": "报告内容不能为空", "data": ""}

    all_data = _load_all_data()
    report_res = _safe_dict(all_data.get("career_report_result"))
    report_res["report_text_markdown"] = report_text
    report_res["report_text"] = report_text
    all_data["career_report_result"] = report_res
    _write_all_data(all_data)
    _write_report_file(report_text)
    return {"success": True, "data": REPORT_FILE_NAME}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api_server:app", host="127.0.0.1", port=8000, reload=True)
