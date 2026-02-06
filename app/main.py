from fastapi import FastAPI, Form
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.requests import Request
from fastapi.staticfiles import StaticFiles
from datetime import datetime
from typing import List, Optional
from app.scheduler import scheduler, load_schedules, start_scheduler
from uuid import uuid4
from app.db import insert_schedule, fetch_schedules, toggle_schedule
import subprocess
import uuid
import os
import json
import time
import signal
import pytz

IST = pytz.timezone("Asia/Kolkata")

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")
app.mount("/static", StaticFiles(directory="app/static"), name="static")

JOB_FILES = {}
JOB_PROCESSES = {}
JOB_HISTORY = []   # stores job metadata

TMP_RETENTION_SECONDS = 24 * 60 * 60  # 24 hours

REPORT_CONFIG = {
    "jira_infosol": {
        "script": "app/Infosol.py",
        "filename": "JIRA-INFOSOL-Report.csv"
    },
    "jira_ops": {
        "script": "app/OPS-Task-Bug.py",
        "filename": "JIRA-OPS-Task-Bug-Report.csv"
    },
    "jira_ops_cr": {
        "script": "app/OPS-CR.py",
        "filename": "JIRA-OPS-CR-Report.csv"
    },
    "jira_asd_incident": {
        "script": "app/ASD-Incident.py",
        "filename": "JIRA-ASD-INCIDENT-Report.csv"
    },
    "jira_asd_pm": {
        "script": "app/ASD-PM.py",
        "filename": "JIRA-ASD-PM-Report.csv"
    },
    "jsm_incident": {
        "script": "app/JSM-Incident.py",
        "filename": "JSM-INCIDENT-Report.csv"
    }
}

def now_ist():
    return datetime.now(IST).isoformat()

def cleanup_tmp_files():
    now = time.time()

    for fname in os.listdir("/tmp"):
        if not (fname.endswith(".csv") or fname.endswith(".json")):
            continue

        fpath = os.path.join("/tmp", fname)

        try:
            mtime = os.path.getmtime(fpath)
        except FileNotFoundError:
            continue

        if now - mtime > TMP_RETENTION_SECONDS:
            try:
                os.remove(fpath)
                print(f"🧹 Deleted old tmp file: {fpath}")
            except Exception as e:
                print(f"⚠️ Failed to delete {fpath}: {e}")

@app.on_event("startup")
def startup_event():
    start_scheduler()

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/start-job")
def start_job(
    report_type: str = Form(...),
    start_date: str = Form(...),
    end_date: Optional[str] = Form(None),
    statuses: List[str] = Form([]),
    till_now: bool = Form(False)
):
    
    if not till_now and not end_date:
        raise HTTPException(
            status_code=400,
            detail="End date is required unless Till now is selected"
        )

    cleanup_tmp_files()
    
    config = REPORT_CONFIG.get(report_type)
    if not config:
        raise HTTPException(status_code=400, detail="Invalid report type")

    job_id = str(uuid.uuid4())
    output_file = f"/tmp/{job_id}.csv"

    JOB_FILES[job_id] = config["filename"]

    # 🔴 CHANGE 1: ADD JOB HISTORY ENTRY (THIS IS STEP-2)
    JOB_HISTORY.append({
        "job_id": job_id,
        "report_type": report_type,
        "report_name": config["filename"],
        "status": "starting",
        "start_time": now_ist(),
        "end_time": None,
        "rows": 0,
        "error": None,
        "filename": config["filename"]
    })

    cmd = [
        "python", config["script"],
        "--start-date", start_date,
        "--output", output_file,
        "--job-id", job_id
    ]

    # 🔴 Add end-date ONLY if present
    if end_date:
        cmd.extend(["--end-date", end_date])

    # 🔴 Add till-now flag if selected
    if till_now:
        cmd.append("--till-now")

    if statuses:
        cmd.extend(["--statuses", ",".join(statuses)])
    
    process = subprocess.Popen(cmd)

    JOB_PROCESSES[job_id] = process.pid

    return {"job_id": job_id}

@app.get("/job-status/{job_id}")
def job_status(job_id: str):
    progress_file = f"/tmp/{job_id}.json"

    if not os.path.exists(progress_file):
        return {
            "status": "starting",
            "completed": 0,
            "total": 0
        }

    with open(progress_file) as f:
        data = json.load(f)

    # 🔴 UPDATE HISTORY ENTRY
    for job in JOB_HISTORY:
        if job["job_id"] == job_id:
            job["status"] = data.get("status")
            job["rows"] = data.get("completed", 0)

            if data.get("status") in ("completed", "failed", "cancelled"):
                job["end_time"] = now_ist()
                job["error"] = data.get("error")

    return data

@app.get("/download/{job_id}")
def download(job_id: str):
    csv_file = f"/tmp/{job_id}.csv"

    if not os.path.exists(csv_file):
        raise HTTPException(status_code=404, detail="File not ready")

    return FileResponse(
        csv_file,
        filename=JOB_FILES.get(job_id, "jira-report.csv"),
        media_type="text/csv"
    )

@app.post("/cancel-job/{job_id}")
def cancel_job(job_id: str):
    pid = JOB_PROCESSES.get(job_id)

    if not pid:
        raise HTTPException(status_code=404, detail="Job not running")

    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass

    # 🔴 WRITE PROGRESS FILE
    progress_file = f"/tmp/{job_id}.json"
    with open(progress_file, "w") as f:
        json.dump({
            "status": "cancelled",
            "completed": 0,
            "total": 0
        }, f)

    # 🔴 CRITICAL FIX: UPDATE JOB HISTORY IMMEDIATELY
    for job in JOB_HISTORY:
        if job["job_id"] == job_id:
            job["status"] = "cancelled"
            job["end_time"] = now_ist()
            job["error"] = "Cancelled by user"

    JOB_PROCESSES.pop(job_id, None)

    return {"status": "cancelled"}

@app.get("/job-history")
def job_history():
    return JOB_HISTORY[::-1]  # latest job first

@app.post("/schedule-job")
def create_schedule(
    report_type: str = Form(...),
    start_date: str = Form(...),
    end_date: str = Form(...),
    statuses: str = Form(None),
    till_now: bool = Form(False),
    range_days: int = Form(...),
    schedule_type: str = Form(...),
    schedule_value: str = Form(None),
    email_to: str = Form(...),
    run_time: str = Form(...)
):
    schedule_id = str(uuid4())

    insert_schedule(
        schedule_id,
        report_type,
        statuses,
        start_date,
        end_date,
        int(till_now),
        schedule_type,
        schedule_value,
        run_time,
        range_days,
        email_to,
        1
    )

    load_schedules()

    return {"status": "ok", "schedule_id": schedule_id}

@app.get("/schedules")
def list_schedules():
    rows = fetch_schedules()
    return [
        {
            "id": r[0],
            "report_type": r[1],
            "statuses": r[2],
            "start_date": r[3],
            "end_date": r[4],
            "till_now": bool(r[5]),
            "enabled": bool(r[6]),
        }
        for r in rows
    ]

@app.post("/schedule/{schedule_id}/toggle")
def toggle(schedule_id: str, enabled: bool = Form(...)):
    toggle_schedule(schedule_id, int(enabled))
    load_schedules()
    return {"status": "ok"}

