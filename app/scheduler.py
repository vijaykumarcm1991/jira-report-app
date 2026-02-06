from apscheduler.schedulers.background import BackgroundScheduler
from app.db import init_db, get_conn
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timedelta
from app.email_utils import send_email_with_attachment
import subprocess
import uuid
import os

scheduler = BackgroundScheduler(timezone="Asia/Kolkata")

REPORT_DISPLAY_NAMES = {
        "jira_infosol": "Infosol",
        "jira_ops": "OPS-Task-Bug",
        "jira_ops_cr": "OPS-CR",
        "jira_asd_incident": "ASD-Incident",
        "jira_asd_pm": "ASD-PM",
        "jsm_incident": "JSM-Incident",
    }

def run_scheduled_job(
    schedule_id,
    report_type,
    statuses,
    start_date,
    end_date,
    till_now,
    range_days,
    email_to,
):
    job_id = str(uuid.uuid4())
    output_file = f"/tmp/{job_id}.csv"

    print(
        f"[SCHEDULER] Running schedule_id={schedule_id} "
        f"job_id={job_id} "
        f"at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} IST"
    )

    now = datetime.now()

    # 🔥 Scheduler uses LAST N DAYS
    # if range_days and range_days > 0:
    #     # Last N days = completed past days only
    #     start_date = (now - timedelta(days=range_days)).strftime("%Y-%m-%d")
    #     end_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    #     till_now = False
    
    # 🔵 MODE A: Rolling window (Last N days)
    if range_days is not None and int(range_days) > 0:
        start_date = (now - timedelta(days=int(range_days))).strftime("%Y-%m-%d")
        end_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        till_now = False

    # 🔵 MODE B: Absolute date range
    else:
        # start_date and end_date come from DB as-is
        # till_now respected as provided
        pass

    report_name = REPORT_DISPLAY_NAMES.get(report_type, report_type)
    safe_report_name = report_name.replace(" ", "_")
    attachment_filename = f"{safe_report_name}_{start_date}_to_{end_date}.csv"

    # 🔑 MUST match Generate Report dropdown values
    script_map = {
        "jira_infosol": "app/Infosol.py",
        "jira_ops": "app/OPS-Task-Bug.py",
        "jira_ops_cr": "app/OPS-CR.py",
        "jira_asd_incident": "app/ASD-Incident.py",
        "jira_asd_pm": "app/ASD-PM.py",
        "jsm_incident": "app/JSM-Incident.py",
    }

    script = script_map.get(report_type)
    if not script:
        # Fail loudly – this should never be silent
        raise ValueError(f"[SCHEDULER] Unknown report_type: {report_type}")

    cmd = [
        "python",
        script,
        "--start-date", start_date,
        "--end-date", end_date,
        "--job-id", job_id,
        "--output", output_file,
    ]

    if statuses:
        cmd.extend(["--statuses", statuses])

    if till_now:
        cmd.append("--till-now")

    print("[SCHEDULER] Executing command:")
    print(" ".join(cmd))

    subprocess.run(cmd, check=True)

    if email_to:
        try:
            send_email_with_attachment(
                to_email=email_to,
                subject=f"Scheduled Jira Report: {report_name}",
                body=(
                    f"Hello,\n\n"
                    f"Please find the attached Jira report.\n\n"
                    f"Report: {report_name}\n"
                    f"Date range: {start_date} to {end_date}\n\n"
                    f"Regards,\nDevOps NOC - Jira Report Scheduler"
                ),
                attachment_path=output_file,
                attachment_filename=attachment_filename
            )
            print(f"[EMAIL] Sent report to {email_to}")
        except Exception as e:
            print(f"[EMAIL][ERROR] Failed to send email: {e}")

    print(
        f"[SCHEDULER] Completed schedule_id={schedule_id} "
        f"job_id={job_id} output={output_file}"
    )

def create_trigger(schedule_type, schedule_value, run_time):
    hour, minute = map(int, run_time.split(":"))

    if schedule_type == "once":
        run_dt = datetime.strptime(
            f"{schedule_value} {run_time}",
            "%Y-%m-%d %H:%M"
        )
        return DateTrigger(run_date=run_dt)

    if schedule_type == "daily":
        return CronTrigger(hour=hour, minute=minute)

    if schedule_type == "weekly":
        # schedule_value = mon,tue,wed...
        return CronTrigger(
            day_of_week=schedule_value,
            hour=hour,
            minute=minute
        )

    if schedule_type == "monthly":
        # schedule_value = 1-31
        return CronTrigger(
            day=schedule_value,
            hour=hour,
            minute=minute
        )

    raise ValueError(f"Unknown schedule_type: {schedule_type}")

def load_schedules():
    # 🔥 IMPORTANT: clear existing jobs first
    scheduler.remove_all_jobs()

    init_db()

    with get_conn() as conn:
        rows = conn.execute("""
            SELECT id, report_type, statuses,
                start_date, end_date, till_now,
                schedule_type, schedule_value, run_time,
                range_days, email_to
            FROM report_schedules
            WHERE enabled = 1
        """).fetchall()

    for row in rows:
        (
            schedule_id, report_type, statuses,
            start_date, end_date, till_now,
            schedule_type, schedule_value, run_time,
            range_days, email_to
        ) = row

        trigger = create_trigger(
            schedule_type,
            schedule_value,
            run_time
        )

        scheduler.add_job(
            run_scheduled_job,
            trigger=trigger,
            id=schedule_id,
            args=[
                schedule_id, report_type, statuses,
                start_date, end_date, till_now,
                range_days, email_to
            ],
            replace_existing=True
        )

        print(
            f"[SCHEDULER] Loaded {schedule_type} schedule "
            f"{schedule_id} at {run_time} IST"
        )

def run_test_job():
    job_id = str(uuid.uuid4())
    output_file = f"/tmp/{job_id}.csv"

    print(
        f"[SCHEDULER] Running test job job_id={job_id} "
        f"at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} IST"
    )

    cmd = [
        "python",
        "app/ASD-Incident.py",
        "--start-date", "2026-02-01",
        "--end-date", "2026-02-05",
        "--statuses", "Resolved",
        "--job-id", job_id,
        "--output", output_file,
    ]

    print("[SCHEDULER] Executing command:")
    print(" ".join(cmd))

    subprocess.run(cmd, check=True)

    print(f"[SCHEDULER] Job {job_id} completed. Output: {output_file}")

def start_scheduler():
    scheduler.start()
    init_db()
    load_schedules()
