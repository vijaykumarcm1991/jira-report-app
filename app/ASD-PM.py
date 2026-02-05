import requests
import pandas as pd
from requests.auth import HTTPBasicAuth
from datetime import datetime
from dateutil import parser as date_parser
import pytz
import argparse
import json
import os
import traceback
import signal
import sys


# ==========================
# ARGUMENTS FROM UI
# ==========================
parser = argparse.ArgumentParser(description="JIRA ASD PM (Problem) Report")
parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
parser.add_argument("--end-date", help="YYYY-MM-DD")
parser.add_argument("--output", required=True, help="Output CSV file")
parser.add_argument("--job-id", required=True)
parser.add_argument("--statuses", help="Comma-separated list of Jira statuses", default="")
parser.add_argument("--till-now", action="store_true", help="If set, end date is current time")

args = parser.parse_args()
START_DATE = args.start_date
END_DATE = args.end_date
OUTPUT_FILE = args.output
JOB_ID = args.job_id
STATUSES = [
    s.strip() for s in args.statuses.split(",") if s.strip()
]

PROGRESS_FILE = f"/tmp/{JOB_ID}.json"
CANCELLED = False

# ==========================
# CONFIGURATION
# ==========================

JIRA_URL = os.getenv("JIRA_URL")
USERNAME = os.getenv("JIRA_USERNAME")
PASSWORD = os.getenv("JIRA_PASSWORD")

if not JIRA_URL:
    raise RuntimeError("JIRA_URL environment variable not set")
if not USERNAME:
    raise RuntimeError("JIRA_USERNAME environment variable not set")
if not PASSWORD:
    raise RuntimeError("JIRA_PASSWORD environment variable not set")

if not args.till_now and not END_DATE:
    raise ValueError("End date is required unless till-now is set")

PAGE_SIZE = 100

def build_ist_range(start_date_str, end_date_str, till_now):
    """
    start_date_str, end_date_str -> 'YYYY-MM-DD'
    Returns IST datetime strings usable directly in JQL
    """

    # Start date always begins at 00:00
    start_dt = f"{start_date_str} 00:00"

    if till_now:
        # Explicit user intent: till now
        end_dt = datetime.now().strftime("%Y-%m-%d %H:%M")
    else:
        # Explicit end date always means full day
        end_dt = f"{end_date_str} 23:59"

    return start_dt, end_dt

start_str, end_str = build_ist_range(
    START_DATE,
    END_DATE,
    args.till_now
)

print(f"[INFO] Effective report range IST: {start_str} → {end_str}")

# ==========================
# DYNAMIC JQL (FROM UI)
# ==========================

status_clause = ""

VALID_STATUSES = [s for s in STATUSES if s]

if VALID_STATUSES:
    quoted_statuses = ",".join(f'"{s}"' for s in VALID_STATUSES)
    status_clause = f"AND status IN ({quoted_statuses})"

JQL = f'''
project = asd
AND issuetype = Problem
AND created >= "{start_str}"
AND created <= "{end_str}"
{status_clause}
ORDER BY created DESC
'''

print("Executing JQL:")
print(JQL)

# ==========================
# FIELDS (UNCHANGED – 26)
# ==========================
FIELDS = [
    "project","issuekey","summary","issuetype","status","assignee",
    "reporter","created","updated","customfield_15960","customfield_15570",
    "customfield_14267","customfield_13862","customfield_10850",
    "customfield_10851","customfield_29660","customfield_15565",
    "customfield_13861","customfield_15560","customfield_15162",
    "customfield_29662","customfield_11266","customfield_13061",
    "customfield_10694","customfield_15262","aggregatetimespent"
]

def update_progress(completed, total, status="running", error=None):
    payload = {
        "completed": completed,
        "total": total,
        "status": status
    }
    if error:
        payload["error"] = error

    with open(PROGRESS_FILE, "w") as f:
        json.dump(payload, f)

def handle_sigterm(signum, frame):
    global CANCELLED
    CANCELLED = True

    update_progress(
        completed=start_at if 'start_at' in globals() else 0,
        total=total_issues if 'total_issues' in globals() else 0,
        status="cancelled"
    )

    print("⚠️ Job cancelled (SIGTERM received)")
    sys.exit(0)

signal.signal(signal.SIGTERM, handle_sigterm)

# ==========================
# HELPERS
# ==========================
def get_value(field):
    if field is None:
        return None
    if isinstance(field, dict):
        return field.get("displayName") or field.get("name") or field.get("value")
    if isinstance(field, list):
        return ", ".join(
            item.get("displayName") or item.get("name") or item.get("value")
            for item in field if isinstance(item, dict)
        )
    return field


def to_ist_datetime(date_str):
    if not date_str:
        return None
    return date_parser.parse(date_str).strftime("%Y-%m-%d %H:%M:%S")

# ==========================
# FETCH DATA
# ==========================

update_progress(0, 0, status="starting")

try:
    start_at = 0
    rows = []
    total_issues = None

    while True:

        if CANCELLED:
            raise KeyboardInterrupt

        payload = {
            "jql": JQL,
            "startAt": start_at,
            "maxResults": PAGE_SIZE,
            "fields": FIELDS
        }

        response = requests.post(
            f"{JIRA_URL}/rest/api/2/search",
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=60
        )

        response.raise_for_status()
        data = response.json()

        if total_issues is None:
            total_issues = data.get("total", 0)
            update_progress(0, total_issues)

        issues = data.get("issues", [])

        if not issues:
            break

        for issue in issues:
            f = issue["fields"]

            rows.append({
                "Project": f["project"]["key"] if f.get("project") else None,
                "Key": issue["key"],
                "Summary": f.get("summary"),
                "Issue Type": get_value(f.get("issuetype")),
                "Status": get_value(f.get("status")),
                "Assignee": get_value(f.get("assignee")),
                "Reporter": get_value(f.get("reporter")),

                "Created": to_ist_datetime(f.get("created")),
                "Updated": to_ist_datetime(f.get("updated")),

                "Application Name": get_value(f.get("customfield_15960")),
                "Unit": get_value(f.get("customfield_15570")),
                "Incident Source": get_value(f.get("customfield_14267")),
                "Investigation Reason": get_value(f.get("customfield_13862")),
                "Root Cause Analysis (RCA)": f.get("customfield_10850"),
                "Corrective & Preventive Action (CAPA)": f.get("customfield_10851"),
                "Known Issue": get_value(f.get("customfield_29660")),
                "Closure Code": get_value(f.get("customfield_15565")),
                "Infra_App": get_value(f.get("customfield_13861")),
                "Incident Geography": get_value(f.get("customfield_15560")),
                "5 Why Analysis": f.get("customfield_15162"),
                "Validator Approved": get_value(f.get("customfield_29662")),
                "Country": get_value(f.get("customfield_11266")),
                "Incident Assigned To": get_value(f.get("customfield_13061")),
                "Category": get_value(f.get("customfield_10694")),
                "Affected_CI": f.get("customfield_15262"),

                "Σ Time Spent (Seconds)": f.get("aggregatetimespent")
            })

        start_at += len(issues)
        update_progress(start_at, total_issues)
        print(f"[INFO] JQL returned {total_issues} issues")

    # ==========================
    # SAVE CSV
    # ==========================
    pd.DataFrame(rows).to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    update_progress(total_issues, total_issues, status="completed")
    print(f"✅ ASD PM report exported: {OUTPUT_FILE}")

except KeyboardInterrupt:
    update_progress(
        completed=start_at if 'start_at' in locals() else 0,
        total=total_issues if 'total_issues' in locals() else 0,
        status="cancelled"
    )
    print("⚠️ Job cancelled by user")
    sys.exit(0)

except Exception as e:
    update_progress(
        completed=start_at if 'start_at' in locals() else 0,
        total=total_issues if 'total_issues' in locals() else 0,
        status="failed",
        error=str(e)
    )

    print("❌ Job failed")
    traceback.print_exc()
    raise