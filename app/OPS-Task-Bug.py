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
parser = argparse.ArgumentParser(description="JIRA OPS Task & Bug Report")
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

PAGE_SIZE = 500

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
project = Operations
AND issuetype in (Bug, Task)
AND created >= "{start_str}"
AND created <= "{end_str}"
{status_clause}
ORDER BY created DESC
'''

print("Executing JQL:")
print(JQL)

# ==========================
# FIELDS (UNCHANGED – 34)
# ==========================
FIELDS = [
    "project","issuekey","summary","issuetype","priority","status",
    "assignee","reporter","customfield_10748","created","updated",
    "resolutiondate","customfield_10072","customfield_10076",
    "customfield_10190","customfield_23875","customfield_10007",
    "customfield_10078","customfield_10001","customfield_11342",
    "customfield_25561","aggregatetimespent","customfield_22360",
    "customfield_11240","customfield_10090","customfield_22162",
    "customfield_21460","customfield_10077","customfield_15060",
    "customfield_22361","customfield_21161","customfield_21160",
    "customfield_23979"
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
                "Priority": get_value(f.get("priority")),
                "Status": get_value(f.get("status")),
                "Assignee": get_value(f.get("assignee")),
                "Reporter": get_value(f.get("reporter")),
                "Resources": get_value(f.get("customfield_10748")),

                "Created": to_ist_datetime(f.get("created")),
                "Updated": to_ist_datetime(f.get("updated")),
                "Resolved": to_ist_datetime(f.get("resolutiondate")),
                "Expected Closure By": to_ist_datetime(f.get("customfield_10072")),
                "Resolution Completion Date": to_ist_datetime(f.get("customfield_10076")),
                "Actual Closure Date": to_ist_datetime(f.get("customfield_10090")),
                "Initial Start Date": to_ist_datetime(f.get("customfield_22360")),
                "Start Date": to_ist_datetime(f.get("customfield_11240")),
                "Issue Reported Date Time": to_ist_datetime(f.get("customfield_22162")),

                "Task Type": get_value(f.get("customfield_10190")),
                "Task Sub-Type": get_value(f.get("customfield_23875")),
                "Request Type": get_value(f.get("customfield_10007")),
                "Product Variant": get_value(f.get("customfield_10078")),
                "Customers": get_value(f.get("customfield_10001")),
                "Circle": f.get("customfield_11342"),
                "Services": get_value(f.get("customfield_25561")),
                "Type of Bug": get_value(f.get("customfield_21460")),
                "Reason for Bug": get_value(f.get("customfield_15060")),
                "Resolved By": get_value(f.get("customfield_22361")),
                "Fault Attribution": get_value(f.get("customfield_23979")),

                "Resolution / Completion Details": f.get("customfield_10077"),
                "Response SLA (Bug)": f.get("customfield_21161"),
                "Resolution SLA (Bug)": f.get("customfield_21160"),

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
    print(f"✅ OPS report exported: {OUTPUT_FILE}")

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