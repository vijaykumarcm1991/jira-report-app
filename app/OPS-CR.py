import requests
import pandas as pd
from requests.auth import HTTPBasicAuth
from datetime import datetime, time
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
parser = argparse.ArgumentParser(description="JIRA OPS CR Report")
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
STATUSES = [
    s.strip() for s in args.statuses.split(",") if s.strip()
]

JOB_ID = args.job_id
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
IST = pytz.timezone("Asia/Kolkata")
UTC = pytz.utc

# Parse dates from UI
start_date_obj = datetime.strptime(START_DATE, "%Y-%m-%d").date()
if END_DATE:
    end_date_obj = datetime.strptime(END_DATE, "%Y-%m-%d").date()
else:
    end_date_obj = None

now_ist = datetime.now(IST)

# Start = start date 00:00 IST
start_dt_ist = IST.localize(datetime.combine(start_date_obj, time.min))
start_dt_utc = start_dt_ist.astimezone(UTC)

# 🔴 FINAL END-DATE DECISION LOGIC (STEP 6)
if args.till_now:
    # ✅ Explicit user intent: Till now
    end_dt_ist = now_ist

elif end_date_obj == now_ist.date():
    # ✅ End date is today → till now
    end_dt_ist = now_ist

else:
    # OPTIONAL SAFETY GUARD
    if not end_date_obj:
        raise RuntimeError("Unexpected state: end_date_obj is None while till-now is False")
    end_dt_ist = IST.localize(datetime.combine(end_date_obj, time.max))

end_dt_utc = end_dt_ist.astimezone(UTC)

print(
    f"[INFO] Effective report range | "
    f"IST: {start_dt_ist} → {end_dt_ist} | "
    f"UTC: {start_dt_utc} → {end_dt_utc}"
)

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
AND issuetype = "DevOpsL3Prod - ChangeRequest"
AND created >= "{start_dt_utc.strftime('%Y-%m-%d %H:%M')}"
AND created <= "{end_dt_utc.strftime('%Y-%m-%d %H:%M')}"
{status_clause}
ORDER BY created DESC
'''

print("Executing JQL:")
print(JQL)

# ==========================
# FIELDS (UNCHANGED – 52)
# ==========================
FIELDS = [
    "project","issuekey","summary","issuetype","priority","status",
    "assignee","reporter","customfield_10748","customfield_26667",
    "customfield_30060","created","updated","resolutiondate",
    "customfield_10072","customfield_12963","customfield_12964",
    "customfield_18176","customfield_18170","customfield_10090",
    "customfield_18464","customfield_14073","customfield_11220",
    "customfield_28262","customfield_10001","customfield_10007",
    "customfield_10078","customfield_18161","customfield_18172",
    "customfield_11332","customfield_18460","customfield_18461",
    "customfield_18162","customfield_22260","customfield_18462",
    "customfield_20960","customfield_19967","customfield_23260",
    "customfield_25070","customfield_26661","customfield_26660",
    "customfield_27571","customfield_28260","customfield_11320",
    "customfield_25561","customfield_29663","customfield_29664",
    "customfield_30062","customfield_30063","customfield_30061",
    "customfield_22362","aggregatetimespent"
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

    dt = date_parser.parse(date_str)

    # If timezone is missing, assume UTC
    if dt.tzinfo is None:
        dt = pytz.utc.localize(dt)

    # Convert to IST only if not already IST
    return dt.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")


def normalize_date_only(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
    except:
        return None

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
                "Accepted By": get_value(f.get("customfield_26667")),
                "MOP Reviewer": get_value(f.get("customfield_30060")),

                "Created": to_ist_datetime(f.get("created")),
                "Updated": to_ist_datetime(f.get("updated")),
                "Resolved": to_ist_datetime(f.get("resolutiondate")),
                "Expected Closure By": to_ist_datetime(f.get("customfield_10072")),
                "Production UAT Start": to_ist_datetime(f.get("customfield_18176")),
                "Production UAT Closed": to_ist_datetime(f.get("customfield_18170")),
                "Actual Closure Date": to_ist_datetime(f.get("customfield_10090")),
                "Production Completion Date": to_ist_datetime(f.get("customfield_18464")),
                "Start Work Date": to_ist_datetime(f.get("customfield_14073")),
                "Accepted Date/Time": to_ist_datetime(f.get("customfield_11220")),
                "Expected Closure Reporting": to_ist_datetime(f.get("customfield_28262")),

                "Planned Start Date": normalize_date_only(f.get("customfield_12963")),
                "Planned End Date": normalize_date_only(f.get("customfield_12964")),

                "Customers": get_value(f.get("customfield_10001")),
                "Request Type": get_value(f.get("customfield_10007")),
                "Product Variant": get_value(f.get("customfield_10078")),
                "Staging Setup Available": get_value(f.get("customfield_18161")),
                "Downtime Taken": get_value(f.get("customfield_18172")),
                "Change Type": get_value(f.get("customfield_11332")),
                "Change Process Owner": get_value(f.get("customfield_18460")),
                "Production UAT Required": get_value(f.get("customfield_18461")),
                "Request Include In Planner": get_value(f.get("customfield_18162")),
                "Change Sub Type": get_value(f.get("customfield_22260")),
                "Staging UAT Required": get_value(f.get("customfield_18462")),
                "QAed Release": get_value(f.get("customfield_20960")),
                "Expectation Met?": get_value(f.get("customfield_19967")),
                "Raised By": get_value(f.get("customfield_23260")),
                "Type of CR": get_value(f.get("customfield_25070")),
                "Change Category": get_value(f.get("customfield_26661")),
                "Emergency": get_value(f.get("customfield_26660")),
                "Type Of Request": get_value(f.get("customfield_27571")),
                "Required Reporting Validation": get_value(f.get("customfield_28260")),
                "Related to Customer Service Team": get_value(f.get("customfield_11320")),
                "Services": get_value(f.get("customfield_25561")),
                "Change Classification": get_value(f.get("customfield_29663")),
                "Is Security Patch": get_value(f.get("customfield_29664")),
                "Change Execution Mode": get_value(f.get("customfield_30062")),
                "OARM_JOB_ID": f.get("customfield_30063"),
                "MOP Documents Attached": get_value(f.get("customfield_30061")),
                "Feasibility Testing": get_value(f.get("customfield_22362")),

                "Σ Time Spent (Seconds)": f.get("aggregatetimespent")
            })

        start_at += len(issues)
        update_progress(start_at, total_issues)

    # ==========================
    # SAVE CSV
    # ==========================
    pd.DataFrame(rows).to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    update_progress(total_issues, total_issues, status="completed")
    print(f"✅ OPS CR report exported: {OUTPUT_FILE}")

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