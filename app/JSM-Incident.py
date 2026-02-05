import requests
import pandas as pd
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
parser = argparse.ArgumentParser(description="JSM Incident Report")
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

JSM_URL = os.getenv("JSM_URL")
PAT_TOKEN = os.getenv("JSM_PAT")

if not JSM_URL:
    raise RuntimeError("JSM_URL environment variable not set")
if not PAT_TOKEN:
    raise RuntimeError("JSM_PAT environment variable not set")

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

HEADERS = {
    "Authorization": f"Bearer {PAT_TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# ==========================
# DYNAMIC JQL (FROM UI)
# ==========================

status_clause = ""

VALID_STATUSES = [s for s in STATUSES if s]

if VALID_STATUSES:
    quoted_statuses = ",".join(f'"{s}"' for s in VALID_STATUSES)
    status_clause = f"AND status IN ({quoted_statuses})"

JQL = f'''
issuetype = Incident
AND created >= "{start_dt_utc.strftime('%Y-%m-%d %H:%M')}"
AND created <= "{end_dt_utc.strftime('%Y-%m-%d %H:%M')}"
{status_clause}
ORDER BY created DESC
'''

print("Executing JQL:")
print(JQL)

# ==========================
# FIELDS (UNCHANGED – 40)
# ==========================
FIELDS = [
    "project","issuekey","summary","issuetype","priority","status",
    "assignee","reporter","created","updated","resolutiondate",
    "customfield_10701","customfield_10300","customfield_10801",
    "customfield_10301","customfield_10123","customfield_10112",
    "customfield_10124","customfield_10126","customfield_10127",
    "customfield_10130","customfield_10131","customfield_10125",
    "customfield_10132","customfield_10133","customfield_10134",
    "customfield_11403","customfield_11402","customfield_11404",
    "customfield_11406","customfield_10143","customfield_10146",
    "customfield_10148","customfield_11500","customfield_10136",
    "customfield_10504","customfield_11001","customfield_10806",
    "customfield_11401","aggregatetimespent"
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
            f"{JSM_URL}/rest/api/2/search",
            headers=HEADERS,
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

                "Created": to_ist_datetime(f.get("created")),
                "Updated": to_ist_datetime(f.get("updated")),
                "Resolved": to_ist_datetime(f.get("resolutiondate")),
                "Assigned Date Bot": to_ist_datetime(f.get("customfield_10701")),
                "Escalation Date L2": to_ist_datetime(f.get("customfield_10300")),
                "Assigned Date L2": to_ist_datetime(f.get("customfield_10801")),
                "Escalation Date L3": to_ist_datetime(f.get("customfield_10301")),
                "Expected Resolution Date/Time": to_ist_datetime(f.get("customfield_11401")),

                "Summary Details": f.get("customfield_10123"),
                "Source": get_value(f.get("customfield_10112")),
                "Application": get_value(f.get("customfield_10124")),
                "Geography": get_value(f.get("customfield_10126")),
                "Country": get_value(f.get("customfield_10127")),
                "Unit": get_value(f.get("customfield_10130")),
                "Site_Location": f.get("customfield_10131"),
                "Affected_CI": f.get("customfield_10125"),
                "Infra_App": get_value(f.get("customfield_10132")),
                "Issue_Category": get_value(f.get("customfield_10133")),
                "Owner_Name": f.get("customfield_10134"),
                "Response SLA": get_value(f.get("customfield_11403")),
                "Resolution SLA": get_value(f.get("customfield_11402")),
                "Reason for Missed Resolution SLA": get_value(f.get("customfield_11404")),
                "Services": get_value(f.get("customfield_11406")),
                "Fault Attribution": get_value(f.get("customfield_10143")),
                "Closure Code": get_value(f.get("customfield_10146")),
                "Resolved By (Team)": get_value(f.get("customfield_10148")),
                "Service Impact": get_value(f.get("customfield_11500")),
                "Hysteresis_State": f.get("customfield_10136"),
                "Hysteresis_Counter": f.get("customfield_10504"),
                "Call Summary": f.get("customfield_11001"),
                "Assigned Back L2 (Yes/No)": get_value(f.get("customfield_10806")),

                "Σ Time Spent (Seconds)": f.get("aggregatetimespent")
            })

        start_at += len(issues)
        update_progress(start_at, total_issues)

    # ==========================
    # SAVE CSV
    # ==========================
    pd.DataFrame(rows).to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    update_progress(total_issues, total_issues, status="completed")
    print(f"✅ JSM Incident report exported: {OUTPUT_FILE}")

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