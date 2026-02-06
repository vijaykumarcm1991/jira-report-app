import sqlite3
from pathlib import Path

DB_PATH = Path("app/data/scheduler.db")


def get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)


def init_db():
    with get_conn() as conn:
        cur = conn.cursor()

        # 1️⃣ Create base table if it doesn't exist
        cur.execute("""
        CREATE TABLE IF NOT EXISTS report_schedules (
            id TEXT PRIMARY KEY,
            report_type TEXT NOT NULL,
            statuses TEXT,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            till_now INTEGER DEFAULT 0,
            enabled INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # 2️⃣ Fetch existing columns
        cur.execute("PRAGMA table_info(report_schedules)")
        existing_columns = {row[1] for row in cur.fetchall()}

        # 3️⃣ Add new columns safely if missing
        if "schedule_type" not in existing_columns:
            cur.execute("ALTER TABLE report_schedules ADD COLUMN schedule_type TEXT")

        if "schedule_value" not in existing_columns:
            cur.execute("ALTER TABLE report_schedules ADD COLUMN schedule_value TEXT")

        if "run_time" not in existing_columns:
            cur.execute("ALTER TABLE report_schedules ADD COLUMN run_time TEXT")

        if "range_days" not in existing_columns:
            cur.execute("ALTER TABLE report_schedules ADD COLUMN range_days INTEGER")
        
        if "email_to" not in existing_columns:
            cur.execute("ALTER TABLE report_schedules ADD COLUMN email_to TEXT")

        conn.commit()

def insert_test_schedule(schedule_id):
    with get_conn() as conn:
        conn.execute("""
        INSERT OR IGNORE INTO report_schedules
        (id, report_type, statuses, start_date, end_date, till_now, enabled)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            schedule_id,
            "ASD-Incident",
            "Resolved",
            "2026-02-01",
            "2026-02-05",
            1,
            1
        ))

def insert_schedule(
    schedule_id,
    report_type,
    statuses,
    start_date,
    end_date,
    till_now,
    schedule_type,
    schedule_value,
    run_time,
    range_days,
    email_to,
    enabled=1
):
    with get_conn() as conn:
        conn.execute("""
        INSERT INTO report_schedules
        (
            id, report_type, statuses,
            start_date, end_date, till_now,
            schedule_type, schedule_value, run_time,
            range_days, email_to, enabled
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            schedule_id, report_type, statuses,
            start_date, end_date, till_now,
            schedule_type, schedule_value, run_time,
            range_days, email_to, enabled
        ))

def fetch_schedules():
    with get_conn() as conn:
        return conn.execute("""
        SELECT id, report_type, statuses, start_date, end_date, till_now, enabled
        FROM report_schedules
        ORDER BY created_at DESC
        """).fetchall()


def toggle_schedule(schedule_id, enabled):
    with get_conn() as conn:
        conn.execute("""
        UPDATE report_schedules
        SET enabled = ?
        WHERE id = ?
        """, (enabled, schedule_id))