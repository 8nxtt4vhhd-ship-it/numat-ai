import json
import os
from pathlib import Path
import sqlite3
from threading import Lock
from datetime import datetime, timezone
from contextlib import closing

from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = BASE_DIR / "data" / "management_checks.db"
TOKEN_SALT = "numat-management-check-comment-v1"
TOKEN_MAX_AGE = 30 * 24 * 60 * 60
DB_LOCK = Lock()


def get_db_path():
    configured = os.getenv("MANAGEMENT_CHECKS_DB_PATH", "").strip()
    return Path(configured).expanduser() if configured else DEFAULT_DB_PATH


def get_public_base_url():
    return os.getenv(
        "MANAGEMENT_CHECKS_PUBLIC_URL",
        "https://salesfocus.numatsystems.com",
    ).strip().rstrip("/")


def get_serializer():
    secret = (
        os.getenv("MANAGEMENT_CHECKS_COMMENT_SECRET", "").strip()
        or os.getenv("APP_SESSION_SECRET", "").strip()
        or "change-this-management-comment-secret"
    )
    return URLSafeTimedSerializer(secret, salt=TOKEN_SALT)


def connect_db():
    path = get_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=15)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS reports (
            report_key TEXT PRIMARY KEY,
            report_date TEXT NOT NULL,
            report_type TEXT NOT NULL,
            subject TEXT NOT NULL,
            summary TEXT NOT NULL DEFAULT '',
            recipients_json TEXT NOT NULL DEFAULT '[]',
            generated_at TEXT NOT NULL,
            sent_at TEXT NOT NULL DEFAULT '',
            delivery_status TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS issues (
            issue_key TEXT PRIMARY KEY,
            report_key TEXT NOT NULL,
            issue_id TEXT NOT NULL,
            title TEXT NOT NULL,
            check_text TEXT NOT NULL,
            why_text TEXT NOT NULL,
            evidence_json TEXT NOT NULL DEFAULT '[]',
            category TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'Open',
            FOREIGN KEY(report_key) REFERENCES reports(report_key)
        );
        CREATE TABLE IF NOT EXISTS comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            issue_key TEXT NOT NULL,
            commenter_name TEXT NOT NULL,
            commenter_email TEXT NOT NULL DEFAULT '',
            comment_text TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(issue_key) REFERENCES issues(issue_key)
        );
        CREATE INDEX IF NOT EXISTS idx_issues_report_key ON issues(report_key);
        CREATE INDEX IF NOT EXISTS idx_comments_issue_key ON comments(issue_key);
        """
    )
    return connection


def report_key(report_date, report_type):
    return f"{str(report_date)}:{str(report_type).strip().lower()}"


def issue_key(report_date, report_type, issue_id):
    return f"{report_key(report_date, report_type)}:{str(issue_id)}"


def comment_token(report_date, report_type, issue_id):
    return get_serializer().dumps({"issue_key": issue_key(report_date, report_type, issue_id)})


def comment_url(report_date, report_type, issue_id):
    return f"{get_public_base_url()}/management-check-comment/{comment_token(report_date, report_type, issue_id)}"


def decode_comment_token(token):
    try:
        payload = get_serializer().loads(str(token or ""), max_age=TOKEN_MAX_AGE)
    except SignatureExpired:
        return {"status": "expired"}
    except BadSignature:
        return {"status": "invalid"}
    key = str((payload or {}).get("issue_key") or "").strip()
    return {"status": "ok", "issue_key": key} if key else {"status": "invalid"}


def save_report(report_type, report, recipients=None, delivery_status="generated"):
    facts = report.get("facts", {})
    report_date = str(facts.get("report_date") or "").strip()
    key = report_key(report_date, report_type)
    generated_at = str(facts.get("generated_at") or datetime.now(timezone.utc).isoformat(timespec="seconds"))
    selected_issues = report.get("issues", [])
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with DB_LOCK, closing(connect_db()) as db:
        db.execute(
            """INSERT INTO reports
               (report_key, report_date, report_type, subject, summary, recipients_json, generated_at, sent_at, delivery_status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(report_key) DO UPDATE SET
                 subject=excluded.subject, summary=excluded.summary,
                 recipients_json=excluded.recipients_json, generated_at=excluded.generated_at,
                 sent_at=CASE WHEN excluded.sent_at <> '' THEN excluded.sent_at ELSE reports.sent_at END,
                 delivery_status=excluded.delivery_status""",
            (
                key, report_date, report_type, str(report.get("subject") or ""),
                str(report.get("selection", {}).get("summary") or ""),
                json.dumps(list(recipients or [])), generated_at,
                now if delivery_status == "sent" else "", delivery_status,
            ),
        )
        for issue in selected_issues:
            item_key = issue_key(report_date, report_type, issue.get("id"))
            db.execute(
                """INSERT INTO issues
                   (issue_key, report_key, issue_id, title, check_text, why_text, evidence_json, category, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'Open')
                   ON CONFLICT(issue_key) DO UPDATE SET
                     title=excluded.title, check_text=excluded.check_text,
                     why_text=excluded.why_text, evidence_json=excluded.evidence_json,
                     category=excluded.category""",
                (
                    item_key, key, str(issue.get("id") or ""), str(issue.get("title") or ""),
                    str(issue.get("check") or ""), str(issue.get("why") or ""),
                    json.dumps(issue.get("evidence", [])), str(issue.get("category") or ""),
                ),
            )
        db.commit()
    return key


def get_issue_by_token(token):
    decoded = decode_comment_token(token)
    if decoded.get("status") != "ok":
        return decoded
    with DB_LOCK, closing(connect_db()) as db:
        row = db.execute(
            """SELECT i.*, r.report_date, r.report_type, r.subject
               FROM issues i JOIN reports r ON r.report_key=i.report_key
               WHERE i.issue_key=?""",
            (decoded["issue_key"],),
        ).fetchone()
        if not row:
            return {"status": "not_found"}
        comments = db.execute(
            "SELECT * FROM comments WHERE issue_key=? ORDER BY id DESC",
            (decoded["issue_key"],),
        ).fetchall()
    item = dict(row)
    item["evidence"] = json.loads(item.pop("evidence_json") or "[]")
    item["comments"] = [dict(comment) for comment in comments]
    return {"status": "ok", "issue": item}


def add_issue_comment(token, commenter_name, commenter_email, comment_text, status):
    result = get_issue_by_token(token)
    if result.get("status") != "ok":
        return result
    name = str(commenter_name or "").strip()
    email = str(commenter_email or "").strip().lower()
    comment = str(comment_text or "").strip()
    normalized_status = str(status or "Acknowledged").strip()
    if normalized_status not in {"Open", "Acknowledged", "Resolved", "No action required"}:
        normalized_status = "Acknowledged"
    if not name or not comment:
        return {"status": "missing_fields", "issue": result["issue"]}
    key = result["issue"]["issue_key"]
    created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with DB_LOCK, closing(connect_db()) as db:
        db.execute(
            "INSERT INTO comments (issue_key, commenter_name, commenter_email, comment_text, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (key, name, email, comment, normalized_status, created_at),
        )
        db.execute("UPDATE issues SET status=? WHERE issue_key=?", (normalized_status, key))
        db.commit()
    return {"status": "ok", "issue_key": key}


def list_report_history(limit=100):
    with DB_LOCK, closing(connect_db()) as db:
        report_rows = db.execute(
            "SELECT * FROM reports ORDER BY report_date DESC, report_type LIMIT ?",
            (max(1, min(int(limit), 500)),),
        ).fetchall()
        results = []
        for report in report_rows:
            report_item = dict(report)
            report_item["recipients"] = json.loads(report_item.pop("recipients_json") or "[]")
            issues = db.execute(
                "SELECT * FROM issues WHERE report_key=? ORDER BY rowid",
                (report_item["report_key"],),
            ).fetchall()
            report_item["issues"] = []
            for issue in issues:
                issue_item = dict(issue)
                issue_item["evidence"] = json.loads(issue_item.pop("evidence_json") or "[]")
                issue_item["comments"] = [dict(row) for row in db.execute(
                    "SELECT * FROM comments WHERE issue_key=? ORDER BY id DESC", (issue_item["issue_key"],)
                ).fetchall()]
                report_item["issues"].append(issue_item)
            results.append(report_item)
    return results
