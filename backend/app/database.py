import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "tool_cb.db"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL;")
    return conn


def init_db() -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS documents (
            doc_id INTEGER PRIMARY KEY,
            doc_type TEXT,
            filename TEXT,
            upload_timestamp TEXT,
            full_text TEXT,
            line_count INTEGER
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS chunks (
            chunk_id INTEGER PRIMARY KEY,
            doc_id INTEGER,
            start_line INTEGER,
            end_line INTEGER,
            chunk_text TEXT,
            FOREIGN KEY (doc_id) REFERENCES documents(doc_id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS rules (
            rule_id INTEGER PRIMARY KEY,
            rule_name TEXT,
            error_type TEXT,
            pattern TEXT,
            condition_logic TEXT,
            severity TEXT,
            enabled INTEGER DEFAULT 1,
            created_timestamp TEXT
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS findings (
            finding_id INTEGER PRIMARY KEY,
            chunk_id INTEGER,
            error_type TEXT,
            severity TEXT,
            line_number INTEGER,
            description TEXT,
            source_reference TEXT,
            rule_id INTEGER,
            detected_timestamp TEXT,
            FOREIGN KEY (chunk_id) REFERENCES chunks(chunk_id)
        );
        """
    )

    # Audit trail of analysis runs (metrics per run)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS analysis_runs (
            run_id INTEGER PRIMARY KEY,
            sow_doc_id INTEGER,
            mom_doc_id INTEGER,
            brd_doc_id INTEGER,
            start_timestamp TEXT,
            end_timestamp TEXT,
            total_findings INTEGER,
            coverage_score REAL,
            FOREIGN KEY (sow_doc_id) REFERENCES documents(doc_id),
            FOREIGN KEY (mom_doc_id) REFERENCES documents(doc_id),
            FOREIGN KEY (brd_doc_id) REFERENCES documents(doc_id)
        );
        """
    )

    conn.commit()
    conn.close()

