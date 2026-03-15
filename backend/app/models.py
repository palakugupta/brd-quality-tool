from datetime import datetime
from typing import Tuple, List, Dict, Any, Optional
import re

from .database import get_connection


# ─────────────────────────────────────────────
# DOCUMENTS
# ─────────────────────────────────────────────

def insert_document(doc_type: str, filename: str, full_text: str) -> Tuple[int, int]:

    lines = full_text.splitlines()
    line_count = len(lines)

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO documents (doc_type, filename, upload_timestamp, full_text, line_count)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            doc_type,
            filename,
            datetime.utcnow().isoformat(),
            full_text,
            line_count,
        ),
    )

    doc_id = cur.lastrowid

    conn.commit()
    conn.close()

    return doc_id, line_count


# ─────────────────────────────────────────────
# CHUNKS
# ─────────────────────────────────────────────

def create_brd_chunks(doc_id: int, full_text: str, chunk_size: int = 50) -> int:

    lines = full_text.splitlines()
    total_lines = len(lines)

    if total_lines == 0:
        return 0

    conn = get_connection()
    cur = conn.cursor()

    chunk_count = 0
    start = 0

    while start < total_lines:

        end = min(start + chunk_size, total_lines)
        chunk_lines = lines[start:end]
        start_line = start + 1
        end_line = end
        chunk_text = "\n".join(chunk_lines)

        cur.execute(
            """
            INSERT INTO chunks (doc_id, start_line, end_line, chunk_text)
            VALUES (?, ?, ?, ?)
            """,
            (doc_id, start_line, end_line, chunk_text),
        )

        chunk_count += 1
        start = end

    conn.commit()
    conn.close()

    return chunk_count


def get_document(doc_id: int) -> Optional[Dict[str, Any]]:

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT * FROM documents WHERE doc_id = ?", (doc_id,))

    row = cur.fetchone()

    conn.close()

    return dict(row) if row else None


def get_chunks_for_brd(doc_id: int) -> List[Dict[str, Any]]:

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT chunk_id, doc_id, start_line, end_line, chunk_text
        FROM chunks
        WHERE doc_id = ?
        ORDER BY start_line
        """,
        (doc_id,),
    )

    rows = cur.fetchall()

    conn.close()

    return [dict(r) for r in rows]


# ─────────────────────────────────────────────
# FINDINGS
# ─────────────────────────────────────────────

def insert_finding(
    chunk_id: int,
    error_type: str,
    severity: str,
    line_number: int,
    description: str,
    source_reference: str = "",
    rule_id: Optional[int] = None,
) -> int:

    conn = get_connection()
    cur = conn.cursor()

    # ── Dedup 1: exact same error_type + line + source_reference ──
    existing = cur.execute(
        """
        SELECT finding_id FROM findings
        WHERE chunk_id = ?
          AND error_type = ?
          AND line_number = ?
          AND source_reference = ?
        LIMIT 1
        """,
        (chunk_id, error_type, line_number, source_reference),
    ).fetchone()

    if existing:
        conn.close()
        return existing[0]

    # ── Dedup 2: same line already flagged for same subject ──────
    # Prevents different detectors flagging the same line for the
    # same root cause (e.g. Google Drive flagged by 3 detectors)
    if line_number and line_number > 0:
        key_words = [
            w for w in re.findall(r"[a-z]{5,}", source_reference.lower())
            if w not in {
                "appears", "found", "missing", "mentioned", "scope",
                "violation", "constraint", "mismatch", "contradiction",
                "reference", "source", "verify", "project", "check",
            }
        ][:3]

        if key_words:
            for kw in key_words:
                dupe = cur.execute(
                    """
                    SELECT finding_id FROM findings
                    WHERE line_number = ?
                      AND chunk_id = ?
                      AND LOWER(source_reference) LIKE ?
                    LIMIT 1
                    """,
                    (line_number, chunk_id, f"%{kw}%"),
                ).fetchone()
                if dupe:
                    conn.close()
                    return dupe[0]

    cur.execute(
        """
        INSERT INTO findings (
            chunk_id,
            error_type,
            severity,
            line_number,
            description,
            source_reference,
            rule_id,
            detected_timestamp
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            chunk_id,
            error_type,
            severity,
            line_number,
            description,
            source_reference,
            rule_id,
            datetime.utcnow().isoformat(),
        ),
    )

    finding_id = cur.lastrowid

    conn.commit()
    conn.close()

    return finding_id


def get_findings_for_brd(doc_id: int) -> List[Dict[str, Any]]:

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT f.*
        FROM findings f
        JOIN chunks c ON f.chunk_id = c.chunk_id
        WHERE c.doc_id = ?
        ORDER BY f.line_number
        """,
        (doc_id,),
    )

    rows = cur.fetchall()

    conn.close()

    return [dict(r) for r in rows]


# ─────────────────────────────────────────────
# ANALYSIS RUN TRACKING
# ─────────────────────────────────────────────

def create_analysis_run(
    sow_doc_id: Optional[int],
    mom_doc_id: Optional[int],
) -> int:

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO analysis_runs (
            sow_doc_id,
            mom_doc_id,
            start_timestamp,
            end_timestamp,
            total_findings,
            coverage_score
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            sow_doc_id,
            mom_doc_id,
            datetime.utcnow().isoformat(),
            None,
            0,
            0.0,
        ),
    )

    run_id = cur.lastrowid

    conn.commit()
    conn.close()

    return run_id


def finalize_analysis_run(
    run_id: int,
    total_findings: int,
    coverage_score: float,
) -> None:

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE analysis_runs
        SET
            end_timestamp = ?,
            total_findings = ?,
            coverage_score = ?
        WHERE run_id = ?
        """,
        (
            datetime.utcnow().isoformat(),
            total_findings,
            coverage_score,
            run_id,
        ),
    )

    conn.commit()
    conn.close()


# ─────────────────────────────────────────────
# RULE ENGINE
# ─────────────────────────────────────────────

def create_rule(
    rule_name: str,
    error_type: str,
    pattern: str,
    condition_logic: str,
    severity: str,
    enabled: int = 1,
) -> int:

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO rules (
            rule_name,
            error_type,
            pattern,
            condition_logic,
            severity,
            enabled,
            created_timestamp
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            rule_name,
            error_type,
            pattern,
            condition_logic,
            severity,
            enabled,
            datetime.utcnow().isoformat(),
        ),
    )

    rule_id = cur.lastrowid

    conn.commit()
    conn.close()

    return rule_id


def get_enabled_rules() -> List[Dict[str, Any]]:

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT *
        FROM rules
        WHERE enabled = 1
        ORDER BY rule_id
        """
    )

    rows = cur.fetchall()

    conn.close()

    return [dict(r) for r in rows]


# ─────────────────────────────────────────────
# DEFAULT RULE SEED
# ─────────────────────────────────────────────

def seed_default_rules() -> None:

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) AS cnt FROM rules")

    row = cur.fetchone()
    count = row["cnt"] if row else 0

    conn.close()

    if count > 0:
        return

    defaults = [
        ("EXIM integration must be covered", "incomplete_data", "EXIM"),
        ("WhatsApp integration must be covered", "incomplete_data", "WhatsApp"),
        ("Data migration must be covered", "incomplete_data", "data migration"),
        ("Invoicing must be covered", "incomplete_data", "Invoicing"),
        ("Quote Management must be covered", "incomplete_data", "Quote Management"),
    ]

    for name, err, pattern in defaults:

        create_rule(
            rule_name=name,
            error_type=err,
            pattern=pattern,
            condition_logic=f"If '{pattern}' appears in SOW it must appear in BRD",
            severity="major",
            enabled=1,
        )