"""
role_responsibility_violation.py

Detects incorrect responsibility assignments.

Examples:
- System admin performing business operations
- Sales user doing configuration tasks
- Finance users performing CRM setup
"""

import re
from typing import List, Dict, Any, Optional
from ..models import insert_finding


ROLE_RULES = [
    {
        "pattern": r"\bsystem\s*admin\b.*\b(create|approve|manage)\b\s+(lead|opportunity|quote|order)",
        "message": "System Admin should not perform business workflow operations.",
    },
    {
        "pattern": r"\bsales\s*(user|agent)\b.*\bconfigure\b",
        "message": "Sales users should not perform system configuration tasks.",
    },
    {
        "pattern": r"\bfinance\b.*\b(create|manage)\b\s+lead",
        "message": "Finance teams typically should not manage CRM lead processes.",
    },
]


def _find_chunk_id(line_no: int, chunks: List[Dict[str, Any]]) -> Optional[int]:

    for ch in chunks:
        if ch["start_line"] <= line_no <= ch["end_line"]:
            return ch["chunk_id"]

    if chunks:
        return chunks[0]["chunk_id"]

    return None


def detect(
    brd_text: str,
    chunks: List[Dict[str, Any]],
) -> None:

    if not brd_text:
        return

    lines = brd_text.splitlines()

    reported_lines = set()
    findings_added = 0

    for line_no, line in enumerate(lines, start=1):

        text = line.lower()

        for rule in ROLE_RULES:

            if re.search(rule["pattern"], text):

                if line_no in reported_lines:
                    continue

                chunk_id = _find_chunk_id(line_no, chunks)

                if not chunk_id:
                    continue

                insert_finding(
                    chunk_id=chunk_id,
                    error_type="role_responsibility_violation",
                    severity="major",
                    line_number=line_no,
                    description=line.strip()[:150],
                    source_reference=f"Role Violation: {rule['message']}",
                )

                reported_lines.add(line_no)
                findings_added += 1
                break

        if findings_added >= 4:
            break