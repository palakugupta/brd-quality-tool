"""
business_rule_violation.py
Detects business logic violations in BRD.
Fixed scope contradiction to use clean subject extraction.
"""

import re
from typing import List, Dict, Any, Optional, Set

from ..models import insert_finding


UNIVERSAL_RULES = [
    {
        "pattern": r"opportunity\s+(created|generated)\s+from\s+lead",
        "message": "Opportunity creation should occur after lead qualification, not directly from lead.",
        "severity": "major",
    },
    {
        "pattern": r"quote\s+(generated|created)\s+(directly\s+)?from\s+lead",
        "message": "Quotes should be created from Opportunities, not directly from Leads.",
        "severity": "major",
    },
    {
        "pattern": r"sales\s+order\s+(created|generated)\s+(without|before)\s+quote",
        "message": "Sales Orders should follow an approved Quote.",
        "severity": "major",
    },
    {
        "pattern": r"system\s*admin\b.{0,60}\b(create|approve|manage)\b.{0,30}\b(lead|opportunity|quote|order)\b",
        "message": "System Admin should not perform business workflow operations — this should be a business user role.",
        "severity": "major",
    },
    {
        "pattern": r"sales\s*(user|agent)\b.{0,40}\bconfigure\b",
        "message": "Sales users should not perform system configuration tasks.",
        "severity": "major",
    },
    {
        "pattern": r"sales\s+order\s+posting\s+to\s+erp",
        "message": "Verify Sales Order posting target — confirm the exact ERP system name per SOW/MoM.",
        "severity": "minor",
    },
]

DOCUMENTATION_SIGNALS = [
    "issue category:",
    "risk category:",
    "root cause:",
    "mitigation plan:",
    "mitigation:",
    "status: identified",
    "severity: pending",
    "notes:",
    "impact:",
    "implication:",
]

# Scope out markers — same as hallucination.py
SCOPE_OUT_MARKERS = [
    "out of scope",
    "not in scope",
    "excluded from scope",
    "scoped out",
    "no planned integration",
    "unless explicitly requested",
    "integration is out of scope",
    "integration out of scope",
]

# Common words — excluded from scope contradiction key word matching
_COMMON = {
    "this", "that", "with", "from", "have", "will", "been", "they",
    "their", "also", "when", "what", "each", "more", "than", "into",
    "after", "before", "other", "about", "which", "where", "while",
    "integration", "system", "platform", "tool", "formal", "enquiry",
    "used", "remain", "customers", "channel", "messaging", "informal",
}


def _is_documentation_line(line: str) -> bool:
    lower = line.lower().strip()
    for signal in DOCUMENTATION_SIGNALS:
        if lower.startswith(signal):
            return True
    return False


def _extract_scope_out_subject(line: str):
    """
    Extract the named subject being marked as out-of-scope.
    Returns (subject_text, key_words) or None.
    Splits on the scope marker to get the LEFT side (the subject).
    """
    lower = line.lower()

    for marker in SCOPE_OUT_MARKERS:
        idx = lower.find(marker)
        if idx == -1:
            continue

        before = line[:idx].strip()
        before = re.sub(r"^[\s\(\[\{]+", "", before)
        before = re.sub(r"[\(\[\{]+$", "", before).strip()

        if not before or len(before) < 4:
            continue

        key_words = [
            w for w in re.findall(r"[a-z]{4,}", before.lower())
            if w not in _COMMON
        ]

        # Need at least 2 meaningful words to avoid false positives
        if len(key_words) >= 2:
            return before.strip(), key_words

    return None


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
    sow_text: str = "",
    mom_text: str = "",
) -> None:

    if not brd_text or not chunks:
        return

    source_lower = (sow_text + "\n" + mom_text).lower()
    lines        = brd_text.splitlines()
    reported_rules: set  = set()
    reported_lines: set  = set()
    reported_scope: set  = set()

    # ── Part 1: Universal CRM rules ─────────────────────────────
    for line_no, line in enumerate(lines, start=1):
        if _is_documentation_line(line):
            continue

        text = line.lower().strip()
        if not text:
            continue

        for rule_idx, rule in enumerate(UNIVERSAL_RULES):
            if rule_idx in reported_rules:
                continue
            if re.search(rule["pattern"], text):
                chunk_id = _find_chunk_id(line_no, chunks)
                if not chunk_id:
                    continue
                insert_finding(
                    chunk_id=chunk_id,
                    error_type="business_rule_violation",
                    severity=rule.get("severity", "major"),
                    line_number=line_no,
                    description=line.strip()[:200],
                    source_reference=f"Business Rule Violation: {rule['message']}",
                )
                reported_rules.add(rule_idx)

    # ── Part 2: Dynamic tool verification ───────────────────────
    if source_lower:
        tool_re = re.compile(
            r"\b([A-Z][a-zA-Z0-9]*(?:\s+[A-Z][a-zA-Z0-9]*){0,2})\s+"
            r"(?:System|Integration|Platform|Tool|Application|App|"
            r"Software|Module|Service|Provider|Drive|Docs|Sheets)\b"
        )
        reported_tools: set = set()

        for line_no, line in enumerate(lines, start=1):
            if _is_documentation_line(line) or line_no in reported_lines:
                continue

            for match in tool_re.finditer(line):
                tool = match.group(0).strip().lower()
                if tool in reported_tools:
                    continue

                tool_words = [
                    w for w in re.findall(r"[a-z]{3,}", tool)
                    if w not in {
                        "system", "tool", "app", "platform", "application",
                        "integration", "service", "module", "software",
                        "drive", "docs", "provider",
                    }
                ]
                if not tool_words:
                    continue

                if not any(w in source_lower for w in tool_words):
                    chunk_id = _find_chunk_id(line_no, chunks)
                    if not chunk_id:
                        continue
                    insert_finding(
                        chunk_id=chunk_id,
                        error_type="business_rule_violation",
                        severity="major",
                        line_number=line_no,
                        description=line.strip()[:200],
                        source_reference=(
                            f"Unverified Tool Reference: '{tool}' appears in BRD "
                            "but is not mentioned in SOW/MoM. "
                            "Verify this tool is confirmed for this project."
                        ),
                    )
                    reported_lines.add(line_no)
                    reported_tools.add(tool)

    # ── Part 3: Dynamic scope contradiction check ───────────────
    if source_lower:
        for line_no, line in enumerate(lines, start=1):
            if _is_documentation_line(line) or line_no in reported_lines:
                continue

            result = _extract_scope_out_subject(line)
            if not result:
                continue

            subject, key_words = result
            key = tuple(sorted(key_words[:3]))
            if key in reported_scope:
                continue

            # need 2+ words of length 5+ that appear in source
            hits = [
                w for w in key_words
                if w in source_lower and len(w) >= 5
            ]

            if len(hits) >= 2:
                chunk_id = _find_chunk_id(line_no, chunks)
                if not chunk_id:
                    continue
                insert_finding(
                    chunk_id=chunk_id,
                    error_type="business_rule_violation",
                    severity="major",
                    line_number=line_no,
                    description=line.strip()[:200],
                    source_reference=(
                        f"Scope Contradiction: BRD marks '{subject[:60]}' "
                        f"as out of scope but SOW/MoM references it "
                        f"({', '.join(hits[:3])}). Verify scope decision."
                    ),
                )
                reported_lines.add(line_no)
                reported_scope.add(key)