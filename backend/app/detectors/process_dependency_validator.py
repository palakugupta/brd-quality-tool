"""
process_dependency_validator.py

Validates process dependencies such as:
Lead -> Opportunity
Opportunity -> Quote
Quote -> Order
Quote -> Costing

Only flags when the LATER step appears WITHOUT the EARLIER step,
or appears BEFORE it. Uses strict context checking to avoid
false positives from words like "in order to".
"""

import re
from typing import List, Dict, Any, Optional
from ..models import insert_finding


DEPENDENCIES = [
    ("lead", "opportunity"),
    ("opportunity", "quote"),
    ("quote", "order"),
    ("quote", "costing"),
]

# Phrases that contain the keyword but are NOT process steps
FALSE_POSITIVE_PATTERNS = {
    "order": [
        r"in order to",
        r"in order for",
        r"order of",
        r"out of order",
        r"order\s+of\s+magnitude",
    ],
    "lead": [
        r"lead\s+time",
        r"leads?\s+to",
        r"leading\s+to",
        r"lead\s+by",
    ],
    "quote": [
        r"quote\s+from",
        r"as\s+quoted",
    ],
}


def _is_false_positive(term: str, line: str) -> bool:
    """Return True if the term match on this line is NOT a process step."""
    lower = line.lower()
    for pat in FALSE_POSITIVE_PATTERNS.get(term, []):
        if re.search(pat, lower):
            return True
    return False


def _find_first_line(term: str, lines: List[str]) -> Optional[int]:
    """Return 1-based line number of first real process-step occurrence."""
    pattern = re.compile(rf"\b{term}\b", re.IGNORECASE)
    for i, line in enumerate(lines, start=1):
        if pattern.search(line) and not _is_false_positive(term, line):
            return i
    return None


def _find_chunk_id(line_no: int, chunks: List[Dict[str, Any]]) -> Optional[int]:
    for ch in chunks:
        if ch["start_line"] <= line_no <= ch["end_line"]:
            return ch["chunk_id"]
    return chunks[0]["chunk_id"] if chunks else None


def detect(brd_text: str, chunks: List[Dict[str, Any]]) -> None:

    if not brd_text or not chunks:
        return

    lines = brd_text.splitlines()

    for earlier, later in DEPENDENCIES:

        earlier_line = _find_first_line(earlier, lines)
        later_line = _find_first_line(later, lines)

        # later step exists but earlier step completely absent
        if later_line is not None and earlier_line is None:
            chunk_id = _find_chunk_id(later_line, chunks)
            insert_finding(
                chunk_id=chunk_id,
                error_type="process_dependency_violation",
                severity="major",
                line_number=later_line,
                description=(
                    f"'{later}' found at line {later_line} "
                    f"but '{earlier}' is absent from BRD."
                ),
                source_reference=(
                    f"Process dependency violation: '{later}' appears without '{earlier}'."
                ),
            )

        # later step appears BEFORE earlier step
        elif (
            earlier_line is not None
            and later_line is not None
            and later_line < earlier_line
            and (earlier_line - later_line) > 5  # ignore if within 5 lines (intro context)
        ):
            chunk_id = _find_chunk_id(later_line, chunks)
            insert_finding(
                chunk_id=chunk_id,
                error_type="process_dependency_violation",
                severity="major",
                line_number=later_line,
                description=(
                    f"'{later}' first appears at line {later_line} "
                    f"before '{earlier}' at line {earlier_line}."
                ),
                source_reference=(
                    f"Process dependency violation: '{later}' appears before '{earlier}'."
                ),
            )