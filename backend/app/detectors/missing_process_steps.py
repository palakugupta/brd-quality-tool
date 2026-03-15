"""
missing_process_steps.py
Detects missing lifecycle steps in BRD compared to SOW process flows.
"""

import re
from typing import List, Dict, Any, Optional

from ..models import insert_finding


PROCESS_STEPS = [
    "lead",
    "qualification",
    "opportunity",
    "quote",
    "order",
    "invoice",
    "payment",
    "purchase order",
    "proforma",
    "dispatch",
]

STEP_SYNONYMS = {
    "quote":          ["quotation", "proforma invoice"],
    "order":          ["sales order"],
    "purchase order": ["po receipt", "customer po", "po/pi"],
    "dispatch":       ["dispatch instructions", "packing list", "shipping"],
    "invoice":        ["proforma invoice", "tax invoice"],
}


def _normalize_text(text: str) -> str:
    return text.lower()


def _extract_steps(text: str) -> List[str]:
    text = _normalize_text(text)
    found = []
    for step in PROCESS_STEPS:
        if re.search(rf"\b{re.escape(step)}\b", text):
            found.append(step)
            continue
        for s in STEP_SYNONYMS.get(step, []):
            if re.search(rf"\b{re.escape(s)}\b", text):
                found.append(step)
                break
    return found


def _find_line_for_step(step: str, lines: List[str], synonyms: List[str]) -> int:
    """Find the first line where a related step is mentioned — for better line reporting."""
    all_terms = [step] + synonyms
    for i, line in enumerate(lines, start=1):
        lower = line.lower()
        for term in all_terms:
            if re.search(rf"\b{re.escape(term)}\b", lower):
                return i
    return 1


def _find_chunk_id(line_no: int, chunks: List[Dict[str, Any]]) -> Optional[int]:
    for ch in chunks:
        if ch["start_line"] <= line_no <= ch["end_line"]:
            return ch["chunk_id"]
    if not chunks:
        return None
    return chunks[0]["chunk_id"]


def detect(sow_text: str, brd_text: str, chunks: List[Dict[str, Any]]) -> None:

    if not sow_text or not brd_text:
        return

    sow_steps = _extract_steps(sow_text)
    brd_steps = _extract_steps(brd_text)

    if not sow_steps:
        return

    missing_steps = [s for s in sow_steps if s not in brd_steps]

    if not missing_steps:
        return

    brd_lines = brd_text.splitlines()
    sow_lines = sow_text.splitlines()

    for step in missing_steps[:5]:

        synonyms = STEP_SYNONYMS.get(step, [])

        # try to find related line in BRD near where it should appear
        line_number = _find_line_for_step(step, brd_lines, synonyms)

        # fallback: find where the step IS mentioned in SOW
        if line_number == 1:
            line_number = _find_line_for_step(step, sow_lines, synonyms)

        chunk_id = _find_chunk_id(line_number, chunks)
        if not chunk_id:
            continue

        insert_finding(
            chunk_id=chunk_id,
            error_type="missing_process_step",
            severity="major",
            line_number=line_number,
            description=f"Missing step: '{step}'",
            source_reference=(
                f"Process Flow Issue: lifecycle step '{step}' appears in SOW "
                "but is missing from BRD process flow."
            ),
        )