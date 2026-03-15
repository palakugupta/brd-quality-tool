"""
process_flow_validator.py
─────────────────────────
Detects incorrect business process flows in the BRD.
Example: Opportunity appearing before Lead qualification.
"""

import re
from typing import List, Dict, Any, Optional
from ..models import insert_finding


EXPECTED_FLOW = [
    "lead",
    "qualification",
    "opportunity",
    "quote",
    "order",
]


STEP_SYNONYMS = {
    "quote": ["quotation", "proforma"],
    "order": ["sales order"],
}


def _contains_step(text: str, step: str) -> bool:

    if re.search(rf"\b{step}\b", text):
        return True

    for syn in STEP_SYNONYMS.get(step, []):
        if re.search(rf"\b{syn}\b", text):
            return True

    return False


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

    step_positions = {}

    # locate first occurrence of each step
    for line_no, line in enumerate(lines, start=1):

        lower = line.lower()

        for step in EXPECTED_FLOW:

            if step not in step_positions and _contains_step(lower, step):

                step_positions[step] = line_no

    if len(step_positions) < 2:
        return

    detected_order = sorted(step_positions.items(), key=lambda x: x[1])

    detected_steps = [s[0] for s in detected_order]

    expected_subset = [s for s in EXPECTED_FLOW if s in step_positions]

    if detected_steps != expected_subset:

        line_no = detected_order[0][1]

        chunk_id = _find_chunk_id(line_no, chunks)

        if not chunk_id:
            return

        insert_finding(
            chunk_id=chunk_id,
            error_type="process_flow_error",
            severity="major",
            line_number=line_no,
            description=" → ".join(detected_steps),
            source_reference=f"Expected process order: {' → '.join(expected_subset)}",
        )