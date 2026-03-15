"""
duplicate_data.py
─────────────────
Detects repeated/duplicate content within the BRD.
Optimized for block-level analysis.
"""

from difflib import SequenceMatcher
from typing import List, Dict, Any, Optional
import re

from ..models import insert_finding


# ─────────────────────────────────────────────
# Heading detector
# ─────────────────────────────────────────────

def _is_heading(text: str) -> bool:

    t = text.strip()

    if not t:
        return True

    # ALL CAPS headings
    if t.upper() == t and len(t.split()) <= 6:
        return True

    # Ends with colon
    if t.endswith(":"):
        return True

    # Numbered headings
    if re.match(r"^\d+[\.\)]\s+\w", t) and len(t) < 50:
        return True

    return False


# ─────────────────────────────────────────────
# Chunk lookup
# ─────────────────────────────────────────────

def _find_chunk_id(line_no: int, chunks: List[Dict[str, Any]]) -> Optional[int]:

    for ch in chunks:
        if ch["start_line"] <= line_no <= ch["end_line"]:
            return ch["chunk_id"]

    if chunks:
        return chunks[0]["chunk_id"]

    return None


# ─────────────────────────────────────────────
# Main detector
# ─────────────────────────────────────────────

def detect(
    brd_text: str,
    chunks: List[Dict[str, Any]],
) -> None:

    if not brd_text:
        return

    lines = brd_text.splitlines()

    sentences = [
        (i, line.strip())
        for i, line in enumerate(lines, start=1)
        if len(line.strip()) >= 70 and not _is_heading(line.strip())
    ]

    if len(sentences) < 2:
        return

    findings_added = 0
    reported_pairs = set()

    # limit comparisons to prevent explosion
    max_sentences = min(len(sentences), 60)

    for i in range(max_sentences):

        line_i, text_i = sentences[i]

        for j in range(i + 1, max_sentences):

            line_j, text_j = sentences[j]

            # ignore nearby lines
            if abs(line_j - line_i) < 5:
                continue

            pair_key = (min(line_i, line_j), max(line_i, line_j))

            if pair_key in reported_pairs:
                continue

            similarity = SequenceMatcher(
                None,
                text_i.lower(),
                text_j.lower(),
            ).ratio()

            if similarity >= 0.88:

                chunk_id = _find_chunk_id(line_i, chunks)

                if not chunk_id:
                    continue

                insert_finding(
                    chunk_id=chunk_id,
                    error_type="duplicate_data",
                    severity="minor",
                    line_number=line_i,
                    description=text_i[:250],
                    source_reference=(
                        f"Duplicate Data: lines {line_i} and {line_j} "
                        f"are {similarity:.0%} similar."
                    ),
                )

                reported_pairs.add(pair_key)
                findings_added += 1

                if findings_added >= 5:
                    return