"""
depth_mismatch.py
Detects when BRD sections are significantly shallower or deeper than SOW.
Works safely with block-level analysis.
"""

import re
from typing import List, Dict, Any, Optional
from ..models import insert_finding


# ─────────────────────────────────────────────
# Section extractor
# ─────────────────────────────────────────────

def _extract_sections(text: str):

    lines = text.splitlines()

    sections = {}
    current_heading = "intro"
    current_lines = []
    current_line_number = 1

    for i, line in enumerate(lines, start=1):

        stripped = line.strip()

        if not stripped:
            continue

        # heading heuristic
        if (
            len(stripped.split()) <= 8
            and stripped[0].isupper()
            and not stripped.endswith(".")
        ):

            if current_lines:
                sections[current_heading] = {
                    "body": " ".join(current_lines),
                    "line": current_line_number,
                }

            current_heading = stripped.lower()
            current_lines = []
            current_line_number = i

        else:
            current_lines.append(stripped)

    if current_lines:
        sections[current_heading] = {
            "body": " ".join(current_lines),
            "line": current_line_number,
        }

    return sections


# ─────────────────────────────────────────────
# Keyword similarity
# ─────────────────────────────────────────────

def _keyword_overlap(text_a: str, text_b: str):

    words_a = set(re.findall(r"[a-z]{4,}", text_a.lower()))
    words_b = set(re.findall(r"[a-z]{4,}", text_b.lower()))

    if not words_a:
        return 0.0

    return len(words_a & words_b) / len(words_a)


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
    sow_text: str,
    brd_text: str,
    chunks: List[Dict[str, Any]],
):

    if not sow_text or not brd_text:
        return

    sow_sections = _extract_sections(sow_text)
    brd_sections = _extract_sections(brd_text)

    if not sow_sections or not brd_sections:
        return

    findings_added = 0
    reported_lines = set()

    for sow_heading, sow_data in sow_sections.items():

        sow_body = sow_data["body"]
        sow_word_count = len(sow_body.split())

        # ignore tiny sections
        if sow_word_count < 30:
            continue

        best_heading = None
        best_score = 0.0

        for brd_heading, brd_data in brd_sections.items():

            score = _keyword_overlap(
                sow_body[:500],
                brd_data["body"][:500],
            )

            if score > best_score:
                best_score = score
                best_heading = brd_heading

        if best_heading is None or best_score < 0.15:
            continue

        brd_data = brd_sections[best_heading]
        brd_body = brd_data["body"]
        line_number = brd_data["line"]

        if line_number in reported_lines:
            continue

        brd_word_count = len(brd_body.split())

        ratio = brd_word_count / sow_word_count if sow_word_count else 1.0

        chunk_id = _find_chunk_id(line_number, chunks)

        if not chunk_id:
            continue

        # BRD too shallow
        if ratio < 0.35:

            insert_finding(
                chunk_id=chunk_id,
                error_type="depth_mismatch",
                severity="major",
                line_number=line_number,
                description=(
                    f"SOW section '{sow_heading[:60]}' has {sow_word_count} words "
                    f"but BRD has only {brd_word_count} ({ratio:.0%})."
                ),
                source_reference="SOW",
            )

            reported_lines.add(line_number)
            findings_added += 1

        # BRD excessively verbose
        elif ratio > 6.0:

            insert_finding(
                chunk_id=chunk_id,
                error_type="depth_mismatch",
                severity="minor",
                line_number=line_number,
                description=(
                    f"BRD section '{best_heading[:60]}' is much longer "
                    f"than SOW ({ratio:.1f}x)."
                ),
                source_reference="SOW",
            )

            reported_lines.add(line_number)
            findings_added += 1

        if findings_added >= 5:
            break