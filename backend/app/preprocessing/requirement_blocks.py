"""
requirement_blocks.py

Extract logical requirement blocks from a BRD document.
"""

import re
from typing import List, Dict


HEADING_PATTERN = re.compile(
    r"^(\d+(\.\d+)*\s+.+|[A-Z][A-Z\s]{4,}|[A-Z][a-z].+:)$"
)


def extract_requirement_blocks(brd_text: str) -> List[Dict]:

    lines = brd_text.splitlines()

    blocks = []
    current_block = None

    for i, line in enumerate(lines):

        stripped = line.strip()

        if not stripped:
            continue

        if HEADING_PATTERN.match(stripped):

            if current_block:
                current_block["end_line"] = i
                blocks.append(current_block)

            current_block = {
                "title": stripped,
                "text": "",
                "start_line": i + 1,
                "end_line": i + 1,
            }

        if current_block:
            current_block["text"] += stripped + "\n"

    if current_block:
        current_block["end_line"] = len(lines)
        blocks.append(current_block)

    return blocks