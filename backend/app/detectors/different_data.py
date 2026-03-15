"""
different_data.py
Detects contradictions between BRD and source documents.
Fixed: numeric comparison now requires phase-specific context overlap.
"""

import re
from typing import Dict, List, Any, Optional, Tuple, Set

from ..models import insert_finding
from ..semantic import embed_sentences, most_similar


NUMBER_UNIT_RE = re.compile(
    r"(?P<num>\d[\d,]*\.?\d*\s*[kKlL]?)\s+(?P<unit>[A-Za-z]{3,})"
)

INTEGRATION_MODE_PATTERNS = [
    {
        "brd_pattern": r"\b(handled\s+in\s+batch|batch\s+only|batch\s+processing\s+only)\b",
        "source_pattern": r"\b(real.time|realtime)\b",
        "message": (
            "BRD describes an integration as batch-only but SOW/MoM "
            "references real-time updates. Verify the correct integration mode."
        ),
    },
    {
        "brd_pattern": r"\b(real.time\s+only|always\s+real.time)\b",
        "source_pattern": r"\b(batch|batches|batch\s+process)\b",
        "message": (
            "BRD describes an integration as real-time only but SOW/MoM "
            "references batch processing. Verify the correct integration mode."
        ),
    },
]

MEANINGFUL_UNITS = {
    "week", "month", "year", "day", "hour",
    "record", "user", "license", "seat",
}

# Phase keywords — a numeric fact must share phase keywords to be flagged
# This prevents "3 weeks design" matching "2 weeks testing"
PHASE_KEYWORD_GROUPS = [
    {"requirement", "gathering", "design", "discovery"},
    {"configuration", "development", "build", "configure"},
    {"testing", "training", "uat", "sit", "test"},
    {"migration", "cutover", "golive", "launch"},
    {"support", "hypercare", "postlive", "warranty"},
]


def _normalize_number(raw: str) -> float:
    raw = raw.lower().replace(",", "").strip()
    if raw.endswith("k"):
        return float(raw[:-1]) * 1000
    if raw.endswith("l"):
        return float(raw[:-1]) * 100000
    return float(raw)


def _sentence_split(text: str) -> List[str]:
    pieces = re.split(r"(?<=[\.\?\!])\s+", text)
    return [p.strip() for p in pieces if p.strip()]


def _extract_keywords(text: str) -> Set[str]:
    return set(re.findall(r"[a-z]{4,}", text.lower()))


def _same_phase(text_a: str, text_b: str) -> bool:
    """
    Returns True only if both texts reference the same project phase.
    Prevents cross-phase numeric comparisons.
    """
    kw_a = _extract_keywords(text_a)
    kw_b = _extract_keywords(text_b)

    for group in PHASE_KEYWORD_GROUPS:
        # both must share at least 1 keyword from the same phase group
        if (kw_a & group) and (kw_b & group):
            return True

    return False


def _extract_numeric_facts(source_text: str) -> List[Dict[str, Any]]:
    sentences = _sentence_split(source_text)
    facts: List[Dict[str, Any]] = []
    for sent in sentences:
        for m in NUMBER_UNIT_RE.finditer(sent):
            raw_num = m.group("num")
            unit    = m.group("unit").lower().rstrip("s")
            if unit not in MEANINGFUL_UNITS:
                continue
            try:
                num = _normalize_number(raw_num)
            except ValueError:
                continue
            start, end = m.span()
            context = sent[max(0, start - 80): min(len(sent), end + 80)].strip()
            facts.append({"text": context, "number": num, "unit": unit})
    return facts


def _extract_brd_numeric_sentences(
    brd_text: str,
) -> List[Tuple[int, str, float, str]]:
    lines  = brd_text.splitlines()
    full   = "\n".join(lines)
    sentences = _sentence_split(full)
    results: List[Tuple[int, str, float, str]] = []
    cursor = 0
    for sent in sentences:
        idx = full.find(sent, cursor)
        if idx == -1:
            continue
        line_no = full.count("\n", 0, idx) + 1
        cursor  = idx + len(sent)
        for m in NUMBER_UNIT_RE.finditer(sent):
            raw_num = m.group("num")
            unit    = m.group("unit").lower().rstrip("s")
            if unit not in MEANINGFUL_UNITS:
                continue
            try:
                num = _normalize_number(raw_num)
            except ValueError:
                continue
            results.append((line_no, sent, num, unit))
    return results


def _check_integration_mode_contradictions(
    sow_text: str,
    mom_text: str,
    brd_text: str,
    chunks: List[Dict[str, Any]],
) -> None:
    source_lower = (sow_text + "\n" + mom_text).lower()
    lines        = brd_text.splitlines()

    def find_chunk_id(line_no):
        for ch in chunks:
            if ch["start_line"] <= line_no <= ch["end_line"]:
                return ch["chunk_id"]
        return chunks[0]["chunk_id"] if chunks else None

    reported: set = set()
    for rule_idx, rule in enumerate(INTEGRATION_MODE_PATTERNS):
        if rule_idx in reported:
            continue
        if (
            re.search(rule["brd_pattern"], brd_text.lower())
            and re.search(rule["source_pattern"], source_lower)
        ):
            for i, line in enumerate(lines, start=1):
                if re.search(rule["brd_pattern"], line.lower()):
                    chunk_id = find_chunk_id(i)
                    if chunk_id:
                        insert_finding(
                            chunk_id=chunk_id,
                            error_type="different_data",
                            severity="major",
                            line_number=i,
                            description=line.strip()[:200],
                            source_reference=(
                                f"Integration Mode Contradiction: {rule['message']}"
                            ),
                        )
                    reported.add(rule_idx)
                    break


def detect(
    sow_text: str,
    mom_text: str,
    brd_text: str,
    chunks: List[Dict[str, Any]],
    similarity_threshold: float = 0.72,
) -> None:

    # ── Part 1: Integration mode contradictions ─────────────────
    _check_integration_mode_contradictions(sow_text, mom_text, brd_text, chunks)

    # ── Part 2: Numeric contradictions ─────────────────────────
    source_text = (sow_text or "") + "\n" + (mom_text or "")
    facts       = _extract_numeric_facts(source_text)

    if not facts:
        return

    fact_texts = [f["text"] for f in facts]
    fact_embs  = embed_sentences(fact_texts)

    brd_items = _extract_brd_numeric_sentences(brd_text)
    if not brd_items:
        return

    brd_sentences_list = [item[1] for item in brd_items]
    brd_embs           = embed_sentences(brd_sentences_list)

    def find_chunk_id(line_no: int) -> Optional[int]:
        for ch in chunks:
            if ch["start_line"] <= line_no <= ch["end_line"]:
                return ch["chunk_id"]
        return None

    def fmt(n: float) -> str:
        return str(int(n)) if n == int(n) else str(n)

    findings_added = 0
    seen_sentences: set = set()

    for idx, (line_no, brd_sent, brd_num, brd_unit) in enumerate(brd_items):
        key = (line_no, brd_sent)
        if key in seen_sentences:
            continue

        sims = most_similar(brd_embs[idx], fact_embs, top_k=1)
        if not sims:
            continue

        fact_idx, sim = sims[0]
        if sim < similarity_threshold:
            continue

        fact    = facts[fact_idx]
        src_num = fact["number"]

        if fact["unit"] != brd_unit or src_num == brd_num:
            continue

        # ── Phase guard: only flag if same project phase ────────
        if not _same_phase(brd_sent, fact["text"]):
            continue

        chunk_id = find_chunk_id(line_no)
        if not chunk_id:
            continue

        critical_units = {"week", "month", "record"}
        severity = "critical" if brd_unit in critical_units else "major"

        insert_finding(
            chunk_id=chunk_id,
            error_type="different_data",
            severity=severity,
            line_number=line_no,
            description=brd_sent.strip(),
            source_reference=(
                f"Different Data: BRD {fmt(brd_num)} {brd_unit} vs "
                f"source {fmt(src_num)} {brd_unit} (\"{fact['text']}\")"
            ),
        )

        seen_sentences.add(key)
        findings_added += 1

        if findings_added >= 5:
            break