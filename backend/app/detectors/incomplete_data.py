"""
incomplete_data.py
Detects SOW topics/features missing from BRD.
Fully dynamic. Guards against short/redacted SOW and fragment topics.
"""

import re
from typing import List, Dict, Any, Optional

from ..models import insert_finding, get_enabled_rules
from ..semantic import embed_sentences, most_similar


SKIP_WORDS = {
    "this", "that", "with", "from", "have", "will", "been", "they",
    "their", "also", "when", "what", "each", "more", "than", "into",
    "after", "before", "other", "about", "which", "where", "while",
    "details", "fields", "layouts", "process", "processes", "management",
    "standard", "custom", "business", "attributes", "values", "provided",
    "configuration", "implementation", "scope", "assumptions", "dependencies",
    "project", "system", "platform", "requirements", "document", "description",
    "category", "section", "module", "overview", "current", "proposed",
    "redacted", "statement", "work", "between", "outlines", "engagement",
    "organization", "client", "safety", "nda", "confidential", "purposes",
    "only", "internal", "please", "note", "above", "below", "following",
    "based", "using", "provide", "ensure", "support", "manage", "enable",
    "track", "create", "update", "define", "generate", "include", "testing",
    "training", "reports", "dashboards", "migration", "discovery", "workshops",
    "requirement", "gathering", "user", "role", "lead", "account", "contact",
    "product", "vendor", "order", "dispatch", "payment", "case", "data",
    "integrations", "external", "systems", "live", "support", "post",
    "simple", "medium", "complex", "level", "levels", "will", "implemented",
    "reminders", "outlook", "email", "calendar", "formatted", "properly",
    "volume", "discounting", "based", "file", "files", "upload", "uploads",
    "click", "button", "enter", "field", "type", "select", "check",
}

BOILERPLATE_PATTERNS = [
    r"statement of work",
    r"this document",
    r"project engagement",
    r"redacted",
    r"for nda",
    r"nda safety",
    r"confidential",
    r"^\d+\s*$",
    r"^[a-z\s]{1,8}$",
    r"^(table of contents|appendix|revision history|document history)",
    r"^page \d+",
    r"all .{0,30} have been removed",
    r"implementation partner",
    r"^[-•]\s",
    r"^(simple|medium|complex)\s+(to|process|level)",
    r"\bup to \d+\b",
    r"^[a-z].{0,20}\(.*\)$",
    # fragment patterns — incomplete sentences
    r"^[A-Z].{0,40}(•|–|—|\|)",   # lines with bullets or pipes mid-sentence
    r"\ba properly formatted\b",
    r"\bvolume based\b",
    r"\bdiscounting\b",
    r"^[A-Z][a-z]+ \d",            # starts with word + number (e.g. "Page 3")
]

MIN_TOPIC_LENGTH = 35   # raised further to filter fragments
MAX_TOPIC_LENGTH = 100
MIN_TOPIC_WORDS  = 6    # raised — needs to be a full meaningful phrase
MIN_SOW_WORDS    = 100


def _is_boilerplate(text: str) -> bool:
    lower = text.lower().strip()
    for pat in BOILERPLATE_PATTERNS:
        if re.search(pat, lower):
            return True
    # skip if it contains a bullet character
    if "•" in text or "–" in text:
        return True
    # skip if more than 30% of words are skip words
    words = lower.split()
    if words:
        skip_ratio = sum(1 for w in words if w in SKIP_WORDS) / len(words)
        if skip_ratio > 0.5:
            return True
    return False


def _extract_candidate_topics(sow_text: str) -> List[str]:
    topics: List[str] = []
    lines = sow_text.splitlines()

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        if "|" in stripped:
            candidate = stripped.split("|", 1)[0].strip()
        else:
            candidate = stripped

        if not (MIN_TOPIC_LENGTH <= len(candidate) <= MAX_TOPIC_LENGTH):
            continue

        if len(candidate.split()) < MIN_TOPIC_WORDS:
            continue

        candidate = re.sub(r"^[-•*\d\.]+\s*", "", candidate).strip()

        if not candidate or not candidate[0].isalpha():
            continue

        if _is_boilerplate(candidate):
            continue

        first_word = candidate.split()[0].lower()
        if first_word in SKIP_WORDS:
            continue

        # need 3+ non-skip meaningful words of length 5+
        meaningful = [
            w for w in candidate.lower().split()
            if len(w) >= 5 and w not in SKIP_WORDS
        ]
        if len(meaningful) < 3:
            continue

        # must look like a complete sentence or requirement — ends with
        # punctuation or is a full noun phrase (not a fragment)
        if len(candidate.split()) < 6:
            continue

        topics.append(candidate)

    seen   = set()
    unique: List[str] = []
    for t in topics:
        low = t.lower()
        if low not in seen:
            seen.add(low)
            unique.append(t)

    return unique


def _sentence_split(text: str) -> List[str]:
    parts = re.split(r"(?<=[\.\?\!])\s+", text)
    return [p.strip() for p in parts if p.strip()]


def _find_chunk_id(line_no: int, chunks: List[Dict[str, Any]]) -> Optional[int]:
    for ch in chunks:
        if ch["start_line"] <= line_no <= ch["end_line"]:
            return ch["chunk_id"]
    return chunks[0]["chunk_id"]


def detect(
    sow_text: str,
    brd_text: str,
    chunks: List[Dict[str, Any]],
    semantic_threshold: float = 0.45,
    max_semantic_findings: int = 4,
) -> None:

    if not chunks:
        return

    sow_lower = sow_text.lower()
    brd_lower = brd_text.lower()

    # ── Part 1: Rule-based ──────────────────────────────────────
    rules = [r for r in get_enabled_rules() if r["error_type"] == "incomplete_data"]
    for rule in rules:
        token = (rule["pattern"] or "").strip()
        if not token:
            continue
        token_lower = token.lower()
        if token_lower in sow_lower and token_lower not in brd_lower:
            insert_finding(
                chunk_id=chunks[0]["chunk_id"],
                error_type="incomplete_data",
                severity=rule["severity"] or "major",
                line_number=1,
                description="",
                source_reference=(
                    f"Incomplete Data (rule '{rule['rule_name']}'): "
                    f"'{token}' appears in SOW but is missing from BRD."
                ),
                rule_id=rule["rule_id"],
            )

    # ── Part 2: Semantic coverage ───────────────────────────────
    if len(sow_text.split()) < MIN_SOW_WORDS:
        return

    topics = _extract_candidate_topics(sow_text)
    if not topics:
        return

    brd_sentences = _sentence_split(brd_text)
    if not brd_sentences:
        return

    topic_embs = embed_sentences(topics)
    brd_embs   = embed_sentences(brd_sentences)

    reported = 0
    for i, topic in enumerate(topics):
        tokens = [
            w for w in topic.lower().split()
            if len(w) >= 5 and w not in SKIP_WORDS
        ]
        if tokens and any(tok in brd_lower for tok in tokens):
            continue

        sims = most_similar(topic_embs[i], brd_embs, top_k=3)
        if not sims:
            continue

        best_idx, best_sim = sims[0]
        if best_sim >= semantic_threshold:
            continue

        sentence    = brd_sentences[best_idx]
        line_number = brd_text[: brd_text.find(sentence)].count("\n") + 1
        chunk_id    = _find_chunk_id(line_number, chunks)

        insert_finding(
            chunk_id=chunk_id,
            error_type="incomplete_data",
            severity="major",
            line_number=line_number,
            description=sentence.strip(),
            source_reference=(
                f"Incomplete Data: SOW topic '{topic}' not found in BRD."
            ),
        )

        reported += 1
        if reported >= max_semantic_findings:
            break