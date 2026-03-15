"""
terminology_drift.py
Detects business terminology introduced in the BRD that does not appear
in the SOW or MoM. Ignores common English and standard BRD/CRM vocabulary.
"""

import re
from typing import List, Dict, Any, Optional
from ..models import insert_finding


STOPWORDS = {
    # common english verbs / adjectives / connectors
    "this", "that", "with", "from", "have", "will", "been", "they",
    "their", "also", "when", "what", "each", "more", "than", "into",
    "after", "before", "other", "about", "which", "where", "while",
    "across", "through", "within", "between", "would", "should", "could",
    "these", "those", "there", "here", "then", "them", "were", "your",
    # common action / description words
    "improve", "capture", "reduce", "deliver", "ensure", "provide",
    "enable", "support", "manage", "create", "update", "review", "track",
    "control", "handle", "perform", "apply", "define", "include", "allow",
    "maintain", "monitor", "submit", "assign", "convert", "generate",
    "implement", "implementing", "modernize", "modernize", "streamline",
    "standardize", "standardized", "centralize", "centralized", "automate",
    "automated", "structure", "structured", "configure", "configured",
    "address", "present", "serve", "intend", "intended", "deliver",
    "delivering", "define", "defines", "present", "presents",
    # project / BRD boilerplate
    "project", "document", "objective", "goals", "goal", "scope",
    "overview", "section", "module", "phase", "stage", "approach",
    "strategy", "solution", "platform", "system", "process", "processes",
    "requirement", "requirements", "specification", "implementation",
    "configuration", "deployment", "release", "timeline", "milestone",
    # crm / salesforce domain
    "salesforce", "cloud", "sales", "customer", "business", "management",
    "information", "user", "users", "role", "roles", "screen", "button",
    "workflow", "integration", "report", "dashboard", "lead", "leads",
    "opportunity", "opportunities", "quote", "order", "account", "contact",
    "field", "record", "records", "fields", "table", "layout", "profile",
    "permission", "validation", "trigger", "lookup", "formula", "picklist",
    "custom", "metadata", "sandbox", "production", "status", "pipeline",
    "approval", "discount", "pricing", "product", "tracking", "segment",
    "segments", "channel", "channels", "intake", "routing", "scoring",
    "enrichment", "enriched", "ingest", "ingested", "lifecycle", "cadence",
    # commonly drifted but valid words
    "experience", "efficiency", "consistent", "consistency", "facing",
    "modernize", "effort", "efforts", "capabilities", "capability",
    "greenfield", "turnkey", "unified", "consolidated", "streamlined",
    "improve", "improved", "improvement", "improvements", "measurable",
    "governance", "collaboration", "accelerate", "accelerated", "quality",
    "centralized", "aware", "cycle", "errors", "handoff", "reduces",
    "delivers", "controlled", "capture", "workflows", "reduce",
}

WORD_RE = re.compile(r"\b[a-z]{5,}\b")

# Minimum unique drift terms before flagging — raised to reduce noise
MIN_DRIFT_TERMS = 6


def _extract_terms(text: str):
    return {w for w in WORD_RE.findall(text.lower()) if w not in STOPWORDS}


def _find_chunk_id(line_no: int, chunks: List[Dict[str, Any]]) -> Optional[int]:
    for ch in chunks:
        if ch["start_line"] <= line_no <= ch["end_line"]:
            return ch["chunk_id"]
    if chunks:
        return chunks[0]["chunk_id"]
    return None


def detect(
    sow_text: str,
    mom_text: str,
    brd_text: str,
    chunks: List[Dict[str, Any]],
) -> None:

    if not brd_text:
        return

    source_text = (sow_text or "") + "\n" + (mom_text or "")
    source_terms = _extract_terms(source_text)

    lines = brd_text.splitlines()

    findings_added = 0
    reported_lines = set()

    for line_no, line in enumerate(lines, start=1):

        lower = line.lower().strip()

        # skip short lines
        if len(lower) < 80:
            continue

        words = WORD_RE.findall(lower)

        drift_terms = [
            w for w in words
            if w not in source_terms and w not in STOPWORDS
        ]

        unique_drift = sorted(set(drift_terms))

        if len(unique_drift) < MIN_DRIFT_TERMS:
            continue

        if line_no in reported_lines:
            continue

        chunk_id = _find_chunk_id(line_no, chunks)
        if not chunk_id:
            continue

        insert_finding(
            chunk_id=chunk_id,
            error_type="terminology_drift",
            severity="minor",
            line_number=line_no,
            description=line.strip()[:150],
            source_reference=(
                "Terminology Drift: terms not present in SOW/MoM "
                f"({', '.join(unique_drift[:5])})"
            ),
        )

        reported_lines.add(line_no)
        findings_added += 1

        if findings_added >= 4:
            break