"""
hallucination.py
Detects BRD content not grounded in source documents.
Fixed: EXIM and single-word proper nouns now trigger scope contradictions.
"""

import re
from typing import List, Dict, Any, Optional, Set

from ..models import insert_finding
from ..semantic import embed_sentences, most_similar


COMMON_WORDS = {
    "this", "that", "with", "from", "have", "will", "been", "they",
    "their", "also", "when", "what", "each", "more", "than", "into",
    "after", "before", "other", "about", "which", "where", "while",
    "system", "include", "based", "using", "create", "update", "delete",
    "manage", "track", "enable", "support", "define", "generate", "process",
    "store", "send", "receive", "review", "approve", "complete", "connect",
    "access", "display", "provide", "require", "implement", "configure",
    "integrate", "submit", "assign", "convert", "prepare", "migrate",
    "sales", "customer", "business", "management", "information", "status",
    "teams", "users", "roles", "module", "section", "field", "table",
    "report", "screen", "workflow", "document", "quote", "order", "leads",
    "opportunities", "pricing", "product", "approval", "discount",
    "dashboard", "pipeline", "tracking", "qualification", "assignment",
    "creation", "mapping", "integration", "migration", "existing", "partner",
    "forms", "finance", "account", "contacts", "segment", "salesforce",
    "platform", "object", "profile", "permission", "validation", "trigger",
    "lookup", "formula", "picklist", "layout", "custom", "metadata",
    "sandbox", "production", "deployment", "release",
    "enrichment", "enriched", "retained", "visible", "canonical",
    "confidence", "configured", "threshold", "suggested", "records",
    "converted", "centralized", "channel", "automated", "scoring",
    "routing", "intake", "capture", "accelerate", "improve", "quality",
    "standardized", "collaboration", "workflows", "controlled", "handoff",
    "reduces", "delivers", "governance", "measurable", "structured",
    "auditable", "configurable", "continuous", "detection", "duplicate",
    "clarify", "logging", "mitigation", "reconciliation", "retries",
    "completeness", "entries", "ensures", "consistent", "formal",
    "remain", "informal", "enquiry", "systems", "acknowledged",
    "greenfield", "unified", "modernize", "streamline", "standardize",
    "implementation", "cloud", "technology", "capabilities", "solution",
    "merge", "duplicates", "deduplication", "matching", "conversion",
    "segmentation", "nurturing", "funnel",
}

WORD_RE      = re.compile(r"[a-z]{5,}")
LONG_WORD_RE = re.compile(r"[a-z]{7,}")

MIN_NOVEL_WORDS  = 7
SEMANTIC_CEILING = 0.34
MIN_SOURCE_WORDS = 150

ALWAYS_IN_SCOPE = {
    "lead", "opportunity", "quote", "order", "account", "contact",
    "invoice", "payment", "approval", "workflow", "report", "dashboard",
}

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

# Proper noun pattern — capitalized words that are likely named systems/tools
PROPER_NOUN_RE = re.compile(r"\b([A-Z][a-zA-Z]{2,20})\b")


def _extract_scope_out_subject(line: str):
    """
    Extract subject being marked out-of-scope.
    Returns (subject, key_words, has_proper_noun) or None.
    """
    lower = line.lower()

    for marker in SCOPE_OUT_MARKERS:
        idx = lower.find(marker)
        if idx == -1:
            continue

        before = line[:idx].strip()
        before = re.sub(r"^[\s\(\[\{]+", "", before)
        before = re.sub(r"[\(\[\{]+$", "", before).strip()

        if not before or len(before) < 3:
            continue

        key_words = [
            w for w in re.findall(r"[a-z]{4,}", before.lower())
            if w not in COMMON_WORDS and w not in ALWAYS_IN_SCOPE
        ]

        # Check for proper nouns (capitalized named systems like EXIM, Exotel)
        proper_nouns = [
            m.group(1).lower()
            for m in PROPER_NOUN_RE.finditer(before)
            if m.group(1).lower() not in COMMON_WORDS
            and m.group(1).lower() not in ALWAYS_IN_SCOPE
            and len(m.group(1)) >= 3
        ]

        has_proper_noun = len(proper_nouns) > 0

        # Include proper nouns in key words
        all_keys = list(set(key_words + proper_nouns))

        if all_keys:
            return before.strip(), all_keys, has_proper_noun

    return None


def _build_source_tokens(sow_text: str, mom_text: str):
    source     = (sow_text + "\n" + mom_text).lower()
    words      = set(WORD_RE.findall(source))
    token_list = re.findall(r"[a-z]{4,}", source)
    bigrams    = set()
    for i in range(len(token_list) - 1):
        bigrams.add(token_list[i] + " " + token_list[i + 1])
    return words, bigrams


def _extract_named_entities(text: str) -> Set[str]:
    entities   = set()
    entity_re  = re.compile(
        r"\b([A-Z][a-zA-Z0-9]{2,20}(?:\s+[A-Z][a-zA-Z0-9]{2,20}){1,2})\b"
    )
    context_re = re.compile(
        r"\b(system|integration|platform|tool|application|app|"
        r"provider|service|module|software|channel|telephony|"
        r"messaging|erp|crm|api|connector)\b",
        re.IGNORECASE,
    )

    lines = text.splitlines()
    for line in lines:
        if not context_re.search(line):
            continue
        for match in entity_re.finditer(line):
            term  = match.group(1).strip().lower()
            words = term.split()
            meaningful = [
                w for w in words
                if w not in COMMON_WORDS and len(w) >= 4
            ]
            if (
                len(meaningful) >= 2
                and not any(w in ALWAYS_IN_SCOPE for w in meaningful)
            ):
                entities.add(term)

    return entities


def _find_chunk(line_no: int, chunks: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for ch in chunks:
        if ch["start_line"] <= line_no <= ch["end_line"]:
            return ch
    if chunks:
        return chunks[0]
    return None


def _check_scope_contradictions(
    sow_text: str,
    mom_text: str,
    brd_text: str,
    chunks: List[Dict[str, Any]],
) -> None:
    source_lower    = (sow_text + "\n" + mom_text).lower()
    source_entities = _extract_named_entities(sow_text + "\n" + mom_text)
    lines           = brd_text.splitlines()
    reported: set   = set()

    for line_no, line in enumerate(lines, start=1):

        result = _extract_scope_out_subject(line)
        if not result:
            continue

        subject, key_words, has_proper_noun = result

        key = tuple(sorted(key_words[:3]))
        if key in reported:
            continue

        matching_entities = [
            e for e in source_entities
            if any(kw in e for kw in key_words)
        ]

        # For proper nouns (named systems like EXIM, Exotel) — 1 hit is enough
        # For generic words — require 2 hits to avoid noise
        min_hits = 1 if has_proper_noun else 2

        direct_hits = [
            w for w in key_words
            if w in source_lower and len(w) >= 4
        ]

        if matching_entities or len(direct_hits) >= min_hits:
            ch = _find_chunk(line_no, chunks)
            if not ch:
                continue

            evidence = (
                matching_entities[0] if matching_entities
                else ", ".join(direct_hits[:3])
            )

            insert_finding(
                chunk_id=ch["chunk_id"],
                error_type="hallucination",
                severity="major",
                line_number=line_no,
                description=line.strip()[:200],
                source_reference=(
                    f"Scope Contradiction: BRD marks '{subject[:60]}' "
                    f"as out of scope but SOW/MoM references it "
                    f"({evidence}). Verify scope decision."
                ),
            )
            reported.add(key)


def _check_invented_content(
    sow_text: str,
    mom_text: str,
    brd_text: str,
    chunks: List[Dict[str, Any]],
) -> None:
    source_text        = (sow_text or "") + "\n" + (mom_text or "")
    source_words_count = len(source_text.split())

    if source_words_count < MIN_SOURCE_WORDS:
        return

    source_lower      = source_text.lower()
    brd_entities      = _extract_named_entities(brd_text)
    lines             = brd_text.splitlines()
    reported_entities: set = set()
    findings_added    = 0

    for entity in brd_entities:
        if entity in reported_entities or findings_added >= 4:
            break

        words = [
            w for w in entity.split()
            if w not in COMMON_WORDS and len(w) >= 4
        ]
        if len(words) < 2:
            continue

        if any(w in ALWAYS_IN_SCOPE for w in words):
            continue

        if any(w in source_lower for w in words):
            continue

        for line_no, line in enumerate(lines, start=1):
            if entity in line.lower():
                ch = _find_chunk(line_no, chunks)
                if not ch:
                    continue
                insert_finding(
                    chunk_id=ch["chunk_id"],
                    error_type="hallucination",
                    severity="major",
                    line_number=line_no,
                    description=line.strip()[:200],
                    source_reference=(
                        f"Invented Content: '{entity}' appears in BRD "
                        "but is not mentioned in SOW/MoM. "
                        "May have been copied from another project."
                    ),
                )
                reported_entities.add(entity)
                findings_added += 1
                break


def detect(
    sow_text: str,
    mom_text: str,
    brd_text: str,
    chunks: List[Dict[str, Any]],
) -> None:

    _check_scope_contradictions(sow_text, mom_text, brd_text, chunks)
    _check_invented_content(sow_text, mom_text, brd_text, chunks)

    source_words, source_bigrams = _build_source_tokens(sow_text, mom_text)

    source_sentences = [
        s.strip()
        for s in (sow_text + "\n" + mom_text).splitlines()
        if len(s.strip()) > 20
    ]

    if not source_sentences:
        return

    source_embs = embed_sentences(source_sentences)
    lines       = brd_text.splitlines()
    brd_lines   = [l.strip() for l in lines if len(l.strip()) > 40]

    if not brd_lines:
        return

    brd_embs       = embed_sentences(brd_lines)
    hallucinations = []
    seen_lines     = set()

    for i, line in enumerate(brd_lines, start=1):
        lower = line.lower()

        if len(lower.split()) < 12:
            continue

        tokens      = set(LONG_WORD_RE.findall(lower))
        novel_words = [
            t for t in tokens
            if t not in source_words and t not in COMMON_WORDS
        ]
        novel_words = [
            w for w in novel_words
            if not any(w in bg for bg in source_bigrams)
        ]

        sims           = most_similar(brd_embs[i - 1], source_embs, top_k=1)
        semantic_score = sims[0][1] if sims else 0.0

        if len(novel_words) >= MIN_NOVEL_WORDS and semantic_score < SEMANTIC_CEILING:
            if i in seen_lines:
                continue
            ch = _find_chunk(i, chunks)
            if not ch:
                continue
            hallucinations.append({
                "chunk_id":    ch["chunk_id"],
                "line_number": i,
                "words":       sorted(novel_words)[:5],
                "line_text":   line[:120],
            })
            seen_lines.add(i)

    for h in hallucinations[:5]:
        insert_finding(
            chunk_id=h["chunk_id"],
            error_type="hallucination",
            severity="major",
            line_number=h["line_number"],
            description=h["line_text"],
            source_reference=(
                "Hallucination: low semantic grounding with sources "
                f"({', '.join(h['words'])})."
            ),
        )