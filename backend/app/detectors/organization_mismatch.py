"""
organization_mismatch.py
Detects organizational team mismatches.
"""

import re
from typing import List, Dict, Any, Optional, Set
from ..models import insert_finding


TEAM_STOPWORDS = {
    "this", "that", "with", "from", "have", "will", "been", "they",
    "their", "also", "when", "what", "each", "more", "than", "into",
    "after", "before", "other", "about", "which", "where", "while",
    "data", "quality", "process", "customer", "management", "business",
    "account", "general", "external", "internal", "project", "product",
    "system", "platform", "module", "service", "function", "level",
    "senior", "junior", "lead", "head", "chief", "main", "primary",
    "secondary", "global", "local", "regional", "national", "central",
    "code", "codes", "type", "types", "form", "forms", "list", "sheet",
    "report", "note", "item", "line", "flow", "step", "stage", "phase",
}

ABBR_STOPLIST = {
    "CRM", "ERP", "API", "PDF", "SQL", "MQL", "UAT", "SIT", "SOW",
    "BRD", "CSV", "PO", "PI", "SO", "TDS", "CPQ", "KPI", "SLA",
    "CAD", "IVR", "URL", "ID", "NA", "TBC", "TBD", "AI", "ML", "BI",
    "GST", "VAT", "SKU", "UOM", "FAQ", "ETA", "EOD", "FYI", "ASAP",
    "MVP", "RCA", "SRS", "USD", "EUR", "GBP", "ISO", "FOB", "CIF",
    "CFR", "LC", "MOU", "LME", "HOD", "AVP", "PPC", "COE", "DOF",
    "MOQ", "RFQ", "BOQ", "INR", "LP", "SP", "SF", "BO", "PC",
    "FRP", "MFG", "OCL", "SND", "KAM", "MJP", "CSP", "HSN", "ASC",
    "QWS", "TCS", "NSE", "BSE", "SAP", "FOR", "DIV", "REF", "OF",
    "IN", "TO", "AT", "BY", "OR", "IF", "NO", "ON", "UP", "AND",
    "THE", "NOT", "BUT", "ALL", "ARE", "WAS", "HAS",
}

# Vendor/company names — dynamically built from SOW header + hardcoded common ones
# We extract vendor names from the first 5 lines of SOW/MoM (header section)
STATIC_VENDOR_PATTERN = re.compile(
    r"\b(accenture|deloitte|infosys|wipro|cognizant|capgemini|"
    r"salesforce|microsoft|oracle|google|amazon|aws|azure|"
    r"implementation\s+partner|consulting|consultancy|"
    r"pvt|ltd|llc|inc|corp|gmbh|partners)\b",
    re.IGNORECASE,
)

HEADER_PATTERNS = re.compile(
    r"(minutes of meeting|discovery workshop|statement of work|"
    r"client organization|implementation partner|reviewed by|"
    r"prepared by|authored by)",
    re.IGNORECASE,
)

SESSION_RE = re.compile(
    r"^session\s+\d+\s*[–\-:]\s*(.+?)(?:\s*$)",
    re.IGNORECASE | re.MULTILINE,
)

NAMED_TEAM_RE = re.compile(
    r"\b((?:[A-Z][a-z]{2,}\s+){1,4}"
    r"(?:Team|Department|Group|Office|Desk|Unit|Operations|Function|Division|Squad))\b"
)

ABBR_TEAM_RE = re.compile(
    r"\b([A-Z]{2,5})\s+"
    r"(?:team|group|department|desk|unit|function|staff|engineer|user|operator|process)\b",
    re.IGNORECASE,
)


def _extract_vendor_names(sow_text: str, mom_text: str) -> Set[str]:
    """
    Dynamically extract vendor/company names from document headers.
    Looks at first 20 lines of SOW and MoM for proper nouns that
    appear near 'client', 'partner', 'by', 'between' keywords.
    """
    vendors = set()
    header_re = re.compile(
        r"\b(?:client|partner|between|prepared by|reviewed by|authored by"
        r"|implementation partner|and)\s+([A-Z][a-zA-Z]{2,20})\b",
        re.IGNORECASE,
    )
    # Check first 20 lines of each document — that's where names appear
    for text in [sow_text, mom_text]:
        for line in text.splitlines()[:20]:
            for match in header_re.finditer(line):
                name = match.group(1).strip().lower()
                if len(name) >= 3:
                    vendors.add(name)

    return vendors


def _find_chunk_id(line_no: int, chunks: List[Dict[str, Any]]) -> Optional[int]:
    for ch in chunks:
        if ch["start_line"] <= line_no <= ch["end_line"]:
            return ch["chunk_id"]
    if chunks:
        return chunks[0]["chunk_id"]
    return None


def _clean(name: str) -> str:
    return re.sub(r"[^a-z\s]", "", name.lower()).strip()


def _is_vendor(name: str, dynamic_vendors: Set[str]) -> bool:
    if STATIC_VENDOR_PATTERN.search(name):
        return True
    name_lower = name.lower().strip()
    for vendor in dynamic_vendors:
        if vendor in name_lower:
            return True
    return False


def _is_header_context(line: str) -> bool:
    return bool(HEADER_PATTERNS.search(line))


def _extract_named_teams(text: str) -> Set[str]:
    found = set()
    for match in NAMED_TEAM_RE.finditer(text):
        name = _clean(match.group(1))
        words = name.split()
        if any(w not in TEAM_STOPWORDS and len(w) > 3 for w in words):
            found.add(name)
    return found


def _extract_session_teams(text: str, dynamic_vendors: Set[str]) -> Set[str]:
    found = set()
    for match in SESSION_RE.finditer(text):
        raw  = match.group(1).strip()
        if _is_vendor(raw, dynamic_vendors) or _is_header_context(raw):
            continue
        raw  = re.split(r"[,&]", raw)[0].strip()
        name = _clean(raw)
        name = re.sub(r"[\d\.\-]+$", "", name).strip()
        words = name.split()
        if (
            name
            and len(name) > 4
            and len(words) >= 2
            and any(w not in TEAM_STOPWORDS and len(w) > 3 for w in words)
            and not _is_vendor(name, dynamic_vendors)
        ):
            found.add(name)
    return found


def _extract_abbreviations(text: str) -> Set[str]:
    found = set()
    for match in ABBR_TEAM_RE.finditer(text):
        abbr = match.group(1)
        if abbr not in ABBR_STOPLIST and len(abbr) >= 2:
            found.add(abbr)
    return found


def _term_in_text(term: str, text_lower: str) -> bool:
    words = [w for w in term.split() if len(w) > 3 and w not in TEAM_STOPWORDS]
    return bool(words) and any(w in text_lower for w in words)


def detect(
    sow_text: str,
    mom_text: str,
    brd_text: str,
    chunks: List[Dict[str, Any]],
) -> None:

    if not brd_text:
        return

    # Extract vendor names dynamically from document headers
    dynamic_vendors = _extract_vendor_names(sow_text, mom_text)

    source_text  = (sow_text or "") + "\n" + (mom_text or "")
    source_lower = source_text.lower()
    brd_lower    = brd_text.lower()

    source_named   = _extract_named_teams(source_text)
    source_session = _extract_session_teams(source_text, dynamic_vendors)
    source_abbrs   = _extract_abbreviations(source_text)
    brd_named      = _extract_named_teams(brd_text)
    brd_abbrs      = _extract_abbreviations(brd_text)

    lines          = brd_text.splitlines()
    reported_lines: set = set()
    findings_added = 0

    # ── Case 1: Named team in BRD not in source ─────────────────
    for line_no, line in enumerate(lines, start=1):
        if line_no in reported_lines or findings_added >= 5:
            break
        if _is_header_context(line):
            continue
        for team in _extract_named_teams(line):
            if _is_vendor(team, dynamic_vendors):
                continue
            if not _term_in_text(team, source_lower):
                chunk_id = _find_chunk_id(line_no, chunks)
                if not chunk_id:
                    continue
                insert_finding(
                    chunk_id=chunk_id,
                    error_type="organization_mismatch",
                    severity="major",
                    line_number=line_no,
                    description=line.strip()[:150],
                    source_reference=(
                        f"Organization mismatch: '{team}' appears in BRD "
                        "but is not mentioned in SOW/MoM."
                    ),
                )
                reported_lines.add(line_no)
                findings_added += 1
                break

    # ── Case 2: Session teams from MoM missing from BRD ─────────
    all_source = source_named | source_session
    missing    = [
        t for t in all_source
        if not _term_in_text(t, brd_lower)
        and not _is_vendor(t, dynamic_vendors)
    ]

    if missing and chunks:
        chunk_id = chunks[0]["chunk_id"]
        for team in sorted(missing)[:4]:
            insert_finding(
                chunk_id=chunk_id,
                error_type="organization_mismatch",
                severity="major",
                line_number=1,
                description=f"Team '{team}' not found in BRD",
                source_reference=(
                    f"Organization mismatch: '{team}' is defined in SOW/MoM "
                    "but not referenced in the BRD."
                ),
            )

    # ── Case 3: Role abbreviations in source missing from BRD ───
    missing_abbrs = [
        a for a in source_abbrs
        if a not in brd_abbrs and a not in ABBR_STOPLIST
    ]

    if missing_abbrs and chunks:
        chunk_id = chunks[0]["chunk_id"]
        for abbr in sorted(missing_abbrs)[:3]:
            insert_finding(
                chunk_id=chunk_id,
                error_type="organization_mismatch",
                severity="minor",
                line_number=1,
                description=f"Role/team '{abbr}' not found in BRD",
                source_reference=(
                    f"Organization mismatch: '{abbr}' is referenced in SOW/MoM "
                    "but does not appear in the BRD."
                ),
            )