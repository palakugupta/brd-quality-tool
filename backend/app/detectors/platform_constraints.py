"""
platform_constraints.py
Detects BRD statements that violate known platform limitations.
Platform rules are generic (Salesforce/CRM constraints that apply universally).
No project-specific hardcoding.
"""

import re
from typing import List, Dict, Any
from ..models import insert_finding


# Generic platform constraints — apply to ANY Salesforce/CRM project
PLATFORM_LIMITATIONS = [
    {
        "platform": "salesforce",
        "patterns": [
            r"auto.?merge\s+leads?",
            r"automatic\s+lead\s+merging",
            r"automatically\s+merge\s+leads?",
            r"merge\s+leads?\s+automatically",
        ],
        "message": "Salesforce does not support automatic lead merging natively. Manual review or custom implementation is required.",
    },
    {
        "platform": "salesforce",
        "patterns": [
            r"auto.?merge\s+accounts?",
            r"automatic\s+account\s+merging",
        ],
        "message": "Salesforce does not automatically merge accounts.",
    },
    {
        "platform": "salesforce",
        "patterns": [
            r"\bai.?\s*scoring\b",
            r"\bai.?based\s+scoring\b",
            r"\bai\s+lead\s+scoring\b",
            r"\bmachine\s+learning\s+scoring\b",
            r"\bml\s+scoring\b",
        ],
        "message": "AI/ML scoring is not a standard Salesforce capability. It requires custom implementation or a licensed add-on (e.g. Einstein).",
    },
    {
        "platform": "salesforce",
        "patterns": [
            r"confidence.{0,30}auto.?merge",
            r"auto.?merge.{0,30}confidence",
            r"confidence\s+(exceeds|above|threshold).{0,30}merge",
        ],
        "message": "Confidence-based auto-merge is not natively supported in Salesforce. Custom logic or a third-party deduplication app is required.",
    },
    {
        "platform": "salesforce",
        "patterns": [
            r"dedupli.{0,10}leads?.{0,30}(against|with)\s+(existing\s+)?(contacts?|accounts?)",
            r"leads?.{0,30}(contacts?\s+and\s+accounts?|accounts?\s+and\s+contacts?)",
            r"match\s+leads?.{0,30}(contacts?|accounts?)",
        ],
        "message": "Salesforce native deduplication only matches Leads against other Leads, not against Contacts or Accounts.",
    },
    {
        "platform": "crm",
        "patterns": [
            r"partial\s+(sales\s+)?order\s+fulfillment.{0,60}(salesforce|crm|system)",
            r"(salesforce|crm).{0,60}partial\s+(sales\s+)?order\s+fulfillment",
            r"partial\s+so\s+fulfillment.{0,60}(accommodate|salesforce|crm)",
        ],
        "message": "Partial Sales Order fulfillment constraints from ERP must be explicitly designed for in the CRM — this is not handled natively.",
    },
    {
        "platform": "crm",
        "patterns": [
            r"automatic.{0,30}dispatch\s+instructions?.{0,30}(updated?|generat)",
            r"dispatch\s+instructions?.{0,30}automatically.{0,30}(updated?|generat)",
        ],
        "message": "Automatic generation/update of dispatch instructions from SO/PI data requires custom implementation — verify feasibility.",
    },
]

DOCUMENTATION_SIGNALS = [
    "issue category:",
    "risk category:",
    "root cause:",
    "mitigation plan:",
    "mitigation:",
    "status: identified",
    "severity: pending",
    "notes:",
    "impact:",
    "implication:",
]


def _is_documentation_line(line: str) -> bool:
    lower = line.lower().strip()
    for signal in DOCUMENTATION_SIGNALS:
        if lower.startswith(signal):
            return True
    return False


def detect(
    brd_text: str,
    chunks: List[Dict[str, Any]],
) -> None:

    if not brd_text or not chunks:
        return

    lines = brd_text.splitlines()

    def find_chunk_id(line_no: int):
        for ch in chunks:
            if ch["start_line"] <= line_no <= ch["end_line"]:
                return ch["chunk_id"]
        return chunks[0]["chunk_id"]

    # Fire each rule only once
    reported_rules: set = set()

    for i, line in enumerate(lines, start=1):

        if _is_documentation_line(line):
            continue

        lower = line.lower()

        for rule_idx, rule in enumerate(PLATFORM_LIMITATIONS):

            if rule_idx in reported_rules:
                continue

            for pattern in rule["patterns"]:
                if re.search(pattern, lower):
                    insert_finding(
                        chunk_id=find_chunk_id(i),
                        error_type="platform_constraint",
                        severity="critical",
                        line_number=i,
                        description=line.strip()[:200],
                        source_reference=(
                            f"Platform Constraint ({rule['platform']}): "
                            f"{rule['message']}"
                        ),
                    )
                    reported_rules.add(rule_idx)
                    break