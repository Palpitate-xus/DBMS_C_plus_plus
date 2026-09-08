#!/usr/bin/env python3
"""Check that the current audit and its progress ledger cover the same gaps."""

import argparse
from collections import Counter
import json
from pathlib import Path
import re


STATES = {"unverified", "partial", "in_progress", "complete", "deferred_by_user"}
ITEM = re.compile(r"^- \[([ xX])\] \*\*([A-Z0-9]+-\d+)(?:[：:]|\*\*)", re.MULTILINE)


def validate(audit, ledger, root):
    entries = ITEM.findall(audit)
    source = dict((item_id, checked.lower() == "x") for checked, item_id in entries)
    errors = []
    if len(source) != len(entries):
        errors.append("duplicate audit IDs")
    rows = ledger.get("items", [])
    ids = [row.get("id") for row in rows]
    if len(set(ids)) != len(ids):
        errors.append("duplicate progress IDs")
    if set(ids) != set(source):
        errors.append("audit/progress IDs differ")
    for row in rows:
        item_id = row.get("id")
        state = row.get("status")
        if state not in STATES:
            errors.append(f"{item_id}: invalid status")
        if source.get(item_id, False) != (state == "complete"):
            errors.append(f"{item_id}: audit checkbox and completion status differ")
        evidence = row.get("evidence", [])
        if not isinstance(evidence, list) or not all(isinstance(p, str) for p in evidence):
            errors.append(f"{item_id}: evidence must be a list of paths")
            continue
        if state == "complete" and (not evidence or not row.get("commits") or
                                    not row.get("tests")):
            errors.append(f"{item_id}: completion requires evidence, tests and commits")
        for path in evidence + row.get("tests", []):
            if not (root / path).is_file():
                errors.append(f"{item_id}: missing evidence file {path}")
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--require-complete", action="store_true",
                        help="fail while any gap remains incomplete, including deferred gaps")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    ledger = json.loads((root / "docs/gap-progress.json").read_text(encoding="utf-8"))
    audit = (root / ledger["source"]).read_text(encoding="utf-8")
    errors = validate(audit, ledger, root)
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    counts = Counter(row["status"] for row in ledger["items"])
    print(f"Total: {len(ledger['items'])}; " +
          "; ".join(f"{state}: {counts[state]}" for state in sorted(STATES)))
    if args.require_complete and counts["complete"] != len(ledger["items"]):
        print("NOT COMPLETE: the full audit still has outstanding gaps")
        return 1
    print("Audit/progress coverage is consistent (this is not a completion claim)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
