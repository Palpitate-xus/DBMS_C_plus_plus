#!/usr/bin/env python3
"""Regression checks for audit coverage and false completion detection."""

import copy
import importlib.util
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location("gap_progress", ROOT / "scripts/check_gap_progress.py")
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


class GapProgressTest(unittest.TestCase):
    def setUp(self):
        self.ledger = json.loads((ROOT / "docs/gap-progress.json").read_text(encoding="utf-8"))
        self.audit = (ROOT / self.ledger["source"]).read_text(encoding="utf-8")

    def check_errors(self, expected):
        self.assertTrue(any(expected in error for error in CHECK.validate(
            self.audit, self.ledger, ROOT)))

    def test_current_coverage(self):
        self.assertEqual(len(self.ledger["items"]), 273)
        self.assertEqual(CHECK.validate(self.audit, self.ledger, ROOT), [])

    def test_missing_gap(self):
        self.ledger["items"].pop()
        self.check_errors("IDs differ")

    def test_duplicate_gap(self):
        self.ledger["items"].append(copy.deepcopy(self.ledger["items"][0]))
        self.check_errors("duplicate progress")

    def test_false_completion(self):
        self.ledger["items"][0]["status"] = "complete"
        self.check_errors("checkbox")
        self.check_errors("requires evidence")

    def test_missing_evidence(self):
        self.ledger["items"][0]["evidence"] = ["docs/nonexistent-gap-evidence.md"]
        self.check_errors("missing evidence")

    def test_invalid_status(self):
        self.ledger["items"][0]["status"] = "looks_done"
        self.check_errors("invalid status")


if __name__ == "__main__":
    unittest.main()
