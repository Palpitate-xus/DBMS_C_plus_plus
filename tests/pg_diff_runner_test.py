#!/usr/bin/env python3
"""The differential oracle must preserve values instead of hiding mismatches."""

import importlib.util
from pathlib import Path
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location("pgdiff", ROOT / "tests/compat/pg_diff_runner.py")
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


class DifferentialValuesTest(unittest.TestCase):
    def test_empty_is_not_null(self):
        self.assertNotEqual(RUNNER.normalize_rows([[""]]), RUNNER.normalize_rows([[None]]))

    def test_values_are_not_rewritten(self):
        rows = [[None, "", "NULL", "NULLMARK", "OID 123", "CREATE TABLE", "a\nb", "a\x1fb"]]
        self.assertEqual(RUNNER.normalize_rows(rows), rows)

    def test_null_empty_mismatch_is_reported(self):
        with mock.patch.object(RUNNER, "reference_multi", return_value=[([[""]], None, None, "")]), \
             mock.patch.object(RUNNER, "ours_query", return_value=([[None]], None, "", [])):
            diffs = RUNNER.run_case("null-empty", ["SELECT ''"], None, None)
        self.assertTrue(any("rows differ" in diff for diff in diffs), diffs)

    def test_identical_values_still_pass(self):
        rows = [[None, "", "x"]]
        with mock.patch.object(RUNNER, "reference_multi", return_value=[(rows, None, None, "")]), \
             mock.patch.object(RUNNER, "ours_query", return_value=(rows, None, "", [])):
            self.assertEqual(RUNNER.run_case("same", ["SELECT NULL, '', 'x'"], None, None), [])


if __name__ == "__main__":
    unittest.main()
