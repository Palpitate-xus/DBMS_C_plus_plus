#!/usr/bin/env python3
"""The differential oracle must preserve values instead of hiding mismatches."""

import importlib.util
from pathlib import Path
import subprocess
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

    def test_reference_preserves_values_that_look_like_command_tags(self):
        output = subprocess.CompletedProcess(
            [], 0, b"CREATE TABLE\nINSERT 0 2\nSELECT 1\nBEGIN\nCOMMIT\n", b"")
        with mock.patch.object(RUNNER.subprocess, "run", return_value=output):
            rows = RUNNER.reference_query("SELECT value FROM rows;")[0]
        self.assertEqual(rows, [["CREATE TABLE"], ["INSERT 0 2"], ["SELECT 1"], ["BEGIN"], ["COMMIT"]])

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


class DifferentialErrorsTest(unittest.TestCase):
    def test_reference_requests_and_reads_sqlstate(self):
        output = subprocess.CompletedProcess([], 0, b"", b"ERROR:  22012\n")
        with mock.patch.object(RUNNER.subprocess, "run", return_value=output) as run:
            _, state, _, _ = RUNNER.reference_query("SELECT 1/0;")
        self.assertIn("VERBOSITY=sqlstate", run.call_args.args[0])
        self.assertEqual(state, "22012")

    def test_notice_is_not_an_error(self):
        output = subprocess.CompletedProcess([], 0, b"", b"NOTICE:  00000\n")
        with mock.patch.object(RUNNER.subprocess, "run", return_value=output):
            self.assertIsNone(RUNNER.reference_query("DO ...;")[1])

    def test_reference_tool_failure_is_not_a_successful_empty_result(self):
        output = subprocess.CompletedProcess([], 1, b"", b"No such container: pgref\n")
        with mock.patch.object(RUNNER.subprocess, "run", return_value=output):
            with self.assertRaises(RuntimeError):
                RUNNER.reference_query("SELECT 1;")

    def test_unknown_reference_code_cannot_match_arbitrary_ours_code(self):
        with mock.patch.object(RUNNER, "reference_multi", return_value=[([], "ERROR", None, "")]), \
             mock.patch.object(RUNNER, "ours_query", return_value=([], "XX000", "", [])):
            diffs = RUNNER.run_case("unknown-code", ["SELECT broken"], None, None)
        self.assertTrue(any("sqlstate differs" in diff for diff in diffs), diffs)

    def test_different_error_codes_are_reported(self):
        with mock.patch.object(RUNNER, "reference_multi", return_value=[([], "22012", None, "")]), \
             mock.patch.object(RUNNER, "ours_query", return_value=([], "XX000", "", [])):
            diffs = RUNNER.run_case("wrong-code", ["SELECT 1/0"], None, None)
        self.assertTrue(any("sqlstate differs" in diff for diff in diffs), diffs)


class DifferentialSessionTest(unittest.TestCase):
    def test_reference_case_uses_one_psql_session(self):
        stdout = (
            b"__PGDIFF_TOKEN_BEGIN_0__\n"
            b"The command has no result, or the result has no columns.\n"
            b"__PGDIFF_TOKEN_DESC_END_0__\n"
            b"__PGDIFF_TOKEN_END_0__ false 00000 0\n"
            b"__PGDIFF_TOKEN_BEGIN_1__\n"
            b"?column?\x1finteger\n?column?\x1ftext\n"
            b"__PGDIFF_TOKEN_DESC_END_1__\n"
            b"1\x1ftemp value\n"
            b"__PGDIFF_TOKEN_END_1__ false 00000 1\n"
        )
        stderr = (
            b"__PGDIFF_TOKEN_ERROR_BEGIN_0__\n"
            b"__PGDIFF_TOKEN_ERROR_END_0__\n"
            b"__PGDIFF_TOKEN_ERROR_BEGIN_1__\n"
            b"__PGDIFF_TOKEN_ERROR_END_1__\n"
        )
        output = subprocess.CompletedProcess([], 0, stdout, stderr)
        fake_uuid = mock.Mock(hex="TOKEN")
        with mock.patch.object(RUNNER.uuid, "uuid4", return_value=fake_uuid), \
             mock.patch.object(RUNNER.subprocess, "run", return_value=output) as run:
            results = RUNNER.reference_multi([
                "BEGIN", "SELECT 1, 'temp value'",
            ])
        self.assertEqual(results, [
            ([], None, None, "", []),
            ([["1", "temp value"]], None, None, "",
             ["?column?", "?column?"]),
        ])
        run.assert_called_once()
        sent = run.call_args.kwargs["input"]
        self.assertIn(b"BEGIN\n\\gdesc", sent)
        self.assertIn(b"SELECT 1, 'temp value'\n\\gdesc", sent)

    def test_reference_case_reads_each_statement_sqlstate(self):
        stdout = (
            b"__PGDIFF_TOKEN_BEGIN_0__\n"
            b"__PGDIFF_TOKEN_DESC_END_0__\n"
            b"__PGDIFF_TOKEN_END_0__ true 22012 0\n"
            b"__PGDIFF_TOKEN_BEGIN_1__\n"
            b"__PGDIFF_TOKEN_DESC_END_1__\n"
            b"__PGDIFF_TOKEN_END_1__ true 25P02 0\n"
        )
        stderr = (
            b"__PGDIFF_TOKEN_ERROR_BEGIN_0__\n"
            b"ERROR:  22012\n"
            b"__PGDIFF_TOKEN_ERROR_END_0__\n"
            b"__PGDIFF_TOKEN_ERROR_BEGIN_1__\n"
            b"ERROR:  25P02\n"
            b"__PGDIFF_TOKEN_ERROR_END_1__\n"
        )
        output = subprocess.CompletedProcess([], 0, stdout, stderr)
        fake_uuid = mock.Mock(hex="TOKEN")
        with mock.patch.object(RUNNER.uuid, "uuid4", return_value=fake_uuid), \
             mock.patch.object(RUNNER.subprocess, "run", return_value=output):
            results = RUNNER.reference_multi(["SELECT 1/0", "SELECT 1"])
        self.assertEqual([result[1] for result in results], ["22012", "25P02"])
        self.assertEqual([result[3] for result in results],
                         ["ERROR:  22012", "ERROR:  25P02"])

    def test_run_case_uses_same_session_headers(self):
        reference = [([["1"]], None, None, "", ["session column"])]
        ours = ([['1']], None, "", ["session column"])
        with mock.patch.object(RUNNER, "reference_multi", return_value=reference), \
             mock.patch.object(RUNNER, "reference_headers") as describe, \
             mock.patch.object(RUNNER, "ours_query", return_value=ours):
            self.assertEqual(
                RUNNER.run_case("session-header", ["SELECT 1"], None, None), [])
        describe.assert_not_called()

class DifferentialHeadersTest(unittest.TestCase):
    def test_headers_are_described_without_executing_query_again(self):
        output = subprocess.CompletedProcess([], 0, b"Column,Type\nlabel,bigint\n", b"")
        with mock.patch.object(RUNNER.subprocess, "run", return_value=output) as run:
            headers = RUNNER.reference_headers("SELECT nextval('sequence_probe') AS label;")
        self.assertEqual(headers, ["label"])
        self.assertIn("--csv", run.call_args.args[0])
        self.assertEqual(run.call_args.kwargs["input"],
                         b"SELECT nextval('sequence_probe') AS label\n\\gdesc\n")

    def test_quoted_multiline_header_names_are_preserved(self):
        output = subprocess.CompletedProcess(
            [], 0, b'Column,Type\n"a,b",text\n"two\nlines",text\n"""quoted""",text\n', b"")
        with mock.patch.object(RUNNER.subprocess, "run", return_value=output):
            self.assertEqual(RUNNER.reference_headers("SELECT ..."),
                             ["a,b", "two\nlines", '"quoted"'])

    def test_no_result_command_has_no_headers(self):
        output = subprocess.CompletedProcess(
            [], 0, b"The command has no result, or the result has no columns.\n", b"")
        with mock.patch.object(RUNNER.subprocess, "run", return_value=output):
            self.assertEqual(RUNNER.reference_headers("BEGIN"), [])

    def test_unexpected_descriptor_output_cannot_silently_skip_comparison(self):
        output = subprocess.CompletedProcess([], 0, b"unexpected output\n", b"")
        with mock.patch.object(RUNNER.subprocess, "run", return_value=output):
            with self.assertRaises(RuntimeError):
                RUNNER.reference_headers("SELECT 1")

    def test_terminal_semicolon_before_comment_does_not_execute_the_query(self):
        output = subprocess.CompletedProcess([], 0, b"Column,Type\nx,integer\n", b"")
        with mock.patch.object(RUNNER.subprocess, "run", return_value=output) as run:
            self.assertEqual(RUNNER.reference_headers("SELECT 1 AS x; -- trailing comment"), ["x"])
        self.assertEqual(run.call_args.kwargs["input"], b"SELECT 1 AS x\n\\gdesc\n")

    def test_multiple_statements_are_rejected_before_calling_psql(self):
        with mock.patch.object(RUNNER.subprocess, "run") as run:
            with self.assertRaises(RuntimeError):
                RUNNER.reference_headers("SELECT 1; SELECT 2;")
            run.assert_not_called()

    def test_quoted_semicolons_and_comment_quotes_are_not_terminators(self):
        statements = ["SELECT ';' AS x", 'SELECT 1 AS "semi;colon"',
                      "SELECT $$semi;colon$$ AS x", "SELECT E'it\\'s;' AS x",
                      "SELECT /* ' ; /* nested */ */ 1 AS x"]
        output = subprocess.CompletedProcess([], 0, b"Column,Type\nx,text\n", b"")
        for sql in statements:
            with self.subTest(sql=sql), mock.patch.object(RUNNER.subprocess, "run", return_value=output) as run:
                RUNNER.reference_headers(sql + "; -- end")
                self.assertEqual(run.call_args.kwargs["input"], (sql + "\n\\gdesc\n").encode())


if __name__ == "__main__":
    unittest.main()
