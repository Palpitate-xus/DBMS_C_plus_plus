#!/usr/bin/env python3
"""The differential oracle must preserve values instead of hiding mismatches."""

import importlib.util
from pathlib import Path
import shutil
import tempfile
import struct
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


class DifferentialCleanupTest(unittest.TestCase):
    def test_start_failure_before_spawn_removes_data_directory(self):
        created = []
        original_make_dir = tempfile.mkdtemp

        def make_dir(*args, **kwargs):
            directory = original_make_dir(*args, **kwargs)
            created.append(directory)
            return directory

        with mock.patch.object(RUNNER.tempfile, "mkdtemp",
                               side_effect=make_dir), \
             mock.patch.object(RUNNER.subprocess, "Popen",
                               side_effect=FileNotFoundError("dbms_main")):
            with self.assertRaises(FileNotFoundError):
                RUNNER.start_ours(mock.Mock())
        directory = Path(created[0])
        try:
            self.assertFalse(directory.exists())
        finally:
            shutil.rmtree(directory, ignore_errors=True)

    def test_startup_failure_stops_spawned_server(self):
        created = []
        original_make_dir = tempfile.mkdtemp

        def make_dir(*args, **kwargs):
            directory = original_make_dir(*args, **kwargs)
            created.append(directory)
            return directory

        probe = mock.Mock()
        probe.getsockname.return_value = ("127.0.0.1", 54321)
        wire = mock.Mock()
        process = mock.Mock()
        client = mock.Mock()
        client.startup.side_effect = RuntimeError("startup failed")
        with mock.patch.object(RUNNER.tempfile, "mkdtemp", side_effect=make_dir), \
             mock.patch.object(RUNNER.socket, "socket", side_effect=[probe, wire]), \
             mock.patch.object(RUNNER.subprocess, "Popen", return_value=process):
            with self.assertRaisesRegex(RuntimeError, "startup failed"):
                RUNNER.start_ours(client)
        process.terminate.assert_called_once()
        process.wait.assert_called_once()
        wire.close.assert_called_once()
        probe.close.assert_called_once()
        self.assertFalse(Path(created[0]).exists())

    def test_stuck_server_is_killed_and_temporary_data_removed(self):
        with tempfile.TemporaryDirectory(prefix="dbms-pgdiff-") as directory:
            process = mock.Mock()
            process.wait.side_effect = [
                subprocess.TimeoutExpired("dbms_main", 10), 0]
            sock = mock.Mock()
            RUNNER.stop_ours({"sock": sock, "process": process,
                              "dir": directory})
            sock.close.assert_called_once()
            process.terminate.assert_called_once()
            process.kill.assert_called_once()
            self.assertFalse(Path(directory).exists())

    def test_cleanup_rejects_an_unowned_directory(self):
        with tempfile.TemporaryDirectory(prefix="different-owner-") as directory:
            process = mock.Mock()
            with self.assertRaises(ValueError):
                RUNNER.stop_ours({"sock": mock.Mock(), "process": process,
                                  "dir": directory})
            process.terminate.assert_not_called()
            self.assertTrue(Path(directory).exists())


class DifferentialErrorsTest(unittest.TestCase):
    def test_local_timeout_identifies_the_case_and_sql_without_retry(self):
        with mock.patch.object(RUNNER, "reference_multi", return_value=[]), \
             mock.patch.object(RUNNER, "ours_query",
                               side_effect=TimeoutError("wire timeout")) as query:
            with self.assertRaisesRegex(
                    RuntimeError, "stuck_case: local query did not complete") as raised:
                RUNNER.run_case("stuck_case", ["CREATE TABLE stuck (id INT)"],
                                None, None)
        self.assertIn("CREATE TABLE stuck (id INT)", str(raised.exception))
        self.assertIsInstance(raised.exception.__cause__, TimeoutError)
        query.assert_called_once()

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
    def test_local_cases_get_fresh_sessions(self):
        first = mock.Mock()
        second = mock.Mock()
        client = mock.Mock()
        server = {"sock": first, "port": 54321}
        with mock.patch.object(RUNNER.socket, "create_connection",
                               return_value=second) as connect:
            RUNNER.reconnect_ours(server, client)
        first.close.assert_called_once()
        connect.assert_called_once_with(("127.0.0.1", 54321), timeout=15)
        client.startup.assert_called_once_with(second, "alice", "info")
        self.assertIs(server["sock"], second)

    def test_main_reconnects_between_cases(self):
        server = {"sock": mock.Mock(), "port": 54321}
        cases = [("first", ["SET TIME ZONE 'Asia/Shanghai'"]),
                 ("second", ["SELECT 1"])]
        with mock.patch.object(RUNNER, "load_protocol_client",
                               return_value=mock.Mock()), \
             mock.patch.object(RUNNER, "reference_multi", return_value=[]), \
             mock.patch.object(RUNNER, "start_ours", return_value=server), \
             mock.patch.object(RUNNER, "load_cases", return_value=cases), \
             mock.patch.object(RUNNER, "run_case", return_value=[]), \
             mock.patch.object(RUNNER, "reconnect_ours") as reconnect, \
             mock.patch.object(RUNNER, "stop_ours"), \
             mock.patch("sys.argv", ["pg_diff_runner.py"]):
            self.assertEqual(RUNNER.main(), 0)
        reconnect.assert_called_once()

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
            results = RUNNER._reference_psql_multi([
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
            results = RUNNER._reference_psql_multi(["SELECT 1/0", "SELECT 1"])
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

    def test_wire_reference_preserves_null_and_control_characters(self):
        def data_row(values):
            body = struct.pack("!H", len(values))
            for value in values:
                if value is None:
                    body += struct.pack("!i", -1)
                else:
                    encoded = value.encode()
                    body += struct.pack("!i", len(encoded)) + encoded
            return b"D", body

        row_description = (
            b"T", struct.pack("!H", 5) +
            b"n\0" + b"\0" * 18 +
            b"e\0" + b"\0" * 18 +
            b"m\0" + b"\0" * 18 +
            b"l\0" + b"\0" * 18 +
            b"s\0" + b"\0" * 18)
        messages = [
            row_description,
            data_row([None, "", "NULLMARK", "a\nb", "a\x1fb"]),
            (b"C", b"SELECT 1\0"),
        ]
        self.assertEqual(RUNNER.decode_wire_result(messages), (
            [[None, "", "NULLMARK", "a\nb", "a\x1fb"]],
            None, "", ["n", "e", "m", "l", "s"], "SELECT 1"))

    def test_wire_reference_reuses_one_connection(self):
        class FakeClient:
            def __init__(self):
                self.started = []
                self.queries = []

            def startup(self, sock, user, database, password):
                self.started.append((sock, user, database, password))

            def simple_query(self, sock, sql):
                self.queries.append((sock, sql))
                return [(b"C", ("SELECT 1" if sql.startswith("SELECT")
                                 else sql).encode() + b"\0")]

        client = FakeClient()
        sock = mock.Mock()
        settings = ("127.0.0.1", 55432, "postgres", "postgres", "secret")
        with mock.patch.object(RUNNER, "_reference_connection_settings",
                               return_value=settings), \
             mock.patch.object(RUNNER.socket, "create_connection",
                               return_value=sock) as connect, \
             mock.patch.object(RUNNER, "verify_reference_version") as verify:
            results = RUNNER.reference_multi(["BEGIN", "SELECT 1"], client)
        connect.assert_called_once_with(("127.0.0.1", 55432), timeout=15)
        self.assertEqual(len(client.started), 1)
        self.assertEqual([query[1] for query in client.queries],
                         ["BEGIN", "SELECT 1"])
        self.assertEqual([result[2] for result in results],
                         ["BEGIN", "SELECT 1"])
        self.assertEqual([result[5] for result in results], [[], []])
        verify.assert_called_once_with(client, sock)
        sock.close.assert_called_once()

    def test_wire_reference_uses_reference_startup_policy(self):
        class FakeClient:
            def __init__(self):
                self.dbms_startups = 0
                self.reference_startups = 0

            def startup(self, *_args, **_kwargs):
                self.dbms_startups += 1

            def startup_reference(self, *_args, **_kwargs):
                self.reference_startups += 1

            def simple_query(self, _sock, _sql):
                return [(b"C", b"SELECT 1\0")]

        client = FakeClient()
        sock = mock.Mock()
        with mock.patch.object(
                RUNNER, "_reference_connection_settings",
                return_value=("127.0.0.1", 55432, "postgres", "postgres", "secret")), \
             mock.patch.object(RUNNER.socket, "create_connection",
                               return_value=sock), \
             mock.patch.object(RUNNER, "verify_reference_version"):
            RUNNER.reference_multi(["SELECT 1"], client)
        self.assertEqual(client.reference_startups, 1)
        self.assertEqual(client.dbms_startups, 0)

    def test_reference_version_requires_exact_18_6(self):
        client = mock.Mock()
        sock = mock.Mock()
        with mock.patch.object(RUNNER, "decode_wire_result",
                               return_value=([['180006']], None, '',
                                             ['server_version_num'], 'SHOW', [25])):
            RUNNER.verify_reference_version(client, sock)
        client.simple_query.assert_called_once_with(
            sock, "SHOW server_version_num")

        with mock.patch.object(RUNNER, "decode_wire_result",
                               return_value=([['170002']], None, '',
                                             ['server_version_num'], 'SHOW', [25])):
            with self.assertRaisesRegex(RuntimeError, "must be 18.6"):
                RUNNER.verify_reference_version(client, sock)

    def test_wrong_reference_stops_before_local_server_start(self):
        with mock.patch.object(RUNNER, "load_protocol_client",
                               return_value=mock.Mock()), \
             mock.patch.object(RUNNER, "reference_multi",
                               side_effect=RuntimeError("must be 18.6")), \
             mock.patch.object(RUNNER, "start_ours") as start, \
             mock.patch.object(RUNNER.sys, "argv", ["pg_diff_runner.py"]), \
             mock.patch.object(RUNNER.sys, "stderr"):
            self.assertEqual(RUNNER.main(), 2)
        start.assert_not_called()

    def test_command_tag_mismatch_is_reported(self):
        reference = [([['1']], None, "SELECT 1", "", ["?column?"])]
        ours = ([['1']], None, "", ["?column?"], "SELECT 0")
        with mock.patch.object(RUNNER, "reference_multi", return_value=reference), \
             mock.patch.object(RUNNER, "ours_query", return_value=ours):
            diffs = RUNNER.run_case("tag", ["SELECT 1"], None, None)
        self.assertTrue(any("command tag differs" in diff for diff in diffs), diffs)

    def test_column_type_mismatch_is_reported(self):
        reference = [([['1']], None, "SELECT 1", "", ["?column?"], [23])]
        ours = ([['1']], None, "", ["?column?"], "SELECT 1", [25])
        with mock.patch.object(RUNNER, "reference_multi", return_value=reference), \
             mock.patch.object(RUNNER, "ours_query", return_value=ours):
            diffs = RUNNER.run_case("type", ["SELECT 1"], None, None)
        self.assertTrue(any("column type OIDs differ" in diff for diff in diffs),
                        diffs)

class DifferentialHeadersTest(unittest.TestCase):
    def test_zero_row_result_still_compares_headers(self):
        reference = [([], None, "SELECT 0", "", ["expected"])]
        ours = ([], None, "", ["wrong"], "SELECT 0")
        with mock.patch.object(RUNNER, "reference_multi", return_value=reference), \
             mock.patch.object(RUNNER, "ours_query", return_value=ours):
            diffs = RUNNER.run_case("zero-row-header", ["SELECT 1 WHERE false"],
                                    None, None)
        self.assertTrue(any("headers differ" in diff for diff in diffs), diffs)

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
