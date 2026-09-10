#!/usr/bin/env python3
"""CREATE DATABASE options never succeed without their promised semantics."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "cat08_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def execute(sql):
        return runner.ours_query(client, server["sock"], sql)

    def expect_error(sql, state, database):
        rows, actual, message, _ = execute(sql)
        assert rows == [], (sql, rows)
        assert actual == state, (sql, actual, message)
        assert not (Path(server["dir"]) / database).exists(), sql
        assert not (Path(server["dir"]) / (database + ".archive")).exists(), sql
        recovered, recovered_state, recovered_message, _ = execute("SELECT 1")
        assert recovered_state is None, (sql, recovered_state, recovered_message)
        assert recovered == [["1"]], (sql, recovered)

    try:
        unsupported = [
            "OWNER alice", "TEMPLATE template0", "STRATEGY WAL_LOG",
            "LOCALE 'C'", "LC_COLLATE 'C'", "LC_CTYPE 'C'",
            "BUILTIN_LOCALE 'C'", "ICU_LOCALE 'und'",
            "ICU_RULES '&V << w'", "LOCALE_PROVIDER builtin",
            "COLLATION_VERSION '1.0'", "TABLESPACE pg_default",
            "ALLOW_CONNECTIONS true", "CONNECTION LIMIT -1",
            "IS_TEMPLATE false", "OID 16384", "LOCATION '/unused'",
        ]
        for index, clause in enumerate(unsupported):
            database = "cat08_wire_unsupported_%d" % index
            expect_error(
                "CREATE DATABASE %s %s" % (database, clause),
                "0A000", database)

        for index, encoding in enumerate((
                "LATIN1", "'SQL_ASCII'", "'ISO-8859-1'", "windows1252",
                "0", "34")):
            database = "cat08_wire_encoding_%d" % index
            expect_error(
                "CREATE DATABASE %s ENCODING %s" % (database, encoding),
                "0A000", database)

        expect_error(
            "CREATE DATABASE cat08_wire_bad_encoding ENCODING no_such_encoding",
            "42704", "cat08_wire_bad_encoding")
        expect_error(
            "CREATE DATABASE cat08_wire_quoted_code ENCODING '6'",
            "42704", "cat08_wire_quoted_code")

        malformed = [
            ("cat08_wire_if", "CREATE DATABASE IF NOT EXISTS cat08_wire_if"),
            ("with", "CREATE DATABASE with"),
            ("123", "CREATE DATABASE 123"),
            ("cat08_wire_qualified", "CREATE DATABASE cat08_wire_qualified.extra"),
            ("cat08_wire_unknown",
             "CREATE DATABASE cat08_wire_unknown UNKNOWN_OPTION value"),
            ("cat08_wire_duplicate",
             "CREATE DATABASE cat08_wire_duplicate ENCODING UTF8 ENCODING UTF8"),
            ("cat08_wire_missing", "CREATE DATABASE cat08_wire_missing OWNER"),
            ("cat08_wire_reserved_value",
             "CREATE DATABASE cat08_wire_reserved_value OWNER SELECT"),
            ("cat08_wire_bad_numeric",
             "CREATE DATABASE cat08_wire_bad_numeric OWNER 123abc"),
            ("cat08_wire_connection",
             "CREATE DATABASE cat08_wire_connection CONNECTION value"),
            ("cat08_wire_comma",
             "CREATE DATABASE cat08_wire_comma ENCODING UTF8, OWNER alice"),
            ("cat08_wire_trailing",
             "CREATE DATABASE cat08_wire_trailing ENCODING UTF8 garbage"),
        ]
        for database, sql in malformed:
            expect_error(sql, "42601", database)

        for index, clause in enumerate((
                "WITH", "ENCODING DEFAULT", "ENCODING UTF8",
                "ENCODING 'UTF-8'", "ENCODING 'u_t-f 8'",
                "ENCODING UNICODE", "ENCODING +6", '"encoding" UTF8')):
            database = "cat08_wire_utf8_%d" % index
            rows, state, message, _ = execute(
                "CREATE DATABASE %s %s" % (database, clause))
            assert state is None, (clause, state, message)
            assert rows == [], (clause, rows)
            database_path = Path(server["dir"]) / database
            assert database_path.is_dir(), clause
            assert (database_path / ".charset").read_text(encoding="utf-8") == "utf8\n"
            rows, state, message, _ = execute("DROP DATABASE " + database)
            assert state is None, (clause, state, message)
            assert not database_path.exists(), clause

        print("[CREATE DATABASE OPTIONS PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
