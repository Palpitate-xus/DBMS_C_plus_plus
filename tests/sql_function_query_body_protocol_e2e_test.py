"""Whole scalar SQL-function queries retain their namespace and typed cells."""
import importlib.util
import socket
import struct
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("sql_body_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = "--reference18" in sys.argv
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    failures = []
    checked = 0

    def check(sql, rows=None, oids=None):
        nonlocal checked
        checked += 1
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        print("SQL_FUNCTION_BODY", sql, result, flush=True)
        try:
            assert result[1] is None, (sql, result)
            if rows is not None:
                assert result[0] == rows and result[4] == "SELECT " + str(len(rows)), (sql, result, rows)
            if oids is not None:
                assert result[5] == oids, (sql, result, oids)
        except AssertionError as error:
            failures.append(error.args)

    try:
        if reference:
            check("BEGIN")
        check("CREATE TABLE sql_body_rows(id INT, txt TEXT)")
        check("INSERT INTO sql_body_rows VALUES(1,'a b'),(2,NULL),(3,'NULL'),(4,'')")
        bodies = (
            ("sql_body_reader(wanted INT)", "INT", "SELECT id FROM sql_body_rows WHERE id > wanted ORDER BY id DESC"),
            ("sql_body_column(id INT)", "INT", "SELECT id FROM sql_body_rows ORDER BY id DESC"),
            ("sql_body_arg(id INT)", "INT", "SELECT sql_body_arg.id FROM sql_body_rows ORDER BY sql_body_rows.id DESC"),
            ("sql_body_alias(id INT)", "INT", "SELECT sql_body_alias.id FROM sql_body_rows AS sql_body_alias ORDER BY sql_body_alias.id DESC"),
            ("sql_body_position(BIGINT)", "BIGINT", "SELECT $1 + 1"),
            ("sql_body_text(t TEXT)", "TEXT", "SELECT t FROM sql_body_rows WHERE id=1"),
            ("sql_body_literal()", "TEXT", "SELECT 'a b'"),
            ("sql_body_null()", "TEXT", "SELECT NULL"),
            ("sql_body_local()", "INT", "WITH sql_body_rows AS (SELECT 7 AS id) SELECT id FROM sql_body_rows"),
        )
        for signature, returns, body in bodies:
            check("CREATE FUNCTION " + signature + " RETURNS " + returns + " LANGUAGE SQL AS $$" + body + "$$")
        cases = (
            ("SELECT sql_body_reader(0)", [["4"]], [23]),
            ("SELECT sql_body_reader(4)", [[None]], [23]),
            ("SELECT sql_body_reader(NULL::int)", [[None]], [23]),
            ("SELECT sql_body_column(99)", [["4"]], [23]),
            ("SELECT sql_body_arg(99)", [["99"]], [23]),
            ("SELECT sql_body_alias(99)", [["4"]], [23]),
            ("SELECT sql_body_position(2147483648::bigint)", [["2147483649"]], [20]),
            ("SELECT sql_body_position(NULL::bigint)", [[None]], [20]),
            ("SELECT sql_body_literal()", [["a b"]], [25]),
            ("SELECT sql_body_null()", [[None]], [25]),
            ("SELECT sql_body_local()", [["7"]], [23]),
            ("WITH sql_body_rows AS (SELECT 99 AS id) SELECT sql_body_reader(0)", [["4"]], [23]),
            ("WITH sql_body_reader AS (SELECT 99 AS id) SELECT sql_body_reader(0)", [["4"]], [23]),
            ("SELECT id,sql_body_text(txt) FROM sql_body_rows ORDER BY id",
             [["1", "a b"], ["2", None], ["3", "NULL"], ["4", ""]], [23,25]),
        )
        for case in cases:
            check(*case)
        # Real runtime TEXT cells must not be reclassified as SQL literals.
        sql = "SELECT sql_body_text($1) AS value"
        sock.sendall(client.typed(b"P", b"sql_body_statement\0" + sql.encode() + b"\0" +
                                 struct.pack("!HI", 1, 25)) +
                     client.typed(b"D", b"Ssql_body_statement\0") + client.typed(b"S"))
        messages = client.read_until_ready(sock)
        assert not any(kind == b"E" for kind, _ in messages), messages
        assert client.row_description_fields(messages) == [(b"value",0,0,25,-1,-1,0)], messages
        for value in (None, "NULL", "", "O'Brien"):
            encoded = struct.pack("!i", -1) if value is None else struct.pack("!i", len(value.encode())) + value.encode()
            sock.sendall(client.typed(b"B", b"\0sql_body_statement\0" + struct.pack("!HH",0,1) +
                                     encoded + struct.pack("!H",0)) +
                         client.typed(b"E", b"\0" + struct.pack("!I",0)) + client.typed(b"S"))
            result = runner.decode_wire_result(client.read_until_ready(sock), include_types=True)
            print("SQL_FUNCTION_BODY_EXTENDED", repr(value), result, flush=True)
            assert result[1] is None and result[0] == [[value]] and result[4] == "SELECT 1", result
        print("SQL_FUNCTION_BODY_COMPLETE", checked, "FAILED", len(failures), failures, flush=True)
        assert not failures, failures
    finally:
        if reference:
            try:
                client.simple_query(sock, "ROLLBACK")
            finally:
                sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
