"""Actual EXISTS cursors own row presence, ignored targets and correlation."""
import importlib.util
import socket
import struct
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("exists_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = "--reference18" in sys.argv
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client);sock = server["sock"]
    checked = 0;failures = []

    def check(sql, rows=None, oids=None, state=None):
        nonlocal checked
        checked += 1
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        print("PREPARED_EXISTS", sql, result, flush=True)
        try:
            assert result[1] == state, (sql, result, state)
            if rows is not None:
                assert result[0] == rows and result[4] == "SELECT " + str(len(rows)), (sql, result, rows)
            if oids is not None: assert result[5] == oids, (sql, result, oids)
        except AssertionError as error: failures.append(error.args)
        return result

    def error(sql, state):
        if reference: check("SAVEPOINT exists_error")
        check(sql, state=state)
        if reference: check("ROLLBACK TO exists_error");check("RELEASE exists_error")

    try:
        if reference: check("BEGIN")
        check("CREATE TABLE exists_rows(id INT,k INT)")
        check("INSERT INTO exists_rows VALUES(1,1),(2,1)")
        for sql, value in (
            ("SELECT EXISTS(SELECT id FROM exists_rows ORDER BY k FETCH FIRST 1 ROW WITH TIES)", "t"),
            ("SELECT EXISTS(SELECT id FROM exists_rows ORDER BY k FETCH FIRST 0 ROWS WITH TIES)", "f"),
            ("SELECT EXISTS(SELECT id,k FROM exists_rows)", "t"),
            ("SELECT EXISTS(SELECT id,k FROM exists_rows WHERE false)", "f"),
            ("SELECT EXISTS(SELECT 1)", "t"),
            ("SELECT EXISTS(SELECT NULL)", "t"),
            ("SELECT EXISTS(SELECT NULL,'text')", "t"),
            ("SELECT EXISTS(SELECT 1/0 FROM exists_rows)", "t"),
            ("SELECT EXISTS(SELECT id FROM exists_rows ORDER BY 1/0)", "t"),
            ("SELECT EXISTS(SELECT DISTINCT id FROM exists_rows)", "t"),
            ("SELECT EXISTS(SELECT id,k FROM exists_rows OFFSET 1)", "t"),
            ("SELECT EXISTS(SELECT id,k FROM exists_rows OFFSET 2)", "f"),
            ("SELECT NOT EXISTS(SELECT id FROM exists_rows WHERE false)", "t"),
            ("SELECT CASE WHEN false THEN EXISTS(SELECT 1/0 FROM exists_rows) ELSE false END", "f")):
            check(sql, [[value]], [16])
        check("SELECT o.id,EXISTS(SELECT i.id,i.k FROM exists_rows i WHERE i.id=o.id) FROM exists_rows o ORDER BY o.id", [["1", "t"], ["2", "t"]], [23,16])
        for sql, state in (
            ("SELECT EXISTS(SELECT missing FROM exists_rows)", "42703"),
            ("SELECT EXISTS(SELECT id FROM missing_relation)", "42P01"),
            ("SELECT (SELECT id,k FROM exists_rows)", "42601"),
            ("SELECT (SELECT id FROM exists_rows)", "21000")):
            error(sql, state)
        check("CREATE SEQUENCE exists_effects")
        check("SELECT nextval('exists_effects')", [["1"]], [20])
        check("SELECT EXISTS(SELECT nextval('exists_effects') FROM exists_rows)", [["t"]], [16])
        check("SELECT EXISTS(SELECT id FROM exists_rows ORDER BY nextval('exists_effects'))", [["t"]], [16])
        check("SELECT currval('exists_effects')", [["1"]], [20])
        sql = "SELECT EXISTS(SELECT nextval('exists_effects'),id FROM exists_rows) AS present"
        sock.sendall(client.typed(b"P", b"exists_statement\0" + sql.encode() + b"\0" + struct.pack("!H", 0)) +
                     client.typed(b"D", b"Sexists_statement\0") + client.typed(b"S"))
        description = client.read_until_ready(sock)
        assert not any(kind == b"E" for kind, _ in description), description
        assert client.row_description_fields(description) == [(b"present",0,0,16,1,-1,0)], description
        check("SELECT currval('exists_effects')", [["1"]], [20])
        sock.sendall(client.typed(b"B", b"\0exists_statement\0" + struct.pack("!HHH",0,0,0)) +
                     client.typed(b"E", b"\0" + struct.pack("!I",0)) + client.typed(b"S"))
        result = runner.decode_wire_result(client.read_until_ready(sock), include_types=True)
        assert result[1] is None and result[0] == [["t"]], result
        check("SELECT currval('exists_effects')", [["1"]], [20])
        print("PREPARED_EXISTS_COMPLETE", checked, "FAILED", len(failures), failures, flush=True)
        assert not failures, failures
    finally:
        if reference:
            try: client.simple_query(sock,"ROLLBACK")
            finally: sock.close()
        else: runner.stop_ours(server)


if __name__ == "__main__": main()
