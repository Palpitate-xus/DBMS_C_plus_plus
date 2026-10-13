#!/usr/bin/env python3
"""Missing INSERT INTO / DELETE FROM is rejected before preparation/effects."""
import importlib.util
import socket
import struct
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("dml_keywords_runner", root / "tests/compat/pg_diff_runner.py")
    r = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(r)
    c = r.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = r._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=r.wire_timeout())
        c.startup_reference(sock, user, database, password)
        r.verify_reference_version(c, sock)
    else:
        server = r.start_ours(c)
        sock = server["sock"]
    failures = []
    controls = 0

    def query(sql, expected=None):
        result = r.decode_wire_result(c.simple_query(sock, sql))
        assert result[1] is None, (sql, result)
        if expected is not None:
            assert result[0] == expected, (sql, result)
        return result

    def reject(sql, mode):
        nonlocal controls
        controls += 1
        query("SAVEPOINT keyword_case")
        name = "bad_words_" + str(controls)
        if mode == "parse":
            body = name.encode() + b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0)
            sock.sendall(c.typed(b"P", body) + c.typed(b"S"))
            messages = c.read_until_ready(sock)
        else:
            messages = c.simple_query(sock, "PREPARE " + name + " AS " + sql if mode == "prepare" else sql)
        states = [c.diagnostic_fields(body).get(b"C", b"").decode() for kind, body in messages if kind == b"E"]
        valid = states == ["42601"] and not any(kind in (b"D", b"C", b"1") for kind, _ in messages)
        print("REQUIRED_DML_KEYWORD", mode, sql, "states", states, "match", valid, flush=True)
        if not valid:
            failures.append((mode, sql, states, messages))
        query("ROLLBACK TO SAVEPOINT keyword_case")
        query("RELEASE SAVEPOINT keyword_case")

    invalid = [
        "INSERT keyword_rows VALUES(nextval('keyword_calls')) RETURNING id",
        "insert /* note */ keyword_rows(id) values(7) returning id",
        "INSERT keyword_rows DEFAULT VALUES",
        "INSERT keyword_rows SELECT 8",
        "DELETE keyword_rows WHERE id=1 RETURNING id",
        "delete /* note */ keyword_rows WHERE id=2 returning id",
        "DELETE ONLY keyword_rows AS d WHERE d.id=1 RETURNING d.id",
        "DELETE keyword_missing WHERE id=1",
    ]
    try:
        query("BEGIN")
        query("CREATE TEMP TABLE keyword_rows(id INTEGER)")
        query("CREATE TEMP SEQUENCE keyword_calls")
        query("INSERT INTO keyword_rows VALUES(1),(2)")
        for mode in ("simple", "parse", "prepare"):
            for sql in invalid:
                reject(sql, mode)
        query("SELECT id FROM keyword_rows ORDER BY id", [["1"], ["2"]])
        query("SAVEPOINT sequence_state")
        sequence = r.decode_wire_result(c.simple_query(sock, "SELECT currval('keyword_calls')"))
        print("REQUIRED_DML_NO_SEQUENCE_EVALUATION", sequence, flush=True)
        if sequence[1] != "55000":
            failures.append(("sequence changed", sequence))
        query("ROLLBACK TO SAVEPOINT sequence_state")
        query("RELEASE SAVEPOINT sequence_state")
        # Keep the independent valid PREPARE boundary observable even while
        # the following pre-existing ONLY execution defect is being repaired.
        query("SAVEPOINT valid_prepare_case")
        query("PREPARE valid_syntax_words(INT) AS INSERT INTO keyword_rows VALUES($1) RETURNING id")
        query("EXECUTE valid_syntax_words(9)", [["9"]])
        query("DEALLOCATE valid_syntax_words")
        query("ROLLBACK TO SAVEPOINT valid_prepare_case")
        query("RELEASE SAVEPOINT valid_prepare_case")
        query("InSeRt /* note */ InTo keyword_rows VALUES(3) RETURNING id", [["3"]])
        query("DeLeTe /* note */ FrOm ONLY keyword_rows AS d WHERE d.id=3 RETURNING d.id", [["3"]])
        query("PREPARE valid_words(INT) AS INSERT INTO keyword_rows VALUES($1) RETURNING id")
        query("EXECUTE valid_words(4)", [["4"]])
        query("DEALLOCATE valid_words")
        query("SELECT id FROM keyword_rows ORDER BY id", [["1"], ["2"], ["4"]])
        assert controls == 24, controls
        assert not failures, failures
        print("[DML REQUIRED KEYWORDS] 24 rejection controls and effects/valid syntax passed")
    finally:
        try:
            c.simple_query(sock, "ROLLBACK")
        finally:
            if server:
                r.stop_ours(server)
            else:
                sock.close()


if __name__ == "__main__":
    main()
