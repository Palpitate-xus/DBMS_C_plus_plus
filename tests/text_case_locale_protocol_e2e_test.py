#!/usr/bin/env python3
"""Explicit C.utf8 character casing differs from C/POSIX byte casing."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        "text_case_locale_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ["--reference"]
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    # libc C.utf8 uses single-code-point mapping, not full Unicode expansion
    # or contextual Greek final-sigma rules. C/POSIX remain ASCII-only.
    values = (
        ("", ("", "", ""), ("", "", "")),
        ("abc DEF", ("abc def", "ABC DEF", "Abc Def"), ("abc def", "ABC DEF", "Abc Def")),
        ("élÈVE déjà_vu", ("élève déjà_vu", "ÉLÈVE DÉJÀ_VU", "Élève Déjà_Vu"),
         ("élÈve déjà_vu", "éLÈVE DéJà_VU", "éLÈVe DéJà_Vu")),
        ("ÄÉΣİẞǅ𐐀", ("äéσißǆ𐐨", "ÄÉΣİẞǄ𐐀", "Äéσißǆ𐐨"),
         ("ÄÉΣİẞǅ𐐀", "ÄÉΣİẞǅ𐐀", "ÄÉΣİẞǅ𐐀")),
        ("äéσıßǆ𐐨", ("äéσıßǆ𐐨", "ÄÉΣIßǄ𐐀", "Äéσıßǆ𐐨"),
         ("äéσıßǆ𐐨", "äéσıßǆ𐐨", "äéσıßǆ𐐨")),
        ("ΣΟΣ σος", ("σοσ σος", "ΣΟΣ ΣΟΣ", "Σοσ Σος"),
         ("ΣΟΣ σος", "ΣΟΣ σος", "ΣΟΣ σος")),
        ("123éABC aßTEST", ("123éabc aßtest", "123ÉABC AßTEST", "123éabc Aßtest"),
         ("123éabc aßtest", "123éABC AßTEST", "123éAbc AßTest")),
        ("😀éABC_中DEF", ("😀éabc_中def", "😀ÉABC_中DEF", "😀Éabc_中def"),
         ("😀éabc_中def", "😀éABC_中DEF", "😀éAbc_中Def")),
        ("NULL", ("null", "NULL", "Null"), ("null", "NULL", "Null")),
        (None, (None, None, None), (None, None, None)),
    )
    def execute(sql, mode):
        if mode == "simple":
            return client.simple_query(sock, sql)
        sock.sendall(
            client.typed(b"P", b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0)) +
            client.typed(b"B", b"\0\0" + struct.pack("!HHHH", 0, 0, 1, int(mode == "binary"))) +
            client.typed(b"D", b"P\0") +
            client.typed(b"E", b"\0" + struct.pack("!I", 0)) + client.typed(b"S"))
        return client.read_until_ready(sock)

    failures, checks = [], 0

    def check(sql, mode, expected):
        nonlocal checks
        messages = execute(sql, mode)
        try:
            result = runner.decode_wire_result(messages, include_types=True)
        except UnicodeDecodeError:
            # Preserve invalid server bytes as a failed control, not a
            # lossy replacement string or an early stop before later cases.
            result = ("INVALID_UTF8", [(kind.decode(), body.hex()) for kind, body in messages])
            checks += 1
            print("TEXT_CASE_LOCALE", mode, sql, result, "EXPECTED", expected, "PASS", False, flush=True)
            failures.append((mode, sql, result, expected))
            return
        good = result[1] is None and result[0] == [list(expected)] and result[5] == [25, 25, 25]
        good = good and result[4] == "SELECT 1" and messages[-1] == (b"Z", b"I")
        checks += 1
        print("TEXT_CASE_LOCALE", mode, sql, result, "EXPECTED", expected, "PASS", good, flush=True)
        if not good:
            failures.append((mode, sql, result, expected))

    table = "text_case_locale_values"
    created = False
    try:
        for mode in ("simple", "extended", "binary"):
            for collation in ("C.utf8", "C", "POSIX"):
                for value, unicode_expected, ascii_expected in values:
                    for declaration in ("text", "varchar", "char(32)"):
                        literal = "NULL" if value is None else "'" + value.replace("'", "''") + "'"
                        expression = literal + "::" + declaration + ' COLLATE "' + collation + '"'
                        sql = "SELECT " + ",".join(name + "(" + expression + ")" for name in ("lower", "upper", "initcap"))
                        expected = unicode_expected if collation == "C.utf8" else ascii_expected
                        check(sql, mode, expected)
        ddl = 'CREATE TABLE ' + table + '(id integer,t text COLLATE "C.utf8",v varchar(32) COLLATE "C.utf8",b char(32) COLLATE "C.utf8")'
        assert runner.decode_wire_result(execute(ddl, "simple"))[1] is None
        created = True
        for index, (value, _, _) in enumerate(values):
            literal = "NULL" if value is None else "'" + value.replace("'", "''") + "'"
            sql = "INSERT INTO " + table + " VALUES(" + str(index) + "," + ",".join([literal] * 3) + ")"
            assert runner.decode_wire_result(execute(sql, "simple"))[1] is None
        for mode in ("simple", "extended", "binary"):
            for index, (_, expected, _) in enumerate(values):
                for column in ("t", "v", "b"):
                    sql = "SELECT " + ",".join(name + "(" + column + ")" for name in ("lower", "upper", "initcap"))
                    check(sql + " FROM " + table + " WHERE id=" + str(index), mode, expected)
        assert checks == 360, checks
        print("TEXT_CASE_LOCALE_COMPLETE", checks, "FAILURES", len(failures), flush=True)
        assert not failures, failures
    finally:
        if created:
            execute("DROP TABLE " + table, "simple")
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == "__main__":
    main()
