"""Quoted ordinary SET targets retain GUC identity, scope and literal data."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("quoted_set_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert reference or not sys.argv[1:]
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    checks, failures = 0, []

    def report(good, label, detail):
        nonlocal checks
        checks += 1
        print("QUOTED_SET_PARAMETER", checks, label, "match", good, repr(detail), flush=True)
        if not good:
            failures.append((label, detail))

    def request(sql, extended):
        if not extended:
            return client.simple_query(sock, sql)
        packet = client.typed(b"P", b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0))
        packet += client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0))
        packet += client.typed(b"D", b"P\0") + client.typed(b"E", b"\0" + struct.pack("!I", 0))
        sock.sendall(packet + client.typed(b"S"))
        return client.read_until_ready(sock)

    def utility(sql, extended, error=False, ready=b"I"):
        messages = request(sql, extended)
        result = runner.decode_wire_result(messages, include_types=True)
        kinds = [kind for kind, _ in messages]
        good = messages[-1] == (b"Z", ready) and result[0] == [] and result[3] == [] and result[5] == []
        good = good and not any(kind in (b"T", b"D", b"N") for kind in kinds)
        if extended:
            good = good and kinds.count(b"1") == kinds.count(b"2") == kinds.count(b"n") == 1
        if error:
            good = good and result[1] is not None and len(result[1]) == 5
            good = good and kinds.count(b"E") == 1 and b"C" not in kinds
        else:
            good = good and result[1] is None and b"E" not in kinds
            tag = sql.split()[0].upper().encode() + b"\0"
            good = good and [body for kind, body in messages if kind == b"C"] == [tag]
        report(good, ("Extended " if extended else "Simple ") + sql, (result, kinds, messages[-1]))

    def query(sql, value, name, oid=25, ready=b"I", extended=False, tag="SHOW"):
        messages = request(sql, extended)
        result = runner.decode_wire_result(messages, include_types=True)
        good = messages[-1] == (b"Z", ready) and result[0] == [[value]] and result[1] is None
        good = good and result[3] == [name] and result[5] == [oid] and result[4] == tag
        report(good, sql, (result, messages[-1]))

    def show(name, value, **kwargs):
        query("SHOW " + name, value, name, **kwargs)

    try:
        settings = {
            "application_name": (("'r142 ordinary'", "r142 ordinary"),
                                 ("'r142 a=b compatibility_mode'", "r142 a=b compatibility_mode")),
            "search_path": (("public", "public"), ("pg_catalog", "pg_catalog")),
            "client_encoding": (("'UTF8'", "UTF8"),),
            "lc_monetary": (("'C'", "C"),),
        }
        forms = ("SET {} = {}", "SET {} TO {}", "SET SESSION {} = {}", "SET SESSION {} TO {}")
        for extended in (False, True):
            for name, values in settings.items():
                mixed = "".join(char.upper() if index % 2 else char for index, char in enumerate(name))
                for parameter in (name, '"' + name + '"', '"' + name.upper() + '"', '"' + mixed + '"'):
                    for form in forms:
                        for literal, expected in values:
                            utility(form.format(parameter, literal), extended)
                            show(name, expected, extended=extended)

        baselines = (("application_name", "'r142 stable'", "r142 stable"),
                     ("search_path", "public", "public"), ("client_encoding", "'UTF8'", "UTF8"),
                     ("lc_monetary", "'C'", "C"))
        for name, literal, _ in baselines:
            utility("SET " + name + " = " + literal, False)
        negatives = ('"application_name_extra"', '"xapplication_name"', '"application""_name"',
                     '"search_path_extra"', '"client_encoding_extra"', '"lc_monetary_extra"',
                     '"@r142_quote_guard"')
        for extended in (False, True):
            for parameter in negatives:
                utility("SET " + parameter + " = 'r142 unknown'", extended, error=True)
                for name, _, expected in baselines:
                    show(name, expected, extended=extended)
            for parameter in ('"search_path"', '"SEARCH_PATH"', '"SeArCh_PaTh"'):
                for form in ("SET LOCAL {} = pg_catalog", "SET LOCAL {} TO pg_catalog"):
                    utility("SET search_path TO public", extended)
                    utility("BEGIN", extended, ready=b"T")
                    utility(form.format(parameter), extended, ready=b"T")
                    show("search_path", "pg_catalog", ready=b"T", extended=extended)
                    utility("ROLLBACK", extended)
                    show("search_path", "public", extended=extended)

        if not reference:
            # A quoted GUC identifier is never the project's raw @variable syntax.
            utility("SET compatibility_mode = extended", False)
            utility("SET @r142_quote_guard = 7", False)
            query("SELECT @r142_quote_guard AS v", "7", "v", 23, tag="SELECT 1")
            for sql in ('SET "@r142_quote_guard" = 9', 'SET "@R142_QUOTE_GUARD" TO 11',
                        'SET SESSION "@r142_quote_guard" = 13'):
                utility(sql, False, error=True)
                query("SELECT @r142_quote_guard AS v", "7", "v", 23, tag="SELECT 1")
                show("compatibility_mode", "extended")
            utility("SET compatibility_mode = postgresql18", False)
            show("compatibility_mode", "postgresql18")

        expected = 530 if reference else 544
        print("QUOTED_SET_PARAMETER_COMPLETE", checks, "FAILURES", len(failures), flush=True)
        assert checks == expected and not failures, (checks, expected, failures)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == "__main__":
    main()
