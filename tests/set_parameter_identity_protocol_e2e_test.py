"""SET parameter identity must not be inferred from unrelated value bytes."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("set_identity_runner", root / "tests/compat/pg_diff_runner.py")
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
        print("SET_PARAMETER_IDENTITY", checks, label, "match", good, repr(detail), flush=True)
        if not good:
            failures.append((label, detail))

    def request(sql, extended=False, connection=None):
        connection = sock if connection is None else connection
        if not extended:
            return client.simple_query(connection, sql)
        packet = client.typed(b"P", b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0))
        packet += client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0))
        packet += client.typed(b"D", b"P\0") + client.typed(b"E", b"\0" + struct.pack("!I", 0))
        connection.sendall(packet + client.typed(b"S"))
        return client.read_until_ready(connection)

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
            expected = sql.split()[0].upper().encode() + b"\0"
            good = good and result[1] is None and b"E" not in kinds
            good = good and [body for kind, body in messages if kind == b"C"] == [expected]
        report(good, ("Extended " if extended else "Simple ") + sql, (result, kinds, messages[-1]))

    def show(name, value, connection=None):
        messages = request("SHOW " + name, connection=connection)
        result = runner.decode_wire_result(messages, include_types=True)
        good = messages[-1] == (b"Z", b"I") and result[0] == [[value]] and result[1] is None
        good = good and result[3] == [name] and result[5] == [25] and result[4] == "SHOW"
        report(good, "SHOW " + name, result)

    try:
        if not reference:
            show("compatibility_mode", "postgresql18")
        application_values = (
            "compatibility_mode", "before compatibility_mode after",
            "compatibility_mode=extended", "compatibility_mode = extended",
            "EXTENDED compatibility_mode", "@compatibility_mode",
            "compatibility_mode.extra", "literal = compatibility_mode",
        )
        forms = ("SET {name} = {value}", "SET SESSION {name} = {value}",
                 "SET {name} TO {value}", "SET SESSION {name} TO {value}")
        for extended in (False, True):
            for form in forms:
                for value in application_values:
                    utility(form.format(name="application_name", value="'" + value + "'"), extended)
                    show("application_name", value)
                    if not reference:
                        show("compatibility_mode", "postgresql18")
                for value in ("compatibility_mode", "public, compatibility_mode"):
                    utility(form.format(name="search_path", value=value), extended)
                    show("search_path", value)
                    if not reference:
                        show("compatibility_mode", "postgresql18")
        if not reference:
            final_application = application_values[-1]
            false_parameters = ("compatibility_mode_extra", "extra_compatibility_mode",
                                "compatibility_mode.extra", "extra.compatibility_mode",
                                '"compatibility_mode_extra"', '"compatibility_mode".extra')
            for extended in (False, True):
                for parameter in false_parameters:
                    for value in ("extended", "postgresql18"):
                        utility("SET compatibility_mode = postgresql18", extended)
                        utility("SET " + parameter + " = " + value, extended, error=True)
                        show("compatibility_mode", "postgresql18")
                        show("application_name", final_application)
                for form in ("SET compatibility_mode = {}", "SET SESSION compatibility_mode = {}",
                             'SET "compatibility_mode" = {}'):
                    for value in ("extended", "postgresql18"):
                        utility(form.format(value), extended)
                        show("compatibility_mode", value)
                        show("application_name", final_application)
                utility("SET compatibility_mode = mysql", extended, error=True)
                show("compatibility_mode", "postgresql18")
                utility("BEGIN", extended, ready=b"T")
                utility("SET compatibility_mode = extended", extended, error=True, ready=b"E")
                utility("ROLLBACK", extended)
                show("compatibility_mode", "postgresql18")
            utility("SET compatibility_mode = extended", False)
            peer = socket.create_connection(("127.0.0.1", server["port"]), timeout=runner.wire_timeout())
            try:
                client.startup(peer, "alice", "info")
                show("compatibility_mode", "postgresql18", connection=peer)
                show("compatibility_mode", "extended")
            finally:
                peer.close()
            utility("SET compatibility_mode = postgresql18", False)
        expected = 160 if reference else 389
        print("SET_PARAMETER_IDENTITY_COMPLETE", checks, "FAILURES", len(failures), flush=True)
        assert checks == expected and not failures, (checks, expected, failures)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == "__main__":
    main()
