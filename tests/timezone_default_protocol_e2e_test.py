"""TimeZone reset keywords versus quoted values; actual PostgreSQL 18 oracle."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("timezone_default_runner", root / "tests/compat/pg_diff_runner.py")
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
        print("TIMEZONE_DEFAULT", checks, label, "match", good, repr(detail), flush=True)
        if not good:
            failures.append((label, detail))

    def request(sql, extended=False):
        if not extended:
            return client.simple_query(sock, sql)
        packet = client.typed(b"P", b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0))
        packet += client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0))
        packet += client.typed(b"D", b"P\0") + client.typed(b"E", b"\0" + struct.pack("!I", 0))
        sock.sendall(packet + client.typed(b"S"))
        return client.read_until_ready(sock)

    def utility(sql, extended, state=None):
        messages = request(sql, extended)
        result = runner.decode_wire_result(messages, include_types=True)
        kinds = [kind for kind, _ in messages]
        good = messages[-1] == (b"Z", b"I") and result[1] == state and result[0] == []
        good = good and not any(kind in (b"T", b"D", b"N") for kind in kinds)
        if extended:
            good = good and kinds.count(b"1") == kinds.count(b"2") == kinds.count(b"n") == 1
        if state:
            good = good and kinds.count(b"E") == 1 and b"C" not in kinds
        else:
            tag = b"RESET\0" if sql.lower().startswith("reset ") else b"SET\0"
            good = good and b"E" not in kinds and [body for kind, body in messages if kind == b"C"] == [tag]
        report(good, ("Extended " if extended else "Simple ") + sql, (result, kinds))

    def query(sql, rows=None, names=None, types=None, tag=None):
        messages = request(sql)
        result = runner.decode_wire_result(messages, include_types=True)
        good = messages[-1] == (b"Z", b"I") and result[1] is None and result[0] != []
        if rows is not None:
            good = good and result[0] == rows
        good = good and result[3] == names and result[5] == types and result[4] == tag
        report(good, sql, result)
        return result[0]

    try:
        baseline = query("SHOW TimeZone", names=["TimeZone"], types=[25], tag="SHOW")
        value_sql = "SELECT '2000-01-01 00:00:00+00'::timestamptz AS v"
        baseline_value = query(value_sql, names=["v"], types=[1184], tag="SELECT 1")
        forms = ("SET TimeZone TO {}", "SET TimeZone = {}", "SET TIME ZONE {}",
                 "SET SESSION TimeZone TO {}", "SET SESSION TimeZone = {}", "SET SESSION TIME ZONE {}")
        reset_commands = [form.format("DEFAULT") for form in forms]
        # LOCAL is a reset keyword only in the dedicated TIME ZONE grammar;
        # the generic TimeZone variable must treat it as an invalid zone name.
        reset_commands += ["SET TIME ZONE LOCAL", "SET SESSION TIME ZONE LOCAL",
                           "RESET TimeZone", "RESET TIME ZONE", "RESET ALL"]
        for command in reset_commands:
            for extended in (False, True):
                utility("SET TimeZone TO 'Asia/Shanghai'", extended)
                query("SHOW TimeZone", [["Asia/Shanghai"]], ["TimeZone"], [25], "SHOW")
                utility(command, extended)
                query("SHOW TimeZone", baseline, ["TimeZone"], [25], "SHOW")
                query(value_sql, baseline_value, ["v"], [1184], "SELECT 1")
        errors = [form.format(value) for form in forms
                  for value in ("'DEFAULT'", "'LOCAL'", '"DEFAULT"', '"LOCAL"')]
        errors += [forms[index].format("LOCAL") for index in (0, 1, 3, 4)]
        for command in errors:
            for extended in (False, True):
                utility("SET TimeZone TO 'Asia/Shanghai'", extended)
                utility(command, extended, state="22023")
                query("SHOW TimeZone", [["Asia/Shanghai"]], ["TimeZone"], [25], "SHOW")
        print("TIMEZONE_DEFAULT_COMPLETE", checks, "FAILURES", len(failures), flush=True)
        assert checks == 280 and not failures, (checks, failures)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == "__main__":
    main()
