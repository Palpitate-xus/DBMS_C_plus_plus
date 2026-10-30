"""Deferred R146 draft: complete PK values and exact-RID maintenance.

User skipped this issue; this is not an accepted or registered regression.
"""
import importlib.util
from pathlib import Path
import socket
import struct
import subprocess
import sys
import time
import uuid


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def cases():
    prefix = "abcdefghijklmnopqrst"
    ascii_keys = [(literal(prefix + letter),) for letter in "ABCDE"]
    pairs = lambda left, right: [(left, right(i)) for i in range(1, 6)]
    return [
        ("text", [("v", "TEXT")], ascii_keys, [9, 3, 2]),
        ("binary", [("v", 'TEXT COLLATE "C"')], ascii_keys, [9, 3, 2]),
        ("utf8", [("v", 'VARCHAR(128) COLLATE "C"')],
         [(literal("汉" * 7 + letter),) for letter in "ABCDE"], [9, 3, 2]),
        ("char", [("v", 'CHAR(64) COLLATE "C"')], ascii_keys, [9, 3, 2]),
        ("numeric", [("v", "NUMERIC")],
         [(str(123456789012345678900 + i),) for i in range(1, 6)], [9, 3, 2]),
        ("bigpair", [("a", "BIGINT"), ("b", "BIGINT")],
         pairs("1234567890123456789", str), [9, 3, 2]),
        ("micropair", [("a", "TIMESTAMP"), ("b", "TIMESTAMP")],
         pairs(literal("2000-01-01 00:00:00"), lambda i: literal("2000-01-01 00:00:00." + str(i).zfill(6))),
         [9, 3, 2]),
        ("textpair", [("a", 'TEXT COLLATE "C"'), ("b", "INT")],
         pairs(literal(prefix + "same"), str), [9, 3, 2]),
        ("separator", [("a", 'TEXT COLLATE "C"'), ("b", 'TEXT COLLATE "C"')],
         [(literal(a), literal(b)) for a, b in
          (("a\x01b", "c"), ("a", "b\x01c"), ("a\x01b", "c2"), ("a", "b\x01d"), ("a\x01c", "c"))],
         [2, 9, 3]),
    ]


def operations(table, columns, keys, ordered):
    names = [name for name, _ in columns]
    where = lambda key: " AND ".join(name + "=" + value for name, value in zip(names, key))
    values = lambda key, payload: "(" + ",".join(key) + "," + str(payload) + ",'')"
    update = lambda key: ",".join(name + "=" + value for name, value in zip(names, key))
    order = ",".join(names)
    ops = []
    def add(sql, rows=None, tag=None, state=None, ready=b"T"):
        ops.append((sql, rows, tag, state, ready))
    add("BEGIN", tag="BEGIN")
    add("CREATE TEMP TABLE " + table + "(" + ",".join(n + " " + t for n, t in columns) +
        ",p INT,note TEXT,PRIMARY KEY(" + order + "))", tag="CREATE TABLE")
    for i in range(3):
        add("INSERT INTO " + table + " VALUES" + values(keys[i], i + 1), tag="INSERT 0 1")
    add("UPDATE " + table + " SET note=" + literal("x" * 300) + " WHERE " + where(keys[1]), tag="UPDATE 1")
    add("SELECT length(note) AS n,p FROM " + table + " WHERE " + where(keys[1]), [["300", "2"]])
    add("SELECT p FROM " + table + " ORDER BY p", [["1"], ["2"], ["3"]])
    for i in range(3):
        add("SELECT p FROM " + table + " WHERE " + where(keys[i]), [[str(i + 1)]])
    add("SELECT p FROM " + table + " WHERE " + where(keys[3]), [])
    add("INSERT INTO " + table + " VALUES" + values(keys[0], 8), state="23505", ready=b"E")
    add("SELECT count(*) AS n FROM " + table, [["3"]])
    add("UPDATE " + table + " SET " + update(keys[3]) + " WHERE " + where(keys[1]), tag="UPDATE 1")
    add("SELECT p FROM " + table + " WHERE " + where(keys[1]), [])
    add("SELECT p FROM " + table + " WHERE " + where(keys[3]), [["2"]])
    add("SELECT p FROM " + table + " WHERE " + where(keys[0]), [["1"]])
    add("UPDATE " + table + " SET " + update(keys[0]) + " WHERE " + where(keys[3]), state="23505", ready=b"E")
    add("SELECT p FROM " + table + " WHERE " + where(keys[3]), [["2"]])
    add("SAVEPOINT r146_outer", tag="SAVEPOINT")
    add("DELETE FROM " + table + " WHERE " + where(keys[0]), tag="DELETE 1")
    add("SELECT p FROM " + table + " WHERE " + where(keys[0]), [])
    add("SELECT p FROM " + table + " WHERE " + where(keys[3]), [["2"]])
    add("UPDATE " + table + " SET " + update(keys[1]) + " WHERE " + where(keys[3]), tag="UPDATE 1")
    add("INSERT INTO " + table + " VALUES" + values(keys[4], 4), tag="INSERT 0 1")
    add("SELECT p FROM " + table + " WHERE " + where(keys[1]), [["2"]])
    add("ROLLBACK TO r146_outer", tag="ROLLBACK")
    add("RELEASE r146_outer", tag="RELEASE")
    add("SELECT p FROM " + table + " WHERE " + where(keys[0]), [["1"]])
    add("SELECT p FROM " + table + " WHERE " + where(keys[3]), [["2"]])
    add("SELECT p FROM " + table + " WHERE " + where(keys[1]), [])
    add("SELECT count(*) AS n FROM " + table, [["3"]])
    add("DELETE FROM " + table + " WHERE " + where(keys[0]), tag="DELETE 1")
    add("SELECT p FROM " + table + " WHERE " + where(keys[3]), [["2"]])
    add("INSERT INTO " + table + " VALUES" + values(keys[0], 9), tag="INSERT 0 1")
    add("SELECT p FROM " + table + " ORDER BY p", [["2"], ["3"], ["9"]])
    add("REINDEX TABLE " + table, tag="REINDEX")
    for key, payload in ((keys[3], 2), (keys[2], 3), (keys[0], 9)):
        add("SELECT p FROM " + table + " WHERE " + where(key), [[str(payload)]])
    add("COMMIT", tag="COMMIT", ready=b"I")
    add("SELECT p FROM " + table + " ORDER BY " + order, [[str(p)] for p in ordered], ready=b"I")
    add("SELECT p FROM " + table + " ORDER BY " + ",".join(n + " DESC" for n in names) +
        " LIMIT 2 OFFSET 1", [[str(p)] for p in list(reversed(ordered))[1:]], ready=b"I")
    add("DROP TABLE " + table, tag="DROP TABLE", ready=b"I")
    return ops


def alter_operations(table):
    p = "abcdefghijklmnopqrst"
    ops = [("BEGIN", None, "BEGIN", None, b"T"),
           ("CREATE TEMP TABLE " + table + '(v TEXT COLLATE "C",p INT)', None, "CREATE TABLE", None, b"T"),
           ("INSERT INTO " + table + " VALUES" + ",".join("(" + literal(p + x) + "," + str(i) + ")" for i, x in enumerate("ABC", 1)), None, "INSERT 0 3", None, b"T"),
           ("ALTER TABLE " + table + " ADD PRIMARY KEY(v)", None, "ALTER TABLE", None, b"T")]
    for i, x in enumerate("ABC", 1):
        ops.append(("SELECT p FROM " + table + " WHERE v=" + literal(p + x), [[str(i)]], None, None, b"T"))
    ops += [("INSERT INTO " + table + " VALUES(" + literal(p + "B") + ",9)", None, None, "23505", b"E"),
            ("REINDEX TABLE " + table, None, "REINDEX", None, b"T"),
            ("DELETE FROM " + table + " WHERE v=" + literal(p + "B"), None, "DELETE 1", None, b"T"),
            ("SAVEPOINT r146_restore", None, "SAVEPOINT", None, b"T"),
            ("INSERT INTO " + table + " VALUES(" + literal(p + "B") + ",7)", None, "INSERT 0 1", None, b"T"),
            ("ROLLBACK TO r146_restore", None, "ROLLBACK", None, b"T"),
            ("RELEASE r146_restore", None, "RELEASE", None, b"T"),
            ("SELECT p FROM " + table + " WHERE v=" + literal(p + "B"), [], None, None, b"T"),
            ("INSERT INTO " + table + " VALUES(" + literal(p + "B") + ",8)", None, "INSERT 0 1", None, b"T"),
            ("COMMIT", None, "COMMIT", None, b"I"),
            ("SELECT p FROM " + table + " ORDER BY p", [["1"], ["3"], ["8"]], None, None, b"I"),
            ("DROP TABLE " + table, None, "DROP TABLE", None, b"I")]
    return ops


def split_operations(table):
    p = "abcdefghijklmnopqrst"
    key = lambda i: literal(p + str(i).zfill(4))
    rows = ",".join("(" + key(i) + "," + str(i) + ")" for i in reversed(range(240)))
    ops = [("BEGIN", None, "BEGIN", None, b"T"),
           ("CREATE TEMP TABLE " + table + '(v VARCHAR(128) COLLATE "C" PRIMARY KEY,p INT)', None, "CREATE TABLE", None, b"T"),
           ("INSERT INTO " + table + " VALUES" + rows, None, "INSERT 0 240", None, b"T"),
           ("SELECT count(*) AS n FROM " + table, [["240"]], None, None, b"T")]
    for i in (0, 119, 120, 239):
        ops.append(("SELECT p FROM " + table + " WHERE v=" + key(i), [[str(i)]], None, None, b"T"))
    ops += [("SELECT p FROM " + table + " WHERE v>=" + key(0) + " AND v<=" + key(239) + " ORDER BY v LIMIT 5 OFFSET 118", [[str(i)] for i in range(118, 123)], None, None, b"T"),
            ("UPDATE " + table + " SET v=" + literal(p + "zzzz") + " WHERE v=" + key(120), None, "UPDATE 1", None, b"T"),
            ("SELECT p FROM " + table + " WHERE v=" + key(120), [], None, None, b"T"),
            ("SELECT p FROM " + table + " WHERE v=" + literal(p + "zzzz"), [["120"]], None, None, b"T"),
            ("DELETE FROM " + table + " WHERE p<10", None, "DELETE 10", None, b"T"),
            ("SELECT count(*) AS n FROM " + table, [["230"]], None, None, b"T"),
            ("SAVEPOINT r146_split", None, "SAVEPOINT", None, b"T"),
            ("INSERT INTO " + table + " VALUES(" + key(0) + ",500)", None, "INSERT 0 1", None, b"T"),
            ("REINDEX TABLE " + table, None, "REINDEX", None, b"T"),
            ("SELECT p FROM " + table + " WHERE v=" + key(0), [["500"]], None, None, b"T"),
            ("ROLLBACK TO r146_split", None, "ROLLBACK", None, b"T"),
            ("RELEASE r146_split", None, "RELEASE", None, b"T"),
            ("SELECT p FROM " + table + " WHERE v=" + key(0), [], None, None, b"T"),
            ("SELECT p FROM " + table + " WHERE v=" + key(119), [["119"]], None, None, b"T"),
            ("SELECT p FROM " + table + " WHERE v=" + literal(p + "zzzz"), [["120"]], None, None, b"T"),
            ("COMMIT", None, "COMMIT", None, b"I"),
            ("DROP TABLE " + table, None, "DROP TABLE", None, b"I")]
    return ops


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("pk_full_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert reference or not sys.argv[1:]
    server = None
    if reference:
        h, p, u, d, pw = runner._reference_connection_settings()
        sock = socket.create_connection((h, p), timeout=runner.wire_timeout())
        client.startup_reference(sock, u, d, pw); runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client); sock = server["sock"]
    controls, failures, recoveries = 0, [], 0
    prefix = "r146_" + uuid.uuid4().hex[:8]

    def request(sql, mode):
        if mode == "simple": return client.simple_query(sock, sql)
        packet = client.typed(b"P", b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0))
        packet += client.typed(b"B", b"\0\0" + struct.pack("!HHHh", 0, 0, 1, mode))
        packet += client.typed(b"D", b"P\0") + client.typed(b"E", b"\0" + struct.pack("!I", 0))
        sock.sendall(packet + client.typed(b"S")); return client.read_until_ready(sock)

    def guard(sql):
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        print("PK_FULL_GUARD", repr(sql), repr(result), repr(messages[-1]), flush=True)
        assert result[1] is None and messages[-1] == (b"Z", b"T"), (sql, result)

    def decode(messages):
        # Read binary integer fields losslessly; the generic diff reader is
        # text-only and must not interpret int4/int8 payloads as UTF-8.
        result = list(runner.decode_wire_result(
            [(k, b) for k, b in messages if k != b"D"], include_types=True))
        fields = []
        for kind, body in messages:
            if kind != b"T": continue
            count = struct.unpack_from("!H", body)[0]
            at = 2
            for _ in range(count):
                at = body.index(b"\0", at) + 1
                _, _, oid, _, _, fmt = struct.unpack_from("!IhIhih", body, at)
                at += 18
                fields.append((oid, fmt))
            assert at == len(body)
        rows = []
        for kind, body in messages:
            if kind != b"D": continue
            count = struct.unpack_from("!H", body)[0]
            assert count == len(fields)
            at, values = 2, []
            for oid, fmt in fields:
                length = struct.unpack_from("!i", body, at)[0]; at += 4
                if length == -1: values.append(None); continue
                assert length >= 0 and at + length <= len(body)
                value = body[at:at + length]; at += length
                if fmt == 1 and oid in (20, 23):
                    assert length == (8 if oid == 20 else 4), (oid, length)
                    values.append(str(int.from_bytes(value, "big", signed=True)))
                else:
                    values.append(value.decode())
            assert at == len(body)
            rows.append(values)
        result[0] = rows
        return result

    def run(op, mode):
        nonlocal controls, recoveries
        sql, rows, tag, state, ready = op
        first = sql.split()[0].upper()
        protected = ready in (b"T", b"E") and first in ("INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "REINDEX")
        if protected: guard("SAVEPOINT r146_statement_guard")
        messages = request(sql, mode)
        result = decode(messages)
        expected_oids = ([20] if "count(*)" in sql else [23] * len(rows[0]) if rows else [23]) if rows is not None else []
        good = result[1] == state and messages[-1] == (b"Z", ready)
        if rows is not None:
            good = good and result[0] == rows and result[5] == expected_oids and result[4] == "SELECT " + str(len(rows))
            good = good and result[3] == (["n", "p"] if sql.startswith("SELECT length(") else ["n"] if "count(*)" in sql else ["p"])
        elif state:
            good = good and result[0] == [] and result[4] is None and sum(k == b"E" for k, _ in messages) == 1
        else:
            good = good and result[0] == [] and result[4] == tag and not any(k in (b"T", b"D", b"E") for k, _ in messages)
        controls += 1
        print("PK_FULL_PROTOCOL", controls, repr((sql, mode)), "match", good, repr(result), repr(messages[-1]), flush=True)
        if not good: failures.append((controls, sql, result, ready))
        if protected:
            if messages[-1] == (b"Z", b"E"):
                guard("ROLLBACK TO r146_statement_guard"); recoveries += 1
            guard("RELEASE r146_statement_guard")
        return result

    def cold_restart():
        nonlocal sock
        process = server["process"]
        assert process.poll() is None
        process.kill()
        assert process.wait(timeout=10) == -9
        sock.close(); server["sock"] = None
        server["process"] = subprocess.Popen(
            [runner.DBMS_MAIN, "--data-dir", server["dir"], "--server", str(server["port"]), "--insecure"],
            cwd=server["dir"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 20
        while True:
            assert server["process"].poll() is None, ("owned restart failed", server["process"].returncode)
            connection = socket.socket(); connection.settimeout(runner.wire_timeout())
            try:
                connection.connect(("127.0.0.1", server["port"])); sock = connection; break
            except OSError:
                connection.close()
                if time.monotonic() >= deadline: raise
                time.sleep(0.05)
        server["sock"] = sock
        client.startup(sock, "alice", "info")
        print("PK_FULL_OWNED_COLD_RESTART", server["process"].pid, flush=True)

    try:
        groups = []
        for mi, mode in enumerate(("simple", 0, 1)):
            for label, columns, keys, ordered in cases():
                groups.append((mode, operations(prefix + "_" + str(mi) + "_" + label, columns, keys, ordered)))
            groups.append((mode, alter_operations(prefix + "_" + str(mi) + "_alter")))
            groups.append((mode, split_operations(prefix + "_" + str(mi) + "_split")))
        expected_common = sum(len(ops) for _, ops in groups)
        assert expected_common == 1347, expected_common
        for mode, ops in groups:
            for op in ops: run(op, mode)
        print("PK_FULL_COMMON_COMPLETE", controls, "EXPECTED", expected_common, "FAILURES", len(failures), flush=True)
        if not reference:
            table = prefix + "_durable"
            p = "abcdefghijklmnopqrst"
            run(("CREATE TABLE " + table + '(v TEXT COLLATE "C" PRIMARY KEY,p INT)', None, "CREATE TABLE", None, b"I"), "simple")
            for i, ch in enumerate("ABC", 1):
                run(("INSERT INTO " + table + " VALUES(" + literal(p + ch) + "," + str(i) + ")", None, "INSERT 0 1", None, b"I"), "simple")
            cold_restart()
            for i, ch in enumerate("ABC", 1):
                run(("SELECT p FROM " + table + " WHERE v=" + literal(p + ch), [[str(i)]], None, None, b"I"), 1)
            run(("BEGIN", None, "BEGIN", None, b"T"), "simple")
            run(("DELETE FROM " + table + " WHERE v=" + literal(p + "A"), None, "DELETE 1", None, b"T"), "simple")
            run(("UPDATE " + table + " SET v=" + literal(p + "D") + " WHERE v=" + literal(p + "B"), None, "UPDATE 1", None, b"T"), "simple")
            run(("INSERT INTO " + table + " VALUES(" + literal(p + "E") + ",5)", None, "INSERT 0 1", None, b"T"), "simple")
            cold_restart()
            for i, ch in enumerate("ABC", 1):
                run(("SELECT p FROM " + table + " WHERE v=" + literal(p + ch), [[str(i)]], None, None, b"I"), 1)
            for ch in "DE":
                run(("SELECT p FROM " + table + " WHERE v=" + literal(p + ch), [], None, None, b"I"), 1)
            run(("REINDEX TABLE " + table, None, "REINDEX", None, b"I"), "simple")
            run(("SELECT p FROM " + table + " ORDER BY v", [["1"], ["2"], ["3"]], None, None, b"I"), 1)
        expected = expected_common + (18 if not reference else 0)
        print("PK_FULL_PROTOCOL_COMPLETE", controls, "EXPECTED", expected, "FAILURES", len(failures), "RECOVERIES", recoveries, flush=True)
        assert controls == expected and not failures, (controls, expected, len(failures), failures[:30])
    finally:
        if server: runner.stop_ours(server)
        else:
            try: client.simple_query(sock, "ROLLBACK")
            finally: sock.close()


if __name__ == "__main__":
    main()
