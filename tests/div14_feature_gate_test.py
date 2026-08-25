#!/usr/bin/env python3
"""DIV-14 regression: capability-gated commands must fail with 0A000.

Blueprint item DIV-14 / CAT-22: commands that previously only stored a
record in .pg_compat_objects while reporting success (CREATE EXTENSION,
CREATE OPERATOR, CREATE RULE, CREATE LANGUAGE, CREATE AGGREGATE,
CREATE TEXT SEARCH ..., CREATE FOREIGN ..., CREATE SERVER, ALTER/DROP
counterparts, IMPORT FOREIGN SCHEMA, LOAD) must instead return
feature_not_supported (SQLSTATE 0A000) in the default postgresql18
compatibility mode.  Nothing may be written to .pg_compat_objects.

The explicit "extended" compatibility mode keeps the legacy record layer
so existing project tooling can opt in.
"""

import os
import socket
import struct
import subprocess
import tempfile
import time

DBMS_MAIN = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dbms_main"))
SOCKET_TIMEOUT = float(os.environ.get("DBMS_PROTOCOL_TEST_TIMEOUT", "10"))
STARTUP_TIMEOUT = float(os.environ.get("DBMS_PROTOCOL_STARTUP_TIMEOUT", "15"))

# Reuse the shared protocol helpers from the main protocol regression.
import importlib.util as _ilu
_spec = _ilu.spec_from_file_location(
    "pg_protocol_helpers",
    os.path.join(os.path.dirname(__file__), "postgres_protocol_test.py"))
_helpers = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(_helpers)

startup = _helpers.startup
simple_query = _helpers.simple_query
read_until_ready = _helpers.read_until_ready
write_auth_catalog = _helpers.write_auth_catalog


def error_of(messages):
    """Return (sqlstate, message) of the first ErrorResponse, or None."""
    for kind, body in messages:
        if kind != b"E":
            continue
        state = None
        message = ""
        for field in body.rstrip(b"\0").split(b"\0"):
            if not field:
                continue
            tag, value = field[:1].decode(), field[1:].decode()
            if tag == "C":
                state = value
            elif tag == "M":
                message = value
        return state, message
    return None


def expect_0a000(sock, sql, label):
    messages = simple_query(sock, sql)
    err = error_of(messages)
    assert err is not None, "%s: expected ErrorResponse, got %r" % (label, messages)
    state, message = err
    assert state == "0A000", "%s: expected SQLSTATE 0A000, got %r (%s)" % (label, state, message)
    assert "feature not supported" in message, "%s: unexpected message %r" % (label, message)


def expect_command_tag(sock, sql, label):
    messages = simple_query(sock, sql)
    assert any(kind == b"C" for kind, _ in messages), \
        "%s: expected CommandComplete, got %r" % (label, messages)


def main():
    if not os.path.exists(DBMS_MAIN):
        raise SystemExit("run scripts/build.sh first")

    work_dir = tempfile.mkdtemp(prefix="dbms-div14-")
    process = None
    try:
        os.mkdir(os.path.join(work_dir, "info"))
        open(os.path.join(work_dir, "info", "tlist.lst"), "wb").close()
        write_auth_catalog(work_dir, "alice", "secret")
        with open(os.path.join(work_dir, "pg_hba.conf"), "w", encoding="utf-8") as hba:
            hba.write("host all alice 127.0.0.1/32 scram-sha-256\n")

        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()
        process = subprocess.Popen(
            [DBMS_MAIN, "--server", str(port), "--insecure"],
            cwd=work_dir,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        sock = socket.socket()
        sock.settimeout(SOCKET_TIMEOUT)
        deadline = time.time() + STARTUP_TIMEOUT
        while True:
            try:
                sock.connect(("127.0.0.1", port))
                break
            except OSError:
                if time.time() >= deadline:
                    raise
                time.sleep(0.05)

        startup(sock, "alice", "info")

        # Default mode is postgresql18 and is observable.
        messages = simple_query(sock, "SHOW compatibility_mode")
        assert any(kind == b"D" and b"postgresql18" in body
                   for kind, body in messages), \
            "SHOW compatibility_mode must report postgresql18: %r" % (messages,)

        # Every capability-gated command fails with 0A000 and a clear message.
        gated_create = [
            "CREATE EXTENSION hstore",
            "CREATE OPERATOR === (LEFTARG = int, RIGHTARG = int)",
            "CREATE RULE r1 AS ON SELECT TO t DO INSTEAD SELECT 1",
            "CREATE LANGUAGE plpython3u HANDLER h",
            "CREATE AGGREGATE mysum(int) (SFUNC = f, STYPE = int)",
            "CREATE TEXT SEARCH CONFIGURATION fr (PARSER = default)",
            "CREATE TEXT SEARCH DICTIONARY d (TEMPLATE = simple)",
            "CREATE FOREIGN DATA WRAPPER dummy",
            "CREATE SERVER s1 FOREIGN DATA WRAPPER dummy",
            "CREATE FOREIGN TABLE ft (id int) SERVER s1",
            "CREATE USER MAPPING FOR alice SERVER s1",
            "CREATE ACCESS METHOD heaps TYPE TABLE HANDLER h",
            "CREATE EVENT TRIGGER e1 ON ddl_command_start EXECUTE FUNCTION f",
            "CREATE TRANSFORM FOR int LANGUAGE sql (FROM SQL WITH f, TO SQL WITH g)",
            "CREATE ASSERTION a CHECK (1 = 1)",
            "CREATE OPERATOR CLASS oc FOR TYPE int USING btree AS OPERATOR 1 <,",
            "CREATE OPERATOR FAMILY of1 USING btree",
            "CREATE SUBSCRIPTION sub1 CONNECTION 'c' PUBLICATION p",
        ]
        for sql in gated_create:
            expect_0a000(sock, sql, sql)

        gated_alter = [
            "ALTER EXTENSION hstore UPDATE",
            "ALTER OPERATOR === (int, int) OWNER TO alice",
            "ALTER RULE r1 ON t RENAME TO r2",
            "ALTER LANGUAGE plpython3u OWNER TO alice",
            "ALTER AGGREGATE mysum(int) RENAME TO mysum2",
            "ALTER TEXT SEARCH CONFIGURATION fr ALTER MAPPING FOR d WITH simple",
            "ALTER FOREIGN DATA WRAPPER dummy RENAME TO dummy2",
            "ALTER SERVER s1 RENAME TO s2",
            "ALTER FOREIGN TABLE ft RENAME TO ft2",
            "ALTER USER MAPPING FOR alice SERVER s1 OPTIONS (ADD k 'v')",
            "ALTER EVENT TRIGGER e1 DISABLE",
            "ALTER OPERATOR CLASS oc USING btree RENAME TO oc2",
            "ALTER OPERATOR FAMILY of1 USING btree RENAME TO of2",
            "ALTER SUBSCRIPTION sub1 CONNECTION 'c2'",
        ]
        for sql in gated_alter:
            expect_0a000(sock, sql, sql)

        gated_drop = [
            "DROP EXTENSION hstore",
            "DROP OPERATOR === (int, int)",
            "DROP RULE r1 ON t",
            "DROP LANGUAGE plpython3u",
            "DROP AGGREGATE mysum(int)",
            "DROP TEXT SEARCH CONFIGURATION fr",
            "DROP TEXT SEARCH DICTIONARY d",
            "DROP FOREIGN DATA WRAPPER dummy",
            "DROP SERVER s1",
            "DROP FOREIGN TABLE ft",
            "DROP USER MAPPING FOR alice SERVER s1",
            "DROP ACCESS METHOD heaps",
            "DROP EVENT TRIGGER e1",
            "DROP TRANSFORM FOR int LANGUAGE sql",
            "DROP ASSERTION a",
            "DROP OPERATOR CLASS oc USING btree",
            "DROP OPERATOR FAMILY of1 USING btree",
            "DROP SUBSCRIPTION sub1",
        ]
        for sql in gated_drop:
            expect_0a000(sock, sql, sql)

        expect_0a000(sock, "IMPORT FOREIGN SCHEMA fs FROM SERVER s1 INTO public",
                     "IMPORT FOREIGN SCHEMA")
        expect_0a000(sock, "LOAD 'auto_explain'", "LOAD")

        # The transaction stays usable after a gated failure (error aborts
        # only the statement, matching PostgreSQL statement semantics).
        expect_command_tag(sock, "BEGIN", "BEGIN")
        expect_0a000(sock, "CREATE EXTENSION hstore", "in-txn gated create")
        expect_command_tag(sock, "COMMIT", "COMMIT")

        # No compatibility records were written: the store must not exist in
        # any database directory of the work dir.
        for root, _dirs, files in os.walk(work_dir):
            if ".pg_compat_objects" in files:
                raise AssertionError(
                    "compat object store created in %s despite 0A000 gate" % root)

        # Extended mode keeps the legacy record layer (explicit opt-in).
        expect_command_tag(sock, "SET compatibility_mode = extended",
                           "SET compatibility_mode")
        messages = simple_query(sock, "CREATE EXTENSION hstore")
        assert any(kind == b"C" for kind, _ in messages), \
            "extended mode CREATE EXTENSION must keep legacy success: %r" % (messages,)
        compat_store = os.path.join(work_dir, "info", ".pg_compat_objects")
        assert os.path.exists(compat_store), \
            "extended mode must keep writing the legacy compat store"

        # Mode cannot flip inside a transaction (DIV framework: session-start
        # restricted).
        expect_command_tag(sock, "SET compatibility_mode = postgresql18",
                           "back to postgresql18")
        expect_command_tag(sock, "BEGIN", "BEGIN")
        messages = simple_query(sock, "SET compatibility_mode = extended")
        err = error_of(messages)
        assert err is not None, "in-transaction SET compatibility_mode must fail"
        expect_command_tag(sock, "ROLLBACK", "ROLLBACK")

        # Unknown values are rejected.
        messages = simple_query(sock, "SET compatibility_mode = mysql")
        assert error_of(messages) is not None, "invalid mode value must fail"

        sock.close()
        print("[DIV-14] capability gate E2E OK")
    finally:
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        subprocess.run(["rm", "-rf", "--", work_dir], check=False)


if __name__ == "__main__":
    main()
