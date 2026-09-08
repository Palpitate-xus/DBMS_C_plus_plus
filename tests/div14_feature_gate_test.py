#!/usr/bin/env python3
"""DIV compatibility regression for capability and syntax gates.

Blueprint item DIV-14 / CAT-22: commands that previously only stored a
record in .pg_compat_objects while reporting success (CREATE EXTENSION,
CREATE OPERATOR, CREATE RULE, CREATE LANGUAGE, CREATE AGGREGATE,
CREATE TEXT SEARCH ..., CREATE FOREIGN ..., CREATE SERVER, ALTER/DROP
counterparts, IMPORT FOREIGN SCHEMA, LOAD) must instead return
feature_not_supported (SQLSTATE 0A000) in the default postgresql18
or the explicit extended mode.  Nothing may be written to
.pg_compat_objects; extended mode only enables project-native syntax that
has a real implementation.
"""

import os
import socket
import struct
import subprocess
import tempfile
import time

DBMS_MAIN = os.path.abspath(os.environ.get(
    "DBMS_MAIN", os.path.join(os.path.dirname(__file__), "..", "dbms_main")))
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


def expect_error(sock, sql, expected_state, label, message_fragment=None):
    messages = simple_query(sock, sql)
    err = error_of(messages)
    assert err is not None, "%s: expected ErrorResponse, got %r" % (label, messages)
    state, message = err
    assert state == expected_state, \
        "%s: expected SQLSTATE %s, got %r (%s)" % (
            label, expected_state, state, message)
    if message_fragment is not None:
        assert message_fragment in message, \
            "%s: expected %r in %r" % (label, message_fragment, message)


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
            "ALTER PUBLICATION p ADD TABLE t6d",
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

        # DIV-06: MySQL/SQL Server type aliases are rejected with the
        # canonical type in the message (SQLSTATE 42704).
        for sql, canon in [("CREATE TABLE t6a (a TINYINT)", "smallint"),
                           ("CREATE TABLE t6b (a DATETIME)", "timestamp"),
                           ("CREATE TABLE t6c (a NVARCHAR(10))", "varchar")]:
            messages = simple_query(sock, sql)
            err = error_of(messages)
            assert err is not None and "does not exist" in err[1] and canon in err[1], \
                "%s must fail with type error mentioning %s: %r" % (sql, canon, err)
        # A canonical type still works in postgresql18 mode.
        expect_command_tag(sock, "CREATE TABLE t6d (a SMALLINT)", "pg type create")

        # DIV-07: MySQL-style fulltext shortcut syntax -> 42601.
        for sql, hint in [
            ("CREATE FULLTEXT INDEX fi ON t6d (a)", "USING gin"),
            ("DROP FULLTEXT INDEX fi ON t6d", "DROP INDEX"),
        ]:
            messages = simple_query(sock, sql)
            err = error_of(messages)
            assert err is not None and err[0] == "42601" and hint in err[1], \
                "%s must fail with 42601 syntax error mentioning %s: %r" % (sql, hint, err)

        # DIV-10: project-only admin grammar is a PostgreSQL syntax error;
        # retain tool hints without misclassifying it as a server capability.
        for sql, tool in [
            ("DUMP DATABASE info TO '/tmp/x.sql'", "pg_dump"),
            ("BACKUP DATABASE info TO '/tmp/x.bak'", "pg_basebackup"),
            ("RESTORE DATABASE info FROM '/tmp/x.bak'", "pg_restore"),
            ("CLEAR PLAN CACHE", "project extension"),
        ]:
            expect_error(sock, sql, "42601", sql, tool)

        # DIV-08: CREATE ASSERTION is unsupported in BOTH modes, exactly
        # like PostgreSQL 18 (which never implemented SQL assertions).
        expect_0a000(sock, "CREATE ASSERTION a CHECK (1 = 1)", "DIV-08 assertion")

        # DIV-12: project pool/TDE surface is hidden in postgresql18 mode.
        messages = simple_query(sock, "SHOW TDE STATUS")
        err = error_of(messages)
        assert err is not None and err[0] == "0A000" and "TDE" in err[1], \
            "SHOW TDE STATUS must be 0A000: %r" % (err,)
        for guc in ("pool_mode", "pool_size", "max_client_conn"):
            messages = simple_query(sock, "SHOW %s" % guc)
            err = error_of(messages)
            assert err is not None and err[0] == "42704" and guc in err[1], \
                "SHOW %s must be unrecognized parameter 42704: %r" % (guc, err)

        # DIV-09: plain-SQL slot management and SHOW LOGICAL are not
        # PostgreSQL SQL grammar; PostgreSQL uses the replication protocol.
        for sql in ["CREATE REPLICATION SLOT s1",
                    "DROP REPLICATION SLOT s1",
                    "SHOW REPLICATION SLOTS",
                    "SHOW LOGICAL CHANGES FOR SLOT s1"]:
            expect_error(sock, sql, "42601", sql)

        # DIV-02/03/04: MySQL-style syntax is rejected with 42601 in
        # postgresql18 mode.
        for sql in ["REPLACE INTO t VALUES (1)",
                    "LOAD DATA INFILE '/tmp/x.csv' INTO TABLE t",
                    "SELECT * FROM t INTO OUTFILE '/tmp/x.csv'"]:
            messages = simple_query(sock, sql)
            err = error_of(messages)
            assert err is not None and err[0] == "42601", \
                "%s must fail with 42601: %r" % (sql, err)

        # DIV-05: project meta-command grammar is 42601.  A one-token SHOW
        # name is instead parsed as a GUC lookup and returns 42704.
        for sql in ["DESC t", "DESCRIBE t", "VIEW TABLE t", "VIEW DATABASE"]:
            expect_error(sock, sql, "42601", sql)
        for sql, parameter in [("SHOW USERS", "users"),
                               ("SHOW ROLES", "roles"),
                               ("SHOW POOLS", "pools")]:
            expect_error(sock, sql, "42704", sql, parameter)

        # DIV-01: USE DATABASE is not PostgreSQL SQL; the connection must
        # stay alive and the session state must be untouched.
        err = error_of(simple_query(sock, "USE DATABASE info"))
        assert err is not None and err[0] == "42601", \
            "USE DATABASE must fail with 42601 in postgresql18 mode: %r" % (err,)
        err = error_of(simple_query(sock, "use info"))
        assert err is not None and err[0] == "42601", \
            "short 'use' form must fail with 42601 too: %r" % (err,)
        alive = [m for m in simple_query(sock, "SELECT 1 + 1") if m[0] == b"D"]
        assert alive, "connection must stay usable after gated USE DATABASE"

        # DIV-11: SET GLOBAL is MySQL-style syntax, not a PostgreSQL GUC
        # statement.  postgresql18 mode rejects it with 42601.
        err = error_of(simple_query(sock, "SET GLOBAL auto_vacuum = on"))
        assert err is not None and err[0] == "42601", \
            "SET GLOBAL must fail with 42601 in postgresql18 mode: %r" % (err,)
        # Plain SET on the same parameter remains valid PostgreSQL syntax.
        expect_command_tag(sock, "SET statement_timeout = 1234", "plain SET")

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

        # Extended mode does not make unimplemented PostgreSQL objects real.
        expect_command_tag(sock, "SET compatibility_mode = extended",
                           "SET compatibility_mode")
        expect_0a000(sock, "CREATE EXTENSION hstore",
                     "extended CREATE EXTENSION")
        expect_0a000(sock, "ALTER PUBLICATION p ADD TABLE t6d",
                     "extended ALTER PUBLICATION")
        expect_0a000(sock,
                     "IMPORT FOREIGN SCHEMA fs FROM SERVER s1 INTO public",
                     "extended IMPORT FOREIGN SCHEMA")
        expect_0a000(sock, "LOAD 'auto_explain'", "extended LOAD")
        for root, _dirs, files in os.walk(work_dir):
            assert ".pg_compat_objects" not in files, \
                "extended mode wrote a fake compat object in %s" % root

        # DIV-01 / DIV-11 in extended mode: project commands work again.
        expect_command_tag(sock, "USE DATABASE info", "extended USE DATABASE")
        expect_command_tag(sock, "SET GLOBAL auto_vacuum = on",
                           "extended SET GLOBAL")
        # DIV-06 in extended mode: alias mapping keeps working.
        expect_command_tag(sock, "CREATE TABLE t6e (a TINYINT)",
                           "extended TINYINT")

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
