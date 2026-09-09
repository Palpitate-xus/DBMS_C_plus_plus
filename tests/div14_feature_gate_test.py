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
from pathlib import Path
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
read_message = _helpers.read_message
typed = _helpers.typed
write_auth_catalog = _helpers.write_auth_catalog
setting_value = _helpers.setting_value
data_row_values = _helpers.data_row_values
row_description_fields = _helpers.row_description_fields


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
        os.mkdir(os.path.join(work_dir, "other_db"))
        open(os.path.join(work_dir, "other_db", "tlist.lst"), "wb").close()
        write_auth_catalog(work_dir, "alice", "secret")
        with open(os.path.join(work_dir, "pg_hba.conf"), "w", encoding="utf-8") as hba:
            hba.write("host all all 127.0.0.1/32 scram-sha-256\n")

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
        messages = simple_query(sock, "SHOW dbms.extensions")
        assert any(kind == b"D" and b"none" in body for kind, body in messages)
        assert all(b"compat_object_record_layer" not in body
                   for _kind, body in messages)

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

        # DIV-06: MySQL/SQL Server type aliases are rejected with the
        # canonical type in the message (SQLSTATE 42704).
        for sql, canon in [("CREATE TABLE t6a (a TINYINT)", "smallint"),
                           ("CREATE TABLE t6b (a DATETIME)", "timestamp"),
                           ("CREATE TABLE t6c (a NVARCHAR(10))", "varchar"),
                           ("CREATE TABLE t6long (a LONG)", "bigint"),
                           ("CREATE TABLE t6blob (a BLOB)", "bytea"),
                           ("CREATE TABLE t6nchar (a NCHAR(4))", "char"),
                           ("CREATE TABLE t6bin (a BINARY(4))", "bytea"),
                           ("CREATE TABLE t6vbin (a VARBINARY(4))", "bytea")]:
            messages = simple_query(sock, sql)
            err = error_of(messages)
            assert err is not None and "does not exist" in err[1] and canon in err[1], \
                "%s must fail with type error mentioning %s: %r" % (sql, canon, err)
        for sql, fragment in [
                ("CREATE TABLE t6auto (a INTEGER AUTO_INCREMENT)",
                 "GENERATED AS IDENTITY"),
                ("CREATE TABLE t6unsigned (a INTEGER UNSIGNED)",
                 "CHECK constraint"),
        ]:
            expect_error(sock, sql, "42601", sql, fragment)
        for relation in ("t6auto", "t6unsigned"):
            expect_error(sock, "SELECT * FROM %s" % relation, "42P01",
                         "failed DIV-06 DDL left no relation")
        # A canonical type still works in postgresql18 mode.
        expect_command_tag(sock, "CREATE TABLE t6d (a SMALLINT)", "pg type create")
        expect_command_tag(sock, "SELECT a INTO select_into_copy FROM t6d",
                           "PostgreSQL SELECT INTO table")
        expect_command_tag(sock, "DROP TABLE select_into_copy",
                           "drop PostgreSQL SELECT INTO table")
        expect_error(
            sock, "ALTER TABLE t6d ADD COLUMN ext_auto INTEGER AUTO_INCREMENT",
            "42601", "ALTER TABLE AUTO_INCREMENT gate", "GENERATED AS IDENTITY")
        expect_error(
            sock, "ALTER TABLE t6d ADD COLUMN ext_unsigned INTEGER UNSIGNED",
            "42601", "ALTER TABLE UNSIGNED gate", "CHECK constraint")

        # REPL-08: publication membership changes use the real publication
        # catalog and persist atomically; they never reach .pg_compat_objects.
        invalid_publication_path = os.path.join(
            work_dir, "info", "invalid_pub.publication")
        escaped_publication_path = os.path.join(
            work_dir, "publication_escape.publication")
        expect_error(
            sock, "CREATE PUBLICATION ../publication_escape",
            "42601", "path-like publication name")
        assert not os.path.exists(escaped_publication_path)
        expect_error(
            sock, "CREATE PUBLICATION invalid_pub trailing_clause",
            "42601", "unknown publication clause")
        assert not os.path.exists(invalid_publication_path)
        expect_0a000(
            sock, "CREATE PUBLICATION invalid_pub FOR TABLE t6d (a)",
            "publication column list")
        expect_0a000(
            sock, "CREATE PUBLICATION invalid_pub FOR TABLE t6d WHERE (a > 0)",
            "publication row filter")
        expect_0a000(
            sock, "CREATE PUBLICATION invalid_pub FOR TABLES IN SCHEMA public",
            "schema publication")
        expect_0a000(
            sock,
            "CREATE PUBLICATION invalid_pub "
            "WITH (publish_via_partition_root = true)",
            "publication partition-root option")
        assert not os.path.exists(invalid_publication_path)
        expect_command_tag(sock, "CREATE TABLE pub_member (a INT)",
                           "publication member table")
        expect_command_tag(sock, "CREATE ROLE pub_owner",
                           "publication owner role")
        expect_command_tag(
            sock,
            "CREATE PUBLICATION pub_gate FOR TABLE t6d "
            "WITH (publish = 'insert')",
                           "create real publication")
        publication_path = os.path.join(work_dir, "info", "pub_gate.publication")
        with open(publication_path, encoding="utf-8") as publication_file:
            publication_lines = publication_file.read().splitlines()
        assert publication_lines[0].split()[1:] == ["1", "0", "0", "0", "0"], \
            publication_lines
        expect_command_tag(sock, "ALTER PUBLICATION pub_gate ADD TABLE pub_member",
                           "add publication member")
        with open(publication_path, encoding="utf-8") as publication_file:
            membership = publication_file.read().splitlines()[1:]
        assert membership == ["t6d", "pub_member"], membership
        expect_command_tag(
            sock,
            "ALTER PUBLICATION pub_gate SET (publish = 'insert, delete')",
            "change publication operations")
        with open(publication_path, encoding="utf-8") as publication_file:
            publication_lines = publication_file.read().splitlines()
        assert publication_lines[0].split()[1:] == ["1", "0", "1", "0", "0"], \
            publication_lines
        expect_command_tag(
            sock,
            "ALTER PUBLICATION pub_gate SET (publish = 'truncate')",
            "enable truncate publication operation")
        with open(publication_path, encoding="utf-8") as publication_file:
            publication_lines = publication_file.read().splitlines()
        assert publication_lines[0].split()[1:] == ["0", "0", "0", "1", "0"], \
            publication_lines
        expect_command_tag(
            sock,
            "ALTER PUBLICATION pub_gate SET (publish = 'insert, delete')",
            "restore publication operations")
        persisted_before_error = Path(publication_path).read_bytes()
        expect_0a000(
            sock,
            "ALTER PUBLICATION pub_gate SET (publish = 'merge')",
            "unsupported publication operation")
        expect_error(
            sock,
            "ALTER PUBLICATION pub_gate SET (publish = '')",
            "42601", "empty publication operation list")
        assert error_of(simple_query(
            sock, "ALTER PUBLICATION pub_gate ADD TABLE pub_member")) is not None
        assert error_of(simple_query(
            sock, "ALTER PUBLICATION pub_gate DROP TABLE missing_member")) is not None
        assert error_of(simple_query(
            sock, "ALTER PUBLICATION pub_gate ADD TABLE missing_table")) is not None
        assert Path(publication_path).read_bytes() == persisted_before_error
        expect_command_tag(sock, "ALTER PUBLICATION pub_gate DROP TABLE t6d",
                           "drop publication member")
        expect_command_tag(sock, "ALTER PUBLICATION pub_gate SET TABLE t6d",
                           "replace publication membership")
        with open(publication_path, encoding="utf-8") as publication_file:
            membership = publication_file.read().splitlines()[1:]
        assert membership == ["t6d"], membership
        expect_command_tag(
            sock, "CREATE PUBLICATION pub_collision FOR TABLE t6d",
            "publication rename collision")
        persisted_before_rename = Path(publication_path).read_bytes()
        assert error_of(simple_query(
            sock,
            "ALTER PUBLICATION pub_gate RENAME TO pub_collision")) is not None
        assert Path(publication_path).read_bytes() == persisted_before_rename
        expect_command_tag(sock, "DROP PUBLICATION pub_collision",
                           "drop publication rename collision")
        expect_command_tag(sock,
                           "ALTER PUBLICATION pub_gate RENAME TO pub_renamed",
                           "rename publication")
        renamed_path = os.path.join(work_dir, "info", "pub_renamed.publication")
        assert not os.path.exists(publication_path)
        assert Path(renamed_path).read_bytes() == persisted_before_rename
        assert error_of(simple_query(
            sock,
            "ALTER PUBLICATION pub_renamed OWNER TO missing_pub_owner")) \
            is not None
        assert Path(renamed_path).read_bytes() == persisted_before_rename
        expect_command_tag(sock,
                           "ALTER PUBLICATION pub_renamed OWNER TO pub_owner",
                           "change publication owner")
        with open(renamed_path, encoding="utf-8") as publication_file:
            publication_lines = publication_file.read().splitlines()
        assert publication_lines[0].split()[0] == "pub_owner", publication_lines
        expect_command_tag(sock, "DROP PUBLICATION pub_renamed",
                           "drop real publication")
        expect_command_tag(sock, "DROP ROLE pub_owner",
                           "drop publication owner role")

        expect_command_tag(
            sock, "CREATE PUBLICATION drop_first FOR TABLE t6d",
            "create first batch-drop publication")
        expect_command_tag(
            sock, "CREATE PUBLICATION drop_second FOR TABLE t6d",
            "create second batch-drop publication")
        drop_first_path = os.path.join(
            work_dir, "info", "drop_first.publication")
        drop_second_path = os.path.join(
            work_dir, "info", "drop_second.publication")
        expect_error(
            sock,
            "DROP PUBLICATION drop_first, missing_drop, drop_second",
            "42704", "batch publication drop prevalidation")
        assert os.path.exists(drop_first_path) and os.path.exists(drop_second_path)
        expect_command_tag(
            sock,
            "DROP PUBLICATION IF EXISTS "
            "drop_first, missing_drop, drop_second CASCADE",
            "batch publication drop")
        assert not os.path.exists(drop_first_path)
        assert not os.path.exists(drop_second_path)

        # DIV-07: MySQL-style fulltext shortcut syntax -> 42601.
        for sql, hint in [
            ("CREATE FULLTEXT INDEX fi ON t6d (a)", "USING gin"),
            ("CREATE HASH INDEX hi ON t6d (a)", "USING hash"),
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
        expect_error(sock, "SHOW VARIABLES", "42704",
                     "MySQL SHOW VARIABLES", "variables")
        expect_error(sock, "SET @project_value = 7", "42601",
                     "MySQL user variable")
        missing_reload = simple_query(sock, "SELECT pg_reload_conf()")
        assert data_row_values(missing_reload) == [[b"f"]], missing_reload
        # Plain SET on the same parameter remains valid PostgreSQL syntax.
        expect_command_tag(sock, "SET statement_timeout = 1234", "plain SET")

        # The transaction stays usable after a gated failure (error aborts
        # only the statement, matching PostgreSQL statement semantics).
        expect_command_tag(sock, "BEGIN", "BEGIN")
        expect_0a000(sock, "CREATE EXTENSION hstore", "in-txn gated create")
        expect_command_tag(sock, "COMMIT", "COMMIT")
        expect_error(sock, "SHOW COMPAT OBJECTS", "42601",
                     "postgresql18 fake-object introspection")

        # No compatibility records were written: the store must not exist in
        # any database directory of the work dir.
        for root, _dirs, files in os.walk(work_dir):
            if ".pg_compat_objects" in files:
                raise AssertionError(
                    "compat object store created in %s despite 0A000 gate" % root)
        expect_error(sock, "SHOW COMPAT OBJECTS", "42601",
                     "removed PostgreSQL-mode compat catalog")

        # Extended mode does not make unimplemented PostgreSQL objects real.
        expect_command_tag(sock, "SET compatibility_mode = extended",
                           "SET compatibility_mode")
        messages = simple_query(sock, "SHOW dbms.extensions")
        assert any(kind == b"D" and b"project_sql_extensions" in body
                   for kind, body in messages), messages
        assert all(b"compat_object_record_layer" not in body
                   for _kind, body in messages)
        expect_0a000(sock, "SHOW COMPAT OBJECTS",
                     "removed extended-mode compat catalog")
        expect_0a000(sock, "CREATE EXTENSION hstore",
                     "extended CREATE EXTENSION")
        expect_0a000(sock,
                     "IMPORT FOREIGN SCHEMA fs FROM SERVER s1 INTO public",
                     "extended IMPORT FOREIGN SCHEMA")
        expect_0a000(sock, "LOAD 'auto_explain'", "extended LOAD")
        expect_0a000(sock, "SHOW COMPAT OBJECTS",
                     "extended fake-object introspection")
        # DIV-08 remains permanently unsupported in extended mode too.  Test
        # create/alter/drop independently so a future generic compat runtime
        # cannot accidentally revive a fake assertion catalog entry.
        for sql in [
                "CREATE ASSERTION extended_a CHECK (1 = 1)",
                "ALTER ASSERTION extended_a RENAME TO extended_b",
                "DROP ASSERTION IF EXISTS extended_a",
        ]:
            expect_0a000(sock, sql, "extended DIV-08 assertion")
        for root, _dirs, files in os.walk(work_dir):
            assert ".pg_compat_objects" not in files, \
                "extended mode wrote a fake compat object in %s" % root

        # DIV-01: the project database switch is allowed only from a clean
        # session context. Every rejected attempt keeps the current database.
        expect_error(sock, "USE DATABASE", "42601", "missing USE target")
        expect_error(sock, "USE DATABASE info trailing", "42601",
                     "trailing USE input")
        expect_error(sock, "USE DATABASE missing_db", "3D000",
                     "missing USE database")
        assert data_row_values(simple_query(sock, "SELECT current_database()")) \
            == [[b"info"]]

        expect_command_tag(sock, "BEGIN", "USE transaction")
        expect_error(sock, "USE DATABASE other_db", "25001",
                     "transactional USE")
        expect_command_tag(sock, "ROLLBACK", "USE transaction rollback")

        expect_command_tag(sock, "CREATE TEMP TABLE use_temp (id INTEGER)",
                           "USE temporary table blocker")
        expect_error(sock, "USE DATABASE other_db", "55006",
                     "temporary table USE")
        expect_command_tag(sock, "DROP TABLE use_temp", "drop USE temp table")

        expect_command_tag(sock, "LISTEN use_channel", "USE LISTEN blocker")
        expect_error(sock, "USE DATABASE other_db", "55006", "LISTEN USE")
        expect_command_tag(sock, "UNLISTEN use_channel", "clear USE LISTEN")

        portal_parse = b"use_stmt\0SELECT 1\0" + struct.pack("!H", 0)
        sock.sendall(typed(b"P", portal_parse))
        assert read_message(sock) == (b"1", b"")
        portal_bind = (b"use_portal\0use_stmt\0" + struct.pack("!H", 0) +
                       struct.pack("!H", 0) + struct.pack("!H", 0))
        sock.sendall(typed(b"B", portal_bind))
        assert read_message(sock) == (b"2", b"")
        expect_error(sock, "USE DATABASE other_db", "55006", "portal USE")
        sock.sendall(typed(b"C", b"Puse_portal\0") +
                     typed(b"C", b"Suse_stmt\0") + typed(b"S"))
        closed = read_until_ready(sock)
        assert sum(kind == b"3" for kind, _ in closed) == 2, closed

        # Catalog-dependent state must not cross a successful switch. A
        # currval/prepared name established in info is unavailable in other_db.
        expect_command_tag(sock, "CREATE SEQUENCE use_seq", "source USE sequence")
        assert data_row_values(simple_query(sock, "SELECT nextval('use_seq')")) \
            == [[b"1"]]
        expect_command_tag(sock, "PREPARE use_prepared FROM 'SELECT 1'",
                           "source USE prepared statement")
        expect_command_tag(sock, "USE DATABASE other_db", "extended USE switch")
        assert data_row_values(simple_query(sock, "SELECT current_database()")) \
            == [[b"other_db"]]
        expect_command_tag(sock, "CREATE SEQUENCE use_seq", "target USE sequence")
        assert error_of(simple_query(sock, "SELECT currval('use_seq')")) is not None
        assert error_of(simple_query(sock, "EXECUTE use_prepared")) is not None
        expect_command_tag(sock, "DROP SEQUENCE use_seq", "drop target USE sequence")
        expect_command_tag(sock, "USE DATABASE info", "extended USE return")
        expect_command_tag(sock, "DROP SEQUENCE use_seq", "drop source USE sequence")

        # DIV-05: project meta commands return typed catalog-style rows. In
        # particular, defaults containing spaces and SQL NULL stay intact.
        expect_command_tag(
            sock,
            "CREATE TABLE div5_meta (id INTEGER PRIMARY KEY, "
            "note TEXT DEFAULT 'hello world')",
            "DIV-05 metadata table")
        expect_command_tag(sock, "CREATE ROLE div5_role", "DIV-05 role")
        expect_command_tag(
            sock, "CREATE USER div5_user WITH PASSWORD 'Div5Pass9!'",
            "DIV-05 non-admin user")
        expect_command_tag(sock, "CREATE TABLE div5_visible (id INTEGER)",
                           "DIV-05 visible table")
        expect_command_tag(sock, "GRANT SELECT ON div5_visible TO div5_user",
                           "DIV-05 visible table grant")
        desc_messages = simple_query(sock, "DESC div5_meta")
        desc_fields = row_description_fields(desc_messages)
        assert [field[0] for field in desc_fields] == [
            b"field", b"type", b"null", b"key", b"default", b"extra"]
        assert [field[3] for field in desc_fields] == [25] * 6, desc_fields
        desc_rows = data_row_values(desc_messages)
        assert len(desc_rows) == 2 and desc_rows[0][0] == b"id", desc_rows
        assert desc_rows[0][4] is None, desc_rows
        assert desc_rows[1][0] == b"note" and \
            desc_rows[1][4] == b"'hello world'", desc_rows
        view_table_messages = simple_query(sock, "VIEW TABLE div5_meta")
        assert data_row_values(view_table_messages) == desc_rows
        view_database_messages = simple_query(sock, "VIEW DATABASE")
        assert [field[0] for field in row_description_fields(
            view_database_messages)] == [b"table_name"]
        assert [b"div5_meta"] in data_row_values(view_database_messages)
        users_messages = simple_query(sock, "SHOW USERS")
        assert [field[3] for field in row_description_fields(users_messages)] \
            == [25, 23]
        assert [b"alice", b"1"] in data_row_values(users_messages)
        roles_messages = simple_query(sock, "SHOW ROLES")
        assert row_description_fields(roles_messages)[0][3] == 25
        assert [b"div5_role"] in data_row_values(roles_messages)
        pools_messages = simple_query(sock, "SHOW POOLS")
        assert [field[3] for field in row_description_fields(pools_messages)] \
            == [25] + [20] * 8
        assert len(data_row_values(pools_messages)) == 1

        viewer = socket.socket()
        viewer.settimeout(SOCKET_TIMEOUT)
        viewer.connect(("127.0.0.1", port))
        startup(viewer, "div5_user", "info", password="Div5Pass9!")
        expect_command_tag(viewer, "SET compatibility_mode = extended",
                           "non-admin extended mode")
        viewer_tables = data_row_values(simple_query(viewer, "VIEW DATABASE"))
        assert [b"div5_visible"] in viewer_tables, viewer_tables
        assert [b"div5_meta"] not in viewer_tables, viewer_tables
        assert error_of(simple_query(viewer, "DESC div5_meta")) is not None
        assert data_row_values(simple_query(viewer, "DESC div5_visible"))[0][0] \
            == b"id"
        # Role metadata is public, like pg_roles, but no secret fields exist.
        assert [b"div5_user", b"0"] in data_row_values(
            simple_query(viewer, "SHOW USERS"))
        viewer.sendall(typed(b"X"))
        viewer.close()

        # DIV-09: extended SQL is only a typed wrapper over the logical slot
        # runtime. Physical slots remain gated until physical replication is
        # real, and durable slot mutations cannot escape transaction rollback.
        expect_0a000(sock, "CREATE REPLICATION SLOT div9_physical PHYSICAL",
                     "fake physical replication slot")
        expect_command_tag(sock, "BEGIN", "transactional slot create")
        expect_error(
            sock,
            "CREATE REPLICATION SLOT div9_tx LOGICAL dbms_test_decoding",
            "25001", "transactional slot create")
        expect_command_tag(sock, "ROLLBACK", "transactional slot rollback")
        create_slot = simple_query(
            sock,
            "CREATE REPLICATION SLOT div9_slot LOGICAL dbms_test_decoding")
        assert [field[3] for field in row_description_fields(create_slot)] \
            == [25, 20]
        assert data_row_values(create_slot) == [[b"div9_slot", b"0"]]
        expect_command_tag(sock, "CREATE TABLE div9_data (id INTEGER)",
                           "logical slot source table")
        expect_command_tag(
            sock, "CREATE PUBLICATION div9_pub FOR TABLE div9_data",
            "logical slot source publication")
        expect_command_tag(sock, "INSERT INTO div9_data VALUES (9)",
                           "logical slot source change")
        slot_messages = simple_query(sock, "SHOW REPLICATION SLOTS")
        assert [field[3] for field in row_description_fields(slot_messages)] \
            == [25, 25, 25, 25, 20, 16, 16, 20]
        slot_rows = data_row_values(slot_messages)
        div9_row = next(row for row in slot_rows if row[0] == b"div9_slot")
        assert div9_row[1:4] == [b"logical", b"dbms_test_decoding", b"info"]
        assert div9_row[7] == b"1", div9_row
        change_messages = simple_query(
            sock, "SHOW LOGICAL CHANGES FOR SLOT div9_slot")
        assert row_description_fields(change_messages)[0][3] == 25
        change_rows = data_row_values(change_messages)
        assert len(change_rows) == 1 and \
            b"table div9_data: INSERT:" in change_rows[0][0], change_rows
        expect_command_tag(sock, "BEGIN", "transactional slot confirm")
        expect_error(sock, "SHOW LOGICAL CONFIRM FOR SLOT div9_slot", "25001",
                     "transactional slot confirm")
        expect_command_tag(sock, "ROLLBACK", "slot confirm rollback")
        confirm_messages = simple_query(
            sock, "SHOW LOGICAL CONFIRM FOR SLOT div9_slot")
        assert row_description_fields(confirm_messages)[0][3] == 20
        assert len(data_row_values(confirm_messages)) == 1
        assert data_row_values(simple_query(
            sock, "SHOW LOGICAL CHANGES FOR SLOT div9_slot")) == []
        expect_command_tag(sock, "BEGIN", "transactional slot drop")
        expect_error(sock, "DROP REPLICATION SLOT div9_slot", "25001",
                     "transactional slot drop")
        expect_command_tag(sock, "ROLLBACK", "slot drop rollback")
        expect_command_tag(sock, "DROP REPLICATION SLOT div9_slot",
                           "drop logical slot")
        expect_command_tag(sock, "DROP PUBLICATION div9_pub",
                           "drop logical slot publication")
        assert all(row[0] != b"div9_slot" for row in data_row_values(
            simple_query(sock, "SHOW REPLICATION SLOTS")))

        # DIV-11 in extended mode: project commands work again.
        assert setting_value(simple_query(sock, "SELECT * FROM pg_settings"),
                             "auto_vacuum") == b"on"
        expect_command_tag(sock, "SET GLOBAL auto_vacuum = off",
                           "extended SET GLOBAL")
        expect_command_tag(sock, "SET GLOBAL auto_analyze = off",
                           "second extended SET GLOBAL")
        # ALTER SYSTEM semantics persist without changing the live value.
        assert setting_value(simple_query(sock, "SELECT * FROM pg_settings"),
                             "auto_vacuum") == b"on"
        persisted = Path(work_dir, "dbms.conf").read_text(encoding="utf-8")
        assert "auto_vacuum=off\n" in persisted, persisted
        assert "auto_analyze=off\n" in persisted, persisted
        assert error_of(simple_query(sock, "SHOW VARIABLES")) is None
        reload_messages = simple_query(sock, "SELECT pg_reload_conf()")
        assert data_row_values(reload_messages) == [[b"t"]], reload_messages
        assert setting_value(simple_query(sock, "SELECT * FROM pg_settings"),
                             "auto_vacuum") == b"off"
        expect_command_tag(sock, "SET @project_value = 7",
                           "extended user variable")
        variable_messages = simple_query(sock, "SELECT @project_value")
        assert any(kind == b"D" and b"7" in body
                   for kind, body in variable_messages), variable_messages
        for sql in ["SELECT '@project_value'", "SELECT $$@project_value$$"]:
            literal_messages = simple_query(sock, sql)
            assert any(kind == b"D" and b"@project_value" in body
                       for kind, body in literal_messages), literal_messages
        expect_command_tag(sock, "BEGIN", "extended SET GLOBAL transaction")
        expect_error(sock, "SET GLOBAL auto_vacuum = on", "25001",
                     "transactional SET GLOBAL")
        expect_command_tag(sock, "ROLLBACK", "extended SET GLOBAL rollback")
        expect_command_tag(sock, "BEGIN", "ALTER SYSTEM transaction")
        expect_error(sock, "ALTER SYSTEM SET auto_vacuum = on", "25001",
                     "transactional ALTER SYSTEM")
        expect_command_tag(sock, "ROLLBACK", "ALTER SYSTEM rollback")
        # DIV-06 in extended mode: alias mapping keeps working.
        expect_command_tag(
            sock,
            "CREATE TABLE t6e (id INTEGER AUTO_INCREMENT PRIMARY KEY, "
            "u INTEGER UNSIGNED)",
            "extended AUTO_INCREMENT/UNSIGNED")
        expect_command_tag(sock, "INSERT INTO t6e (u) VALUES (1)",
                           "extended auto-increment insert")
        assert data_row_values(simple_query(sock, "SELECT id, u FROM t6e")) \
            == [[b"1", b"1"]]
        assert error_of(simple_query(sock, "INSERT INTO t6e (u) VALUES (-1)")) \
            is not None, "extended UNSIGNED must reject negative input"
        expect_command_tag(
            sock, "ALTER TABLE t6e ADD COLUMN later INTEGER UNSIGNED",
            "extended ALTER TABLE UNSIGNED")
        expect_command_tag(
            sock,
            "CREATE TABLE t6aliases (a TINYINT, b DATETIME, c NVARCHAR(4), "
            "d LONG, e BLOB, f NCHAR(4), g BINARY(4), h VARBINARY(4))",
            "extended type aliases")

        # DIV-07 extended mode: shortcut syntax must use the same named-index
        # metadata and physical lifecycle as canonical CREATE/DROP INDEX.
        expect_command_tag(sock, "CREATE TABLE t7e (id INTEGER, body TEXT)",
                           "extended shortcut-index table")
        expect_command_tag(sock, "INSERT INTO t7e VALUES (1, 'hello world')",
                           "extended shortcut-index row")
        expect_command_tag(sock, "CREATE HASH INDEX id_hash ON t7e (id)",
                           "extended HASH shortcut")
        expect_command_tag(sock, "DROP INDEX id_hash",
                           "canonical drop of HASH shortcut")
        expect_command_tag(
            sock, "CREATE FULLTEXT INDEX body_ft ON t7e (body)",
            "extended FULLTEXT shortcut")
        expect_command_tag(sock, "DROP INDEX body_ft",
                           "canonical drop of FULLTEXT shortcut")
        expect_command_tag(
            sock, "CREATE FULLTEXT INDEX body_ft2 ON t7e (body)",
            "second extended FULLTEXT shortcut")
        expect_command_tag(sock, "DROP FULLTEXT INDEX body_ft2 ON t7e",
                           "shortcut drop of FULLTEXT shortcut")
        expect_error(
            sock,
            "SELECT id, body FROM t7e INTO OUTFILE 'forbidden_outfile.csv'",
            "42601", "extended INTO OUTFILE gate", "COPY")
        assert not os.path.exists(os.path.join(work_dir, "forbidden_outfile.csv"))
        expect_command_tag(sock, "COPY t7e TO 'canonical_export.csv'",
                           "canonical COPY TO export")
        with open(os.path.join(work_dir, "canonical_export.csv"),
                  encoding="utf-8") as exported:
            assert exported.read() == "id,body\n1,hello world\n"

        # DIV-02 extended mode: delete every row conflicting with any key,
        # including composite primary keys, and roll the whole multi-row
        # statement back if a later replacement is invalid.
        expect_command_tag(
            sock,
            "CREATE TABLE replace_t (a INTEGER, b INTEGER, email TEXT UNIQUE, "
            "payload TEXT, PRIMARY KEY (a, b))",
            "extended REPLACE table")
        expect_command_tag(
            sock,
            "INSERT INTO replace_t VALUES "
            "(1, 2, 'first@example', 'old'), "
            "(3, 4, 'second@example', 'keep')",
            "extended REPLACE seed")
        expect_command_tag(
            sock,
            "REPLACE INTO replace_t VALUES "
            "(1, 2, 'second@example', 'replacement')",
            "extended composite/multiple-conflict REPLACE")
        assert data_row_values(simple_query(
            sock, "SELECT a, b, email, payload FROM replace_t")) == [
                [b"1", b"2", b"second@example", b"replacement"]]
        expect_error(
            sock,
            "REPLACE INTO replace_t VALUES "
            "(1, 2, 'second@example', 'must_rollback'), "
            "('not_an_integer', 9, 'bad@example', 'bad')",
            "XX000", "extended REPLACE statement rollback")
        assert data_row_values(simple_query(
            sock, "SELECT a, b, email, payload FROM replace_t")) == [
                [b"1", b"2", b"second@example", b"replacement"]]

        # DIV-03 extended mode is strictly COPY FROM syntax sugar. A malformed
        # later CSV row rolls back earlier rows instead of being skipped.
        load_ok = Path(work_dir, "load_ok.csv")
        load_ok.write_text("1,alpha\n2,beta\n", encoding="utf-8")
        load_bad = Path(work_dir, "load_bad.csv")
        load_bad.write_text("3,gamma\nmalformed_only_one_field\n",
                            encoding="utf-8")
        expect_command_tag(sock, "CREATE TABLE load_t (id INTEGER, v TEXT)",
                           "extended LOAD table")
        expect_command_tag(
            sock, "LOAD DATA INFILE 'load_ok.csv' INTO TABLE load_t",
            "extended LOAD through COPY")
        assert data_row_values(simple_query(
            sock, "SELECT id, v FROM load_t ORDER BY id")) == [
                [b"1", b"alpha"], [b"2", b"beta"]]
        expect_error(
            sock, "LOAD DATA INFILE 'load_bad.csv' INTO TABLE load_t",
            "22P04", "extended LOAD atomic bad CSV")
        assert data_row_values(simple_query(
            sock, "SELECT id, v FROM load_t ORDER BY id")) == [
                [b"1", b"alpha"], [b"2", b"beta"]]

        # A malformed publication sidecar must fail the whole catalog scan;
        # SHOW must not silently hide it or expose a partial snapshot.
        corrupt_publication = Path(work_dir, "info", "corrupt.publication")
        corrupt_publication.write_text(
            "admin 1 1 1 1 1 trailing\norders\n", encoding="utf-8")
        err = error_of(simple_query(sock, "SHOW PUBLICATIONS"))
        assert err is not None and "invalid publication file" in err[1], \
            "corrupt publication catalog must fail closed: %r" % (err,)
        corrupt_publication.unlink()

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
