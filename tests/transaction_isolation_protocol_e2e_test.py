#!/usr/bin/env python3
"""SET TRANSACTION rejects isolation changes after snapshot use."""

import importlib.util
import tempfile
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "transaction_isolation_pgdiff",
        root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    copy_file = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", delete=False,
        prefix="dbms_readonly_copy_", suffix=".csv")
    copy_file.write("2\n")
    copy_file.close()
    copy_path = Path(copy_file.name)
    server = runner.start_ours(client)
    try:
        def execute(sql):
            return runner.decode_wire_result(
                client.simple_query(server["sock"], sql), include_types=True)

        rows, state, message, _, _, _ = execute(
            "CREATE TABLE isolation_guard (id INT);")
        assert state is None and rows == [], (state, message, rows)
        rows, state, message, _, _, _ = execute(
            "INSERT INTO isolation_guard VALUES (1);")
        assert state is None and rows == [], (state, message, rows)
        _, state, message, _, _, _ = execute(
            "CREATE SEQUENCE isolation_sequence START WITH 10;")
        assert state is None, (state, message)

        # Sequence allocation is non-transactional, but PostgreSQL still
        # prohibits nextval/setval in a read-only transaction.  A rejected
        # call must not consume a durable value.
        _, state, message, _, _, _ = execute("BEGIN READ ONLY;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "SELECT nextval('isolation_sequence');")
        assert state == "25006", (state, message)
        assert "nextval()" in message, message
        _, state, message, _, _, _ = execute("ROLLBACK;")
        assert state is None, (state, message)
        rows, state, message, _, _, _ = execute(
            "SELECT nextval('isolation_sequence');")
        assert state is None and rows == [["10"]], (state, message, rows)

        _, state, message, _, _, _ = execute("BEGIN;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;")
        assert state is None, (state, message)
        rows, state, message, _, _, _ = execute(
            "SELECT id FROM isolation_guard;")
        assert state is None and rows == [["1"]], (state, message, rows)

        _, state, message, _, _, _ = execute(
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
        assert state == "25001", (state, message)
        assert "before any query" in message, message
        _, state, message, _, _, _ = execute("ROLLBACK;")
        assert state is None, (state, message)

        # Utility writes are subject to the same read-only transaction gate.
        # Reject them before DDL can create files or implicitly end the
        # transaction, and keep the backend failed until explicit recovery.
        _, state, message, _, _, _ = execute("BEGIN READ ONLY;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "CREATE TABLE readonly_ddl_leak (id INT);")
        assert state == "25006", (state, message)
        assert "read-only transaction" in message, message
        _, state, message, _, _, _ = execute("SELECT 1;")
        assert state == "25P02", (state, message)
        _, state, message, _, _, _ = execute("ROLLBACK;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "SELECT id FROM readonly_ddl_leak;")
        assert state == "42P01", (state, message)

        # ALTER and TRUNCATE are transactional in PostgreSQL.  They must join
        # the existing transaction instead of committing earlier DML and
        # starting a detached DDL transaction.
        _, state, message, _, _, _ = execute(
            "CREATE TABLE transactional_ddl (id INT);")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "INSERT INTO transactional_ddl VALUES (1);")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute("BEGIN;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "INSERT INTO transactional_ddl VALUES (2);")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "ALTER TABLE transactional_ddl ADD COLUMN rolled_back INT;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute("ROLLBACK;")
        assert state is None, (state, message)
        rows, state, message, headers, _, _ = execute(
            "SELECT * FROM transactional_ddl ORDER BY id;")
        assert state is None and rows == [["1"]], (state, message, rows)
        assert headers == ["id"], headers

        # Every later file-rewriting statement needs its own atomicity image.
        # The first added nullable column also verifies that a subsequent
        # rewrite preserves SQL NULL instead of coercing it to an empty value.
        _, state, message, _, _, _ = execute("BEGIN;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "ALTER TABLE transactional_ddl ADD COLUMN first_change INT;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "ALTER TABLE transactional_ddl ADD COLUMN second_change INT;")
        assert state is None, (state, message)
        rows, state, message, headers, _, _ = execute(
            "SELECT * FROM transactional_ddl ORDER BY id;")
        assert state is None and rows == [["1", None, None]], (
            state, message, rows)
        assert headers == ["id", "first_change", "second_change"], headers
        _, state, message, _, _, _ = execute("ROLLBACK;")
        assert state is None, (state, message)
        rows, state, message, headers, _, _ = execute(
            "SELECT * FROM transactional_ddl ORDER BY id;")
        assert state is None and rows == [["1"]], (state, message, rows)
        assert headers == ["id"], headers

        # A savepoint before the first physical DDL combines the transaction
        # image with row undo, so both intervening DML and schema changes go.
        _, state, message, _, _, _ = execute("BEGIN;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute("SAVEPOINT before_rewrite;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "INSERT INTO transactional_ddl VALUES (2);")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "ALTER TABLE transactional_ddl ADD COLUMN discarded_value INT;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "ROLLBACK TO SAVEPOINT before_rewrite;")
        assert state is None, (state, message)
        rows, state, message, headers, _, _ = execute(
            "SELECT * FROM transactional_ddl ORDER BY id;")
        assert state is None and rows == [["1"]], (state, message, rows)
        assert headers == ["id"], headers
        _, state, message, _, _, _ = execute("COMMIT;")
        assert state is None, (state, message)

        # A savepoint after physical DDL retains earlier schema changes while
        # rolling back later rewrites. RELEASE drops its auxiliary image.
        _, state, message, _, _, _ = execute("BEGIN;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "ALTER TABLE transactional_ddl ADD COLUMN retained_value INT;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute("SAVEPOINT after_rewrite;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "ALTER TABLE transactional_ddl ADD COLUMN discarded_later INT;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "ROLLBACK TO SAVEPOINT after_rewrite;")
        assert state is None, (state, message)
        rows, state, message, headers, _, _ = execute(
            "SELECT * FROM transactional_ddl ORDER BY id;")
        assert state is None and rows == [["1", None]], (
            state, message, rows)
        assert headers == ["id", "retained_value"], headers
        _, state, message, _, _, _ = execute("RELEASE SAVEPOINT after_rewrite;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute("ROLLBACK;")
        assert state is None, (state, message)

        # A row written after the DDL snapshot is removed by snapshot restore,
        # not replayed against the restored older schema.
        _, state, message, _, _, _ = execute("BEGIN;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "ALTER TABLE transactional_ddl ADD COLUMN later_value INT;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "INSERT INTO transactional_ddl VALUES (4, 9);")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute("ROLLBACK;")
        assert state is None, (state, message)
        rows, state, message, headers, _, _ = execute(
            "SELECT * FROM transactional_ddl ORDER BY id;")
        assert state is None and rows == [["1"]], (state, message, rows)
        assert headers == ["id"], headers

        _, state, message, _, _, _ = execute("BEGIN;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "INSERT INTO transactional_ddl VALUES (3);")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute("TRUNCATE transactional_ddl;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute("ROLLBACK;")
        assert state is None, (state, message)
        rows, state, message, headers, _, _ = execute(
            "SELECT * FROM transactional_ddl ORDER BY id;")
        assert state is None and rows == [["1"]], (state, message, rows)
        assert headers == ["id"], headers

        # PostgreSQL does not implement CREATE/DROP DATABASE by committing an
        # open transaction. Both commands are prohibited in a transaction
        # block and leave the protocol transaction failed until ROLLBACK.
        _, state, message, _, _, _ = execute("BEGIN;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "CREATE DATABASE forbidden_transaction_database;")
        assert state == "25001", (state, message)
        assert "cannot run inside a transaction block" in message, message
        _, state, message, _, _, _ = execute("SELECT 1;")
        assert state == "25P02", (state, message)
        _, state, message, _, _, _ = execute("ROLLBACK;")
        assert state is None, (state, message)

        _, state, message, _, _, _ = execute(
            "CREATE DATABASE forbidden_transaction_database;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute("BEGIN;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "DROP DATABASE forbidden_transaction_database;")
        assert state == "25001", (state, message)
        _, state, message, _, _, _ = execute("ROLLBACK;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "DROP DATABASE forbidden_transaction_database;")
        assert state is None, (state, message)

        # COPY FROM is a table write. A read-only transaction must reject the
        # command instead of converting the storage error into skipped rows.
        _, state, message, _, _, _ = execute(
            "CREATE TABLE readonly_copy_target (id INT);")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute("BEGIN READ ONLY;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            f"COPY readonly_copy_target FROM '{copy_path}';")
        assert state == "25006", (state, message)
        assert "COPY FROM" in message, message
        _, state, message, _, _, _ = execute("ROLLBACK;")
        assert state is None, (state, message)
        rows, state, message, _, _, _ = execute(
            "SELECT id FROM readonly_copy_target;")
        assert state is None and rows == [], (state, message, rows)

        # PostgreSQL's read-only exception for an existing temporary table
        # applies to COPY FROM just as it does to INSERT/UPDATE/DELETE.
        _, state, message, _, _, _ = execute(
            "CREATE TEMP TABLE readonly_copy_temp (id INT);")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute("BEGIN READ ONLY;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            f"COPY readonly_copy_temp FROM '{copy_path}';")
        assert state is None, (state, message)
        rows, state, message, _, _, _ = execute(
            "SELECT id FROM readonly_copy_temp;")
        assert state is None and rows == [["2"]], (state, message, rows)
        _, state, message, _, _, _ = execute("ROLLBACK;")
        assert state is None, (state, message)
        rows, state, message, _, _, _ = execute(
            "SELECT id FROM readonly_copy_temp;")
        assert state is None and rows == [], (state, message, rows)

        # COPY FROM is one statement, not a series of independently committed
        # INSERTs. A conversion failure must roll back earlier input rows and
        # report the error instead of silently counting it as skipped.
        copy_path.write_text("10\nnot_an_integer\n11\n", encoding="utf-8")
        _, state, message, _, _, _ = execute(
            "CREATE TABLE atomic_copy_target (id INT);")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            f"COPY atomic_copy_target FROM '{copy_path}';")
        assert state == "22023", (state, message)
        assert "COPY FROM failed" in message, message
        rows, state, message, _, _, _ = execute(
            "SELECT id FROM atomic_copy_target ORDER BY id;")
        assert state is None and rows == [], (state, message, rows)

        _, state, message, _, _, _ = execute("BEGIN;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "INSERT INTO atomic_copy_target VALUES (5);")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute("SAVEPOINT before_copy;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            f"COPY atomic_copy_target FROM '{copy_path}';")
        assert state == "22023", (state, message)
        _, state, message, _, _, _ = execute(
            "ROLLBACK TO SAVEPOINT before_copy;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute("COMMIT;")
        assert state is None, (state, message)
        rows, state, message, _, _, _ = execute(
            "SELECT id FROM atomic_copy_target ORDER BY id;")
        assert state is None and rows == [["5"]], (state, message, rows)

        # Without HEADER or ON_ERROR, a malformed field count is a COPY file
        # format error. The first bad line is not an implicit header.
        copy_path.write_text("1\n2,ok\n", encoding="utf-8")
        _, state, message, _, _, _ = execute(
            "CREATE TABLE shaped_copy_target (id INT, label TEXT);")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            f"COPY shaped_copy_target FROM '{copy_path}';")
        assert state == "22P04", (state, message)
        rows, state, message, _, _, _ = execute(
            "SELECT id FROM shaped_copy_target;")
        assert state is None and rows == [], (state, message, rows)

        # CSV field contents are data. COPY must not trim spaces from quoted
        # or unquoted text values while constructing INSERT input.
        copy_path.write_text(
            '1,"  quoted edge  "\n2,unquoted edge  \n', encoding="utf-8")
        _, state, message, _, _, _ = execute(
            "CREATE TABLE text_copy_target (id INT, value TEXT);")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            f"COPY text_copy_target FROM '{copy_path}';")
        assert state is None, (state, message)
        rows, state, message, _, _, _ = execute(
            "SELECT value FROM text_copy_target ORDER BY id;")
        assert state is None and rows == [
            ["  quoted edge  "], ["unquoted edge  "]
        ], (state, message, rows)

        # SET TRANSACTION read modes are transaction characteristics, not
        # configuration parameters.  Tightening an active transaction to
        # READ ONLY is allowed even after a read, and every subsequent DML
        # command must fail with PostgreSQL's read_only_sql_transaction code.
        _, state, message, _, _, _ = execute("BEGIN;")
        assert state is None, (state, message)
        rows, state, message, _, _, _ = execute(
            "SELECT id FROM isolation_guard;")
        assert state is None and rows == [["1"]], (state, message, rows)
        _, state, message, _, _, _ = execute("SET TRANSACTION READ ONLY;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "INSERT INTO isolation_guard VALUES (2);")
        assert state == "25006", (state, message)
        assert "read-only transaction" in message, message
        _, state, message, _, _, _ = execute("ROLLBACK;")
        assert state is None, (state, message)

        # Relaxing READ ONLY after the transaction has taken a snapshot is
        # forbidden, while doing so before the first query restores writes.
        _, state, message, _, _, _ = execute("BEGIN READ ONLY;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute("SET TRANSACTION READ WRITE;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "INSERT INTO isolation_guard VALUES (2);")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute("ROLLBACK;")
        assert state is None, (state, message)

        _, state, message, _, _, _ = execute(
            "CREATE TEMP TABLE isolation_temp (id INT);")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute("BEGIN READ ONLY;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "INSERT INTO isolation_temp VALUES (9);")
        assert state is None, (state, message)
        rows, state, message, _, _, _ = execute(
            "SELECT id FROM isolation_temp;")
        assert state is None and rows == [["9"]], (state, message, rows)
        _, state, message, _, _, _ = execute("COMMIT;")
        assert state is None, (state, message)

        _, state, message, _, _, _ = execute("BEGIN READ ONLY;")
        assert state is None, (state, message)
        rows, state, message, _, _, _ = execute(
            "SELECT id FROM isolation_guard;")
        assert state is None and rows == [["1"]], (state, message, rows)
        _, state, message, _, _, _ = execute("SET TRANSACTION READ WRITE;")
        assert state == "25001", (state, message)
        assert "before any query" in message, message
        _, state, message, _, _, _ = execute("ROLLBACK;")
        assert state is None, (state, message)

        _, state, message, _, _, _ = execute("BEGIN;")
        assert state is None, (state, message)
        rows, state, message, _, _, _ = execute(
            "SELECT id FROM isolation_guard;")
        assert state is None and rows == [["1"]], (state, message, rows)
        _, state, message, _, _, _ = execute(
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
        assert state == "25001", (state, message)
        _, state, message, _, _, _ = execute(
            "/* not local recovery */ ROLLBACK PREPARED 'missing';")
        assert state == "25P02", (state, message)
        _, state, message, _, _, _ = execute(
            "ROLLBACK /* comment separates keywords */ PREPARED 'missing';")
        assert state == "25P02", (state, message)
        _, state, message, _, _, _ = execute(
            "-- leading recovery comment\n"
            "/* outer /* nested */ comment */ ROLLBACK;")
        assert state is None, (state, message)

        # Comment contents are trivia, not transaction-chain options.  A
        # failed transaction ended by this COMMIT must return to idle.
        _, state, message, _, _, _ = execute("BEGIN;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute("SELECT 1 / 0;")
        assert state == "22012", (state, message)
        commit_messages = client.simple_query(
            server["sock"], "COMMIT /* and chain */;")
        _, state, message, _, _, _ = runner.decode_wire_result(
            commit_messages, include_types=True)
        assert state is None, (state, message)
        ready = [payload for kind, payload in commit_messages if kind == b"Z"]
        assert ready == [b"I"], ready

        # Conversely, comments may separate real option tokens.  CHAIN must
        # start a replacement transaction after rolling the failed one back.
        _, state, message, _, _, _ = execute("BEGIN;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute("SELECT 1 / 0;")
        assert state == "22012", (state, message)
        chain_messages = client.simple_query(
            server["sock"], "COMMIT AND /* separator */ CHAIN;")
        _, state, message, _, _, _ = runner.decode_wire_result(
            chain_messages, include_types=True)
        assert state is None, (state, message)
        ready = [payload for kind, payload in chain_messages if kind == b"Z"]
        assert ready == [b"T"], ready
        _, state, message, _, _, _ = execute("ROLLBACK;")
        assert state is None, (state, message)
        print("[TRANSACTION ISOLATION PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)
        copy_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
