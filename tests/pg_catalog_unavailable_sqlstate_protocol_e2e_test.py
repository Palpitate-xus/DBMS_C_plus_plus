#!/usr/bin/env python3
"""Unimplemented pg_catalog relations must not masquerade as lock failures."""

import importlib.util
from pathlib import Path


UNIMPLEMENTED_CATALOGS = (
    "pg_constraint",
    "pg_am",
    "pg_opclass",
    "pg_cast",
    "pg_collation",
    "pg_rewrite",
    "pg_trigger",
    "pg_policy",
    "pg_authid",
    "pg_auth_members",
    "pg_default_acl",
    "pg_tablespace",
    "pg_namespace",
    "pg_stats",
    "pg_statistic",
    "pg_statistic_ext",
    "pg_subscription",
    "pg_publication",
    "pg_replication_origin",
    "pg_index",
    "pg_operator",
    "pg_type",
    "pg_enum",
    "pg_roles",
)


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "catalog_error_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    sock = server["sock"]
    try:
        for relation in UNIMPLEMENTED_CATALOGS:
            for name in (relation, "pg_catalog." + relation):
                messages = client.simple_query(
                    sock, "SELECT * FROM %s" % name)
                result = runner.decode_wire_result(messages, include_types=True)
                assert result[1] == "0A000", (name, result)
                assert messages[-1] == (b"Z", b"I"), (name, messages[-1])

        # pg_database exposes only its verified typed subset. The complete
        # PostgreSQL schema is not fabricated, so SELECT * and real but
        # unsupported columns fail closed. pg_stats/pg_statistic also fail
        # closed until their distinct PostgreSQL shapes have typed execution.
        messages = client.simple_query(
            sock, "SELECT datname, encoding FROM pg_catalog.pg_database "
            "WHERE datname = 'info'")
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] is None, result
        assert result[0] == [["info", "6"]], result
        assert result[3] == ["datname", "encoding"], result
        assert result[5] == [19, 23], result
        assert messages[-1] == (b"Z", b"I"), messages[-1]

        # An unqualified reference reaches the virtual catalog only while no
        # same-named user relation is present in the current database.
        messages = client.simple_query(
            sock, "SELECT encoding FROM pg_database WHERE datname = 'info'")
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] is None and result[0] == [["6"]], result
        assert result[3] == ["encoding"] and result[5] == [23], result
        assert messages[-1] == (b"Z", b"I"), messages[-1]

        messages = client.simple_query(
            sock, "SELECT count(*), count(NULL) FROM pg_catalog.pg_database")
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] is None and result[0] == [["1", "0"]], result
        assert result[3] == ["count", "count"] and result[5] == [20, 20], result
        assert messages[-1] == (b"Z", b"I"), messages[-1]

        for sql in (
                "SELECT * FROM pg_catalog.pg_database",
                "SELECT datcollate FROM pg_catalog.pg_database"):
            messages = client.simple_query(sock, sql)
            result = runner.decode_wire_result(messages, include_types=True)
            assert result[1] == "0A000", (sql, result)
            assert messages[-1] == (b"Z", b"I"), (sql, messages[-1])

        # A user relation with the same unqualified name still wins normal
        # lookup; only the absent unqualified catalog is gated.
        for sql in (
                "CREATE TABLE pg_constraint (id INTEGER)",
                "INSERT INTO pg_constraint VALUES (7)"):
            result = runner.decode_wire_result(
                client.simple_query(sock, sql), include_types=True)
            assert result[1] is None, (sql, result)
        messages = client.simple_query(sock, "SELECT * FROM pg_constraint")
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] is None and result[0] == [["7"]], result

        # Once a current-database table exists, the unqualified name follows
        # the project's normal same-name relation path rather than the
        # unsupported-catalog gate.
        for sql in (
                "CREATE TABLE pg_statistic (id INTEGER)",
                "INSERT INTO pg_statistic VALUES (9)"):
            result = runner.decode_wire_result(
                client.simple_query(sock, sql), include_types=True)
            assert result[1] is None, (sql, result)
        messages = client.simple_query(sock, "SELECT * FROM pg_statistic")
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] is None and result[0] == [["9"]], result
        assert result[3] == ["id"] and result[5] == [23], result

        for sql in (
                "CREATE TABLE pg_namespace (id INTEGER)",
                "INSERT INTO pg_namespace VALUES (11)"):
            result = runner.decode_wire_result(
                client.simple_query(sock, sql), include_types=True)
            assert result[1] is None, (sql, result)
        messages = client.simple_query(sock, "SELECT * FROM pg_namespace")
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] is None and result[0] == [["11"]], result
        assert result[3] == ["id"] and result[5] == [23], result

        for relation, value in (("pg_type", 13), ("pg_enum", 15),
                                ("pg_roles", 19)):
            for sql in (
                    "CREATE TABLE %s (id INTEGER)" % relation,
                    "INSERT INTO %s VALUES (%d)" % (relation, value)):
                result = runner.decode_wire_result(
                    client.simple_query(sock, sql), include_types=True)
                assert result[1] is None, (sql, result)
            messages = client.simple_query(
                sock, "SELECT * FROM %s" % relation)
            result = runner.decode_wire_result(messages, include_types=True)
            assert result[1] is None and result[0] == [[str(value)]], result
            assert result[3] == ["id"] and result[5] == [23], result

        # An ordinary view named after an unimplemented system view likewise
        # follows normal lookup rather than the unsupported-catalog gate.
        for sql in (
                "CREATE TABLE catalog_view_source (id INTEGER)",
                "INSERT INTO catalog_view_source VALUES (17)",
                "DROP TABLE pg_roles",
                "CREATE VIEW pg_roles AS SELECT id FROM catalog_view_source"):
            result = runner.decode_wire_result(
                client.simple_query(sock, sql), include_types=True)
            assert result[1] is None, (sql, result)
        messages = client.simple_query(sock, "SELECT * FROM pg_roles")
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] is None and result[0] == [["17"]], result
        assert result[3] == ["id"] and result[5] == [23], result

        # The explicit unsupported-catalog error is statement-scoped; it must
        # not poison a following query in the same connection.
        messages = client.simple_query(sock, "SELECT 1")
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] is None and result[0] == [["1"]], result
        assert messages[-1] == (b"Z", b"I"), messages[-1]
    finally:
        runner.stop_ours(server)

    print("[PG CATALOG SQLSTATE] unsupported pg_catalog relations fail closed")


if __name__ == "__main__":
    main()
