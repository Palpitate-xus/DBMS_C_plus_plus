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
    "pg_statistic_ext",
    "pg_subscription",
    "pg_publication",
    "pg_replication_origin",
    "pg_index",
    "pg_operator",
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

        # Existing virtual relations are not part of the unsupported set.
        for relation in ("pg_database", "pg_statistic"):
            messages = client.simple_query(
                sock, "SELECT * FROM pg_catalog." + relation)
            result = runner.decode_wire_result(messages, include_types=True)
            assert result[1] is None, (relation, result)
            assert messages[-1] == (b"Z", b"I"), (relation, messages[-1])

        # A user relation with the same unqualified name still wins normal
        # search-path lookup; only the absent unqualified catalog is gated.
        for sql in (
                "CREATE TABLE pg_constraint (id INTEGER)",
                "INSERT INTO pg_constraint VALUES (7)"):
            result = runner.decode_wire_result(
                client.simple_query(sock, sql), include_types=True)
            assert result[1] is None, (sql, result)
        messages = client.simple_query(sock, "SELECT * FROM pg_constraint")
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] is None and result[0] == [["7"]], result

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
