#!/usr/bin/env python3
"""Unimplemented pg_catalog relations must not masquerade as lock failures."""

import importlib.util
from pathlib import Path


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
        for relation in ("pg_index", "pg_operator"):
            for name in (relation, "pg_catalog." + relation):
                messages = client.simple_query(
                    sock, "SELECT * FROM %s" % name)
                result = runner.decode_wire_result(messages, include_types=True)
                assert result[1] == "0A000", (name, result)
                assert messages[-1] == (b"Z", b"I"), (name, messages[-1])

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
