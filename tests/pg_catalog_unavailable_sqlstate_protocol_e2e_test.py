#!/usr/bin/env python3
"""Unimplemented pg_catalog relations must not masquerade as lock failures."""

import importlib.util
from pathlib import Path
import struct


UNIMPLEMENTED_CATALOGS = (
    "pg_constraint",
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


def check_typed_query(client, sock, sql, columns, rows):
    simple = client.simple_query(sock, sql)
    parse = client.typed(b"P", b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0))
    sync = client.typed(b"S")
    sock.sendall(parse + client.typed(b"D", b"S\0") + sync)
    described = client.read_until_ready(sock)
    bind = client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0))
    execute = client.typed(b"E", b"\0" + struct.pack("!I", 0))
    sock.sendall(parse + bind + client.typed(b"D", b"P\0") + execute + sync)
    executed = client.read_until_ready(sock)

    data_frames = [b"D"] * len(rows)
    assert [kind for kind, _ in simple] == [b"T", *data_frames, b"C", b"Z"], (sql, simple)
    assert [kind for kind, _ in described] == [b"1", b"t", b"T", b"Z"], (sql, described)
    assert [kind for kind, _ in executed] == [b"1", b"2", b"T", *data_frames, b"C", b"Z"], (sql, executed)
    tag = (b"C", ("SELECT %d" % len(rows)).encode() + b"\0")
    assert simple[-2] == executed[-2] == tag, (sql, simple[-2], executed[-2])
    assert client.data_row_values(simple) == client.data_row_values(executed) == rows, sql
    fields = client.row_description_fields(simple)
    assert fields == client.row_description_fields(described) == client.row_description_fields(executed), sql
    assert all(field[1] != 0 for field in fields), (sql, fields)
    expected = [(name.encode(), i, oid, size, -1, 0)
                for i, (name, oid, size) in enumerate(columns, 1)]
    assert [(field[0], *field[2:]) for field in fields] == expected, (sql, fields)
    assert simple[-1] == described[-1] == executed[-1] == (b"Z", b"I"), sql
    return fields


def check_command(client, runner, sock, sql, tag=None, state=None):
    messages = client.simple_query(sock, sql)
    rows, error, _, _, actual_tag = runner.decode_wire_result(messages)
    assert (rows, error, actual_tag, messages[-1]) == ([], state, tag, (b"Z", b"I")), (sql, messages)


def check_extended_write_error(client, runner, sock, sql, state):
    parse = client.typed(b"P", b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0))
    bind = client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0))
    execute = client.typed(b"E", b"\0" + struct.pack("!I", 0))
    sock.sendall(parse + bind + execute + client.typed(b"S"))
    messages = client.read_until_ready(sock)
    rows, error, _, _, tag = runner.decode_wire_result(messages)
    assert (rows, error, tag, messages[-1]) == ([], state, None, (b"Z", b"I")), (sql, messages)


def check_pg_type_merge_returning(client, sock, sql, columns):
    name = b"pg_type_merge\0"
    parse = client.typed(b"P", name + sql.encode() + b"\0" + struct.pack("!H", 0))
    sync = client.typed(b"S")
    sock.sendall(parse + client.typed(b"D", b"S" + name) + sync)
    described = client.read_until_ready(sock)
    assert [kind for kind, _ in described] == [b"1", b"t", b"T", b"Z"], (sql, described)
    assert described[-1] == (b"Z", b"I"), (sql, described)
    fields = check_typed_query(client, sock, "SELECT * FROM public.pg_type", columns, [[b"13"]])
    assert client.row_description_fields(described) == fields, (sql, described, fields)

    bind = client.typed(b"B", b"\0" + name + struct.pack("!HHH", 0, 0, 0))
    execute = client.typed(b"E", b"\0" + struct.pack("!I", 0))
    sock.sendall(bind + client.typed(b"D", b"P\0") + execute + sync)
    executed = client.read_until_ready(sock)
    assert [kind for kind, _ in executed] == [b"2", b"T", b"D", b"C", b"Z"], (sql, executed)
    assert client.row_description_fields(executed) == fields, (sql, executed, fields)
    assert client.data_row_values(executed) == [[b"17"]], (sql, executed)
    assert executed[-2:] == [(b"C", b"MERGE 1\0"), (b"Z", b"I")], (sql, executed)


def check_stats_catalog(client, runner, sock):
    for sql, tag in (
            ("CREATE TABLE audit_catalog_stats (id INTEGER)", "CREATE TABLE"),
            ("INSERT INTO audit_catalog_stats VALUES (2), (2), (8)", "INSERT 0 3"),
            ("ANALYZE audit_catalog_stats", "ANALYZE")):
        check_command(client, runner, sock, sql, tag)

    # PostgreSQL 18.6 returns the same descriptor even when the filter has no rows.
    columns = (
        ("schemaname", 19, 64), ("tablename", 19, 64), ("attname", 19, 64),
        ("inherited", 16, 1), ("null_frac", 700, 4), ("avg_width", 23, 4),
        ("n_distinct", 700, 4), ("most_common_vals", 2277, -1),
        ("most_common_freqs", 1021, -1), ("histogram_bounds", 2277, -1),
        ("correlation", 700, 4), ("most_common_elems", 2277, -1),
        ("most_common_elem_freqs", 1021, -1), ("elem_count_histogram", 1021, -1),
        ("range_length_histogram", 2277, -1), ("range_empty_frac", 700, 4),
        ("range_bounds_histogram", 2277, -1),
    )
    for source in ("pg_stats", "pg_catalog.pg_stats"):
        check_typed_query(client, sock, "SELECT * FROM " + source + " WHERE FALSE", columns, [])
        check_typed_query(client, sock,
                          "SELECT schemaname, tablename, attname, inherited, null_frac, avg_width "
                          f"FROM {source} WHERE schemaname='public' AND tablename='audit_catalog_stats' "
                          "ORDER BY attname", columns[:6],
                          [[b"public", b"audit_catalog_stats", b"id", b"f", b"0", b"4"]])


def check_pg_type_catalog(client, runner, sock):
    builtin = "SELECT oid, typname FROM pg_type WHERE oid = 23"
    builtin_columns = (("oid", 26, 4), ("typname", 19, 64))
    builtin_row = [[b"23", b"int4"]]
    shadow_columns = (("id", 23, 4),)

    check_command(client, runner, sock, "RESET search_path", "RESET")
    check_command(client, runner, sock, "CREATE TABLE public.pg_type (id INTEGER)", "CREATE TABLE")
    check_typed_query(client, sock, builtin, builtin_columns, builtin_row)
    check_command(client, runner, sock, "SELECT * FROM pg_type", state="0A000")
    rejected = "INSERT INTO pg_type VALUES (912345)"
    check_command(client, runner, sock, rejected, state="0A000")
    check_extended_write_error(client, runner, sock, rejected, "0A000")
    check_typed_query(client, sock, "SELECT * FROM public.pg_type", shadow_columns, [])
    check_typed_query(client, sock, "SELECT oid, typname FROM pg_type WHERE oid = 912345", builtin_columns, [])

    check_command(client, runner, sock, "SET search_path = public, pg_catalog", "SET")
    check_command(client, runner, sock, "INSERT INTO pg_type VALUES (13)", "INSERT 0 1")
    check_typed_query(client, sock, "SELECT * FROM pg_type", shadow_columns, [[b"13"]])
    check_typed_query(client, sock, "SELECT oid, typname FROM pg_catalog.pg_type WHERE oid = 23",
                      builtin_columns, builtin_row)
    check_command(client, runner, sock, "SET search_path = pg_catalog, public", "SET")
    check_typed_query(client, sock, builtin, builtin_columns, builtin_row)
    check_typed_query(client, sock, "SELECT * FROM public.pg_type", shadow_columns, [[b"13"]])

    check_command(client, runner, sock, "RESET search_path", "RESET")
    check_command(client, runner, sock,
                  "CREATE TABLE public.pg_type_merge_source (id INTEGER, val INTEGER)", "CREATE TABLE")
    check_command(client, runner, sock,
                  "INSERT INTO public.pg_type_merge_source VALUES (13, 17)", "INSERT 0 1")
    merge = ("MERGE INTO pg_type AS dst USING public.pg_type_merge_source AS src "
             "ON dst.id = src.id WHEN MATCHED THEN UPDATE SET id = src.val")
    check_command(client, runner, sock, merge, state="0A000")
    check_extended_write_error(client, runner, sock, merge, "0A000")
    check_typed_query(client, sock, "SELECT * FROM public.pg_type", shadow_columns, [[b"13"]])
    check_command(client, runner, sock, "SET search_path = public, pg_catalog", "SET")
    check_pg_type_merge_returning(client, sock, merge + " RETURNING dst.id", shadow_columns)
    check_typed_query(client, sock, "SELECT * FROM public.pg_type", shadow_columns, [[b"17"]])
    check_command(client, runner, sock, "DROP TABLE public.pg_type_merge_source", "DROP TABLE")

    # Test the temporary shadow without a public table of the same name.
    check_command(client, runner, sock, "DROP TABLE public.pg_type", "DROP TABLE")
    check_command(client, runner, sock, "RESET search_path", "RESET")
    check_command(client, runner, sock, "CREATE TEMP TABLE pg_type (id INTEGER)", "CREATE TABLE")
    check_command(client, runner, sock, "INSERT INTO pg_temp.pg_type VALUES (19)", "INSERT 0 1")
    check_typed_query(client, sock, "SELECT * FROM pg_type", shadow_columns, [[b"19"]])
    check_command(client, runner, sock, "SET search_path = pg_catalog, pg_temp, public", "SET")
    check_typed_query(client, sock, builtin, builtin_columns, builtin_row)
    check_command(client, runner, sock, "SET search_path = pg_temp, pg_catalog, public", "SET")
    check_typed_query(client, sock, "SELECT * FROM pg_type", shadow_columns, [[b"19"]])
    check_command(client, runner, sock, "DROP TABLE pg_temp.pg_type", "DROP TABLE")
    check_command(client, runner, sock, "RESET search_path", "RESET")


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

        check_stats_catalog(client, runner, sock)

        # pg_database exposes only its verified typed subset. The complete
        # PostgreSQL schema is not fabricated, so SELECT * and real but
        # unsupported columns fail closed.
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

        check_pg_type_catalog(client, runner, sock)

        for relation, value in (("pg_enum", 15), ("pg_roles", 19)):
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

        # Early typed-catalog routing must not consume ordinary user shadows.
        for sql in (
                "CREATE TABLE pg_database (id INTEGER)",
                "INSERT INTO pg_database VALUES (23)"):
            result = runner.decode_wire_result(
                client.simple_query(sock, sql), include_types=True)
            assert result[1] is None, (sql, result)
        for sql in ("SELECT id FROM pg_database WHERE id = 23",
                    "SELECT id FROM public.pg_database WHERE id = 23"):
            result = runner.decode_wire_result(
                client.simple_query(sock, sql), include_types=True)
            assert result[1] is None and result[0] == [["23"]], (sql, result)
            assert result[3] == ["id"] and result[5] == [23], result
        for sql in (
                "DROP TABLE pg_database",
                "CREATE VIEW pg_database AS SELECT id FROM catalog_view_source"):
            result = runner.decode_wire_result(
                client.simple_query(sock, sql), include_types=True)
            assert result[1] is None, (sql, result)
        result = runner.decode_wire_result(client.simple_query(
            sock, "SELECT id FROM pg_database WHERE id = 17"), include_types=True)
        assert result[1] is None and result[0] == [["17"]], result
        assert result[3] == ["id"] and result[5] == [23], result
        messages = client.simple_query(sock,
            "SELECT d.datname, d.encoding FROM pg_catalog.pg_database AS d "
            "WHERE d.datname = 'info'")
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] is None and result[0] == [["info", "6"]], result
        assert result[3] == ["datname", "encoding"] and result[5] == [19, 23], result
        assert messages[-1] == (b"Z", b"I"), messages[-1]
    finally:
        runner.stop_ours(server)

    print("[PG CATALOG SQLSTATE] unsupported pg_catalog relations fail closed")


if __name__ == "__main__":
    main()
