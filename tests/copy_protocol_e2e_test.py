#!/usr/bin/env python3
"""PostgreSQL COPY wire flow, export formats, recovery, and atomicity."""

import importlib.util
import socket
import struct
import time
from pathlib import Path


def state(messages):
    for kind, body in messages:
        if kind != b"E":
            continue
        for field in body.rstrip(b"\0").split(b"\0"):
            if field.startswith(b"C"):
                return field[1:].decode()
    return None


def command_tags(messages):
    return [body[:-1] for kind, body in messages if kind == b"C"]


def expect_copy_response(body, columns):
    assert body == b"\0" + struct.pack("!H", columns) + b"\0\0" * columns, body


def parse(name, sql):
    return name.encode() + b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0)


def bind(portal, statement):
    return (portal.encode() + b"\0" + statement.encode() + b"\0" +
            struct.pack("!HHH", 0, 0, 0))


def execute(portal=""):
    return portal.encode() + b"\0" + struct.pack("!I", 0)


def simple(client, sock, sql):
    messages = client.simple_query(sock, sql)
    return messages, state(messages)


def begin_copy(client, sock, sql, response=b"G", columns=3):
    sock.sendall(client.typed(b"Q", sql.encode() + b"\0"))
    kind, body = client.read_message(sock)
    assert kind == response, (kind, body)
    expect_copy_response(body, columns)


def finish_simple_copy(client, sock):
    messages = client.read_until_ready(sock)
    assert messages[-1] == (b"Z", b"I"), messages
    return messages


def copy_out(client, sock, sql, columns):
    begin_copy(client, sock, sql, b"H", columns)
    messages = []
    while True:
        message = client.read_message(sock)
        messages.append(message)
        if message[0] == b"Z":
            return messages


def check_pg_type_copy_selection(client, runner, sock):
    sync = client.typed(b"S")

    def command(sql, tag, ready=b"I"):
        messages, sqlstate = simple(client, sock, sql)
        assert (sqlstate, command_tags(messages), messages[-1]) == (
            None, [tag], (b"Z", ready)), (sql, messages)

    def send_extended(sql, flush=False):
        request = (client.typed(b"P", parse("", sql)) +
                   client.typed(b"B", bind("", "")) +
                   client.typed(b"E", execute()))
        if flush:
            request += client.typed(b"H")
        sock.sendall(request)
        assert client.read_message(sock) == (b"1", b""), sql
        assert client.read_message(sock) == (b"2", b""), sql

    def reject(sql, expected, extended, ready=b"I"):
        if extended:
            send_extended(sql, flush=True)
        else:
            sock.sendall(client.typed(b"Q", sql.encode() + b"\0"))
        first = client.read_message(sock)
        assert first[0] == b"E", (sql, first)
        if extended:
            sock.sendall(sync)
        messages = [first, *client.read_until_ready(sock)]
        assert [kind for kind, _ in messages] == [b"E", b"Z"], (sql, messages)
        assert state(messages) == expected and messages[-1] == (b"Z", ready), (sql, messages)

    def out_rows(sql, columns, extended):
        if extended:
            send_extended(sql, flush=True)
            kind, body = client.read_message(sock)
            assert kind == b"H", (sql, kind, body)
            expect_copy_response(body, columns)
            sock.sendall(sync)
            messages = client.read_until_ready(sock)
        else:
            messages = copy_out(client, sock, sql, columns)
        rows = [body for kind, body in messages if kind == b"d"]
        assert [kind for kind, _ in messages] == [b"d"] * len(rows) + [b"c", b"C", b"Z"], (sql, messages)
        assert command_tags(messages) == [f"COPY {len(rows)}".encode()], (sql, messages)
        assert messages[-1] == (b"Z", b"I"), (sql, messages)
        return rows

    def insert_row(sql, row, extended):
        if extended:
            send_extended(sql)
            kind, body = client.read_message(sock)
            assert kind == b"G", (sql, kind, body)
            expect_copy_response(body, 1)
        else:
            begin_copy(client, sock, sql, columns=1)
        completion = client.typed(b"d", row) + client.typed(b"c")
        sock.sendall(completion + (sync if extended else b""))
        assert finish_simple_copy(client, sock) == [(b"C", b"COPY 1\0"), (b"Z", b"I")], sql

    def table_rows(sql, expected):
        messages = client.simple_query(sock, sql)
        rows, sqlstate, _, _, tag = runner.decode_wire_result(messages)
        assert (rows, sqlstate, tag, messages[-1]) == (
            [[value] for value in expected], None, f"SELECT {len(expected)}", (b"Z", b"I")), (sql, messages)

    for extended in (False, True):
        command("RESET search_path", b"RESET")
        command("CREATE TABLE public.pg_type (id INT)", b"CREATE TABLE")
        command("INSERT INTO public.pg_type VALUES (11)", b"INSERT 0 1")
        reject("COPY pg_type (id) TO STDOUT", "42703", extended)
        reject("COPY pg_type (id) FROM STDIN", "42703", extended)
        builtin = out_rows("COPY pg_type (oid, typname) TO STDOUT", 2, extended)
        assert builtin.count(b"23\tint4\n") == builtin.count(b"25\ttext\n") == 1, builtin
        assert all(row.endswith(b"\n") and row.count(b"\t") == 1 for row in builtin), builtin
        reject("COPY pg_catalog.pg_type (oid, typname) FROM STDIN", "0A000", extended)

        command("CREATE TEMP TABLE pg_type (id INT)", b"CREATE TABLE")
        command("INSERT INTO pg_temp.pg_type VALUES (31)", b"INSERT 0 1")
        command("SET search_path = public, pg_catalog, pg_temp", b"SET")
        insert_row("COPY pg_type (id) FROM STDIN", b"17\n", extended)
        assert sorted(out_rows("COPY pg_type (id) TO STDOUT", 1, extended)) == [b"11\n", b"17\n"]
        for relation in ("pg_type", "public.pg_type"):
            command("BEGIN READ ONLY", b"BEGIN", b"T")
            reject(f"COPY {relation} (id) FROM STDIN", "25006", extended, b"E")
            command("ROLLBACK", b"ROLLBACK")

        command("SET search_path = pg_catalog, pg_temp, public", b"SET")
        reject("COPY pg_type (id) TO STDOUT", "42703", extended)
        command("SET search_path = pg_temp, pg_catalog, public", b"SET")
        insert_row("COPY pg_type (id) FROM STDIN", b"47\n", extended)
        assert sorted(out_rows("COPY pg_type (id) TO STDOUT", 1, extended)) == [b"31\n", b"47\n"]
        table_rows("SELECT id FROM public.pg_type ORDER BY id", ["11", "17"])
        table_rows("SELECT id FROM pg_temp.pg_type ORDER BY id", ["31", "47"])
        command("DROP TABLE pg_temp.pg_type", b"DROP TABLE")
        command("DROP TABLE public.pg_type", b"DROP TABLE")
        command("RESET search_path", b"RESET")
    print("COPY_PG_TYPE_RELATION_MODES_COMPLETE 2")


def check_copy_schema_lock(client, owner, reader):
    def command(sock, sql, tag, ready=b"I"):
        messages, sqlstate = simple(client, sock, sql)
        assert (sqlstate, command_tags(messages), messages[-1]) == (
            None, [tag], (b"Z", ready)), (sql, messages)

    command(owner, "CREATE TABLE copy_schema_lock (id INT, marker INT)", b"CREATE TABLE")
    command(owner, "INSERT INTO copy_schema_lock VALUES (1, 29)", b"INSERT 0 1")
    command(reader, "SET lock_timeout = 50", b"SET")
    command(owner, "BEGIN", b"BEGIN", b"T")
    sql = "COPY copy_schema_lock (marker) TO STDOUT"
    try:
        command(owner, "ALTER TABLE copy_schema_lock DROP COLUMN marker", b"ALTER TABLE", b"T")
        messages, sqlstate = simple(client, reader, sql)
        assert [kind for kind, _ in messages] == [b"E", b"Z"], (sql, messages)
        assert (sqlstate, messages[-1]) == ("55P03", (b"Z", b"I")), (sql, messages)
    finally:
        command(owner, "ROLLBACK", b"ROLLBACK")

    exported = copy_out(client, reader, sql, 1)
    assert exported == [(b"d", b"29\n"), (b"c", b""), (b"C", b"COPY 1\0"), (b"Z", b"I")], exported
    command(owner, "DROP TABLE copy_schema_lock", b"DROP TABLE")
    print("COPY_SCHEMA_LOCK_CHECK_COMPLETE 1")


def check_query_copy_output(client, sock):
    def command(sql, tag, ready=b"I"):
        messages, sqlstate = simple(client, sock, sql)
        assert (sqlstate, command_tags(messages), messages[-1]) == (
            None, [tag], (b"Z", ready)), (sql, messages)

    sync = client.typed(b"S")

    def request(sql, extended):
        if extended:
            sock.sendall(client.typed(b"P", parse("", sql)) + client.typed(b"B", bind("", "")) +
                         client.typed(b"E", execute()) + sync)
        else:
            sock.sendall(client.typed(b"Q", sql.encode() + b"\0"))
        first = client.read_message(sock)
        if extended and first == (b"1", b""):
            assert client.read_message(sock) == (b"2", b""), sql
            first = client.read_message(sock)
        elif extended:
            assert first[0] == b"E", (sql, first)
        assert first[0] in (b"H", b"E"), (sql, first)
        return [first, *client.read_until_ready(sock)]

    def exported(sql, width, rows, extended, ready=b"I", header=None):
        messages = request(sql, extended)
        assert messages[0][0] == b"H", (sql, messages)
        expect_copy_response(messages[0][1], width)
        ending = [(b"c", b""), (b"C", f"COPY {len(rows)}\0".encode()), (b"Z", ready)]
        payload = rows if header is None else [header, *rows]
        assert messages[1:] == [(b"d", row) for row in payload] + ending, (sql, messages)

    def rejected(sql, sqlstate, extended, ready=b"I"):
        messages = request(sql, extended)
        assert [kind for kind, _ in messages] == [b"E", b"Z"], (sql, messages)
        assert (state(messages), messages[-1]) == (sqlstate, (b"Z", ready)), (sql, messages)

    command("CREATE TABLE copy_query_fixture (id INT PRIMARY KEY, payload TEXT)", b"CREATE TABLE")
    begin_copy(client, sock, "COPY copy_query_fixture (id, payload) FROM STDIN", columns=2)
    escaped = b"left\\tright\\\\slash\\nnext\\rend|marker"
    seed = b"1\toutside\n2\t\\N\n3\t\n4\t" + escaped + b"\n5\tNULL\n6\tNULL\n"
    sock.sendall(client.typed(b"d", seed) + client.typed(b"c"))
    assert finish_simple_copy(client, sock) == [(b"C", b"COPY 6\0"), (b"Z", b"I")]
    command("CREATE ROLE copy_query_reader", b"CREATE ROLE")
    command("GRANT SELECT(id) ON copy_query_fixture TO copy_query_reader", b"GRANT SELECT(ID)")

    ordered = ("COPY (SELECT id+10 AS n, payload FROM copy_query_fixture "
               "WHERE id>=2 AND id<=5 ORDER BY id DESC) TO STDOUT")
    ordered_rows = [b"15\tNULL\n", b"14\t" + escaped + b"\n", b"13\t\n", b"12\t\\N\n"]
    zero = "COPY (SELECT id, payload, NULL::text AS extra FROM copy_query_fixture WHERE id<0) TO STDOUT"
    missing = "COPY (SELECT missing FROM copy_query_fixture) TO STDOUT"
    for extended in (False, True):
        exported("COPY copy_query_fixture (payload,id) TO STDOUT WITH CSV HEADER", 2,
                 [b"outside,1\n", b",2\n", b'"",3\n', b'"left\tright\\slash\nnext\rend|marker",4\n',
                  b"NULL,5\n", b"NULL,6\n"], extended, header=b"payload,id\n")
        exported('COPY (SELECT id AS "key,col", payload FROM copy_query_fixture WHERE id<0) '
                 'TO STDOUT (FORMAT csv, HEADER)', 2, [], extended, header=b'"key,col",payload\n')
        exported("COPY (SELECT id, payload FROM copy_query_fixture WHERE id BETWEEN 2 AND 5 ORDER BY id) "
                 "TO STDOUT (NULL 'NULL', DELIMITER '|', FORCE_QUOTE (id), FORMAT CSV)", 2,
                 [b'"2"|NULL\n', b'"3"|\n', b'"4"|"left\tright\\slash\nnext\rend|marker"\n',
                  b'"5"|"NULL"\n'], extended)
        exported(r"""COPY (VALUES ('a!b^c'::text), ('^'::text), (NULL::text)) TO STDOUT
                    (QUOTE '!', ESCAPE '^', FORMAT csv, FORCE_QUOTE *)""", 1,
                 [b"!a^!b^^c!\n", b"!^^!\n", b"\n"], extended)
        exported("COPY (VALUES ('a!b'::text), ('a^b'::text)) TO STDOUT (FORMAT csv, QUOTE '!')", 1,
                 [b"!a!!b!\n", b"a^b\n"], extended)
        exported(r"""COPY (VALUES ('\.'::text), ('a"b'::text)) TO STDOUT (FORMAT csv)""",
                 1, [b'"\\."\n', b'"a""b"\n'], extended)
        exported(r"COPY (VALUES (7, 'a,b'::text)) TO STDOUT (FORMAT csv, DELIMITER E'\t', HEADER false)",
                 2, [b"7\ta,b\n"], extended)
        exported(zero + " (HEADER on)", 3, [], extended, header=b"id\tpayload\textra\n")
        rejected("COPY copy_query_fixture TO STDOUT (FORMAT csv, QUOTE ',')", "22023", extended)
        rejected("COPY copy_query_fixture TO STDOUT (FORMAT csv, NULL 'x,y')", "22023", extended)
        rejected("COPY copy_query_fixture TO STDOUT (FORMAT csv, HEADER MATCH)", "0A000", extended)
        rejected("COPY copy_query_fixture TO STDOUT (FORCE_QUOTE *)", "0A000", extended)
        rejected("COPY copy_query_fixture TO STDOUT (FORMAT csv, QUOTE 'é')", "0A000", extended)
        rejected("COPY copy_query_fixture (id) TO STDOUT (FORMAT csv, FORCE_QUOTE (payload))", "42P10", extended)
        rejected("COPY copy_query_fixture TO STDOUT (FORMAT csv, FORCE_QUOTE (absent))", "42703", extended)
        rejected("COPY copy_query_fixture TO STDOUT (FORMAT csv, FORCE_QUOTE (id,id))", "42701", extended)

        exported(ordered, 2, ordered_rows, extended)
        exported(zero, 3, [], extended)
        exported("COPY (SELECT payload FROM copy_query_fixture WHERE id>=5 ORDER BY id) TO STDOUT",
                 1, [b"NULL\n", b"NULL\n"], extended)
        exported("COPY (SELECT id, payload FROM copy_query_fixture WHERE id>=2 AND id<=5 ORDER BY id) "
                 "TO STDOUT WITH (DELIMITER '|', NULL 'NUL')", 2,
                 [b"2|NUL\n", b"3|\n", b"4|" + escaped.replace(b"|", b"\\|") + b"\n", b"5|NULL\n"], extended)
        exported("COPY (SELECT '('::text AS first, ')'::text AS second FROM copy_query_fixture "
                 "WHERE id=1 /* ) */) TO STDOUT", 2, [b"(\t)\n"], extended)
        exported("COPY (VALUES (1, NULL::text), (2, ''::text)) TO STDOUT", 2,
                 [b"1\t\\N\n", b"2\t\n"], extended)
        exported("COPY (WITH source AS (SELECT id FROM copy_query_fixture WHERE id<=2) "
                 "SELECT id FROM source ORDER BY id DESC) TO STDOUT", 1,
                 [b"2\n", b"1\n"], extended)
        rejected("COPY (SELECT id FROM copy_query_fixture) FROM STDIN", "42601", extended)
        rejected(missing, "42703", extended)
        rejected("COPY (SELECT id FROM copy_query_absent) TO STDOUT", "42P01", extended)
        rejected("COPY (SELECT 1/0) TO STDOUT", "22012", extended)
        partial = request(
            "COPY (SELECT id, 10/(2-id) FROM (SELECT id FROM copy_query_fixture "
            "WHERE id<=2 ORDER BY id) AS sorted) TO STDOUT", extended)
        assert [kind for kind, _ in partial] == [b"H", b"d", b"E", b"Z"], partial
        expect_copy_response(partial[0][1], 2)
        assert (partial[1][1], state(partial), partial[-1]) == (
            b"1\t10\n", "22012", (b"Z", b"I")), partial

        command("BEGIN READ ONLY", b"BEGIN", b"T")
        exported(ordered, 2, ordered_rows, extended, b"T")
        command("SELECT 1", b"SELECT 1", b"T")
        command("ROLLBACK", b"ROLLBACK")
        command("BEGIN", b"BEGIN", b"T")
        rejected(missing, "42703", extended, b"E")
        messages, sqlstate = simple(client, sock, "SELECT 1")
        assert (sqlstate, messages[-1]) == ("25P02", (b"Z", b"E")), messages
        command("ROLLBACK", b"ROLLBACK")
        exported(zero, 3, [], extended)

        command("SET ROLE copy_query_reader", b"SET")
        try:
            exported("COPY (SELECT id FROM copy_query_fixture WHERE id=1) TO STDOUT", 1,
                     [b"1\n"], extended)
            rejected("COPY (SELECT l.id FROM copy_query_fixture l JOIN copy_query_fixture r "
                     "ON l.payload=r.payload) TO STDOUT", "42501", extended)
        finally:
            command("RESET ROLE", b"RESET")

    command("DROP TABLE copy_query_fixture", b"DROP TABLE")
    command("DROP ROLE copy_query_reader", b"DROP ROLE")
    print("COPY_QUERY_OUTPUT_MODES_COMPLETE 2")


def check_csv_copy_input(client, runner, sock):
    def command(sql, tag, ready=b"I"):
        messages, sqlstate = simple(client, sock, sql)
        assert (sqlstate, command_tags(messages), messages[-1]) == (
            None, [tag], (b"Z", ready)), (sql, messages)

    def load(sql, data, extended, *, count=None, error=None, ready=b"I", columns=3, notices=()):
        if extended:
            sock.sendall(client.typed(b"P", parse("", sql)) + client.typed(b"B", bind("", "")) +
                         client.typed(b"E", execute()))
            assert client.read_message(sock) == (b"1", b""), sql
            assert client.read_message(sock) == (b"2", b""), sql
            kind, body = client.read_message(sock)
            assert kind == b"G", (sql, kind, body)
            expect_copy_response(body, columns)
        else:
            begin_copy(client, sock, sql, columns=columns)
        # Split data records at every byte, including inside doubled quotes,
        # CRLF and a UTF-8 code point. Each CopyData message is complete.
        payload = b"".join(client.typed(b"d", data[i:i + 1]) for i in range(len(data)))
        sock.sendall(payload + client.typed(b"c") + (client.typed(b"S") if extended else b""))
        reply = client.read_until_ready(sock)
        fields = [client.diagnostic_fields(body) for kind, body in reply if kind == b"N"]
        assert [(field[b"C"], field[b"M"]) for field in fields] == [
            (b"00000", notice.encode()) for notice in notices], (sql, reply)
        assert (state(reply), reply[-1]) == (error, (b"Z", ready)), (sql, reply)
        terminal = [(b"C", f"COPY {count}\0".encode()), (b"Z", ready)]
        if error is None:
            assert reply[len(notices):] == terminal, (sql, reply)
        else:
            assert [kind for kind, _ in reply[len(notices):]] == [b"E", b"Z"], (sql, reply)

    for extended in (False, True):
        command("CREATE TABLE copy_csv_input (id INT PRIMARY KEY, payload TEXT, aux TEXT)", b"CREATE TABLE")
        load("COPY copy_csv_input FROM STDIN (FORMAT csv, HEADER MATCH)",
             b'id,payload,aux\r\n1,,""\r\n2,"line\r\nnext","a ""quote"""\r\n'
             b'3,caf\xc3\xa9,\\.\r\n4,ab"xy", tail', extended, count=4)
        load("COPY copy_csv_input FROM STDIN WITH (FORMAT csv, HEADER, DELIMITER '|', "
             "NULL 'nil', QUOTE '!', ESCAPE '^', FORCE_NULL (payload), FORCE_NOT_NULL (payload))",
             b"arbitrary header\n5|nil|nil\n6|!nil!|!nil!\n7|!a^!b^^c!|!end!\n", extended, count=3)
        load("COPY copy_csv_input (id, aux, payload) FROM STDIN (FORMAT csv, HEADER MATCH, FORCE_NOT_NULL *)",
             b'id,aux,payload\r8,,abc\r', extended, count=1)
        load("COPY copy_csv_input FROM STDIN CSV HEADER", b'ignored\n9,legacy,ok\n', extended, count=1)
        command("CREATE TABLE copy_csv_marker (payload TEXT)", b"CREATE TABLE")
        load("COPY copy_csv_marker FROM STDIN (FORMAT csv)", b'\\.\n', extended, count=1, columns=1)
        marker = runner.decode_wire_result(client.simple_query(sock, "SELECT payload FROM copy_csv_marker"))
        assert marker[0] == [["\\."]] and marker[1] is None, marker

        csv = "COPY copy_csv_input FROM STDIN (FORMAT csv)"
        load("COPY copy_csv_input FROM STDIN (FORMAT csv, HEADER MATCH)",
             b'id,aux,payload\n10,wrong,order\n', extended, error="22P04")
        load("COPY copy_csv_input FROM STDIN (FORMAT csv, HEADER MATCH)", b'', extended, error="22P04")
        for data, code in ((b'10,too-few\n', "22P04"), (b'10,x,y,z\n', "22P04"),
                           (b'10,a,b\n11,c,d\r\n', "22P04"), (b'10,a,\xff\n', "22021"),
                           (b'2147483648,x,y\n', "22003")):
            load(csv, data, extended, error=code)
        command("BEGIN", b"BEGIN", b"T")
        load(csv, b'10,first,row\n11,"unclosed', extended, error="22P04", ready=b"E")
        messages, sqlstate = simple(client, sock, "SELECT 1")
        assert (sqlstate, messages[-1]) == ("25P02", (b"Z", b"E")), messages
        command("ROLLBACK", b"ROLLBACK")

        rows, sqlstate, _, _, tag = runner.decode_wire_result(client.simple_query(
            sock, "SELECT id, payload, aux FROM copy_csv_input ORDER BY id"))
        assert (sqlstate, tag, rows) == (None, "SELECT 9", [
            ["1", None, ""], ["2", "line\r\nnext", 'a "quote"'], ["3", "café", "\\."],
            ["4", "abxy", " tail"], ["5", "nil", None], ["6", None, "nil"],
            ["7", "a!b^c", "end"], ["8", "abc", ""], ["9", "legacy", "ok"],
        ]), (sqlstate, tag, rows)
        command("DROP TABLE copy_csv_marker", b"DROP TABLE")
        command("DROP TABLE copy_csv_input", b"DROP TABLE")

        command("CREATE SEQUENCE copy_default_seq START WITH 40", b"CREATE SEQUENCE")
        command("CREATE TABLE copy_defaults (id INT PRIMARY KEY, val TEXT DEFAULT 'fallback', "
                "n INT DEFAULT nextval('copy_default_seq'), absent TEXT, extra INT DEFAULT 77)", b"CREATE TABLE")
        selected = "COPY copy_defaults (id, val, n, absent) FROM STDIN"
        load(selected + " (FORMAT csv, HEADER MATCH, DEFAULT 'DEF', NULL 'NIL')",
             b'id,val,n,absent\n1,DEF,DEF,plain\n2,"DEF",DEF,"DEF"\n3,NIL,NIL,""\n',
             extended, count=3, columns=4)
        load(selected + r" (FORMAT text, DEFAULT '\D')",
             b'4\t\\D\t88\ttext\n5\t\\\\D\t\\D\t\\N\n', extended, count=2, columns=4)
        load(selected + " (FORMAT csv, DEFAULT '', NULL 'NIL')",
             b'6,,90,""\n', extended, count=1, columns=4)
        load(selected + " (FORMAT csv, DEFAULT 'val', HEADER MATCH)",
             b'id,val,n,absent\n7,val,91,ok\n', extended, count=1, columns=4)

        command("BEGIN", b"BEGIN", b"T")
        load(selected + " (FORMAT csv, DEFAULT 'DEF')",
             b'8,DEF,DEF,will-rollback\n9,DEF,93,DEF\n', extended, error="22P04", ready=b"E", columns=4)
        command("ROLLBACK", b"ROLLBACK")
        seq, sqlstate, _, _, tag = runner.decode_wire_result(client.simple_query(
            sock, "SELECT nextval('copy_default_seq')"))
        assert (seq, sqlstate, tag) == ([["44"]], None, "SELECT 1"), (seq, sqlstate, tag)

        for sql, code in (
            (selected + " (FORMAT csv, DEFAULT '')", "0A000"),
            (selected + " (FORMAT csv, DEFAULT 'a,b')", "0A000"),
            (selected + ''' (FORMAT csv, DEFAULT 'a"b')''', "0A000"),
            (selected + r" (FORMAT csv, DEFAULT E'\n')", "22023"),
            (selected + r" (FORMAT csv, DEFAULT E'\xff')", "22021"),
            ("COPY copy_defaults TO STDOUT (DEFAULT 'DEF')", "0A000"),
        ):
            if extended:
                sock.sendall(client.typed(b"P", parse("", sql)) + client.typed(b"B", bind("", "")) +
                             client.typed(b"E", execute()) + client.typed(b"S"))
                reply = client.read_until_ready(sock)
            else:
                reply = client.simple_query(sock, sql)
            assert (state(reply), reply[-1]) == (code, (b"Z", b"I")), (sql, reply)
            assert all(kind not in (b"G", b"H") for kind, _ in reply), (sql, reply)

        rows, sqlstate, _, _, tag = runner.decode_wire_result(client.simple_query(
            sock, "SELECT id, val, n, absent, extra FROM copy_defaults ORDER BY id"))
        assert (sqlstate, tag, rows) == (None, "SELECT 7", [
            ["1", "fallback", "40", "plain", "77"], ["2", "DEF", "41", "DEF", "77"],
            ["3", None, None, "", "77"], ["4", "fallback", "88", "text", "77"],
            ["5", "\\D", "42", None, "77"], ["6", "fallback", "90", "", "77"],
            ["7", "fallback", "91", "ok", "77"],
        ]), (sqlstate, tag, rows)
        command("DROP TABLE copy_defaults", b"DROP TABLE")
        command("DROP SEQUENCE copy_default_seq", b"DROP SEQUENCE")

        command("CREATE TABLE copy_identity ("
                "id SMALLINT GENERATED ALWAYS AS IDENTITY (START 10 INCREMENT 2) PRIMARY KEY, "
                "stamp BIGINT GENERATED BY DEFAULT AS IDENTITY (START 100 INCREMENT 5), payload TEXT)",
                b"CREATE TABLE")
        identity = "COPY copy_identity FROM STDIN"
        omitted = "COPY copy_identity (payload) FROM STDIN"
        marker = identity + " (FORMAT csv, DEFAULT 'GEN')"
        load("COPY copy_identity (payload, stamp, id) FROM STDIN (FORMAT csv, HEADER MATCH)",
             b"payload,stamp,id\nexplicit,-8,500\nmore,-9,502\n", extended, count=2)
        load(omitted, b"generated\n", extended, count=1, columns=1)
        load(marker, b"GEN,GEN,markers\n", extended, count=1)

        command("BEGIN", b"BEGIN", b"T")
        load(marker, b"GEN,GEN,to-rollback\n", extended, count=1, ready=b"T")
        load(identity, b"\\N\t800\tnull-always\n", extended, error="23502", ready=b"E")
        command("ROLLBACK", b"ROLLBACK")
        load(identity, b"503\t\\N\tnull-by-default\n", extended, error="23502")
        load(identity, b"503\t800\tbefore-duplicate\n500\t801\tduplicate\n",
             extended, error="23505")
        load(identity, b"32768\t800\toverflow\n", extended, error="22003")
        load(omitted, b"after rollback\n", extended, count=1, columns=1)

        rows, sqlstate, _, _, tag = runner.decode_wire_result(client.simple_query(
            sock, "SELECT id, stamp, payload FROM copy_identity ORDER BY id"))
        assert (sqlstate, tag, rows) == (None, "SELECT 5", [
            ["10", "100", "generated"], ["12", "105", "markers"],
            ["16", "115", "after rollback"], ["500", "-8", "explicit"], ["502", "-9", "more"],
        ]), (sqlstate, tag, rows)
        messages, sqlstate = simple(client, sock, "INSERT INTO copy_identity VALUES (600, 600, 'plain insert')")
        assert (sqlstate, messages[-1]) == ("428C9", (b"Z", b"I")), messages
        command("DROP TABLE copy_identity", b"DROP TABLE")

        command("CREATE SEQUENCE copy_eval_seq START WITH 30", b"CREATE SEQUENCE")
        command("CREATE TABLE copy_eval (id SMALLINT GENERATED ALWAYS AS IDENTITY (START 10 INCREMENT 2), "
                "first_value INT DEFAULT nextval('copy_eval_seq'), n SMALLINT, "
                "last_value INT DEFAULT nextval('copy_eval_seq'), "
                "omitted_value INT DEFAULT nextval('copy_eval_seq'), limited_text VARCHAR(3))", b"CREATE TABLE")

        def check_sequence(expected):
            result = runner.decode_wire_result(client.simple_query(sock, "SELECT nextval('copy_eval_seq')"))
            assert (result[0], result[1], result[4]) == ([[str(expected)]], None, "SELECT 1"), result

        def ordered_copy(selected, data, **expected):
            sql = f"COPY copy_eval ({selected}) FROM STDIN (FORMAT csv, DEFAULT 'GEN')"
            load(sql, data, extended, **expected)

        ordered_copy("last_value,n,id,first_value", b"GEN,bad,GEN,GEN\n", columns=4, error="22P02")
        check_sequence(31)
        ordered_copy("n,id,first_value,last_value", b"32768,GEN,GEN,GEN\n", columns=4, error="22003")
        check_sequence(32)
        ordered_copy("id,last_value,first_value,n", b"GEN,GEN,GEN,42\n", columns=4, count=1)
        ordered_copy("first_value,limited_text", b"GEN,long\n", columns=2, error="22001")
        check_sequence(37)
        ordered_copy("limited_text,first_value", b"long,GEN\n", columns=2, error="22001")
        check_sequence(38)
        ordered_copy("first_value,n", b"GEN,GEN\n", columns=2, error="22P04")
        check_sequence(39)
        ordered_copy("first_value,n", b"GEN\n", columns=2, error="22P04")
        check_sequence(41)
        ordered_copy("first_value,n", b"GEN,2,extra\n", columns=2, error="22P04")
        check_sequence(42)
        load(r"COPY copy_eval (first_value, n) FROM STDIN (FORMAT text, DEFAULT '\G')",
             b"\\G\n", extended, columns=2, error="22P04")
        check_sequence(44)

        command("BEGIN", b"BEGIN", b"T")
        load(r"COPY copy_eval (id, n) FROM STDIN (FORMAT text, DEFAULT '\G')",
             b"\\G\tbad\n", extended, columns=2, error="22P02", ready=b"E")
        command("ROLLBACK", b"ROLLBACK")
        ordered_copy("n", b"43\n", columns=1, count=1)
        rows, sqlstate, _, _, tag = runner.decode_wire_result(client.simple_query(
            sock, "SELECT id, first_value, n, last_value, omitted_value FROM copy_eval ORDER BY id"))
        assert (sqlstate, tag, rows) == (None, "SELECT 2", [
            ["10", "34", "42", "33", "35"], ["14", "45", "43", "46", "47"],
        ]), (sqlstate, tag, rows)
        command("DROP TABLE copy_eval", b"DROP TABLE")
        command("DROP SEQUENCE copy_eval_seq", b"DROP SEQUENCE")

        command("CREATE SEQUENCE copy_skip_seq START WITH 10", b"CREATE SEQUENCE")
        command("CREATE TABLE copy_skip (id INT PRIMARY KEY, payload TEXT, n SMALLINT CHECK (n > 0), "
                "active BOOLEAN NOT NULL, first_value INT DEFAULT nextval('copy_skip_seq'), "
                "later_value INT DEFAULT nextval('copy_skip_seq'), tail INT DEFAULT nextval('copy_skip_seq'))",
                b"CREATE TABLE")
        skip = "COPY copy_skip (id, payload, n, active) FROM STDIN"
        csv = skip + " (FORMAT csv, ON_ERROR ignore)"
        one_skipped = "1 row was skipped due to data type incompatibility"
        load(skip + " (FORMAT csv, HEADER MATCH, ON_ERROR ignore, LOG_VERBOSITY verbose, REJECT_LIMIT 2)",
             b'id,payload,n,active\n1,"two\nlines",2,true\n2,"bad\nline",bad,true\n'
             b'3,wrong,3,unknown\n4,ok,2,true\n', extended, columns=4, count=2, notices=(
                 'skipping row due to data type incompatibility at line 5 for column "n": "bad"',
                 'skipping row due to data type incompatibility at line 6 for column "active": "unknown"',
                 '2 rows were skipped due to data type incompatibility'))
        load("COPY copy_skip (id, first_value, n, later_value, active) FROM STDIN "
             r"(DEFAULT '\G', ON_ERROR ignore, LOG_VERBOSITY silent)",
             b"5\t\\G\tbad\t\\G\ttrue\n6\t\\G\t6\t\\G\ttrue", extended, columns=5, count=1)
        load(csv, b"7,all skipped,32768,true\n", extended, columns=4, count=0, notices=(one_skipped,))
        load(csv, b"bad\n", extended, columns=4, count=0, notices=(one_skipped,))
        load(csv, b"7,all good,4,true\n", extended, columns=4, count=1)

        rows, sqlstate, _, _, tag = runner.decode_wire_result(client.simple_query(
            sock, "SELECT id,n,first_value,later_value,tail FROM copy_skip ORDER BY id"))
        assert (sqlstate, tag, rows) == (None, "SELECT 4", [
            ["1", "2", "10", "11", "12"], ["4", "2", "19", "20", "21"],
            ["6", "6", "24", "25", "26"], ["7", "4", "33", "34", "35"],
        ]), (sqlstate, tag, rows)

        command("BEGIN", b"BEGIN", b"T")
        load(skip + " (FORMAT csv, ON_ERROR ignore, REJECT_LIMIT 1, LOG_VERBOSITY verbose)",
             b"8,valid,2,true\n9,invalid,no,true\n10,invalid,2,maybe\n11,valid,1,true\n",
             extended, columns=4, error="22P02", ready=b"E", notices=(
                 'skipping row due to data type incompatibility at line 2 for column "n": "no"',
                 'skipping row due to data type incompatibility at line 3 for column "active": "maybe"'))
        command("ROLLBACK", b"ROLLBACK")
        for data, code in ((b"8,row,2,true\n4,duplicate,1,true\n", "23505"),
                           (b"8,row,0,true\n", "23514"), (b"8,required,1,\n", "23502"),
                           (b"8,missing,1\n", "22P04"), (b"bad,extra,1,true,extra\n", "22P04"),
                           (b"8,encoding,1,\xff\n", "22021")):
            load(csv, data, extended, columns=4, error=code)
        load(skip + " (FORMAT csv, ON_ERROR stop)", b"8,explicit stop,not_int,true\n",
             extended, columns=4, error="22P02")
        rows, sqlstate, _, _, tag = runner.decode_wire_result(client.simple_query(
            sock, "SELECT id FROM copy_skip ORDER BY id"))
        assert (sqlstate, tag, rows) == (None, "SELECT 4", [["1"], ["4"], ["6"], ["7"]]), rows

        for sql, code in (
            (skip + " (ON_ERROR invalid)", "22023"),
            (skip + " (REJECT_LIMIT 3)", "22023"),
            (skip + " (ON_ERROR ignore, REJECT_LIMIT -1)", "22023"),
            (skip + " (ON_ERROR ignore, REJECT_LIMIT 0)", "22023"),
            (skip + " (ON_ERROR ignore, REJECT_LIMIT 9223372036854775808)", "22003"),
            (skip + " (ON_ERROR ignore, REJECT_LIMIT 'banana')", "22P02"),
            (skip + " (LOG_VERBOSITY invalid)", "22023"),
            ("COPY copy_skip TO STDOUT (ON_ERROR stop)", "22023"),
        ):
            if extended:
                sock.sendall(client.typed(b"P", parse("", sql)) + client.typed(b"B", bind("", "")) +
                             client.typed(b"E", execute()) + client.typed(b"S"))
                reply = client.read_until_ready(sock)
            else:
                reply = client.simple_query(sock, sql)
            assert (state(reply), reply[-1]) == (code, (b"Z", b"I")), (sql, reply)
            assert all(kind not in (b"G", b"H") for kind, _ in reply), (sql, reply)
        command("DROP TABLE copy_skip", b"DROP TABLE")
        command("DROP SEQUENCE copy_skip_seq", b"DROP SEQUENCE")

        command("CREATE SEQUENCE copy_skip_max START WITH 1 MINVALUE 0 MAXVALUE 1", b"CREATE SEQUENCE")
        command("CREATE TABLE copy_skip_exhaust (id INT, n INT, tail INT DEFAULT nextval('copy_skip_max'))",
                b"CREATE TABLE")
        load("COPY copy_skip_exhaust (id, n) FROM STDIN (FORMAT csv, ON_ERROR ignore, "
             "REJECT_LIMIT 1, LOG_VERBOSITY verbose)", b"1,bad\n2,worse\n", extended, columns=2,
             error="2200H", notices=(
                 'skipping row due to data type incompatibility at line 1 for column "n": "bad"',
                 'skipping row due to data type incompatibility at line 2 for column "n": "worse"'))
        load("COPY copy_skip_exhaust FROM STDIN (FORMAT csv, DEFAULT 'DF', ON_ERROR ignore, "
             "LOG_VERBOSITY verbose)", b"1,9,DF\n", extended, error="2200H")
        command("DROP TABLE copy_skip_exhaust", b"DROP TABLE")
        command("DROP SEQUENCE copy_skip_max", b"DROP SEQUENCE")

        command("CREATE TABLE copy_skip_reference (id INT PRIMARY KEY, typ REGTYPE, rel REGCLASS)", b"CREATE TABLE")
        load("COPY copy_skip_reference FROM STDIN (FORMAT csv, ON_ERROR ignore)",
             b"1,int4,pg_catalog.pg_type\n2,invalid_copy_type,pg_catalog.pg_type\n"
             b"3,text,invalid_copy_relation\n4,text,pg_catalog.pg_class\n", extended, count=2,
             notices=("2 rows were skipped due to data type incompatibility",))
        rows, sqlstate, _, _, tag = runner.decode_wire_result(client.simple_query(
            sock, "SELECT id FROM copy_skip_reference ORDER BY id"))
        assert (sqlstate, tag, rows) == (None, "SELECT 2", [["1"], ["4"]]), rows
        command("DROP TABLE copy_skip_reference", b"DROP TABLE")

        command("CREATE DOMAIN copy_positive AS INT NOT NULL CHECK (VALUE > 0)", b"CREATE DOMAIN")
        command("CREATE DOMAIN copy_digit AS copy_positive CHECK (VALUE < 10)", b"CREATE DOMAIN")
        command("CREATE DOMAIN copy_pair AS INT[] CHECK (cardinality(VALUE) = 2)", b"CREATE DOMAIN")
        command("CREATE SEQUENCE copy_domain_seq START WITH 20", b"CREATE SEQUENCE")
        command("CREATE TABLE copy_domains (id INT PRIMARY KEY, d copy_digit CHECK (d <> 7), "
                "a copy_pair, n INT DEFAULT nextval('copy_domain_seq'))", b"CREATE TABLE")
        copy_domains = "COPY copy_domains (id, d, a) FROM STDIN"
        load(copy_domains + " (FORMAT csv, ON_ERROR ignore, LOG_VERBOSITY verbose)",
             b'1,2,"{1,2}"\n2,,"{1,2}"\n3,-1,"{1,2}"\n4,12,"{1,2}"\n5,3,{1}\n6,4,\n',
             extended, count=2, notices=(
                 'skipping row due to data type incompatibility at line 2 for column "d": null input',
                 'skipping row due to data type incompatibility at line 3 for column "d": "-1"',
                 'skipping row due to data type incompatibility at line 4 for column "d": "12"',
                 'skipping row due to data type incompatibility at line 5 for column "a": "{1}"',
                 '4 rows were skipped due to data type incompatibility'))
        load(copy_domains + " (FORMAT csv, ON_ERROR ignore)", b"7,7,\n", extended, error="23514")
        load("COPY copy_domains (id) FROM STDIN", b"7\n", extended, columns=1, count=1)
        load("COPY copy_domains (id, d, n) FROM STDIN (FORMAT csv, DEFAULT 'DF')",
             b"8,-1,DF\n", extended, error="23514")
        result = runner.decode_wire_result(client.simple_query(sock, "SELECT nextval('copy_domain_seq')"))
        assert (result[0], result[1], result[4]) == ([["28"]], None, "SELECT 1"), result
        load(copy_domains + " (FORMAT csv, ON_ERROR ignore, REJECT_LIMIT 1)",
             b"8,5,\n9,-1,\n10,10,\n", extended, error="22P02")
        rows, sqlstate, _, _, tag = runner.decode_wire_result(client.simple_query(
            sock, "SELECT id,d,a,n FROM copy_domains ORDER BY id"))
        assert (sqlstate, tag, rows) == (None, "SELECT 3", [
            ["1", "2", "{1,2}", "20"], ["6", "4", None, "25"], ["7", None, None, "27"],
        ]), (sqlstate, tag, rows)
        command("DROP TABLE copy_domains", b"DROP TABLE")
        command("DROP DOMAIN copy_digit", b"DROP DOMAIN")
        command("DROP DOMAIN copy_positive", b"DROP DOMAIN")
        command("DROP DOMAIN copy_pair", b"DROP DOMAIN")

        command("CREATE DOMAIN copy_invalid_default AS INT DEFAULT -1 CHECK (VALUE > 0)", b"CREATE DOMAIN")
        command("CREATE TABLE copy_domain_defaults (id INT, d copy_invalid_default, "
                "n INT DEFAULT nextval('copy_domain_seq'))", b"CREATE TABLE")
        load("COPY copy_domain_defaults FROM STDIN (FORMAT csv, ON_ERROR ignore, DEFAULT 'DF')",
             b"1,DF,99\n", extended, error="23514")
        load("COPY copy_domain_defaults (id) FROM STDIN (FORMAT csv, ON_ERROR ignore)",
             b"2\n", extended, columns=1, error="23514")
        result = runner.decode_wire_result(client.simple_query(sock, "SELECT nextval('copy_domain_seq')"))
        assert (result[0], result[1], result[4]) == ([["32"]], None, "SELECT 1"), result
        command("DROP TABLE copy_domain_defaults", b"DROP TABLE")
        command("DROP DOMAIN copy_invalid_default", b"DROP DOMAIN")
        command("DROP SEQUENCE copy_domain_seq", b"DROP SEQUENCE")

        command("CREATE DOMAIN copy_divisor AS INT CHECK (10 / VALUE > 0)", b"CREATE DOMAIN")
        command("CREATE TABLE copy_domain_expression (id INT PRIMARY KEY, d copy_divisor)", b"CREATE TABLE")
        load("COPY copy_domain_expression FROM STDIN (FORMAT csv, ON_ERROR ignore)",
             b"1,2\n2,0\n", extended, columns=2, error="22012")
        rows, sqlstate, _, _, tag = runner.decode_wire_result(client.simple_query(
            sock, "SELECT id,d FROM copy_domain_expression"))
        assert (sqlstate, tag, rows) == (None, "SELECT 0", []), (sqlstate, tag, rows)
        load("COPY copy_domain_expression FROM STDIN (ON_ERROR ignore)",
             b"1\t2\n2\t-1\n", extended, columns=2, count=1, notices=(one_skipped,))
        command("DROP TABLE copy_domain_expression", b"DROP TABLE")
        command("DROP DOMAIN copy_divisor", b"DROP DOMAIN")

        command("CREATE SEQUENCE copy_check_calls START WITH 100", b"CREATE SEQUENCE")
        command("CREATE DOMAIN copy_counted AS INT CHECK (nextval('copy_check_calls') > 0 AND VALUE > 0)",
                b"CREATE DOMAIN")
        command("CREATE TABLE copy_domain_checks (id INT PRIMARY KEY, d copy_counted CHECK (d < 10))",
                b"CREATE TABLE")
        load("COPY copy_domain_checks FROM STDIN (FORMAT csv, ON_ERROR ignore)",
             b"1,4\n", extended, columns=2, count=1)
        load("COPY copy_domain_checks FROM STDIN (FORMAT csv, ON_ERROR ignore)",
             b"2,11\n", extended, columns=2, error="23514")
        result = runner.decode_wire_result(client.simple_query(sock, "SELECT nextval('copy_check_calls')"))
        assert (result[0], result[1], result[4]) == ([["102"]], None, "SELECT 1"), result
        command("DROP TABLE copy_domain_checks", b"DROP TABLE")
        command("DROP DOMAIN copy_counted", b"DROP DOMAIN")
        command("DROP SEQUENCE copy_check_calls", b"DROP SEQUENCE")

        command("CREATE DOMAIN copy_changed_rule AS INT CONSTRAINT copy_positive_rule CHECK (VALUE > 0)",
                b"CREATE DOMAIN")
        command("CREATE TABLE copy_constraint_owner (id INT PRIMARY KEY, d copy_changed_rule CHECK (d <> -2))",
                b"CREATE TABLE")
        command("ALTER DOMAIN copy_changed_rule DROP CONSTRAINT copy_positive_rule", b"ALTER DOMAIN")
        load("COPY copy_constraint_owner FROM STDIN (FORMAT csv, ON_ERROR ignore)",
             b"1,-1\n", extended, columns=2, count=1)
        load("COPY copy_constraint_owner FROM STDIN (FORMAT csv, ON_ERROR ignore)",
             b"2,-2\n", extended, columns=2, error="23514")
        rows, sqlstate, _, _, tag = runner.decode_wire_result(client.simple_query(
            sock, "SELECT id,d FROM copy_constraint_owner ORDER BY id"))
        assert (sqlstate, tag, rows) == (None, "SELECT 1", [["1", "-1"]]), rows
        command("DROP TABLE copy_constraint_owner", b"DROP TABLE")
        command("DROP DOMAIN copy_changed_rule", b"DROP DOMAIN")

        command("CREATE DOMAIN copy_plain_rule AS INT", b"CREATE DOMAIN")
        command("CREATE TABLE copy_comment_check (id INT PRIMARY KEY, d copy_plain_rule)", b"CREATE TABLE")
        command("ALTER TABLE copy_comment_check ADD CONSTRAINT ordinary_check "
                "CHECK ((/*DBMS domain CHECK*/(d > 0)))", b"ALTER TABLE")
        load("COPY copy_comment_check FROM STDIN (FORMAT csv, ON_ERROR ignore)",
             b"1,-1\n", extended, columns=2, error="23514")
        command("DROP TABLE copy_comment_check", b"DROP TABLE")
        command("DROP DOMAIN copy_plain_rule", b"DROP DOMAIN")
    print("COPY_CSV_INPUT_MODES_COMPLETE 2")


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "copy_runner", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    sock = server["sock"]
    slow_reader = None
    try:
        check_pg_type_copy_selection(client, runner, sock)
        with socket.create_connection(("127.0.0.1", server["port"]), 15) as peer:
            client.startup(peer, "alice", "info")
            check_copy_schema_lock(client, sock, peer)
        check_query_copy_output(client, sock)
        check_csv_copy_input(client, runner, sock)
        _, sqlstate = simple(
            client, sock,
            "CREATE TABLE copy_wire (id INT PRIMARY KEY, payload TEXT, optional TEXT DEFAULT 'fallback');")
        assert sqlstate is None, sqlstate

        # CopyData boundaries are not record boundaries.  NULL, empty text,
        # escaped newlines, a custom delimiter, and a final unterminated row
        # retain distinct values.
        begin_copy(
            client, sock,
            "COPY copy_wire (id, payload, optional) FROM STDIN", columns=3)
        sock.sendall(client.typed(b"d", b"1\tal") +
                     client.typed(b"d", b"pha\t\\N\n2\tline\\nvalue\t\n"))
        sock.sendall(client.typed(b"c"))
        copied = finish_simple_copy(client, sock)
        assert state(copied) is None, copied
        assert command_tags(copied) == [b"COPY 2"], copied

        exported = copy_out(
            client, sock,
            "COPY copy_wire (id, payload, optional) TO STDOUT", 3)
        assert [body for kind, body in exported if kind == b"d"] == [
            b"1\talpha\t\\N\n", b"2\tline\\nvalue\t\n"
        ], exported
        assert [kind for kind, _ in exported].count(b"c") == 1, exported
        assert command_tags(exported) == [b"COPY 2"], exported

        begin_copy(
            client, sock,
            "COPY copy_wire (id, payload) FROM STDIN "
            "WITH (FORMAT text, DELIMITER '|', NULL 'NULL', ENCODING 'UTF8')",
            columns=2)
        sock.sendall(client.typed(b"d", b"3|pipe\\|value\n4|tail") +
                     client.typed(b"c"))
        copied = finish_simple_copy(client, sock)
        assert command_tags(copied) == [b"COPY 2"], copied
        exported = copy_out(
            client, sock,
            "COPY copy_wire (id, payload) TO STDOUT "
            "WITH (DELIMITER '|', NULL 'NULL')", 2)
        assert b"".join(body for kind, body in exported if kind == b"d") == (
            b"1|alpha\n2|line\\nvalue\n3|pipe\\|value\n4|tail\n"
        ), exported

        # A server-side parse/insert failure is reported immediately, but the
        # backend drains to CopyFail/CopyDone and rolls every prior row back.
        begin_copy(
            client, sock,
            "COPY copy_wire (id, payload, optional) FROM STDIN", columns=3)
        sock.sendall(client.typed(b"d", b"10\tok\tv\n11\ttoo-few\n"))
        kind, body = client.read_message(sock)
        assert kind == b"E" and b"C22P04\0" in body, (kind, body)
        sock.sendall(client.typed(b"f", b"client stopped after server error\0"))
        assert client.read_until_ready(sock) == [(b"Z", b"I")]
        exported = copy_out(client, sock, "COPY copy_wire (id) TO STDOUT", 1)
        assert b"10\n" not in b"".join(
            body for kind, body in exported if kind == b"d"), exported

        # CopyFail is a statement cancellation and also rolls back earlier
        # chunks from the same COPY.
        begin_copy(
            client, sock,
            "COPY copy_wire (id, payload, optional) FROM STDIN", columns=3)
        sock.sendall(client.typed(b"d", b"12\twill rollback\tv\n") +
                     client.typed(b"f", b"application aborted\0"))
        failed = finish_simple_copy(client, sock)
        assert state(failed) == "57014", failed

        # An error inside an explicit transaction leaves ReadyForQuery in E;
        # ROLLBACK restores usability and no COPY row survives.
        assert simple(client, sock, "BEGIN")[0][-1] == (b"Z", b"T")
        begin_copy(
            client, sock,
            "COPY copy_wire (id, payload, optional) FROM STDIN", columns=3)
        sock.sendall(client.typed(b"d", b"20\tbefore error\tv\n21\tbad\n"))
        kind, body = client.read_message(sock)
        assert kind == b"E" and b"C22P04\0" in body, (kind, body)
        sock.sendall(client.typed(b"c"))
        assert client.read_until_ready(sock) == [(b"Z", b"E")]
        messages, sqlstate = simple(client, sock, "SELECT 1")
        assert sqlstate == "25P02" and messages[-1] == (b"Z", b"E"), messages
        assert simple(client, sock, "ROLLBACK")[0][-1] == (b"Z", b"I")

        # Unsupported semantics fail before CopyInResponse and before writes.
        for sql in (
            "COPY copy_wire FROM STDIN WITH (FORMAT binary)",
            "COPY copy_wire FROM PROGRAM 'producer'",
            "COPY copy_wire FROM STDIN WITH (FREEZE true)",
            "COPY copy_wire FROM STDIN WITH (HEADER true)",
            "COPY copy_wire FROM '/tmp/copy.csv' WITH (FORMAT csv)",
        ):
            messages, sqlstate = simple(client, sock, sql)
            assert sqlstate == "0A000", (sql, messages)
            assert all(kind != b"G" for kind, _ in messages), messages
        messages, sqlstate = simple(
            client, sock,
            "INSERT INTO copy_wire VALUES (99, 'must rollback', 'x'); "
            "COPY copy_wire FROM STDIN")
        assert sqlstate == "0A000", messages
        exported = copy_out(client, sock, "COPY copy_wire (id) TO STDOUT", 1)
        assert b"99\n" not in b"".join(
            body for kind, body in exported if kind == b"d"), exported

        # Malformed CopyDone receives 08P01 and a normal simple-query recovery
        # boundary.  No row from the damaged stream is committed.
        begin_copy(
            client, sock,
            "COPY copy_wire (id, payload, optional) FROM STDIN", columns=3)
        sock.sendall(client.typed(b"d", b"30\tdamaged\tv\n") +
                     client.typed(b"c", b"not-empty"))
        damaged = finish_simple_copy(client, sock)
        assert state(damaged) == "08P01", damaged

        # Extended Query COPY waits for Sync.  A Sync consumed as the error
        # recovery boundary still produces exactly one ReadyForQuery.
        sock.sendall(
            client.typed(b"P", parse(
                "copy_ext", "COPY copy_wire (id, payload) FROM STDIN")) +
            client.typed(b"B", bind("copy_ext_portal", "copy_ext")) +
            client.typed(b"E", execute("copy_ext_portal")))
        assert client.read_message(sock) == (b"1", b"")
        assert client.read_message(sock) == (b"2", b"")
        kind, body = client.read_message(sock)
        assert kind == b"G"
        expect_copy_response(body, 2)
        sock.sendall(client.typed(b"d", b"40\textended\n") +
                     client.typed(b"c") + client.typed(b"S"))
        extended = client.read_until_ready(sock)
        assert state(extended) is None, extended
        assert command_tags(extended) == [b"COPY 1"], extended

        sock.sendall(
            client.typed(b"P", parse(
                "copy_ext_bad", "COPY copy_wire (id, payload) FROM STDIN")) +
            client.typed(b"B", bind("copy_ext_bad_portal", "copy_ext_bad")) +
            client.typed(b"E", execute("copy_ext_bad_portal")))
        assert client.read_message(sock) == (b"1", b"")
        assert client.read_message(sock) == (b"2", b"")
        kind, body = client.read_message(sock)
        assert kind == b"G"
        expect_copy_response(body, 2)
        sock.sendall(client.typed(b"d", b"41\n"))
        kind, body = client.read_message(sock)
        assert kind == b"E" and b"C22P04\0" in body, (kind, body)
        sock.sendall(client.typed(b"S"))
        assert client.read_until_ready(sock) == [(b"Z", b"I")]

        # CancelRequest interrupts an active COPY even while the backend is
        # waiting for the next frontend message, and leaves the implicit
        # transaction empty without requiring another CopyData/CopyFail.
        sock.close()
        sock = socket.create_connection(("127.0.0.1", server["port"]), 15)
        sock.settimeout(15)
        backend_pid, secret = client.startup(sock, "alice", "info")
        server["sock"] = sock
        begin_copy(
            client, sock,
            "COPY copy_wire (id, payload, optional) FROM STDIN", columns=3)
        cancel = socket.create_connection(("127.0.0.1", server["port"]), 15)
        cancel.sendall(struct.pack("!IIII", 16, 80877102, backend_pid, secret))
        cancel.close()
        kind, body = client.read_message(sock)
        assert kind == b"E" and b"C57014\0" in body, (kind, body)
        assert client.read_until_ready(sock) == [(b"Z", b"I")]
        exported = copy_out(client, sock, "COPY copy_wire (id) TO STDOUT", 1)
        assert b"50\n" not in b"".join(
            body for kind, body in exported if kind == b"d"), exported

        # The same active COPY interruption contract also owns Execute's
        # extended-protocol path; preserve all existing rows across its abort.
        kept_ids = [body for kind, body in exported if kind == b"d"]
        sock.sendall(
            client.typed(b"P", parse(
                "copy_ext_cancel", "COPY copy_wire (id, payload, optional) FROM STDIN")) +
            client.typed(b"B", bind("copy_ext_cancel_portal", "copy_ext_cancel")) +
            client.typed(b"E", execute("copy_ext_cancel_portal")))
        assert client.read_message(sock) == (b"1", b"")
        assert client.read_message(sock) == (b"2", b"")
        kind, body = client.read_message(sock)
        assert kind == b"G", (kind, body)
        expect_copy_response(body, 3)
        with socket.create_connection(("127.0.0.1", server["port"]), 15) as cancel:
            cancel.sendall(struct.pack("!IIII", 16, 80877102, backend_pid, secret))
        kind, body = client.read_message(sock)
        assert kind == b"E" and b"C57014\0" in body, (kind, body)
        sock.sendall(client.typed(b"S"))
        assert client.read_until_ready(sock) == [(b"Z", b"I")]
        exported = copy_out(client, sock, "COPY copy_wire (id) TO STDOUT", 1)
        assert [body for kind, body in exported if kind == b"d"] == kept_ids, exported
        print("COPY_CANCEL_MODES_COMPLETE 2")

        # A COPY TO client that stops reading must not pin the backend in a
        # blocking socket write beyond statement_timeout.  Constrain the
        # receive buffer and make the output larger than the kernel buffers.
        payload = "x" * 16384
        large_rows = b"".join(
            f"{1000 + row}\t{payload}\n".encode()
            for row in range(256))
        begin_copy(
            client, sock,
            "COPY copy_wire (id, payload) FROM STDIN", columns=2)
        sock.sendall(client.typed(b"d", large_rows))
        sock.sendall(client.typed(b"c"))
        copied = finish_simple_copy(client, sock)
        assert command_tags(copied) == [b"COPY 256"], copied

        slow_reader = socket.create_connection(
            ("127.0.0.1", server["port"]), 15)
        slow_reader.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        slow_reader.settimeout(5)
        slow_pid, _ = client.startup(slow_reader, "alice", "info")
        timeout_messages, timeout_state = simple(
            client, slow_reader, "SET statement_timeout = 5000")
        assert timeout_state is None, timeout_messages
        settings = client.simple_query(
            slow_reader,
            "SELECT setting FROM pg_catalog.pg_settings "
            "WHERE name = 'statement_timeout'")
        assert runner.decode_wire_result(settings, include_types=True)[0] == [
            ["5000"]
        ], settings
        begin_copy(
            client, slow_reader,
            "COPY copy_wire (id, payload) TO STDOUT", response=b"H",
            columns=2)
        initial_activity = client.simple_query(
            sock,
            "SELECT state, query FROM pg_catalog.pg_stat_activity "
            f"WHERE pid = {slow_pid}")
        initial_rows = runner.decode_wire_result(
            initial_activity, include_types=True)[0]
        assert initial_rows and initial_rows[0][0] == "active", initial_rows
        time.sleep(5.3)
        timeout_finished = False
        final_activity_rows = None
        activity_deadline = time.monotonic() + 3
        while time.monotonic() < activity_deadline:
            activity_messages = client.simple_query(
                sock,
                "SELECT state FROM pg_catalog.pg_stat_activity "
                f"WHERE pid = {slow_pid}")
            activity_rows = runner.decode_wire_result(
                activity_messages, include_types=True)[0]
            if not activity_rows or activity_rows[0][0] != "active":
                timeout_finished = True
                final_activity_rows = activity_rows
                break
            time.sleep(0.05)
        assert timeout_finished, (
            "COPY TO backend remained active after statement_timeout while "
            "the client receive window was not drained")
        assert not final_activity_rows, (
            "COPY TO backend stayed connected/idle instead of closing after "
            "the blocked output frame timed out", final_activity_rows)
        slow_reader.close()
        slow_reader = None

        print("[COPY PROTOCOL E2E] passed")
    finally:
        if slow_reader is not None:
            slow_reader.close()
        server["sock"] = sock
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
