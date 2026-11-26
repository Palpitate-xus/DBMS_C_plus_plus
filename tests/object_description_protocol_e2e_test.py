#!/usr/bin/env python3
"""Read persisted comments using PostgreSQL catalog identities and wire types."""

import re
import socket
import struct
import sys

from session_setting_functions_protocol_e2e_test import Wire, runner


def check_plan(client, wire, sql, reference, count=None):
    frames = wire.send_statements(sql)
    rows = [row[0].decode() for row in client.data_row_values(frames)]
    wire.check(sql, frames, rows=[(row,) for row in rows], tag='EXPLAIN', fields=[('QUERY PLAN',25)])
    scan = 'Index Scan' if reference else 'Catalog Scan'
    description_scans = [row for row in rows if scan in row and 'pg_description' in row]
    assert description_scans, rows
    if count is not None:
        actual = [float(value) for row in description_scans
                  for value in re.findall(r'actual rows=(\d+(?:\.\d+)?) ', row)]
        assert actual == [count], rows


def check_relation(client, wire, reference):
    wire.query("SELECT 'pg_catalog.pg_description'::regclass::oid AS v",
               rows=[('2609',)], fields=[('v', 26)])
    wire.query("SELECT 2609::oid::regclass AS v",
               rows=[('pg_description',)], fields=[('v', 2205)])
    wire.query("SELECT relname,relnamespace,relkind,relnatts FROM pg_catalog.pg_class WHERE oid=2609",
               rows=[('pg_description', '11', 'r', '4')],
               fields=[('relname', 19), ('relnamespace', 26), ('relkind', 18), ('relnatts', 21)])
    wire.query("SELECT attname,atttypid,attnum,attnotnull,attcollation FROM pg_catalog.pg_attribute "
               "WHERE attrelid=2609 AND attnum>0 ORDER BY attnum",
               rows=[('objoid','26','1','t','0'), ('classoid','26','2','t','0'),
                     ('objsubid','23','3','t','0'), ('description','25','4','t','950')],
               fields=[('attname',19), ('atttypid',26), ('attnum',21), ('attnotnull',16), ('attcollation',26)],
               tag='SELECT 4')
    if not reference:
        wire.query('SELECT count(*) AS total, count(description) AS texts FROM pg_catalog.pg_description',
                   rows=[('0', '0')], fields=[('total', 20), ('texts', 20)])

    for analyze, count in (('', None), ('ANALYZE, TIMING OFF, ', 0)):
        sql = f"EXPLAIN ({analyze}COSTS OFF) SELECT description FROM pg_catalog.pg_description WHERE objoid=4294967295"
        check_plan(client, wire, sql, reference, count)

    wire.command('CREATE SCHEMA comment_shadow', 'CREATE SCHEMA')
    wire.command('CREATE TABLE comment_shadow.pg_description(note text)', 'CREATE TABLE')
    wire.command("INSERT INTO comment_shadow.pg_description VALUES ('shadow')", 'INSERT 0 1')
    wire.command('SET search_path=comment_shadow,pg_catalog', 'SET')
    wire.query('SELECT * FROM pg_description', rows=[('shadow',)], fields=[('note',25)])
    wire.query('SELECT count(note) AS n FROM pg_description', rows=[('1',)], fields=[('n',20)])
    wire.query("SELECT 'pg_description'::regclass::oid = 'comment_shadow.pg_description'::regclass::oid AS v",
               rows=[('t',)], fields=[('v',16)])
    wire.query('SELECT 2609::oid::regclass AS v, pg_table_is_visible(2609) AS visible',
               rows=[('pg_catalog.pg_description','f')], fields=[('v',2205),('visible',16)])
    wire.command("UPDATE pg_description SET note='updated shadow'", 'UPDATE 1')
    wire.query('SELECT note FROM pg_description', rows=[('updated shadow',)], fields=[('note',25)])
    wire.command('RESET search_path', 'RESET')
    wire.query("SELECT pg_catalog.pg_table_is_visible(2609) AS v", rows=[('t',)], fields=[('v',16)])


def check_counts(client, wire, table, reference):
    source = 'pg_catalog.pg_description AS d'
    scope = f"d.objoid = '{table}'::oid AND d.classoid = 1259"
    sql = ("SELECT count(*) AS total, count(d.description) FILTER (WHERE d.objsubid > 0) AS columns, "
           "count(d.objoid) FILTER (WHERE 0 = d.objsubid) AS tables, "
           "count(d.objsubid) FILTER (WHERE d.objsubid > 2147483648) AS absent "
           f"FROM {source} WHERE {scope}")
    fields = [('total', 20), ('columns', 20), ('tables', 20), ('absent', 20)]
    wire.query(sql, rows=[('2', '1', '1', '0')], fields=fields)
    Wire(client, wire.sock, extended=True).query(sql, rows=[('2', '1', '1', '0')], fields=fields)
    wire.query(sql + ' OFFSET 1', rows=[], fields=fields, tag='SELECT 0')
    wire.query(sql + ' LIMIT 0', rows=[], fields=fields, tag='SELECT 0')
    # Aggregate emits one row even when there is no matching source row.
    wire.query("SELECT count(description) AS n FROM pg_catalog.pg_description WHERE objoid=4294967295",
               rows=[('0',)], fields=[('n', 20)])
    # DISTINCT and predicates that can fail are executed by the ordinary
    # evaluator. A safe leading key condition does not suppress later errors.
    wire.query(f'SELECT count(DISTINCT d.objsubid) AS n FROM {source} WHERE {scope}',
               rows=[('2',)], fields=[('n', 20)])
    wire.query(f'SELECT count(*) AS n FROM {source} WHERE {scope} AND 1 / d.objsubid > 0',
               state='22012', tag=None)

    for analyze in ('', 'ANALYZE, TIMING OFF, '):
        statement = f'EXPLAIN ({analyze}COSTS OFF) {sql}'
        frames = wire.send_statements(statement)
        lines = [row[0].decode() for row in client.data_row_values(frames)]
        wire.check(statement, frames, rows=[(line,) for line in lines], tag='EXPLAIN', fields=[('QUERY PLAN', 25)])
        node = 'Aggregate' if reference else 'Catalog Count'
        counts = [line for line in lines if node in line]
        assert len(counts) == 1, lines
        if not reference:
            assert 'pg_description' in counts[0] and 'Catalog Scan' not in '\n'.join(lines), lines
        if analyze:
            assert re.search(r'actual rows=1(?:\.\d+)? ', counts[0]), lines


def check(client, sock, reference):
    wire = Wire(client, sock)
    check_relation(client, wire, reference)
    wire.command('CREATE SCHEMA comment_lookup', 'CREATE SCHEMA')
    wire.command('CREATE TABLE comment_lookup.notes(id integer, body text)', 'CREATE TABLE')
    wire.command('CREATE INDEX notes_body ON comment_lookup.notes(body)', 'CREATE INDEX')
    wire.command('CREATE SEQUENCE comment_lookup.note_seq', 'CREATE SEQUENCE')
    wire.command('CREATE VIEW comment_lookup.note_view AS SELECT id FROM comment_lookup.notes', 'CREATE VIEW')
    wire.command("CREATE TYPE comment_lookup.note_state AS ENUM ('new')", 'CREATE TYPE')
    multiline = 'first line\nsecond line, "quoted" and \\ slash'
    wire.command("COMMENT ON SCHEMA comment_lookup IS 'schema details'", 'COMMENT')
    wire.command("COMMENT ON TABLE comment_lookup.notes IS 'table details'", 'COMMENT')
    wire.command(f"COMMENT ON COLUMN comment_lookup.notes.body IS '{multiline}'", 'COMMENT')
    wire.command("COMMENT ON INDEX comment_lookup.notes_body IS 'ordered by body'", 'COMMENT')
    wire.command("COMMENT ON SEQUENCE comment_lookup.note_seq IS 'note number'", 'COMMENT')
    wire.command("COMMENT ON VIEW comment_lookup.note_view IS 'visible note IDs'", 'COMMENT')
    wire.command("COMMENT ON TYPE comment_lookup.note_state IS 'workflow state'", 'COMMENT')

    def oid(sql):
        frames = wire.send_statements(sql)
        return int(client.data_row_values(frames)[0][0])

    namespace = oid("SELECT oid FROM pg_catalog.pg_namespace WHERE nspname='comment_lookup'")
    table = oid("SELECT 'comment_lookup.notes'::regclass::oid")
    index = oid("SELECT 'comment_lookup.notes_body'::regclass::oid")
    sequence = oid("SELECT 'comment_lookup.note_seq'::regclass::oid")
    view = oid("SELECT 'comment_lookup.note_view'::regclass::oid")
    note_type = oid("SELECT oid FROM pg_catalog.pg_type WHERE typname='note_state' AND "
                    f"typnamespace='{namespace}'::oid")
    check_counts(client, wire, table, reference)
    check_plan(client, wire, "EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF) "
               f"SELECT description FROM pg_catalog.pg_description WHERE objoid={table}::oid", reference, 2)
    rows = [(str(table), '1259', '0', 'table details'), (str(table), '1259', '2', multiline)]
    fields = [('objoid', 26), ('classoid', 26), ('objsubid', 23), ('description', 25)]
    catalog = f"SELECT * FROM pg_catalog.pg_description WHERE objoid='{table}'::oid ORDER BY objsubid"
    for mode in (wire, Wire(client, sock, extended=True)):
        mode.query(f"SELECT count(*) AS n FROM pg_catalog.pg_description WHERE objoid>={table} "
                   f"AND classoid=1259 AND objoid<=CAST('{table}' AS oid)", rows=[('2',)], fields=[('n',20)])
        frames = mode.send_statements(catalog)
        mode.check(catalog, frames, rows=rows, fields=fields, tag='SELECT 2')
        assert [field[1:4] for field in client.row_description_fields(frames)] == [
            (2609, 1, 26), (2609, 2, 26), (2609, 3, 23), (2609, 4, 25),
        ], frames
        for expression, expected in (
            (f"pg_catalog.obj_description('{namespace}'::oid, 'pg_namespace')", 'schema details'),
            (f"obj_description('{table}'::oid)", 'table details'),
            (f"obj_description('{table}'::oid, 'pg_class'::text)", 'table details'),
            (f"obj_description('{index}'::oid, 'pg_class')", 'ordered by body'),
            (f"obj_description('{sequence}'::oid, 'pg_class')", 'note number'),
            (f"obj_description('{view}'::oid, 'pg_class')", 'visible note IDs'),
            (f"obj_description('{note_type}'::oid, 'pg_type')", 'workflow state'),
            (f"col_description('{table}'::oid, 0)", 'table details'),
            (f"pg_catalog.col_description('{table}'::oid, 2)", multiline),
            (f"col_description('{table}'::oid, 99)", None),
            (f"obj_description('{table}'::oid, 'PG_CLASS')", None),
            (f"obj_description('{table}'::oid, 'pg_catalog.pg_class')", None),
            (f"obj_description('{table}'::oid, 'pg_type')", None),
            (f"obj_description('{table}'::oid, NULL)", None),
            ('pg_catalog.col_description(NULL, 2)', None),
            ("pg_catalog.obj_description('4294967295'::oid)", None),
        ):
            mode.query(f'SELECT {expression} AS note', rows=[(expected,)], fields=[('note', 25)])
        mode.query("SELECT pg_catalog.col_description('unparseable', 2)", state='22P02', tag=None)
        mode.query('SELECT pg_catalog.obj_description()', state='42883', tag=None)

    statement = b'comment_by_oid'
    sync = wire.packet(b'S')
    frames = wire.exchange(wire.parse('SELECT pg_catalog.obj_description($1, $2) AS note', statement) +
                           wire.packet(b'D', b'S' + statement + b'\0') + sync)
    assert b''.join(kind for kind, _ in frames) == b'1tTZ', frames
    assert frames[1][1] == struct.pack('!HII', 2, 26, 19), frames
    request = wire.bind(statement, (str(namespace).encode(), b'pg_namespace'), result_format=1)
    frames = wire.exchange(request + wire.describe_portal() + wire.execute() + sync)
    assert client.data_row_values(frames) == [[b'schema details']], frames
    assert [(field[3], field[-1]) for field in client.row_description_fields(frames)] == [(25, 1)], frames

    def table_note(expected, ready=b'T'):
        wire.query(f"SELECT obj_description('{table}'::oid) AS note", rows=[(expected,)],
                   fields=[('note', 25)], ready=ready)

    wire.command('BEGIN', 'BEGIN', ready=b'T')
    wire.command('SAVEPOINT before_comment', 'SAVEPOINT', ready=b'T')
    wire.command("COMMENT ON TABLE comment_lookup.notes IS 'pending'", 'COMMENT', ready=b'T')
    table_note('pending')
    wire.command("COMMENT ON COLUMN comment_lookup.notes.body IS NULL", 'COMMENT', ready=b'T')
    count = f"SELECT count(*) AS n FROM pg_catalog.pg_description WHERE objoid='{table}'::oid AND classoid=1259"
    wire.query(count, rows=[('1',)], fields=[('n', 20)], ready=b'T')
    wire.command('ROLLBACK TO before_comment', 'ROLLBACK', ready=b'T')
    wire.query(count, rows=[('2',)], fields=[('n', 20)], ready=b'T')
    wire.query(catalog, rows=rows, fields=fields, tag='SELECT 2', ready=b'T')
    wire.command("COMMENT ON TABLE comment_lookup.notes IS 'pending before index'", 'COMMENT', ready=b'T')
    wire.command('CREATE INDEX pending_note_index ON comment_lookup.notes(id)', 'CREATE INDEX', ready=b'T')
    wire.command("COMMENT ON TABLE comment_lookup.notes IS 'pending after index'", 'COMMENT', ready=b'T')
    table_note('pending after index')
    wire.command('ROLLBACK TO before_comment', 'ROLLBACK', ready=b'T')
    wire.query(catalog, rows=rows, fields=fields, tag='SELECT 2', ready=b'T')
    wire.command('CREATE TABLE comment_lookup.pending_note(k integer)', 'CREATE TABLE', ready=b'T')
    wire.command("COMMENT ON TABLE comment_lookup.pending_note IS 'new table comment'", 'COMMENT', ready=b'T')
    wire.command("COMMENT ON TABLE comment_lookup.notes IS 'pending'", 'COMMENT', ready=b'T')
    table_note('pending')
    wire.command('ROLLBACK', 'ROLLBACK')
    table_note('table details', ready=b'I')
    wire.query('SELECT * FROM comment_lookup.pending_note', state='42P01', tag=None)
    wire.command('ALTER TABLE comment_lookup.notes RENAME COLUMN body TO content', 'ALTER TABLE')
    wire.command('ALTER TABLE comment_lookup.notes RENAME TO journal', 'ALTER TABLE')
    wire.query(catalog, rows=rows, fields=fields, tag='SELECT 2')
    wire.command("COMMENT ON TABLE comment_lookup.journal IS ''", 'COMMENT')
    wire.query(f"SELECT col_description('{table}'::oid,0) AS note", rows=[(None,)], fields=[('note', 25)])
    wire.command('COMMENT ON COLUMN comment_lookup.journal.content IS NULL', 'COMMENT')
    wire.query(catalog, rows=[], fields=fields, tag='SELECT 0')
    wire.query(count, rows=[('0',)], fields=[('n', 20)])
    wire.command('DROP VIEW comment_lookup.note_view', 'DROP VIEW')
    wire.command('DROP TYPE comment_lookup.note_state', 'DROP TYPE')
    wire.command('DROP TABLE comment_lookup.journal', 'DROP TABLE')
    wire.query(f"SELECT obj_description('{index}'::oid) AS note", rows=[(None,)], fields=[('note', 25)])
    wire.command('DROP SEQUENCE comment_lookup.note_seq', 'DROP SEQUENCE')
    wire.command('DROP SCHEMA comment_lookup', 'DROP SCHEMA')
    wire.query(f"SELECT obj_description('{namespace}'::oid) AS note", rows=[(None,)], fields=[('note', 25)])

    if not reference:
        for sql in ("INSERT INTO pg_catalog.pg_description VALUES (1, 1, 0, 'invalid')",
                    "UPDATE pg_catalog.pg_description SET description='invalid'",
                    'DELETE FROM pg_catalog.pg_description'):
            wire.query(sql, state='0A000', tag=None)


def main():
    client = runner.load_protocol_client()
    reference = '--reference18' in sys.argv
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        with socket.create_connection((host, port), timeout=runner.wire_timeout()) as sock:
            client.startup_reference(sock, user, database, password=password)
            runner.verify_reference_version(client, sock)
            check(client, sock, reference)
    else:
        server = runner.start_ours(client)
        try:
            check(client, server['sock'], reference)
        finally:
            runner.stop_ours(server)
    print('OBJECT_DESCRIPTION_PROTOCOL_COMPLETE', flush=True)


if __name__ == '__main__':
    main()
