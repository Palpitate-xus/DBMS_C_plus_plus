#!/usr/bin/env python3
"""Pattern type/ESCAPE errors, NULL priorities, aliases and true SQL children."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('pattern_priority_runner', root/'tests/compat/pg_diff_runner.py')
    r = importlib.util.module_from_spec(spec); spec.loader.exec_module(r)
    c = r.load_protocol_client()
    reference = sys.argv[1:] == ['--reference18']
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = r._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=r.wire_timeout())
        c.startup_reference(sock, user, database, password=password)
        r.verify_reference_version(c, sock)
    else:
        server = r.start_ours(c); sock = server['sock']
    failures = []

    def check(label, actual, expected):
        if actual != expected:
            failures.append((label, actual, expected))
            print('PATTERN_PRIORITY_FAILURE', failures[-1], flush=True)

    def query(sql, state=None, rows=None, tag=None, types=None):
        c.simple_query(sock, 'SAVEPOINT pattern_priority_case;')
        result = r.decode_wire_result(c.simple_query(sock, sql), include_types=True)
        print('PATTERN_PRIORITY', sql, result, flush=True)
        check(sql+' state', result[1], state)
        if rows is not None: check(sql+' rows', result[0], rows)
        if tag is not None: check(sql+' tag', result[4], tag)
        if types is not None: check(sql+' types', result[5], types)
        if state is not None: check(sql+' no partial success', (result[0],result[4]), ([],None))
        c.simple_query(sock, 'ROLLBACK TO pattern_priority_case;')
        c.simple_query(sock, 'RELEASE pattern_priority_case;')

    controls = [
        ('SELECT 1 LIKE 2 WHERE false;', '42883', []),
        ('SELECT 1 NOT LIKE 2 LIMIT 0;', '42883', []),
        ("SELECT true NOT ILIKE 't' WHERE false;", '42883', []),
        ("SELECT 'a' SIMILAR TO 1 WHERE false;", '42883', []),
        ("SELECT 'a' LIKE '%' ESCAPE 1;", '42883', []),
        ("SELECT 'a' LIKE '%' ESCAPE '##' WHERE false;", '22025', []),
        ("SELECT NULL LIKE '%' ESCAPE '##';", '22025', []),
        ("SELECT NULL NOT LIKE '%' ESCAPE '##';", '22025', []),
        ("SELECT NULL ILIKE '%' ESCAPE '##';", '22025', []),
        ("SELECT NULL NOT ILIKE '%' ESCAPE '##';", '22025', []),
        ("SELECT NULL SIMILAR TO '%' ESCAPE '##';", '22025', []),
        ("SELECT NULL NOT SIMILAR TO '%' ESCAPE '##';", '22025', []),
        ("SELECT 'a' LIKE NULL ESCAPE '##';", None, [[None]]),
        ("SELECT NULL LIKE NULL ESCAPE '##';", None, [[None]]),
        ("SELECT 'a' NOT ILIKE NULL ESCAPE '##';", None, [[None]]),
        ("SELECT 'a' SIMILAR TO NULL ESCAPE '##';", None, [[None]]),
        ("SELECT 'a' NOT LIKE '%' ESCAPE NULL;", None, [[None]]),
        ("SELECT CASE WHEN false THEN 'a' LIKE '%' ESCAPE '##' ELSE true END;", None, [['t']]),
        ("SELECT 'a'::VARCHAR NOT LIKE 'b';", None, [['t']]),
        ("SELECT 'a'::CHAR(2) LIKE 'a';", None, [['f']]),
        ("SELECT 'a' LIKE 'a'::CHAR(2);", None, [['t']]),
        ("SELECT 'Ann'::NAME ILIKE 'a%';", None, [['t']]),
        ("SELECT 'a#_' LIKE 'a##_' ESCAPE '#';", None, [['t']]),
        ("SELECT 'a_' LIKE 'aé_' ESCAPE 'é';", None, [['t']]),
        (r"SELECT '\x6162'::BYTEA LIKE '\x615f'::BYTEA;", None, [['t']]),
        (r"SELECT '\xc3a9'::BYTEA LIKE '\x5f'::BYTEA;", None, [['f']]),
        (r"SELECT '\xc3a9'::BYTEA LIKE '\x5f5f'::BYTEA;", None, [['t']]),
        (r"SELECT '\xff'::BYTEA NOT LIKE '\x5f'::BYTEA;", None, [['f']]),
        (r"SELECT '\x615f'::BYTEA LIKE '\x61235f'::BYTEA ESCAPE '\x23'::BYTEA;", None, [['t']]),
        (r"SELECT NULL::BYTEA LIKE '\x25'::BYTEA ESCAPE '\x2323'::BYTEA;", '22025', []),
        (r"SELECT '\x61'::BYTEA ILIKE '\x61'::BYTEA WHERE false;", '42883', []),
        (r"SELECT '\x61'::BYTEA LIKE 'a'::TEXT WHERE false;", '42883', []),
        ("SELECT NULL LIKE '%' ESCAPE NULL;", None, [[None]]),
        ("SELECT NULL LIKE (nextval('priority_pattern_calls'))::TEXT ESCAPE '#';", None, [[None]]),
        ("SELECT currval('priority_pattern_calls');", '55000', []),
        ("SELECT NULL LIKE '%' ESCAPE (nextval('priority_pattern_calls'))::TEXT;", None, [[None]]),
        ("SELECT currval('priority_pattern_calls');", '55000', []),
        ("SELECT NULL LIKE '[' ESCAPE '#';", None, [[None]]),
        ("SELECT NULL SIMILAR TO '[' ESCAPE '#';", None, [[None]]),
        ("SELECT NULL LIKE (SELECT (1/0)::TEXT) ESCAPE '#';", None, [[None]]),
        ("SELECT name LIKE '%' ESCAPE '##' FROM priority_pattern_rows WHERE false;", '22025', []),
        ("SELECT name LIKE name ESCAPE '##' FROM priority_pattern_rows WHERE false;", None, []),
        ("SELECT t LIKE 'a%' FROM priority_pattern_domains;", None, [['t']]),
        ("SELECT ch NOT ILIKE 'a%' FROM priority_pattern_domains;", None, [['t']]),
        ("SELECT n LIKE '1' FROM priority_pattern_domains WHERE false;", '42883', []),
        ("WITH q AS(SELECT t,ch FROM priority_pattern_domains) SELECT q.t LIKE 'a%',q.ch NOT LIKE 'a%' FROM q;", None, [['t','t']]),
        ("SELECT c LIKE 'a' FROM priority_pattern_domains;", None, [['f']]),
        ("SELECT 'a' LIKE c FROM priority_pattern_domains;", None, [['t']]),
    ]
    try:
        assert r.decode_wire_result(c.simple_query(sock, 'BEGIN;'))[1] is None
        for sql in (
            'CREATE TEMP TABLE priority_pattern_rows(id INT,name TEXT);',
            'CREATE TEMP SEQUENCE priority_pattern_calls;',
            "INSERT INTO priority_pattern_rows VALUES(1,'ann'),(2,'bob'),(3,NULL);",
            'CREATE DOMAIN priority_pattern_text AS TEXT;',
            'CREATE DOMAIN priority_pattern_chain AS priority_pattern_text;',
            'CREATE DOMAIN priority_pattern_int AS INT;',
            'CREATE DOMAIN priority_pattern_char AS CHAR(2);',
            'CREATE TEMP TABLE priority_pattern_domains(t priority_pattern_text,ch priority_pattern_chain,n priority_pattern_int,c priority_pattern_char);',
            "INSERT INTO priority_pattern_domains VALUES('ann','bob',1,'a');",
        ):
            result = r.decode_wire_result(c.simple_query(sock, sql), include_types=True)
            print('PATTERN_PRIORITY_SETUP', sql, result, flush=True)
            assert result[1] is None, (sql, result)
        for sql, state, rows in controls:
            types = [16] * (len(rows[0]) if rows else 1) if state is None else None
            query(sql, state, rows, types=types)
        query("DELETE FROM priority_pattern_rows AS p WHERE p.name NOT LIKE 'a%' RETURNING p.id;", rows=[['2']], tag='DELETE 1')
        query("DELETE FROM priority_pattern_rows AS p WHERE p.name NOT LIKE 'a%' AND p.id=ANY(SELECT id FROM priority_pattern_rows) RETURNING p.id;", rows=[['2']], tag='DELETE 1')
        query("DELETE FROM priority_pattern_rows AS p WHERE p.name NOT LIKE 'a%' AND p.id=ALL(SELECT q.id FROM priority_pattern_rows AS q WHERE q.id=p.id) RETURNING p.id;", rows=[['2']], tag='DELETE 1')
        query('SELECT id,name FROM priority_pattern_rows ORDER BY id;', rows=[['1','ann'],['2','bob'],['3',None]])
        query('SELECT 1;', rows=[['1']])
        assert not failures, failures
        print('[PATTERN PRIORITY '+('STRICT PG18.6' if reference else 'PROTOCOL')+'] passed', flush=True)
    finally:
        try: c.simple_query(sock, 'ROLLBACK;')
        finally:
            if server: r.stop_ours(server)
            else: sock.close()


if __name__ == '__main__':
    main()
