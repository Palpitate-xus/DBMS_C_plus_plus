#!/usr/bin/env python3
"""Pattern predicates retain BOOL typing, NULLs, qualifications and DML effects."""
import importlib.util
import socket
import struct
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('pattern_runner', root/'tests/compat/pg_diff_runner.py')
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
            print('PATTERN_FAILURE', failures[-1], flush=True)

    def query(sql, state=None, rows=None, tag=None, oids=None):
        c.simple_query(sock, 'SAVEPOINT pattern_case;')
        result = r.decode_wire_result(c.simple_query(sock, sql), include_types=True)
        print('PATTERN', sql, result, flush=True)
        check(sql+' state', result[1], state)
        if rows is not None: check(sql+' rows', result[0], rows)
        if tag is not None: check(sql+' tag', result[4], tag)
        if oids is not None: check(sql+' OIDs', result[5], oids)
        if state is not None:
            check(sql+' no partial result', (result[0], result[4]), ([], None))
        if result[1] is not None: c.simple_query(sock, 'ROLLBACK TO pattern_case;')
        c.simple_query(sock, 'RELEASE pattern_case;')
        return result

    initial = "INSERT INTO pattern_rows VALUES(1,'ann',0),(2,'bob',0),(3,'cat',0),(4,NULL,0),(5,'a_b',0),(6,'',0),(7,'ANN',0);"
    cases = [
        ("name LIKE 'a%'", [1,5]),
        ("name NOT LIKE 'a%'", [2,3,6,7]),
        ("name ILIKE 'A%'", [1,5,7]),
        ("name NOT ILIKE 'A%'", [2,3,6]),
        ("name SIMILAR TO '(ann|cat)'", [1,3]),
        ("name NOT SIMILAR TO '(ann|cat)'", [2,5,6,7]),
        ("name LIKE 'a#_%' ESCAPE '#'", [5]),
        ("name NOT LIKE 'a#_%' ESCAPE '#'", [1,2,3,6,7]),
        ("name NOT ILIKE 'A#_%' ESCAPE '#'", [1,2,3,6,7]),
        ("name NOT LIKE NULL", []),
        ("name NOT SIMILAR TO NULL", []),
    ]
    try:
        c.simple_query(sock, 'BEGIN;')
        query('CREATE TEMP TABLE pattern_rows(id INT,name TEXT,flag INT);')
        query('CREATE TEMP SEQUENCE pattern_calls;')
        query('CREATE TEMP TABLE pattern_arrays(t TEXT[],c CHAR(2)[],b BYTEA[]);')
        for sql in (
            "SELECT t LIKE '%' FROM pattern_arrays WHERE false;",
            "SELECT c NOT ILIKE '%' FROM pattern_arrays LIMIT 0;",
            "SELECT b LIKE '%' ESCAPE '#' FROM pattern_arrays WHERE false;",
            "SELECT NULL::TEXT[] NOT LIKE '%' LIMIT 0;",
            "SELECT ARRAY['a'] SIMILAR TO '%' WHERE false;",
            "UPDATE pattern_arrays SET t=t WHERE t LIKE '%';",
            "DELETE FROM pattern_arrays WHERE b NOT LIKE '%' RETURNING b;",
        ):
            query(sql, state='42883')
        for index, (predicate, ids) in enumerate(cases):
            query('DELETE FROM pattern_rows;'); query(initial)
            sql = 'SELECT '+predicate+' AS p FROM pattern_rows ORDER BY id;'
            projected = [[None if i == 4 or predicate.endswith('NULL') else 't' if i in ids else 'f'] for i in range(1,8)]
            query(sql, rows=projected, oids=[16])
            statement = ('pattern_s'+str(index)).encode(); portal = ('pattern_p'+str(index)).encode()
            c.simple_query(sock, 'SAVEPOINT pattern_extended;')
            sock.sendall(c.typed(b'P', statement+b'\0'+sql.encode()+b'\0'+struct.pack('!H',0))+
                c.typed(b'D', b'S'+statement+b'\0')+
                c.typed(b'B', portal+b'\0'+statement+b'\0'+struct.pack('!HHH',0,0,0))+
                c.typed(b'D', b'P'+portal+b'\0')+
                c.typed(b'E', portal+b'\0'+struct.pack('!I',0))+c.typed(b'S'))
            messages = c.read_until_ready(sock)
            check(sql+' extended error', any(k == b'E' for k,_ in messages), False)
            descriptions = [c.row_description_fields([m]) for m in messages if m[0] == b'T']
            check(sql+' describe count', len(descriptions), 2)
            for fields in descriptions:
                check(sql+' BOOL descriptor', [(f[3],f[4]) for f in fields], [(16,1)])
            check(sql+' extended rows', c.data_row_values(messages), [[None if v is None else v.encode() for v in row] for row in projected])
            if any(k == b'E' for k,_ in messages): c.simple_query(sock, 'ROLLBACK TO pattern_extended;')
            c.simple_query(sock, 'RELEASE pattern_extended;')
            query('SELECT id FROM pattern_rows WHERE '+predicate+' ORDER BY id;', rows=[[str(i)] for i in ids])
            query('UPDATE pattern_rows SET flag=9 WHERE '+predicate+';', tag='UPDATE '+str(len(ids)))
            query('SELECT id,flag FROM pattern_rows ORDER BY id;', rows=[[str(i),'9' if i in ids else '0'] for i in range(1,8)])
            query('DELETE FROM pattern_rows WHERE '+predicate+';', tag='DELETE '+str(len(ids)))
            query('SELECT id FROM pattern_rows ORDER BY id;', rows=[[str(i)] for i in range(1,8) if i not in ids])
            query('DELETE FROM pattern_rows;')
            query('SELECT '+predicate+' AS p FROM pattern_rows;', rows=[], oids=[16])
            query('UPDATE pattern_rows SET flag=nextval(\'pattern_calls\') WHERE '+predicate+';', tag='UPDATE 0')
        query("SELECT currval('pattern_calls');", state='55000')
        query('UPDATE pattern_rows SET flag=nextval(\'pattern_calls\') WHERE 1;', state='42804')
        query("SELECT currval('pattern_calls');", state='55000')
        query("SELECT 'ann' NOT LIKE 'a%',NULL NOT ILIKE 'x%','' NOT SIMILAR TO 'x';", rows=[['f',None,'t']], oids=[16,16,16])
        query('SELECT 1;', rows=[['1']])
        assert not failures, failures
        print('[PATTERN PREDICATE '+('STRICT PG18.6' if reference else 'PROTOCOL')+'] passed', flush=True)
    finally:
        try: c.simple_query(sock, 'ROLLBACK;')
        finally:
            if server: r.stop_ours(server)
            else: sock.close()


if __name__ == '__main__':
    main()
