#!/usr/bin/env python3
"""Fixed float8 overloads resolve before demand and aggregate/window roles."""
import importlib.util
from pathlib import Path
import socket
import sys
import uuid


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('float8_signature_runner', root / 'tests/compat/pg_diff_runner.py')
    r = importlib.util.module_from_spec(spec); spec.loader.exec_module(r)
    c = r.load_protocol_client()
    reference = sys.argv[1:] == ['--reference18']
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        h, p, u, d, pw = r._reference_connection_settings()
        sock = socket.create_connection((h, p), timeout=r.wire_timeout())
        c.startup_reference(sock, u, d, pw); r.verify_reference_version(c, sock)
    else:
        server = r.start_ours(c); sock = server['sock']
    schema = 'float8_signature_' + uuid.uuid4().hex[:12]
    checks, failures, created = 0, [], False

    def check(sql, state=None, rows=None, oids=None):
        nonlocal checks
        messages = c.simple_query(sock, sql)
        result = r.decode_wire_result(messages, include_types=True)
        good = result[1] == state and (rows is None or result[0] == rows)
        good = good and (oids is None or result[5] == oids)
        good = good and messages[-1] == (b'Z', b'I')
        if state:
            good = good and not result[0] and result[4] is None
            good = good and sum(kind == b'E' for kind, _ in messages) == 1
            good = good and not any(kind in (b'T', b'D', b'C') for kind, _ in messages)
        checks += 1
        print('FLOAT8_SIGNATURE', sql, result, 'PASS', good, flush=True)
        if not good: failures.append((sql, result, state, rows, oids))
        return good

    def invalid(expression, suffixes):
        for suffix in suffixes:
            check('SELECT ' + expression + suffix, state='42883')
            check('SELECT ' + expression + ' OVER()' + suffix, state='42883')

    def good(expression, value, suffixes):
        for suffix in suffixes:
            check('SELECT ' + expression + suffix, rows=[] if suffix else [[value]], oids=[701])

    unary = (
        ('sin', '0', '0'), ('cos', '0', '1'), ('tan', '0', '0'),
        ('asin', '0', '0'), ('acos', '1', '0'), ('atan', '0', '0'),
        ('sinh', '0', '0'), ('cosh', '0', '1'), ('tanh', '0', '0'),
        ('asinh', '0', '0'), ('acosh', '1', '0'), ('atanh', '0', '0'),
        ('degrees', '0', '0'), ('radians', '0', '0'),
        ('cot', '1', '0.6420926159343306'), ('cbrt', '8', '2'))
    try:
        assert check('CREATE SCHEMA ' + schema)
        created = True
        assert check('CREATE TABLE ' + schema + '.empty_rows(i INT,t TEXT)')
        table = schema + '.empty_rows'
        suffixes = ('', ' FROM ' + table + ' WHERE false', ' FROM ' + table + ' LIMIT 0')
        for name, value, expected in unary:
            for argument in ('', '0,0', "'0'::text", 'true', 'NULL::text'):
                invalid(name + '(' + argument + ')', suffixes)
            for suffix in suffixes:
                check('SELECT ' + name + "('bad')" + suffix, state='22P02')
                check('SELECT ' + name + "('bad') OVER()" + suffix, state='42809')
                check('SELECT ' + name + '(' + value + ') OVER()' + suffix, state='42809')
            invalid(name + '(t)', (' FROM ' + table + ' WHERE false', ' FROM ' + table + ' LIMIT 0'))
            for argument, result in ((value + '::integer', expected), (value + '::numeric', expected),
                                     ('NULL::double precision', None)):
                good(name + '(' + argument + ')', result, suffixes)
        for argument in ('', '0', '0,1,2', "'0'::text,1", '0,true', 'NULL::text,1'):
            invalid('atan2(' + argument + ')', suffixes)
        for suffix in suffixes:
            check("SELECT atan2('bad',1)" + suffix, state='22P02')
            check("SELECT atan2('bad',1) OVER()" + suffix, state='42809')
            check('SELECT atan2(0,1) OVER()' + suffix, state='42809')
        for argument, result in (('0,1', '0'), ('0::numeric,1::numeric', '0'),
                                 ("'0','1'", '0'), ('NULL::real,1', None)):
            good('atan2(' + argument + ')', result, suffixes)
        for argument in ('1', 'NULL', "'bad'"):
            invalid('pi(' + argument + ')', suffixes)
        good('pi()', '3.141592653589793', suffixes)
        for suffix in suffixes:
            check('SELECT pi() OVER()' + suffix, state='42809')
        assert check('CREATE FUNCTION ' + schema + '.sin(i INT) RETURNS INT LANGUAGE SQL IMMUTABLE AS $$SELECT i+10$$')
        assert check('SET search_path=pg_catalog,' + schema)
        check('SELECT sin(1)', rows=[['11']], oids=[23])
        check('SELECT ' + schema + '.sin(1)', rows=[['11']], oids=[23])
        check('SELECT pg_catalog.sin(0)', rows=[['0']], oids=[701])
        check('SELECT ' + schema + '.sin(1::double precision)', state='42883')
        check('SELECT sin(1) OVER()', state='42809')
        check('SELECT sin(0::numeric)', rows=[['0']], oids=[701])
        check("SELECT sin('0')", rows=[['0']], oids=[701])
        check('SELECT pg_catalog."Sin"(0)', state='42883')
        check('SELECT abs(cbrt(8))', rows=[['2']], oids=[701])
        check('SELECT abs(sin(0::numeric))', rows=[['0']], oids=[701])
        check('SELECT abs(' + schema + '.sin(1))', rows=[['11']], oids=[23])
        assert check('CREATE FUNCTION ' + schema + '.cos(t TEXT) RETURNS TEXT LANGUAGE SQL IMMUTABLE AS $$SELECT t$$')
        check("SELECT cos('payload')", rows=[['payload']], oids=[25])
        check('SELECT cos(NULL)', rows=[[None]], oids=[25])
        check('SELECT cos(0)', rows=[['1']], oids=[701])
        check("SELECT pg_catalog.cos('payload')", state='22P02')
        check("SELECT length(cos('payload'))", rows=[['7']], oids=[23])
        for suffix in (' WHERE false', ' LIMIT 0'):
            check('SELECT cos(t) FROM ' + table + suffix, rows=[], oids=[25])
        assert check('PREPARE text_cos(TEXT) AS SELECT cos($1)')
        check("EXECUTE text_cos('payload')", rows=[['payload']], oids=[25])
        check('DEALLOCATE text_cos')
        check('SELECT pg_typeof(cos(NULL))::text', rows=[['text']], oids=[25])
        check("SELECT cos(t) FROM(SELECT 'payload'::text AS t)s", rows=[['payload']], oids=[25])
        check('SELECT ' + schema + ".cos('payload')", rows=[['payload']], oids=[25])
        check("SELECT cos('payload') OVER()", state='42809')
        check('PREPARE float8_bad(TEXT) AS SELECT pg_catalog.sin($1)', state='42883')
        assert check('PREPARE float8_good(NUMERIC) AS SELECT pg_catalog.sin($1)')
        check('EXECUTE float8_good(0)', rows=[['0']], oids=[701])
        check('DEALLOCATE float8_good')
        assert check('CREATE SEQUENCE ' + schema + '.effects')
        assert check('CREATE FUNCTION ' + schema + ".effect() RETURNS DOUBLE PRECISION VOLATILE LANGUAGE plpgsql AS $$BEGIN PERFORM nextval('" + schema + ".effects'); RETURN 0; END;$$")
        check('SELECT pg_catalog.sin(' + schema + '.effect()) FROM ' + table + ' LIMIT 0', rows=[], oids=[701])
        check("SELECT nextval('" + schema + ".effects')", rows=[['1']], oids=[20])
        print('FLOAT8_SIGNATURE_COMPLETE', checks, 'FAILURES', len(failures), flush=True)
        assert checks == 951 and not failures, failures
    finally:
        if created:
            c.simple_query(sock, 'RESET search_path')
            c.simple_query(sock, 'DROP SCHEMA ' + schema + ' CASCADE')
        if server: r.stop_ours(server)
        else: sock.close()


if __name__ == '__main__':
    main()
