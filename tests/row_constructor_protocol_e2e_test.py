"""ROW constructors retain typed fields, record output and SQL row demand."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('row_constructor_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = '--reference18' in sys.argv
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server['sock']
    checked = 0
    failures = []

    def check(sql, rows=None, oids=None, state=None, names=None):
        nonlocal checked
        checked += 1
        if reference and state:
            assert runner.decode_wire_result(client.simple_query(sock, 'SAVEPOINT row_error'))[1] is None
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        valid = result[1] == state
        if rows is not None:
            valid = valid and result[0] == rows and result[4] == 'SELECT ' + str(len(rows))
        if oids is not None:
            valid = valid and result[5] == oids
        if names is not None:
            valid = valid and result[3] == names
        if state:
            valid = valid and result[0] == [] and result[4] is None
        print('ROW_CONSTRUCTOR', sql, result, 'pass=', valid, flush=True)
        if not valid:
            failures.append((sql, result, rows, oids, state))
        if reference and state:
            assert runner.decode_wire_result(client.simple_query(sock, 'ROLLBACK TO row_error'))[1] is None
            assert runner.decode_wire_result(client.simple_query(sock, 'RELEASE row_error'))[1] is None

    try:
        if reference:
            check('BEGIN')
        check("SELECT ROW(1,'a'), ROW(), ROW(NULL,''), (1,'a')",
              [['(1,a)','()','(,"")','(1,a)']], [2249]*4, names=['row']*4)
        check("""SELECT ROW('a,b','a"b','a\\b',' a ')""",
              [['("a,b","a""b","a\\\\b"," a ")']], [2249], names=['row'])
        check("SELECT ROW(ROW(1,'a'),ARRAY[1,2])", [['("(1,a)","{1,2}")']], [2249], names=['row'])
        check("SELECT CASE WHEN true THEN ROW(1,'a') ELSE ROW(2,'b') END", [['(1,a)']], [2249], names=['row'])
        check("SELECT (SELECT min(ROW(1,'a')))", [['(1,a)']], [2249], names=['min'])
        check("SELECT (SELECT min(v) FROM(VALUES(ROW(2,'b')),(ROW(1,'a'))) AS t(v))", [['(1,a)']], [2249])
        check("SELECT (SELECT min(v) FROM(VALUES(ROW(1,'b'::TEXT)),(ROW(1,'a'::TEXT))) AS t(v))", [['(1,a)']], [2249])
        check("SELECT (SELECT min(v) FROM(VALUES(ROW(1,'b')),(ROW(1,'a'))) AS t(v))", state='42883')
        check("SELECT (SELECT min(v) FROM(VALUES(ROW(1,NULL::INT)),(ROW(1,2))) AS t(v))", [['(1,2)']], [2249])
        check("SELECT min(v) FROM(VALUES(ROW(NULL)),(ROW(NULL))) AS t(v)", state='42883')
        check("SELECT min(v) FROM(VALUES(ROW(NULL::INT)),(ROW(NULL::INT))) AS t(v)", [['()']], [2249], names=['min'])
        check("SELECT ROW(ROW(NULL))=ROW(ROW(NULL))", state='42883')
        check("SELECT ROW(ROW(NULL::INT))=ROW(ROW(NULL::INT))", [['t']], [16])
        check("SELECT min(v) FROM(VALUES(ROW(1,NULL)),(ROW(2,NULL))) AS t(v)", [['(1,)']], [2249])
        check("SELECT ROW(ROW(1,NULL))<ROW(ROW(2,NULL))", [['t']], [16])
        check("SELECT ROW(1)::record, ROW(1)::TEXT, ROW(1)::VARCHAR(8), ROW(1)::CHAR(8), ROW(1)::NAME",
              [['(1)','(1)','(1)','(1)     ','(1)']], [2249,25,1043,1042,19], names=['row']*5)
        for target in ['INT','NUMERIC','BOOLEAN','JSON','JSONB','"char"']:
            check('SELECT ROW(1)::' + target + ' WHERE false', state='42846')
        for source in ['INT','NUMERIC','BOOLEAN','JSON','"char"']:
            check('SELECT NULL::' + source + '::record WHERE false', state='42846')
        check("SELECT '(1)'::record", state='0A000')
        check("SELECT '(1)'::record WHERE false", state='0A000')
        check("SELECT '(1)'::TEXT::record", state='0A000')
        check("SELECT '(1)'::TEXT::record WHERE false", [], [2249], names=['record'])
        check("SELECT CAST('(1)'::TEXT AS record) WHERE false", [], [2249], names=['record'])
        check("SELECT CAST('(1)'::TEXT AS record)", state='0A000')
        check("SELECT NULL::record, NULL::TEXT::record, NULL::VARCHAR::record, NULL::NAME::record",
              [[None]*4], [2249]*4, names=['record']*4)
        check("SELECT ROW(1,2)<ROW(2,1), ROW(2,1)>ROW(1,2), ROW(1,2)<=ROW(1,2), ROW(1,2)>=ROW(1,2)", [['t']*4], [16]*4)
        check("SELECT ROW(1,NULL)=ROW(1,NULL), ROW(NULL,2)=ROW(NULL,3), ROW(NULL,2)<>ROW(NULL,3)", [[None,'f','t']], [16]*3)
        check("SELECT ROW(NULL,2)<ROW(1,3), ROW(1,NULL)>ROW(1,2)", [[None,None]], [16,16])
        check("SELECT ROW(1,'a')=ROW(1,'a'), ROW(1,'2')=ROW(1,2)", [['t','t']], [16,16])
        check("SELECT ROW(NULL,1) IS NULL, ROW(NULL,1) IS NOT NULL, ROW(NULL,NULL) IS NULL, ROW(1,2) IS NOT NULL, ROW() IS NULL, ROW() IS NOT NULL", [['f','f','t','t','t','t']], [16]*6)
        check("SELECT ROW(1) IN(ROW(1),ROW(2)), ROW(1,NULL) IN(ROW(1,NULL),ROW(2,1)), ROW(1,NULL) NOT IN(ROW(1,NULL),ROW(2,1))", [['t',None,None]], [16]*3)
        check("SELECT ROW(1)=ROW(1,2)", state='42601')
        check("SELECT ROW()=ROW()", state='0A000')
        check('SELECT "ROW"(1)', state='42883')
        check('SELECT pg_catalog.ROW(1)', state='42883')
        check("CREATE SEQUENCE row_constructor_effects")
        check("SELECT nextval('row_constructor_effects')", [['1']], [20])
        check("SELECT ROW(1,nextval('row_constructor_effects'))<ROW(2,nextval('row_constructor_effects'))", [['t']], [16])
        check("SELECT ROW(1,nextval('row_constructor_effects'))=ROW(2,nextval('row_constructor_effects'))", [['f']], [16])
        check("SELECT currval('row_constructor_effects')", [['1']], [20])
        check("SELECT ROW(nextval('row_constructor_effects'),1) IN(ROW(100,2),ROW(101,2))", [['f']], [16])
        check("SELECT currval('row_constructor_effects')", [['1']], [20])
        check("SELECT ROW(nextval('row_constructor_effects'),1) IN(ROW(5,1),ROW(6,1))", [['f']], [16])
        check("SELECT currval('row_constructor_effects')", [['3']], [20])
        check("SELECT ROW(1,1/0)<ROW(2,1)", state='22012')
        print('ROW_CONSTRUCTOR_COMPLETE', checked, 'FAILED', len(failures), failures, flush=True)
        assert not failures, failures
    finally:
        if reference:
            try:
                client.simple_query(sock, 'ROLLBACK')
            finally:
                sock.close()
        else:
            runner.stop_ours(server)


if __name__ == '__main__':
    main()
