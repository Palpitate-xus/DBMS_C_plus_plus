"""Global aggregate children retain actual typed inputs and query ownership."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('global_aggregate_runner', root / 'tests/compat/pg_diff_runner.py')
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
    failures = []
    checked = 0

    def check(sql, rows=None, oids=None, state=None):
        nonlocal checked
        checked += 1
        if reference and state is not None:
            assert runner.decode_wire_result(client.simple_query(sock, 'SAVEPOINT aggregate_error'))[1] is None
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        valid = result[1] == state
        if rows is not None:
            valid = valid and result[0] == rows and result[4] == 'SELECT ' + str(len(rows))
        if oids is not None:
            valid = valid and result[5] == oids
        if state is not None:
            valid = valid and result[0] == [] and result[4] is None
        print('GLOBAL_AGGREGATE', sql, result, 'pass=', valid, flush=True)
        if not valid:
            failures.append((sql, result, rows, oids, state))
        if reference and state is not None:
            assert runner.decode_wire_result(client.simple_query(sock, 'ROLLBACK TO aggregate_error'))[1] is None
            assert runner.decode_wire_result(client.simple_query(sock, 'RELEASE aggregate_error'))[1] is None

    try:
        if reference:
            check('BEGIN')
        check('CREATE TABLE aggregate_child_rows(id INT,v NUMERIC(20,2),b BOOL,t TEXT)')
        check("INSERT INTO aggregate_child_rows VALUES(1,10,true,'NULL'),(2,20,false,''),(3,NULL,NULL,NULL),(4,10,true,'z')")
        for expression, rows, oids in [
            ('count(*)', [['4']], [20]),
            ('count(v)', [['3']], [20]),
            ('count(DISTINCT v)', [['2']], [20]),
            ('sum(v)', [['40.00']], [1700]),
            ('sum(DISTINCT v)', [['30.00']], [1700]),
            ('avg(v)', [['13.3333333333333333']], [1700]),
            ('min(t)', [['']], [25]),
            ('max(t)', [['z']], [25]),
            ('bool_and(b)', [['f']], [16]),
            ('bool_or(b)', [['t']], [16]),
            ('every(b)', [['f']], [16]),
            ('count(*) FILTER(WHERE b)', [['2']], [20]),
            ('sum(v) FILTER(WHERE b)', [['20.00']], [1700]),
            ('count(DISTINCT t)', [['3']], [20]),
            ('sum(v*2)', [['80.00']], [1700]),
        ]:
            check('SELECT (SELECT ' + expression + ' FROM aggregate_child_rows) AS value', rows, oids)
        check('SELECT (SELECT count(*) FROM aggregate_child_rows WHERE false),(SELECT sum(v) FROM aggregate_child_rows WHERE false)', [['0', None]], [20,1700])
        check('SELECT (SELECT count(*)+count(v) FROM aggregate_child_rows)', [['7']], [20])
        check('SELECT (SELECT count(*) FROM aggregate_child_rows HAVING count(*)>4)', [[None]], [20])
        check('SELECT (SELECT count(*) FROM aggregate_child_rows HAVING count(*)=4)', [['4']], [20])
        check('SELECT r.id,(SELECT count(*) FROM aggregate_child_rows q WHERE q.id<=r.id) AS n FROM aggregate_child_rows r ORDER BY r.id', [['1','1'],['2','2'],['3','3'],['4','4']], [23,20])
        check('SELECT (WITH q AS(VALUES(1),(2)) SELECT count(*) FROM q)', [['2']], [20])
        check('CREATE SEQUENCE aggregate_child_effects')
        check("SELECT nextval('aggregate_child_effects')", [['1']], [20])
        check("SELECT (SELECT count(nextval('aggregate_child_effects')) FROM aggregate_child_rows WHERE false)", [['0']], [20])
        check("SELECT (SELECT count(nextval('aggregate_child_effects')) FILTER(WHERE false) FROM aggregate_child_rows)", [['0']], [20])
        check("SELECT currval('aggregate_child_effects')", [['1']], [20])
        check("SELECT (SELECT count(nextval('aggregate_child_effects')) FROM aggregate_child_rows)", [['4']], [20])
        check("SELECT currval('aggregate_child_effects')", [['5']], [20])
        check("SELECT (SELECT CASE WHEN true THEN 1 ELSE sum(nextval('aggregate_child_effects')) END FROM aggregate_child_rows) AS value", [['1']], [1700])
        check("SELECT currval('aggregate_child_effects')", [['5']], [20])
        check('SELECT (SELECT CASE WHEN false THEN sum(1/0) ELSE 1 END FROM aggregate_child_rows) AS value', [['1']], [20])
        check("SELECT EXISTS(SELECT sum(nextval('aggregate_child_effects')) FROM aggregate_child_rows)", [['t']], [16])
        check("SELECT currval('aggregate_child_effects')", [['9']], [20])
        check("SELECT EXISTS(SELECT sum(nextval('aggregate_child_effects')) FROM aggregate_child_rows HAVING count(*)>0)", [['t']], [16])
        check("SELECT currval('aggregate_child_effects')", [['13']], [20])
        check('SELECT EXISTS(SELECT sum(v) FROM aggregate_child_rows WHERE false)', [['t']], [16])
        for expression, rows, oids in [
            ("min('aa'::VARCHAR)", [['aa']], [25]),
            ("min('aa'::NAME)", [['aa']], [25]),
            ("min('a'::\"char\")", [['a']], [25]),
            ("min('10.2.3.0/24'::CIDR)", [['10.2.3.0/24']], [869]),
            ('min(ARRAY[2,1])', [['{2,1}']], [1007]),
            ("min(ROW(1,'a'))", [['(1,a)']], [2249]),
            ('min(NULL)', [[None]], [25]),
        ]:
            check('SELECT (SELECT ' + expression + ') AS value', rows, oids)
        for expression in ["min('{}'::JSON)", "min('{}'::JSONB)", "min(B'01')",
                           "min('00000000-0000-0000-0000-000000000000'::UUID)"]:
            check('SELECT (SELECT ' + expression + ' WHERE false)', state='42883')
        check("SELECT (SELECT count(nextval('aggregate_child_effects')) FROM aggregate_child_rows LIMIT 0)", [[None]], [20])
        check("SELECT currval('aggregate_child_effects')", [['13']], [20])
        check('SELECT (SELECT sum(t) FROM aggregate_child_rows WHERE false)', state='42883')
        check('SELECT (SELECT id+count(*) FROM aggregate_child_rows)', state='42803')
        check('SELECT (SELECT count(count(*)) FROM aggregate_child_rows)', state='42803')
        check('SELECT (SELECT count(*) FROM aggregate_child_rows WHERE count(*)>0)', state='42803')
        print('GLOBAL_AGGREGATE_COMPLETE', checked, 'FAILED', len(failures), failures, flush=True)
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
