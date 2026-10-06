#!/usr/bin/env python3
"""Fromless qualifications and limits control real volatile projection demand."""
import importlib.util
from pathlib import Path
import socket
import sys


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('fromless_demand_runner', root/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = '--reference18' in sys.argv[1:]
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=15)
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
        server = {'sock': sock}
    else:
        server = runner.start_ours(client)
    failures = []

    def query(sql):
        result = runner.decode_wire_result(client.simple_query(server['sock'], sql), include_types=True)
        print('FROMLESS_DEMAND', sql, result, flush=True)
        return result

    def check(label, actual, expected):
        if actual != expected:
            failures.append((label, actual, expected))
            print('FROMLESS_DEMAND_FAILURE', failures[-1], flush=True)

    def setup(sql):
        result = query(sql)
        assert result[1] is None, (sql, result)

    def counter():
        result = query("SELECT currval('fromless_demand_seq');")
        assert result[1] is None, result
        return int(result[0][0][0])

    try:
        setup('BEGIN;')
        setup('CREATE TEMP SEQUENCE fromless_demand_seq;')
        setup('CREATE TEMP TABLE fromless_demand_effects(id INT);')
        setup("CREATE FUNCTION fromless_demand_writer(arg INT) RETURNS INT AS $$ BEGIN INSERT INTO fromless_demand_effects VALUES(arg); PERFORM nextval('fromless_demand_seq'); RETURN arg; END; $$ LANGUAGE plpgsql;")
        setup("SELECT nextval('fromless_demand_seq');")
        cases = (
            ('false nextval', "SELECT nextval('fromless_demand_seq') WHERE false;", None, [], [20], 0),
            ('null nextval', "SELECT nextval('fromless_demand_seq') WHERE NULL;", None, [], [20], 0),
            ('false writer', 'SELECT fromless_demand_writer(11) WHERE false;', None, [], [23], 0),
            ('null writer', 'SELECT fromless_demand_writer(12) WHERE CAST(NULL AS BOOLEAN);', None, [], [23], 0),
            ('zero limit', 'SELECT fromless_demand_writer(13) LIMIT 0;', None, [], [23], 0),
            ('zero predicate demand', 'SELECT fromless_demand_writer(14) WHERE fromless_demand_writer(15)>0 LIMIT 0;', None, [], [23], 0),
            ('offset one', 'SELECT fromless_demand_writer(16) OFFSET 1;', None, [], [23], 1),
            ('offset two', 'SELECT fromless_demand_writer(17) OFFSET 2;', None, [], [23], 1),
            ('false composite', 'SELECT fromless_demand_writer(18)+1,fromless_demand_writer(19) WHERE false;', None, [], [23, 23], 0),
            ('predicate-only demand', 'SELECT fromless_demand_writer(20) WHERE fromless_demand_writer(21)<0;', None, [], [23], 1),
            ('positive demand', 'SELECT fromless_demand_writer(22) WHERE true;', None, [['22']], [23], 1),
            ('positive composite', 'SELECT fromless_demand_writer(23)+1 WHERE true;', None, [['24']], [23], 1),
            ('same order slot', 'SELECT fromless_demand_writer(24) AS v ORDER BY v LIMIT 1;', None, [['24']], [23], 1),
            ('distinct order site', 'SELECT fromless_demand_writer(25) ORDER BY fromless_demand_writer(26) LIMIT 1;', None, [['25']], [23], 2),
            ('unknown function false', 'SELECT fromless_demand_writer(27),missing_fromless_demand_function(1) WHERE false;', '42883', [], None, 0),
            ('unknown function zero', 'SELECT fromless_demand_writer(28),missing_fromless_demand_function(1) LIMIT 0;', '42883', [], None, 0),
            ('unknown column false', 'SELECT fromless_demand_writer(29),missing_fromless_demand_column WHERE false;', '42703', [], None, 0),
            ('invalid input false', "SELECT fromless_demand_writer(30),CAST('bad' AS INT) WHERE false;", '22P02', [], None, 0),
            ('constant divide false', 'SELECT fromless_demand_writer(31),1/0 WHERE false;', '22012', [], None, 0),
            ('constant divide zero', 'SELECT fromless_demand_writer(32),1/0 LIMIT 0;', '22012', [], None, 0),
            ('false CASE dead constant', 'SELECT CASE WHEN false THEN 1/0 ELSE fromless_demand_writer(33) END WHERE false;', None, [], [23], 0),
            ('false CASE runtime guard', 'SELECT CASE WHEN fromless_demand_writer(34)>0 THEN 1/0 ELSE 1 END WHERE false;', '22012', [], None, 0),
            ('scalar child false', 'SELECT (SELECT fromless_demand_writer(35)) WHERE false;', None, [], [23], 0),
            ('scalar constant false', 'SELECT (SELECT 1/0) WHERE false;', '22012', [], None, 0),
        )
        effects = []
        for label, sql, state, rows, oids, demand in cases:
            before = counter()
            setup('SAVEPOINT fromless_demand_case;')
            result = query(sql)
            check(label+' state', result[1], state)
            check(label+' rows', result[0], rows)
            if state is None:
                check(label+' types', result[5], oids)
                check(label+' tag', result[4], 'SELECT '+str(len(rows)))
            else:
                check(label+' no completion', result[4], None)
            if result[1] is not None:
                setup('ROLLBACK TO fromless_demand_case;')
            setup('RELEASE SAVEPOINT fromless_demand_case;')
            check(label+' real sequence demand', counter()-before, demand)
            if state is None:
                if label == 'offset one': effects += [['16']]
                elif label == 'offset two': effects += [['17']]
                elif label == 'predicate-only demand': effects += [['21']]
                elif label == 'positive demand': effects += [['22']]
                elif label == 'positive composite': effects += [['23']]
                elif label == 'same order slot': effects += [['24']]
                elif label == 'distinct order site': effects += [['26'], ['25']]
            result = query('SELECT id FROM fromless_demand_effects ORDER BY id;')
            check(label+' actual writer effects', result[0], sorted(effects, key=lambda row: int(row[0])))
        setup('ROLLBACK;')
        assert not failures, failures
        print('[FROMLESS SELECT DEMAND '+('PG18.6 REFERENCE' if reference else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == '__main__':
    main()

