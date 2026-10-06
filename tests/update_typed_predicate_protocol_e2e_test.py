#!/usr/bin/env python3
"""Typed UPDATE qualifications and SET demand use the old matching row."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('update_predicate_runner', root/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    failures = []

    def query(sql):
        result = runner.decode_wire_result(client.simple_query(server['sock'], sql), include_types=True)
        print('UPDATE_PROBE', sql, result, flush=True)
        return result

    def setup(sql):
        result = query(sql)
        assert result[1] is None, (sql, result)

    def check(label, actual, expected):
        if actual != expected:
            failures.append((label, 'actual', actual, 'expected', expected))
            print('UPDATE_FAILURE', failures[-1], flush=True)

    try:
        setup('CREATE TABLE update_predicate_rows(id INT,v INT);')
        setup('CREATE TABLE update_predicate_effects(id INT);')
        for index, (predicate, set_value, state, rows, effects, called) in enumerate((
            ("id=CAST('1' AS INT)", 'v+10', None, [['1','11'],['2','2'],['3',None]], [], False),
            ('CASE WHEN id=1 THEN true ELSE false END', 'v+10', None, [['1','11'],['2','2'],['3',None]], [], False),
            ('v+1>0', 'v+10', None, [['1','11'],['2','12'],['3',None]], [], False),
            ('false', 'WRITER', None, [['1','1'],['2','2'],['3',None]], [], False),
            ('id<3', 'WRITER', None, [['1','99'],['2','99'],['3',None]], [['99'],['99']], True),
            ('CASE WHEN false THEN missing_value=1 ELSE true END', 'WRITER', '42703', [['1','1'],['2','2'],['3',None]], [], False),
        )):
            setup('DELETE FROM update_predicate_rows;')
            setup('DELETE FROM update_predicate_effects;')
            setup('INSERT INTO update_predicate_rows VALUES(1,1),(2,2),(3,NULL);')
            sequence = 'update_predicate_seq_%s' % index
            function = 'update_predicate_writer_%s' % index
            setup('CREATE SEQUENCE %s;' % sequence)
            setup("CREATE FUNCTION %s(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN INSERT INTO update_predicate_effects VALUES(arg); PERFORM nextval('%s'); RETURN arg; END; $$;" % (function, sequence))
            expression = '%s(99)' % function if set_value == 'WRITER' else set_value
            result = query('UPDATE update_predicate_rows SET v=%s WHERE %s;' % (expression, predicate))
            check('case%s-state' % index, result[1], state)
            check('case%s-rows' % index, query('SELECT id,v FROM update_predicate_rows ORDER BY id;')[0], rows)
            check('case%s-effects' % index, query('SELECT id FROM update_predicate_effects;')[0], effects)
            sequence_result = query("SELECT currval('%s');" % sequence)
            check('case%s-sequence-called' % index, sequence_result[1] is None, called)
            if not called:
                check('case%s-sequence-uncalled-state' % index, sequence_result[1], '55000')
        assert not failures, '%s original UPDATE assertions failed: %r' % (len(failures), failures)
        print('[UPDATE TYPED PREDICATE] all original controls passed')
    finally:
        runner.stop_ours(server)


if __name__ == '__main__':
    main()
