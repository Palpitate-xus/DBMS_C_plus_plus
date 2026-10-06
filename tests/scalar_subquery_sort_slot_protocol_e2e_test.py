#!/usr/bin/env python3
"""Typed SQL-child sort slots share values without merging true target sites."""
import importlib.util
import json
from pathlib import Path

CASES = (
    ('target/sort equivalent', 'SELECT(SELECT sort_slot_writer(74)) FROM sort_slot_rows ORDER BY(SELECT sort_slot_writer(74));', [['-74']]*3, 1),
    ('two genuine SELECT sites', 'SELECT(SELECT sort_slot_writer(75)),(SELECT sort_slot_writer(75)) FROM sort_slot_rows ORDER BY id;', [['-75', '-75']]*3, 2),
    ('two equivalent ORDER keys', 'SELECT id FROM sort_slot_rows ORDER BY(SELECT sort_slot_writer(76)),(SELECT sort_slot_writer(76));', [['1'], ['2'], ['3']], 1),
    ('genuine target ordinals', 'SELECT(SELECT sort_slot_writer(77)),(SELECT sort_slot_writer(77)) FROM sort_slot_rows ORDER BY 1,2;', [['-77', '-77']]*3, 2),
    ('output alias slot', 'SELECT(SELECT sort_slot_writer(78)) AS v FROM sort_slot_rows ORDER BY v,v;', [['-78']]*3, 1),
    ('whitespace slot', 'SELECT(SELECT sort_slot_writer(79)) FROM sort_slot_rows ORDER BY( SELECT sort_slot_writer ( 79 ) );', [['-79']]*3, 1),
    ('canonical integer', 'SELECT(SELECT sort_slot_writer(80)) FROM sort_slot_rows ORDER BY(SELECT sort_slot_writer(080));', [['-80']]*3, 1),
    ('canonical cast', 'SELECT(SELECT sort_slot_writer(CAST(81 AS INT))) FROM sort_slot_rows ORDER BY(SELECT sort_slot_writer(CAST(81 AS INTEGER)));', [['-81']]*3, 1),
    ('same source provenance', 'SELECT(SELECT sort_slot_writer(id) FROM sort_slot_rows s WHERE s.id=1) FROM sort_slot_rows ORDER BY(SELECT sort_slot_writer(s.id) FROM sort_slot_rows s WHERE s.id=1);', [['-1']]*3, 1),
    ('renamed source alias stays separate', 'SELECT(SELECT sort_slot_writer(s.id) FROM sort_slot_rows s WHERE s.id=1) FROM sort_slot_rows ORDER BY(SELECT sort_slot_writer(t.id) FROM sort_slot_rows t WHERE t.id=1);', [['-1']]*3, 2),
    ('alias versus unaliased stays separate', 'SELECT(SELECT sort_slot_writer(s.id) FROM sort_slot_rows s WHERE s.id=1) FROM sort_slot_rows ORDER BY(SELECT sort_slot_writer(id) FROM sort_slot_rows WHERE id=1);', [['-1']]*3, 2),
    ('true ancestor provenance', 'SELECT(SELECT sort_slot_writer(o.id)) FROM sort_slot_rows o ORDER BY(SELECT sort_slot_writer(o.id));', [['-3'], ['-2'], ['-1']], 3),
    ('different child literals stay separate', 'SELECT(SELECT sort_slot_writer(82)) FROM sort_slot_rows ORDER BY(SELECT sort_slot_writer(83));', [['-82']]*3, 2),
    ('duplicate order directions share value', 'SELECT id FROM sort_slot_rows ORDER BY(SELECT sort_slot_writer(84)) ASC,(SELECT sort_slot_writer(84)) DESC;', [['1'], ['2'], ['3']], 1),
)


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('sort_slot_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    failures = []

    def query(sql):
        messages = client.simple_query(server['sock'], sql)
        result = runner.decode_wire_result(messages, include_types=True)
        assert messages[-1] == (b'Z', b'I'), (sql, messages[-1])
        return result

    def ok(sql):
        result = query(sql)
        assert result[1] is None, (sql, result)
        return result

    def calls():
        return int(ok("SELECT currval('sort_slot_sequence');")[0][0][0])

    try:
        for sql in (
            'CREATE TABLE sort_slot_rows(id INT);',
            'INSERT INTO sort_slot_rows VALUES(1),(2),(3);',
            'CREATE SEQUENCE sort_slot_sequence START 1;',
            "CREATE FUNCTION sort_slot_writer(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('sort_slot_sequence'); RETURN -arg; END; $$;",
        ): ok(sql)
        ok("SELECT nextval('sort_slot_sequence');")
        for mode in ('ordinary', 'plain', 'text', 'json'):
            for label, sql, rows, expected in CASES:
                before = calls()
                prefix = '' if mode == 'ordinary' else 'EXPLAIN ' if mode == 'plain' else 'EXPLAIN ANALYZE ' if mode == 'text' else 'EXPLAIN(ANALYZE,FORMAT JSON) '
                result = query(prefix + sql)
                actual = calls() - before
                good = result[1] is None and actual == (0 if mode == 'plain' else expected)
                if mode == 'ordinary': good = good and result[0] == rows
                elif result[1] is None:
                    good = good and bool(result[0])
                    if mode == 'json':
                        plan = json.loads('\n'.join(row[0] for row in result[0]))
                        good = good and plan['actualRows'] == 3
                evidence = (mode, label, 'calls', actual, 'expected', 0 if mode == 'plain' else expected, 'state', result[1], 'rows', result[0])
                print('SCALAR_SORT_SLOT', 'PASS' if good else 'FAIL', evidence, flush=True)
                if not good: failures.append(evidence)
        assert not failures, failures
        print('[SCALAR SUBQUERY SORT SLOT PROTOCOL E2E] passed', flush=True)
    finally:
        runner.stop_ours(server)


if __name__ == '__main__':
    main()
