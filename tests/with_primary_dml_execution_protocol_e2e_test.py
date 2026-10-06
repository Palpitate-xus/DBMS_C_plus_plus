#!/usr/bin/env python3
"""Genuine WITH-final DML: typed CTE rows, fixed command view and atomicity."""
import importlib.util
from pathlib import Path
import socket
import sys


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('with_dml_runner', root/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference18 = '--reference18' in sys.argv[1:]
    if reference18 and '--reference' in sys.argv[1:]:
        raise ValueError('choose PostgreSQL18.6 reference or PG17.2 diagnostic, not both')
    reference = reference18 or '--reference' in sys.argv[1:]
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=120)
        client.startup_reference(sock, user, database, password)
        server = {'sock': sock}
    else:
        server = runner.start_ours(client)
    failures = []

    def query(sql, ready=None):
        messages = client.simple_query(server['sock'], sql)
        result = runner.decode_wire_result(messages, include_types=True)
        if ready is not None and messages[-1] != (b'Z', ready):
            failures.append(('ready', sql, messages[-1], ready))
        print('WITH_DML', sql, result, flush=True)
        return result

    def check(label, sql, rows=None, state=None, types=None, tag=None, ready=None):
        result = query(sql, ready)
        good = result[1] == state
        if rows is not None: good = good and result[0] == rows
        if types is not None: good = good and result[5] == types
        if tag is not None: good = good and result[4] == tag
        if state is not None: good = good and not result[0] and result[4] is None
        if not good: failures.append((label, 'expected', rows, state, types, tag, 'actual', result))
        print('WITH_DML_CONTROL', label, 'PASS' if good else 'FAIL', flush=True)
        return result

    def setup(sql):
        result = query(sql); assert result[1] is None, (sql, result)

    try:
        if reference18:
            runner.verify_reference_version(client, sock)
        elif reference:
            assert query('SHOW server_version_num;')[0] == [['170002']]
        for sql in (
            'CREATE TEMP TABLE with_source(id INT PRIMARY KEY,t TEXT);',
            "INSERT INTO with_source VALUES(1,'old');",
            'CREATE TEMP TABLE with_target(id INT PRIMARY KEY,a INT,b INT,t TEXT);',
            'CREATE TEMP TABLE with_pairs(a INT,b INT);',
            'CREATE TEMP TABLE with_effects(id INT);',
            'CREATE TEMP SEQUENCE with_read_seq;',
            'CREATE TEMP SEQUENCE with_write_seq;',
        ): setup(sql)
        check('physical-target-shadow', "WITH with_target AS(SELECT 999 AS id) INSERT INTO with_target VALUES(1,10,20,'') RETURNING id,a,b,t;",
              [['1', '10', '20', '']], types=[23, 23, 23, 25], tag='INSERT 0 1')
        check('duplicate-output-labels', 'WITH c AS(SELECT 2 AS id,3 AS id) INSERT INTO with_pairs SELECT c.* FROM c RETURNING a,b;',
              [['2', '3']], types=[23, 23], tag='INSERT 0 1')
        check('nullable-values-cte', "WITH c(id,t) AS(VALUES(2,NULL),(3,''),(4,'NULL')) INSERT INTO with_target(id,t) SELECT c.* FROM c RETURNING id,t;",
              [['2', None], ['3', ''], ['4', 'NULL']], types=[23, 25], tag='INSERT 0 3')
        check('quoted-logical-alias', 'WITH "Mixed Range"("Value Name") AS(SELECT 5) INSERT INTO with_target(id) SELECT "r.a"."Value Name" FROM "Mixed Range" AS "r.a" RETURNING id;',
              [['5']], types=[23])
        check('simultaneous-old-row-update', 'WITH input AS(SELECT 1) UPDATE with_target AS x SET a=x.b,b=x.a WHERE x.id=1 RETURNING id,a,b,t;',
              [['1', '20', '10', '']], types=[23, 23, 23, 25], tag='UPDATE 1')
        check('nullable-old-row-update', 'WITH input AS(SELECT 1) UPDATE with_target SET a=7 WHERE id=2 RETURNING id,a,t;',
              [['2', '7', None]])
        check('correlated-set-child', 'WITH input AS(SELECT 1) UPDATE with_target AS x SET a=(SELECT x.a+1) WHERE x.id=2 RETURNING id,a,t;',
              [['2', '8', None]])
        check('delete-physical-target-shadow', 'WITH with_target AS(SELECT 999 AS id) DELETE FROM with_target WHERE id=5 RETURNING id;',
              [['5']], tag='DELETE 1')
        setup('DELETE FROM with_pairs;')
        check('two-writing-ctes-fixed-view', "WITH first AS(INSERT INTO with_source VALUES(2,'new') RETURNING id),second AS(INSERT INTO with_source VALUES(3,'new') RETURNING id) INSERT INTO with_pairs SELECT id,id FROM with_source RETURNING a,b;",
              [['1', '1']], tag='INSERT 0 1')
        check('writes-visible-next-command', 'SELECT id,t FROM with_source ORDER BY id;',
              [['1', 'old'], ['2', 'new'], ['3', 'new']])
        check('returning-explicit-channel', 'WITH writer AS(INSERT INTO with_source VALUES(4,\'channel\') RETURNING id) INSERT INTO with_pairs SELECT id,id FROM writer RETURNING a,b;',
              [['4', '4']])
        check('write-cte-complete-final-limit-zero', 'WITH writer AS(INSERT INTO with_effects VALUES(10),(11),(12) RETURNING id) INSERT INTO with_pairs SELECT id,id FROM writer LIMIT 0 RETURNING a,b;',
              [], tag='INSERT 0 0')
        check('write-cte-all-three', 'SELECT id FROM with_effects ORDER BY id;', [['10'], ['11'], ['12']])
        check('unused-read-cte-not-executed', "WITH unused AS(SELECT nextval('with_read_seq') AS id FROM with_source) INSERT INTO with_pairs VALUES(20,20) RETURNING a;", [['20']])
        check('unused-read-no-sequence-effect', "SELECT currval('with_read_seq');", state='55000')
        check('read-cte-limited-demand', "WITH limited AS(SELECT nextval('with_read_seq') AS id FROM with_source) INSERT INTO with_pairs SELECT id,id FROM limited LIMIT 1 RETURNING a,b;", [['1', '1']])
        check('read-cte-one-call', "SELECT currval('with_read_seq');", [['1']])
        check('read-cte-shared-producer', "WITH once AS(SELECT nextval('with_read_seq') AS id) INSERT INTO with_pairs VALUES((SELECT id FROM once),(SELECT id FROM once)) RETURNING a,b;", [['2', '2']])
        check('read-cte-shared-one-call', "SELECT currval('with_read_seq');", [['2']])
        check('unused-arithmetic-not-folded', 'WITH unused AS(SELECT 1/0 AS id) INSERT INTO with_pairs VALUES(21,21) RETURNING a;', [['21']])
        check('wide-typed-output', 'WITH wide AS(SELECT CAST(2147483648 AS BIGINT) AS id) INSERT INTO with_target(id,a) SELECT 30,1 FROM wide RETURNING CAST(2147483648 AS BIGINT);',
              [['2147483648']], types=[20])
        # Whole static preparation precedes every producer/volatile call,
        # including empty input and short-circuit branches.
        prefix = "WITH writer AS(INSERT INTO with_effects VALUES(nextval('with_write_seq')) RETURNING id) "
        check('identical-duplicate-update-target', prefix+'UPDATE with_target SET a=1,a=2 WHERE false;', state='42601')
        check('canonical-duplicate-update-target', prefix+'UPDATE with_target SET a=1,"a"=2 WHERE false;', state='42601')
        check('duplicate-before-arithmetic-fold', prefix+'UPDATE with_target SET a=1/0,"a"=2 WHERE false;', state='42601')
        check('duplicate-before-numeric-cast-fold', prefix+'UPDATE with_target SET a=CAST(2147483648 AS INT),"a"=2 WHERE false;', state='42601')
        check('unknown-lhs-before-duplicate', prefix+'UPDATE with_target SET missing_target=1,a=1,a=2 WHERE false;', state='42703')
        check('unknown-rhs-before-duplicate', prefix+'UPDATE with_target SET a=missing_input,a=2 WHERE false;', state='42703')
        check('unknown-callee-before-duplicate', prefix+'UPDATE with_target SET a=missing_duplicate_function(1),a=2 WHERE false;', state='42883')
        check('boolean-context-before-duplicate', prefix+'UPDATE with_target SET a=1,a=2 WHERE 1;', state='42804')
        check('unknown-column-pre-effect', prefix+'INSERT INTO with_pairs SELECT missing_with_column,1 FROM with_source WHERE false;', state='42703')
        check('unknown-callee-pre-effect', prefix+'INSERT INTO with_pairs SELECT CASE WHEN false THEN missing_with_function(1) ELSE 1 END,1;', state='42883')
        check('unknown-alias-pre-effect', prefix+'INSERT INTO with_pairs SELECT z.id,1 FROM with_source AS x WHERE false;', state='42P01')
        check('no-sequence-static-errors', "SELECT currval('with_write_seq');", state='55000')
        setup('DELETE FROM with_effects;')
        # An unused writer is finished only after the primary command. If
        # that command fails immediately, the producer has not run yet.
        check('late-error-unit-rollback', prefix+'INSERT INTO with_target(id) VALUES(1);', state='23505')
        check('late-error-no-row-effects', 'SELECT id FROM with_effects;', [])
        check('late-error-unused-producer-no-call', "SELECT currval('with_write_seq');", state='55000')
        # A true RETURNING dependency necessarily runs its writer first.
        # Subsequent row effects roll back, but its sequence call remains.
        check('dependent-late-error-rollback', prefix+'INSERT INTO with_target(id) SELECT 1 FROM writer;', state='23505')
        check('dependent-late-error-no-rows', 'SELECT id FROM with_effects;', [])
        check('dependent-late-error-sequence-effect', "SELECT currval('with_write_seq');", [['1']])
        setup('DELETE FROM with_effects;')
        # The VOLATILE function obtains its own command view: it sees the
        # previous CTE write plus its own INSERT. Returning restores the
        # outer command's fixed view, without discarding transaction CID or
        # combo-CID state; the outer base-table child must still see no 41.
        function = 'pg_temp.with_snapshot_count' if reference else 'with_snapshot_count'
        setup('CREATE FUNCTION '+function+'() RETURNS INT VOLATILE LANGUAGE plpgsql AS $$ DECLARE n INT; BEGIN INSERT INTO with_effects VALUES(42); SELECT count(*) INTO n FROM with_effects; RETURN n; END; $$;')
        check('volatile-body-view-and-fixed-caller',
              'WITH writer AS(INSERT INTO with_effects VALUES(41) RETURNING id) INSERT INTO with_target(id,a,b) SELECT 50,'+function+'(),(SELECT id FROM with_effects WHERE id=41) FROM writer RETURNING id,a,b;',
              [['50', '2', None]])
        check('volatile-body-writes-next-command', 'SELECT id FROM with_effects ORDER BY id;', [['41'], ['42']])
        setup('DELETE FROM with_effects;')
        setup('BEGIN;')
        check('prior-success-in-user-transaction', 'INSERT INTO with_target(id) VALUES(90);', tag='INSERT 0 1', ready=b'T')
        setup('SAVEPOINT keep;')
        check('explicit-transaction-late-error', 'WITH writer AS(INSERT INTO with_effects VALUES(91) RETURNING id) INSERT INTO with_target(id) VALUES(90);', state='23505', ready=b'E')
        check('aborted-user-block', 'SELECT 1;', state='25P02', ready=b'E')
        setup('ROLLBACK TO keep;')
        check('prior-success-retained', 'SELECT id FROM with_target WHERE id=90;', [['90']], ready=b'T')
        check('all-with-writes-rolled-back', 'SELECT id FROM with_effects;', [], ready=b'T')
        setup('ROLLBACK;')
        check('outer-user-rollback', 'SELECT id FROM with_target WHERE id=90;', [])
        assert not failures, failures
        print('[WITH PRIMARY DML '+('PG18.6 REFERENCE' if reference18 else 'PG17.2 DIAGNOSTIC' if reference else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference: sock.close()
        else: runner.stop_ours(server)


if __name__ == '__main__': main()
