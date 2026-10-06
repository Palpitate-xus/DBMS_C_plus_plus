#!/usr/bin/env python3
"""WITH DML transition values retain prepared source occurrence identities."""
import importlib.util
from pathlib import Path
import socket
import sys


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('with_transition_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference='--reference18' in sys.argv[1:]
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=120)
        client.startup_reference(sock,user,database,password)
        runner.verify_reference_version(client,sock);server={'sock':sock}
    else:server=runner.start_ours(client)
    failures=[]

    def query(sql):
        result=runner.decode_wire_result(client.simple_query(server['sock'],sql),include_types=True)
        print('WITH_TRANSITION',sql,result,flush=True);return result

    def check(label,sql,rows=None,state=None,types=None,tag=None):
        result=query(sql)
        good=result[1]==state
        if rows is not None:good=good and result[0]==rows
        if types is not None:good=good and result[5]==types
        if tag is not None:good=good and result[4]==tag
        if state is not None:good=good and not result[0] and result[4] is None
        if not good:failures.append((label,rows,state,types,tag,result))
        print('WITH_TRANSITION_CONTROL',label,'PASS' if good else 'FAIL',flush=True)
        return result

    def setup(sql):
        result=query(sql);assert result[1] is None,(sql,result)

    try:
        setup('BEGIN;')
        setup('CREATE TEMP TABLE transition_rows(id INT PRIMARY KEY,v INT,t TEXT,"V" BIGINT);')
        setup('CREATE TEMP TABLE transition_effects(id INT);')
        setup('CREATE TEMP SEQUENCE transition_sequence;')
        check('insert-old-null-new-actual',"WITH c AS(SELECT 1) INSERT INTO transition_rows VALUES(1,10,'',2147483648),(2,NULL,'NULL',NULL) RETURNING old.id,new.id,old.v,new.v,old.t,new.t,old.\"V\",new.\"V\";",
              [[None,'1',None,'10',None,'',None,'2147483648'],[None,'2',None,None,None,'NULL',None,None]],types=[23,23,23,23,25,25,20,20],tag='INSERT 0 2')
        check('update-actual-old-new','WITH c AS(SELECT 1) UPDATE transition_rows SET v=v+1 WHERE id=1 RETURNING old.v,new.v,v;',
              [['10','11','11']],types=[23,23,23],tag='UPDATE 1')
        check('quoted-compound-source-width', 'WITH c AS(SELECT 1) UPDATE transition_rows SET v=v+1 WHERE id=1 RETURNING WITH(OLD AS o,NEW AS "O") o.v+"O".v,o.t,"O"."V";',
              [['23','','2147483648']],types=[23,25,20],tag='UPDATE 1')
        check('actual-target-masks-default-old', 'WITH c AS(SELECT 1) UPDATE transition_rows AS old SET v=v+1 WHERE id=1 RETURNING old.v,new.v,v;',
              [['13','13','13']],types=[23,23,23],tag='UPDATE 1')
        check('explicit-old-masks-default-new','WITH c AS(SELECT 1) UPDATE transition_rows SET v=v+1 WHERE id=1 RETURNING WITH(OLD AS new) new.v,v;',
              [['13','14']],types=[23,23],tag='UPDATE 1')
        check('explicit-new-masks-default-old','WITH c AS(SELECT 1) UPDATE transition_rows SET v=v+1 WHERE id=1 RETURNING WITH(NEW AS old) old.v,v;',
              [['15','15']],types=[23,23],tag='UPDATE 1')
        check('quoted-dot-qualified-stars', 'WITH c AS(SELECT 1) UPDATE transition_rows SET v=v+1 WHERE id=1 RETURNING WITH(OLD AS "o.x",NEW AS "n.x") "o.x".*,"n.x".*;',
              [['1','15','','2147483648','1','16','','2147483648']],types=[23,23,25,20,23,23,25,20],tag='UPDATE 1')
        check('update-null-image-not-text-null','WITH c AS(SELECT 1) UPDATE transition_rows SET v=v+1 WHERE id=2 RETURNING old.v,new.v,old.t,new.t;',
              [[None,None,'NULL','NULL']],types=[23,23,25,25],tag='UPDATE 1')
        check('delete-new-null-qualified-stars','WITH c AS(SELECT 1) DELETE FROM transition_rows WHERE id=2 RETURNING old.*,new.*;',
              [['2',None,'NULL',None,None,None,None,None]],types=[23,23,25,20,23,23,25,20],tag='DELETE 1')
        check('empty-update-keeps-descriptor','WITH c AS(SELECT 1) UPDATE transition_rows SET v=99 WHERE false RETURNING old.v,new.v;',
              [],types=[23,23],tag='UPDATE 0')
        check('empty-delete-keeps-star-descriptor','WITH c AS(SELECT 1) DELETE FROM transition_rows WHERE false RETURNING old.*,new.*;',
              [],types=[23,23,25,20,23,23,25,20],tag='DELETE 0')
        # A writing CTE exposes its real row images to its consumers. These
        # rows are not recovered by name/value matching against a heap scan.
        check('writing-cte-old-null-channel', "WITH writer AS(INSERT INTO transition_rows VALUES(3,20,NULL,NULL) RETURNING old.id AS prior,new.id AS current) INSERT INTO transition_effects SELECT current FROM writer RETURNING id;",
              [['3']],types=[23],tag='INSERT 0 1')
        check('writing-cte-old-new-output', 'WITH writer AS(UPDATE transition_rows SET v=v+1 WHERE id=1 RETURNING old.v AS prior,new.v AS current) INSERT INTO transition_effects SELECT prior+current FROM writer RETURNING id;',
              [['33']],types=[23],tag='INSERT 0 1')
        prefix="WITH writer AS(INSERT INTO transition_effects VALUES(nextval('transition_sequence')) RETURNING id) "
        for label,sql,state in (
            ('missing-transition-column',prefix+'INSERT INTO transition_rows(id) VALUES(4) RETURNING old.missing;','42703'),
            ('renamed-transition-hides-default',prefix+'UPDATE transition_rows SET v=99 RETURNING WITH(OLD AS before_row) old.v;','42P01'),
            ('duplicate-explicit-transition',prefix+'DELETE FROM transition_rows RETURNING WITH(OLD AS x,NEW AS x) x.id;','42712'),
        ):
            setup('SAVEPOINT transition_error;');check(label,sql,state=state)
            setup('ROLLBACK TO SAVEPOINT transition_error;');setup('RELEASE SAVEPOINT transition_error;')
        setup('SAVEPOINT transition_effect;')
        check('binding-before-cte-effects',"SELECT currval('transition_sequence');",state='55000')
        setup('ROLLBACK TO SAVEPOINT transition_effect;');setup('RELEASE SAVEPOINT transition_effect;')
        check('static-errors-preserve-rows','SELECT id FROM transition_rows ORDER BY id;',[['1'],['3']])
        check('no-repeated-writer','SELECT id FROM transition_effects ORDER BY id;',[['3'],['33']])
        setup('ROLLBACK;')
        assert not failures,failures
        print('[WITH RETURNING TRANSITIONS '+('PG18.6 REFERENCE' if reference else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference:sock.close()
        else:runner.stop_ours(server)


if __name__=='__main__':main()
