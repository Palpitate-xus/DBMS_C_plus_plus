#!/usr/bin/env python3
"""Retain real sort-slot expectations; not a registered supported green test.

Equivalent target/ORDER children and duplicate ORDER keys reuse a sort slot;
two genuine target sites stay separate. Do not lower these counts to pass.
"""
import importlib.util
from pathlib import Path


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('scalar_sort_slot_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();server=runner.start_ours(client)
    failures=[]
    def query(sql):
        messages=client.simple_query(server['sock'],sql)
        result=runner.decode_wire_result(messages,include_types=True)
        assert messages[-1]==(b'Z',b'I'),(sql,messages[-1]);return result
    def ok(sql):
        result=query(sql);assert result[1] is None,(sql,result);return result
    def calls():return int(ok("SELECT currval('ordinary_sort_slot_seq');")[0][0][0])
    try:
        for sql in (
            'CREATE TABLE ordinary_sort_slot_rows(id INT);',
            'INSERT INTO ordinary_sort_slot_rows VALUES(1),(2),(3);',
            'CREATE SEQUENCE ordinary_sort_slot_seq START 1;',
            "CREATE FUNCTION ordinary_sort_slot_writer(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('ordinary_sort_slot_seq');RETURN -arg;END; $$;",
        ):ok(sql)
        ok("SELECT nextval('ordinary_sort_slot_seq');")
        for name,sql,rows,count in (
            ('target/sort equivalent',
             'SELECT(SELECT ordinary_sort_slot_writer(74)) FROM ordinary_sort_slot_rows ORDER BY(SELECT ordinary_sort_slot_writer(74));',
             [['-74']]*3,1),
            ('two genuine SELECT sites',
             'SELECT(SELECT ordinary_sort_slot_writer(75)),(SELECT ordinary_sort_slot_writer(75)) FROM ordinary_sort_slot_rows ORDER BY id;',
             [['-75','-75']]*3,2),
            ('two equivalent ORDER keys',
             'SELECT id FROM ordinary_sort_slot_rows ORDER BY(SELECT ordinary_sort_slot_writer(76)),(SELECT ordinary_sort_slot_writer(76));',
             [['1'],['2'],['3']],1),
        ):
            before=calls();result=query(sql);actual=calls()-before
            good=result[1] is None and result[0]==rows and actual==count
            evidence=(name,'expected calls',count,'actual calls',actual,'result',result)
            print('SCALAR_SORT_SLOT','PASS' if good else 'FAIL',evidence,flush=True)
            if not good:failures.append(evidence)
        assert not failures,failures
        print('[ORDINARY SCALAR SORT SLOTS] all controls passed')
    finally:runner.stop_ours(server)


if __name__=='__main__':main()
