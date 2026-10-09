"""HAVING splits actual AND keywords, not identifier or literal contents."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root=Path(__file__).resolve().parents[1]
    spec=importlib.util.spec_from_file_location('having_and_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference='--reference18' in sys.argv
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password)
        runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client);sock=server['sock']
    failures=[];checked=0

    def check(sql,rows=None,oids=None):
        nonlocal checked
        checked+=1
        result=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
        print('HAVING_AND',sql,result,flush=True)
        try:
            assert result[1] is None,(sql,result)
            if rows is not None:
                assert result[0]==rows and result[4]=='SELECT '+str(len(rows)),(sql,result,rows)
            if oids is not None:assert result[5]==oids,(sql,result,oids)
        except AssertionError as error:failures.append(error.args)

    try:
        if reference:check('BEGIN')
        check('CREATE TABLE having_and(id INT,candy INT,"AND" INT,"a and b" INT,txt TEXT)')
        check("INSERT INTO having_and VALUES(1,1,1,1,'sand'),(2,2,2,2,'rock'),(3,NULL,NULL,NULL,NULL)")
        for key in ('candy','"AND"','"a and b"'):
            for predicate,rows in ((key+'=1',[["1"]]),(key+'>1',[["2"]]),(key+'=NULL',[])):
                check('SELECT '+key+' FROM having_and GROUP BY '+key+' HAVING '+predicate+' ORDER BY '+key,rows,[23])
            check('SELECT h.'+key+' FROM having_and h GROUP BY h.'+key+' HAVING h.'+key+'=1',[["1"]],[23])
        check('SELECT id,candy FROM having_and GROUP BY id,candy HAVING candy=1 AND id=1',[["1","1"]],[23,23])
        check('SELECT id,candy FROM having_and GROUP BY id,candy HAVING candy=1 AND id=2',[],[23,23])
        check("SELECT txt FROM having_and GROUP BY txt HAVING txt='sand'",[["sand"]],[25])
        check("SELECT txt FROM having_and GROUP BY txt HAVING txt='and'",[],[25])
        print('HAVING_AND_COMPLETE',checked,'FAILED',len(failures),failures,flush=True)
        assert not failures,failures
    finally:
        if reference:
            try:client.simple_query(sock,'ROLLBACK')
            finally:sock.close()
        else:runner.stop_ours(server)


if __name__=='__main__':main()
