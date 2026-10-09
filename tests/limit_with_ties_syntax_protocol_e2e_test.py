"""Real ordinary SELECT dispatch must preserve LIMIT grammar rejection."""
import importlib.util
import socket
import struct
import sys
from pathlib import Path


def main():
    root=Path(__file__).resolve().parents[1]
    spec=importlib.util.spec_from_file_location('limit_syntax_runner',root/'tests/compat/pg_diff_runner.py')
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

    def check(sql,rows=None,oids=None,state=None):
        nonlocal checked
        checked+=1
        result=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
        print('LIMIT_SYNTAX',sql,result,flush=True)
        try:
            assert result[1]==state,(sql,result,state)
            if rows is not None:
                assert result[0]==rows and result[4]=='SELECT '+str(len(rows)),(sql,result,rows)
            if oids is not None:assert result[5]==oids,(sql,result,oids)
            if state is not None:assert result[0]==[] and result[4] is None,(sql,result)
        except AssertionError as error:failures.append(error.args)

    try:
        if reference:check('BEGIN')
        check('CREATE TABLE t(id INT)')
        check('INSERT INTO t VALUES(1),(2),(3),(4),(5),(5),(6)')
        check('CREATE SEQUENCE limit_syntax_effects')
        check("SELECT nextval('limit_syntax_effects')",[["1"]],[20])
        for sql in (
            'SELECT id FROM t ORDER BY id LIMIT 5 WITH TIES',
            "SELECT nextval('limit_syntax_effects') FROM t ORDER BY id LIMIT 5 WITH TIES",
            'SELECT id FROM t LIMIT 5 WITH TIES',
            'SELECT id FROM t ORDER BY id LIMIT ALL WITH TIES',
            'SELECT (SELECT id FROM t ORDER BY id LIMIT 5 WITH TIES)',
        ):
            if reference:check('SAVEPOINT limit_error')
            check(sql,state='42601')
            if reference:check('ROLLBACK TO limit_error');check('RELEASE limit_error')
            check("SELECT currval('limit_syntax_effects')",[["1"]],[20])
        check('SELECT id FROM t ORDER BY id FETCH FIRST 5 ROWS WITH TIES',
              [["1"],["2"],["3"],["4"],["5"],["5"]],[23])
        check("SELECT 'LIMIT 5 WITH TIES' AS literal FROM t ORDER BY id FETCH FIRST 1 ROW ONLY",
              [["LIMIT 5 WITH TIES"]],[25])
        # An actual Extended Parse, not a synthetic Describe, must reject it.
        if reference:check('SAVEPOINT limit_extended')
        sql='SELECT id FROM t ORDER BY id LIMIT 5 WITH TIES'
        sock.sendall(client.typed(b'P',b'bad_limit\0'+sql.encode()+b'\0'+struct.pack('!H',0))+
                     client.typed(b'S'))
        result=runner.decode_wire_result(client.read_until_ready(sock),include_types=True)
        print('LIMIT_SYNTAX_EXTENDED',result,flush=True)
        assert result[1]=='42601' and result[0]==[] and result[4] is None,result
        if reference:check('ROLLBACK TO limit_extended');check('RELEASE limit_extended')
        check("SELECT currval('limit_syntax_effects')",[["1"]],[20])
        print('LIMIT_SYNTAX_COMPLETE',checked,'FAILED',len(failures),failures,flush=True)
        assert not failures,failures
    finally:
        if reference:
            try:client.simple_query(sock,'ROLLBACK')
            finally:sock.close()
        else:runner.stop_ours(server)


if __name__=='__main__':main()
