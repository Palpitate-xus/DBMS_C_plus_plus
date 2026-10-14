#!/usr/bin/env python3
"""DEFAULT is assignment grammar, not an unquoted value/column expression."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root=Path(__file__).resolve().parents[1]
    spec=importlib.util.spec_from_file_location('default_role_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client()
    reference=sys.argv[1:]==['--reference18']
    assert not sys.argv[1:] or reference
    server=None
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password)
        runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client);sock=server['sock']
    checks,failures=0,[]

    def check(sql,rows=None,state=None,oids=None):
        nonlocal checks
        messages=client.simple_query(sock,sql)
        result=runner.decode_wire_result(messages,include_types=True)
        valid=result[1]==state and (rows is None or result[0]==rows) and messages[-1]==(b'Z',b'I')
        if oids is not None:valid=valid and result[5]==oids
        if state:valid=valid and not result[0] and result[4] is None
        checks+=1
        print('DEFAULT_VALUE_ROLE',sql,result,'PASS',valid,flush=True)
        if not valid:failures.append((sql,result,rows,state,oids))
        return valid

    try:
        assert check("CREATE TEMP TABLE default_role_rows(\"default\" INT DEFAULT 7,t TEXT DEFAULT 'kept')")
        assert check('INSERT INTO default_role_rows DEFAULT VALUES RETURNING "default",t',rows=[['7','kept']],oids=[23,25])
        for expression in ('DEFAULT','CAST(DEFAULT AS INT)','DEFAULT+1','CASE WHEN false THEN DEFAULT ELSE 7 END'):
            for suffix in ('',' FROM default_role_rows WHERE false',' FROM default_role_rows LIMIT 0'):
                check('SELECT '+expression+suffix,state='42601')
        for sql in ('VALUES(DEFAULT)','VALUES(1,DEFAULT)',
                    "INSERT INTO default_role_rows SELECT DEFAULT,'changed'",
                    "INSERT INTO default_role_rows SELECT DEFAULT,'changed' FROM default_role_rows WHERE false",
                    "WITH c AS(SELECT 1 AS id) INSERT INTO default_role_rows SELECT DEFAULT,'changed' FROM c",
                    'INSERT INTO default_role_rows VALUES(CAST(DEFAULT AS INT),DEFAULT)',
                    'UPDATE default_role_rows SET "default"=DEFAULT+1'):
            check(sql,state='42601')
        check('SELECT 1 AS default',rows=[['1']],oids=[23])
        check("SELECT 'DEFAULT','default'",rows=[['DEFAULT','default']],oids=[25,25])
        check('SELECT t.default FROM default_role_rows AS t',rows=[['7']],oids=[23])
        for suffix in (' WHERE false',' LIMIT 0'):
            check('SELECT "default" FROM default_role_rows'+suffix,rows=[],oids=[23])
        check('SELECT "DEFAULT" FROM default_role_rows',state='42703')
        check('SELECT DEFAULT FROM missing_default_role_relation',state='42P01')
        check('SELECT DEFAULT,missing FROM default_role_rows',state='42601')
        check('SELECT missing,DEFAULT FROM default_role_rows',state='42703')
        check('INSERT INTO default_role_rows VALUES(DEFAULT,DEFAULT) RETURNING "default",t',
              rows=[['7','kept']],oids=[23,25])
        check('UPDATE default_role_rows SET "default"=DEFAULT RETURNING "default",t',
              rows=[['7','kept'],['7','kept']],oids=[23,25])
        check('SELECT "default",t FROM default_role_rows',rows=[['7','kept'],['7','kept']],oids=[23,25])
        print('DEFAULT_VALUE_ROLE_COMPLETE',checks,'FAILURES',len(failures),flush=True)
        assert checks==33 and not failures,failures
    finally:
        if server:runner.stop_ours(server)
        else:sock.close()


if __name__=='__main__':
    main()
