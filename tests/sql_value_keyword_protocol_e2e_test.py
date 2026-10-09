"""SQL-value grammar carries real static types, labels and reached context."""
import importlib.util
import socket
import struct
import sys
from pathlib import Path


def main():
    root=Path(__file__).resolve().parents[1]
    spec=importlib.util.spec_from_file_location('sql_value_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client=runner.load_protocol_client()
    reference='--reference18' in sys.argv
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password)
        runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client)
        sock=server['sock'];user='alice';database='info'
    checked=0;failures=[];created=[]

    def check(sql,rows=None,oids=None,names=None,state=None,describe=False):
        nonlocal checked
        checked+=1
        if describe:
            parse=b'\0'+sql.encode()+b'\0'+struct.pack('!H',0)
            sock.sendall(client.typed(b'P',parse)+client.typed(b'D',b'S\0')+client.typed(b'S'))
            messages=client.read_until_ready(sock)
        else:
            messages=client.simple_query(sock,sql)
        result=runner.decode_wire_result(messages,include_types=True)
        valid=result[1]==state
        if rows is not None:valid=valid and result[0]==rows
        if oids is not None:valid=valid and result[5]==oids
        if names is not None:valid=valid and result[3]==names
        if state:valid=valid and result[0]==[] and result[4] is None
        print('SQL_VALUE_KEYWORD',sql,'describe=',describe,result,'pass=',valid,flush=True)
        if not valid:failures.append((sql,result,rows,oids,names,state))
        return result

    try:
        keywords=['current_user','session_user','current_role','current_catalog','current_schema',
                  'current_date','current_timestamp','localtimestamp','current_time','localtime']
        oids=[19]*5+[1082,1184,1114,1266,1083]
        sql='SELECT '+','.join(keywords)
        result=check(sql,oids=oids,names=keywords)
        if result[1] is None:
            assert len(result[0])==1 and result[0][0][:5]==[user,user,user,database,'public'],result
            assert all(value is not None for value in result[0][0]),result
        check(sql,[],oids,keywords,describe=True)
        check('SELECT current_user=session_user,current_role=current_user,current_catalog=current_database(),current_schema=current_schema()',
              [['t']*4],[16]*4)
        check('SELECT "current_user"(),pg_catalog."current_user"(),current_schema(),pg_catalog.current_schema()',
              [[user,user,'public','public']],[19]*4)
        check("SELECT CASE WHEN false THEN current_user ELSE 'kept'::NAME END",[['kept']],[19])
        for name,oid in [('current_timestamp',1184),('localtimestamp',1114),('current_time',1266),('localtime',1083)]:
            check('SELECT '+name+'(3)',oids=[oid],names=[name])
            check('SELECT '+name+'(3)',[],[oid],[name],describe=True)
        for invalid in ['current_user()','session_user()','current_role()','current_catalog()',
                        'current_date()','localtime()','localtimestamp()',
                        'current_timestamp(1+1)','current_timestamp(-1)']:
            check('SELECT '+invalid,state='42601')
        check('SELECT current_schema(1) WHERE false',state='42883')
        check('SELECT "current_date"() WHERE false',state='42883')
        result=check('CREATE FUNCTION sql_value_keyword_user() RETURNS NAME LANGUAGE sql AS $$ SELECT current_user $$')
        if result[1] is None:created.append('sql_value_keyword_user')
        check('SELECT sql_value_keyword_user()',[[user]],[19])
        check('SELECT sql_value_keyword_user()',[],[19],describe=True)
        result=check('CREATE FUNCTION sql_value_keyword_schema() RETURNS NAME LANGUAGE sql AS $$ SELECT current_schema $$')
        if result[1] is None:created.append('sql_value_keyword_schema')
        check('SELECT sql_value_keyword_schema()',[["public"]],[19])
        result=check('CREATE FUNCTION sql_value_keyword_text_user() RETURNS TEXT LANGUAGE sql AS $$ SELECT current_user $$')
        if result[1] is None:created.append('sql_value_keyword_text_user')
        check('SELECT sql_value_keyword_text_user()',[[user]],[25])
        result=check('CREATE FUNCTION sql_value_keyword_text_schema() RETURNS TEXT LANGUAGE sql AS $$ SELECT current_schema $$')
        if result[1] is None:created.append('sql_value_keyword_text_schema')
        check('SELECT sql_value_keyword_text_schema()',[["public"]],[25])
        print('SQL_VALUE_KEYWORD_COMPLETE',checked,'FAILED',len(failures),failures,flush=True)
        assert not failures,failures
    finally:
        if reference:
            try:
                for name in created:client.simple_query(sock,'DROP FUNCTION '+name+'()')
            finally:sock.close()
        else:runner.stop_ours(server)


if __name__=='__main__':main()
