#!/usr/bin/env python3
"""Complete function bodies survive framing, replacement and process reopen."""
from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import Wire,identifier
import socket
import subprocess
import time
import uuid

PROFILES=(("SELECT 'plain'", 'sql', 'plain'), ("SELECT ''", 'sql', ''), ("SELECT 'null'", 'sql', 'null'), ('SELECT NULL', 'sql', None), ("SELECT 'first\nsecond'", 'sql', 'first\nsecond'), ("SELECT 'first\r\nsecond'", 'sql', 'first\r\nsecond'), ("SELECT 'first\rsecond'", 'sql', 'first\rsecond'), ("SELECT\n'界😀'", 'sql', '界😀'), ("SELECT /* first\nsecond */ 'comment'", 'sql', 'comment'), ("SELECT -- first\n'line'", 'sql', 'line'), ("SELECT 'quote''s\\tail'\n", 'sql', "quote's\\tail"), ("\nSELECT 'trailing'\n\n", 'sql', 'trailing'), ("BEGIN RETURN 'plain'; END;", 'plpgsql', 'plain'), ("BEGIN RETURN ''; END;", 'plpgsql', ''), ("BEGIN RETURN 'null'; END;", 'plpgsql', 'null'), ('BEGIN RETURN NULL; END;', 'plpgsql', None), ("BEGIN RETURN 'first\nsecond'; END;", 'plpgsql', 'first\nsecond'), ("BEGIN RETURN 'first\r\nsecond'; END;", 'plpgsql', 'first\r\nsecond'), ("BEGIN RETURN 'first\rsecond'; END;", 'plpgsql', 'first\rsecond'), ("BEGIN\nRETURN '界😀';\nEND;", 'plpgsql', '界😀'), ("BEGIN /* first\nsecond */ RETURN 'comment'; END;", 'plpgsql', 'comment'), ("BEGIN -- first\nRETURN 'line'; END;", 'plpgsql', 'line'), ("BEGIN RETURN 'quote''s\\tail'; END;\n", 'plpgsql', "quote's\\tail"), ("\nBEGIN RETURN 'trailing'; END;\n\n", 'plpgsql', 'trailing'))
MODES=('simple','extended','binary')

def literal(text):
    return "'"+text.replace("'","''")+"'"

def main():
    review=Review('ROUTINE_BODY_METADATA_PROTOCOL');wire=Wire(review)
    schema='routine_body_'+uuid.uuid4().hex;owned=False;functions=[]
    def definition(name,body,language,arity,quoted,vol,strict,replace=False):
        encoded=literal(body) if quoted else '$body$'+body+'$body$'
        return ('CREATE OR REPLACE FUNCTION ' if replace else 'CREATE FUNCTION ')+name+('()' if arity==0 else '(x TEXT)')+' RETURNS TEXT LANGUAGE '+language+' '+vol+(' STRICT' if strict else ' CALLED ON NULL INPUT')+' AS '+encoded+';'
    try:
        before=len(review.failures);wire.command('CREATE SCHEMA '+identifier(schema)+';','CREATE SCHEMA')
        owned=len(review.failures)==before;assert owned
        for number,(body,language,value) in enumerate(PROFILES):
            for quoted in (False,True):
                for arity in (0,1):
                    name=identifier(schema)+'.f_'+str(len(functions))
                    strict=number%2==0;vol=('IMMUTABLE','STABLE','VOLATILE')[number%3]
                    wire.command(definition(name,body,language,arity,quoted,vol,strict),'CREATE FUNCTION')
                    sql='SELECT '+name+('()' if arity==0 else '('+literal('argument\nvalue')+')')+' AS v;'
                    for mode in MODES:wire.query(mode,sql,(value,),25)
                    statement=('routine_body_'+str(len(functions))).encode()
                    params=(25,) if arity else ();values=('argument\nvalue',) if arity else ()
                    prepared='SELECT '+name+('($1::text)' if arity else '()')+' AS v;'
                    wire.describe(statement,prepared,25,params)
                    wire.bind_describe(statement,25,params,values,False,False)
                    for binary in (False,True):wire.execute(statement,25,(value,),params,values,binary_input=binary,binary_output=binary)
                    wire.close(statement)
                    functions.append((name,body,language,value,arity,quoted,strict,sql))
        wire.command('CHECKPOINT;','CHECKPOINT')
        old=review.sock;old.close()
        if review.server:
            server=review.server;server['sock']=None;old_process=server['process'];old_pid=old_process.pid
            old_process.terminate();old_process.wait(timeout=10)
            server['process']=subprocess.Popen([review.runner.DBMS_MAIN,'--data-dir',server['dir'],'--server',str(server['port']),'--insecure'],cwd=server['dir'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            deadline=time.monotonic()+20
            while True:
                try: fresh=socket.create_connection(('127.0.0.1',server['port']),timeout=review.runner.wire_timeout());break
                except OSError:
                    if time.monotonic()>=deadline:raise
                    time.sleep(0.05)
            review.client.startup(fresh,'alice','info');server['sock']=fresh
            review.record('REAL_PRODUCT_PROCESS_RESTART',old_process.returncode is not None and server['process'].pid!=old_pid,(old_pid,server['process'].pid,old_process.returncode),'old process terminal, new PID, same private data directory')
        else:
            host,port,user,database,password=review.runner._reference_connection_settings()
            fresh=socket.create_connection((host,port),timeout=review.runner.wire_timeout())
            review.client.startup_reference(fresh,user,database,password)
            review.runner.verify_reference_version(review.client,fresh)
            review.record('REFERENCE_FRESH_BACKEND_RECONNECT',fresh is not old,True,True)
        review.sock=fresh;wire=Wire(review)
        for name,body,language,value,arity,quoted,strict,sql in functions:
            for mode in MODES:wire.query(mode,sql,(value,),25)
            wire.command(definition(name,body+'\n-- replacement\n',language,arity,quoted,'VOLATILE',not strict,True),'CREATE FUNCTION')
            for mode in MODES:wire.query(mode,sql,(value,),25)
            if arity:
                for mode in MODES:wire.query(mode,'SELECT '+name+'(NULL::text) AS v;',(value if strict else None,),25)
        sequence=identifier(schema)+'.effects_';effect_name=identifier(schema)+'.effect_fn'
        wire.command('CREATE SEQUENCE '+sequence+';','CREATE SEQUENCE')
        effect_body="BEGIN\nRETURN nextval('"+sequence+"')::text;\nEND;"
        wire.command(definition(effect_name,effect_body,'plpgsql',0,False,'VOLATILE',False),'CREATE FUNCTION')
        effect='SELECT '+effect_name+'() AS v;';statement=b'routine_body_effect'
        wire.describe(statement,effect,25);wire.bind_describe(statement,25,(),(),False,False)
        wire.query('simple',"SELECT nextval('"+sequence+"') AS v;",(1,),20)
        for mode,current,following in zip(MODES,('2','4','6'),(3,5,7)):
            wire.query(mode,effect,(current,),25)
            wire.query('simple',"SELECT nextval('"+sequence+"') AS v;",(following,),20)
        wire.close(statement)
        wire.command('DROP SCHEMA '+identifier(schema)+' CASCADE;','DROP SCHEMA',notices=True);owned=False
        review.complete(1696)
    finally:
        if owned:review.client.simple_query(review.sock,'DROP SCHEMA '+identifier(schema)+' CASCADE;')
        review.close()

if __name__=='__main__':main()
