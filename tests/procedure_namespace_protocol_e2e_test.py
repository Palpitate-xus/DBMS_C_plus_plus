"""Actual qualified procedure bodies, search paths and transactional identities."""
import importlib.util
import socket
import sys
import uuid
from pathlib import Path


def main():
    root=Path(__file__).resolve().parents[1]
    spec=importlib.util.spec_from_file_location("procedure_runner",root/"tests/compat/pg_diff_runner.py")
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference="--reference18" in sys.argv
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password)
        runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client);sock=server["sock"]
    checked=0;failures=[]
    def check(sql,rows=None,state=None):
        nonlocal checked
        checked+=1;result=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
        good=result[1]==state and (rows is None or result[0]==rows)
        if state:good=good and result[0]==[] and result[4] is None
        print("PROCEDURE_NAMESPACE",sql,result,"pass=",good,flush=True)
        if not good:failures.append((sql,result,rows,state))
    suffix=uuid.uuid4().hex[:12];raw_schema="proc_"+suffix+".X";schema='"'+raw_schema+'"'
    name="p_"+suffix;private=schema+" . "+name;public="public."+name;table="public.t_"+suffix
    dotted='public."'+raw_schema+"."+name+'"'
    try:
        check("CREATE SCHEMA "+schema)
        check("CREATE TABLE "+table+"(v INTEGER)")
        check("CREATE PROCEDURE "+public+"() LANGUAGE SQL AS $$INSERT INTO "+table+" VALUES(7)$$")
        check("CREATE PROCEDURE "+private+"() LANGUAGE SQL AS $$INSERT INTO "+table+" VALUES(8)$$")
        check("CALL "+public+"()")
        check("CALL "+private+"()")
        check("SELECT v FROM "+table+" ORDER BY v",[["7"],["8"]])
        check("SET search_path TO "+schema+",public")
        check("CALL "+name+"()")
        check("SELECT v FROM "+table+" ORDER BY v",[["7"],["8"],["8"]])
        check("CREATE OR REPLACE PROCEDURE "+private+"() LANGUAGE SQL AS $$INSERT INTO "+table+" VALUES(9)$$")
        check("CALL "+private+"()")
        check("BEGIN")
        check("DROP PROCEDURE "+private+"()")
        check("ROLLBACK")
        check("CALL "+private+"()")
        check("SELECT v FROM "+table+" ORDER BY v",[["7"],["8"],["8"],["9"],["9"]])
        check("BEGIN")
        check("CREATE PROCEDURE "+schema+".q_"+suffix+"() LANGUAGE SQL AS $$SELECT 1$$")
        check("ROLLBACK")
        check("CALL "+schema+".q_"+suffix+"()",state="42883")
        check("CREATE PROCEDURE "+dotted+"() LANGUAGE SQL AS $$INSERT INTO "+table+" VALUES(11)$$")
        check("DROP SCHEMA "+schema+" RESTRICT",state="2BP01")
        check("CALL "+private+"()")
        check("DROP SCHEMA "+schema+" CASCADE")
        check("CALL "+dotted+"()")
        check("CALL "+public+"()")
        check("SELECT v FROM "+table+" ORDER BY v",[["7"],["7"],["8"],["8"],["9"],["9"],["9"],["11"]])
        check("DROP PROCEDURE "+dotted+"()")
        check("DROP PROCEDURE "+public+"()")
        check("DROP TABLE "+table)
        check("SELECT 1",[["1"]])
        print("PROCEDURE_NAMESPACE_COMPLETE",checked,"FAILED",len(failures),failures,flush=True)
        assert not failures,failures
    finally:
        if reference:sock.close()
        else:runner.stop_ours(server)


if __name__=="__main__":main()
