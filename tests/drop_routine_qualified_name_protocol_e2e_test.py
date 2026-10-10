"""Qualified routine deletion preserves schema/name boundaries."""
import importlib.util
import socket
import sys
import uuid
from pathlib import Path


def main():
    root=Path(__file__).resolve().parents[1]
    spec=importlib.util.spec_from_file_location("drop_runner",root/"tests/compat/pg_diff_runner.py")
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
        print("DROP_QUALIFIED",sql,result,"pass=",good,flush=True)
        if not good:failures.append((sql,result,rows,state))
    suffix=uuid.uuid4().hex[:12];schema='"drop_'+suffix+'.X"'
    name='"F.Y_'+suffix+'"';private=schema+" . "+name;public="public."+name
    try:
        check("CREATE SCHEMA "+schema)
        check("CREATE FUNCTION "+public+"() RETURNS INT LANGUAGE SQL AS $$SELECT 7$$")
        check("CREATE FUNCTION "+private+"() RETURNS INT LANGUAGE SQL AS $$SELECT 8$$")
        check("SELECT "+private+"()",[["8"]])
        check("DROP FUNCTION "+private)
        check("SELECT "+private+"()",state="42883")
        check("SELECT "+public+"()",[["7"]])
        check("CREATE FUNCTION "+private+"() RETURNS INT LANGUAGE SQL AS $$SELECT 9$$")
        check("DROP ROUTINE "+private+" () RESTRICT")
        check("SELECT "+private+"()",state="42883")
        check("SELECT "+public+"()",[["7"]])
        check("CREATE PROCEDURE "+private+"() LANGUAGE SQL AS $$SELECT 1$$")
        check("DROP PROCEDURE "+private+" ()")
        check("DROP PROCEDURE IF EXISTS "+private+" ()")
        check("DROP ROUTINE IF EXISTS "+private+" ()")
        check("DROP FUNCTION IF EXISTS "+private+" ()")
        check("DROP FUNCTION "+public+" ()")
        check("SELECT "+public+"()",state="42883")
        check("DROP SCHEMA "+schema)
        check("SELECT 1",[["1"]])
        print("DROP_QUALIFIED_COMPLETE",checked,"FAILED",len(failures),failures,flush=True)
        assert not failures,failures
    finally:
        if reference:sock.close()
        else:runner.stop_ours(server)


if __name__=="__main__":main()
