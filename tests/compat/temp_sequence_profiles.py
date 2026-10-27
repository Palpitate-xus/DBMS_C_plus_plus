"""Frozen 182-operation temporary sequence lifecycle; primary PG18 oracle."""
import socket
import struct
from quoted_unqualified_scalar_projection_protocol_e2e_test import strict_data_rows

def run(review,observe):
    r=review

    reference=r.server is None

    if reference:
        host,port,user,database,password=r.runner._reference_connection_settings()
    else:
        host,port='127.0.0.1',r.server['port']

    def connect():
        sock=socket.create_connection((host,port),timeout=r.runner.wire_timeout())
        if reference:r.client.startup_reference(sock,user,database,password)
        else:r.client.startup(sock,'alice','info')
        return sock

    owner=r.sock

    peer=connect()

    ordinal=0

    def result(profile,label,messages):
        nonlocal ordinal
        value=r.runner.decode_wire_result(messages,include_types=True)
        observe(dict(ordinal=ordinal,profile=profile,label=label,
            kinds=[kind.decode() for kind,_ in messages],
            dataPayloads=[body.hex() for kind,body in messages if kind==b'D'],
            dataFramesValid=strict_data_rows(messages)[1],
            emptyPayloadsValid=all(not body for kind,body in messages if kind in (b'1',b'2',b'3',b'n',b'I',b's')),rows=value[0],state=value[1],message=value[2],columns=value[3],tag=value[4],oids=value[5],
            ready=messages[-1][1].decode() if messages and messages[-1][0]==b'Z' else None,
            notices=[[r.client.diagnostic_fields(body).get(b'S',b'').decode(),r.client.diagnostic_fields(body).get(b'C',b'').decode()] for kind,body in messages if kind==b'N']))
        ordinal+=1

    def query(sock,profile,label,sql,extended=False):
        if extended:
            packet=r.client.typed(b'P',b'\0'+sql.encode()+b'\0'+struct.pack('!H',0))
            packet+=r.client.typed(b'B',b'\0\0'+struct.pack('!HHH',0,0,0))
            packet+=r.client.typed(b'E',b'\0'+struct.pack('!I',0))+r.client.typed(b'S')
            sock.sendall(packet);messages=r.client.read_until_ready(sock)
        else:messages=r.client.simple_query(sock,sql)
        result(profile,dict(label,sql=sql),messages)

    def finish_owner(sock,label):
        nonlocal ordinal
        sock.sendall(r.client.typed(b'X'))
        # EOF is observed only after this server's backend guard/cleanup returns.
        remaining=sock.recv(1);assert remaining==b'',remaining
        sock.close()
        observe(dict(ordinal=ordinal,profile='owner_terminate_eof',label=label,transportEOF=True));ordinal+=1
    try:
        query(owner,'persistent_setup',{},'CREATE SEQUENCE r309_persistent_control START WITH 900')
        case=0
        for prefix in ('CREATE TEMP SEQUENCE','CREATE TEMPORARY SEQUENCE','CREATE LOCAL TEMP SEQUENCE'):
                for extended in (False,True):
                    label=dict(case=case,prefix=prefix,extended=extended);name='r309_temp_'+str(case);case+=1
                    query(owner,'owner_create',label,prefix+' '+name+' START WITH 11',extended)
                    query(owner,'owner_implicit',label,"SELECT nextval('"+name+"') AS v")
                    query(owner,'owner_explicit_temp',label,"SELECT nextval('pg_temp."+name+"') AS v")
                    query(peer,'peer_without_own',label,"SELECT nextval('"+name+"') AS v")
                    query(peer,'peer_create_own',label,prefix+' '+name+' START WITH 101',extended)
                    query(peer,'peer_own_value',label,"SELECT nextval('"+name+"') AS v")
                    query(owner,'owner_still_own',label,"SELECT nextval('"+name+"') AS v")
                    query(peer,'peer_discard_temp',label,'DISCARD TEMP')
                    query(peer,'peer_after_discard',label,"SELECT nextval('"+name+"') AS v")
                    query(owner,'owner_after_peer_discard',label,"SELECT nextval('"+name+"') AS v")
                    finish_owner(owner,label)
                    query(peer,'peer_after_owner_exit',label,"SELECT nextval('"+name+"') AS v")
                    query(peer,'public_after_owner_exit',label,"SELECT nextval('public."+name+"') AS v")
                    query(peer,'persistent_after_owner_exit',label,"SELECT nextval('r309_persistent_control') AS v")
                    query(peer,'cleanup_public_probe',label,'DROP SEQUENCE IF EXISTS public.'+name)
                    owner=connect();r.sock=owner
        for extended in (False,True):
                label=dict(case=case,extended=extended,extra=True);name='r309_extra_'+str(case);case+=1
                requests=[
                    ('temp_public_forbidden','CREATE TEMP SEQUENCE public.'+name),
                    ('temp_missing_schema','CREATE TEMP SEQUENCE r309_missing.'+name),
                    ('temp_public_ifnot_forbidden','CREATE TEMP SEQUENCE IF NOT EXISTS public.r309_persistent_control'),
                    ('alias_setup','CREATE SEQUENCE pg_temp.'+name+' START WITH 21'),
                    ('alias_probe',"SELECT nextval('pg_temp."+name+"') AS v"),
                    ('alias_drop','DROP SEQUENCE pg_temp.'+name),
                    ('temp_alias_setup','CREATE TEMP SEQUENCE pg_temp.'+name+' START WITH 31'),
                    ('temp_alias_probe',"SELECT nextval('pg_temp."+name+"') AS v"),
                    ('temp_alias_drop','DROP SEQUENCE pg_temp.'+name),
                    ('shadow_public_setup','CREATE SEQUENCE public.'+name+' START WITH 701'),
                    ('shadow_temp_setup','CREATE TEMP SEQUENCE '+name+' START WITH 11'),
                    ('shadow_temp_probe',"SELECT nextval('"+name+"') AS v"),
                    ('shadow_public_probe',"SELECT nextval('public."+name+"') AS v"),
                    ('shadow_peer_probe',"SELECT nextval('"+name+"') AS v"),
                    ('shadow_drop_temp','DROP SEQUENCE '+name),
                    ('shadow_public_fallback',"SELECT nextval('"+name+"') AS v"),
                    ('shadow_drop_public','DROP SEQUENCE public.'+name),
                    ('tx_begin','BEGIN'),('tx_savepoint','SAVEPOINT r309_guard'),
                    ('tx_create','CREATE TEMP SEQUENCE '+name+'_tx START WITH 41'),
                    ('tx_probe',"SELECT nextval('pg_temp."+name+"_tx') AS v"),
                    ('tx_recover','ROLLBACK TO SAVEPOINT r309_guard'),
                    ('tx_missing',"SELECT nextval('pg_temp."+name+"_tx') AS v"),
                    ('tx_rollback','ROLLBACK'),
                    ('tx_public_missing',"SELECT nextval('public."+name+"_tx') AS v"),
                    ('keep_create','CREATE TEMP SEQUENCE '+name+'_keep START WITH 51'),
                    ('keep_begin','BEGIN'),('keep_savepoint','SAVEPOINT r309_guard'),
                    ('keep_drop','DROP SEQUENCE pg_temp.'+name+'_keep'),
                    ('keep_recover','ROLLBACK TO SAVEPOINT r309_guard'),
                    ('keep_probe',"SELECT nextval('pg_temp."+name+"_keep') AS v"),
                    ('keep_rollback','ROLLBACK'),
                    ('keep_after_rollback',"SELECT nextval('pg_temp."+name+"_keep') AS v"),
                    ('keep_final_drop','DROP SEQUENCE pg_temp.'+name+'_keep'),
                    ('table_setup','CREATE TEMP TABLE '+name+'_table(id integer)'),
                    ('table_insert','INSERT INTO '+name+'_table VALUES(9)'),
                    ('first_table_conflict','CREATE TEMP SEQUENCE '+name+'_table START WITH 999'),
                    ('table_still_usable','SELECT id FROM '+name+'_table'),
                    ('table_conflict_public_missing',"SELECT nextval('public."+name+"_table') AS v"),
                    ('table_drop','DROP TABLE '+name+'_table'),
                    ('discard_remaining','DISCARD TEMP'),
                    ('cleanup_keep_public','DROP SEQUENCE IF EXISTS public.'+name+'_keep'),
                    ('cleanup_table_public','DROP SEQUENCE IF EXISTS public.'+name+'_table'),
                    ('cleanup_tx_public','DROP SEQUENCE IF EXISTS public.'+name+'_tx'),
                    ('cleanup_forbidden_public','DROP SEQUENCE IF EXISTS public.'+name),
                ]
                for profile,sql in requests:
                    active=peer if profile=='shadow_peer_probe' else owner
                    query(active,profile,label,sql,extended and profile in ('alias_setup','temp_alias_setup','shadow_temp_setup','tx_create','keep_create','first_table_conflict'))
        query(peer,'persistent_cleanup',{},'DROP SEQUENCE r309_persistent_control')
        assert case==8 and ordinal==92+2*len(requests),(case,ordinal)
        return ordinal
    finally:
        owner.close()
        peer.close()
