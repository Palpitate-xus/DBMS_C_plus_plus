"""Frozen 463-operation Bind/portal profile; no product-derived oracle."""
import struct
from quoted_unqualified_scalar_projection_protocol_e2e_test import strict_data_rows

def run(review, observe):
    r=review
    ordinal=0
    def result(profile,label,messages):
        nonlocal ordinal
        rows,state,message,columns,tag,oids=r.runner.decode_wire_result(messages,include_types=True)
        observe(dict(ordinal=ordinal,profile=profile,label=label,kinds=[k.decode() for k,_ in messages],
            rows=rows,state=state,message=message,columns=columns,tag=tag,oids=oids,
            descriptions=[[field[0].decode(),*field[1:]] for kind,body in messages if kind==b'T' for field in r.client.row_description_fields([(kind,body)])],
            dataPayloads=[body.hex() for kind,body in messages if kind==b'D'],
            dataFramesValid=strict_data_rows(messages)[1],
            emptyPayloadsValid=all(not body for kind,body in messages if kind in (b'1',b'2',b'3',b'n',b'I',b's')),
            parameterPayloads=[body.hex() for kind,body in messages if kind==b't'],
            ready=messages[-1][1].decode() if messages and messages[-1][0]==b'Z' else None))
        ordinal+=1

    def exchange(profile,label,packet):
        r.sock.sendall(packet+r.client.typed(b'S'))
        result(profile,label,r.client.read_until_ready(r.sock))

    def query(profile,label,sql):result(profile,dict(label,sql=sql),r.client.simple_query(r.sock,sql))

    def bind(statement,portal,value,result_format=0):
        raw=value.encode()
        body=portal+b'\0'+statement+b'\0'+struct.pack('!HH',0,1)+struct.pack('!i',len(raw))+raw+struct.pack('!HH',1,result_format)
        return r.client.typed(b'B',body)

    def describe(portal):return r.client.typed(b'D',b'P'+portal+b'\0')

    def execute(portal,limit=0):return r.client.typed(b'E',portal+b'\0'+struct.pack('!I',limit))

    query('setup',{},'CREATE TEMP TABLE r304_rows(n integer)')
    query('setup',{},'INSERT INTO r304_rows VALUES(1),(2)')
    recovery=b'r304_recovery_statement'
    exchange('parse_recovery',{},r.client.typed(b'P',recovery+b'\0ROLLBACK\0'+struct.pack('!H',0)))
    case=0
    for old_state in ('none','bound','completed','suspended'):
            for naming in ('unnamed','named_fresh','named_duplicate'):
                if old_state=='none' and naming=='named_duplicate':continue
                for failure in ('text_input','missing_statement','result_format'):
                    statement=('r304_statement_'+str(case)).encode();old_portal=b'' if naming=='unnamed' else ('r304_old_'+str(case)).encode()
                    target=old_portal if naming!='named_fresh' else ('r304_new_'+str(case)).encode()
                    label=dict(case=case,oldState=old_state,naming=naming,failure=failure);case+=1
                    sql='SELECT $1::integer AS v FROM r304_rows ORDER BY n'
                    exchange('parse',dict(label,sql=sql),r.client.typed(b'P',statement+b'\0'+sql.encode()+b'\0'+struct.pack('!HI',1,23)))
                    query('begin',label,'BEGIN');query('savepoint',label,'SAVEPOINT r304_recovery')
                    if old_state!='none':
                        exchange('old_bind',label,bind(statement,old_portal,'43')+describe(old_portal))
                        if old_state in ('completed','suspended'):
                            exchange('old_execute',label,execute(old_portal,1 if old_state=='suspended' else 0))
                    bad_statement=b'r304_missing' if failure=='missing_statement' else statement
                    exchange('bad_bind',label,bind(bad_statement,target,'bad' if failure=='text_input' else '45',2 if failure=='result_format' else 0))
                    exchange('describe_statement_after_failure',label,r.client.typed(b'D',b'S'+statement+b'\0'))
                    exchange('describe_recovery_after_failure',label,r.client.typed(b'D',b'S'+recovery+b'\0'))
                    exchange('describe_target_after_failure',label,describe(target))
                    exchange('execute_target_after_failure',label,execute(target))
                    if naming=='named_fresh' and old_state!='none':
                        exchange('describe_old_after_failure',label,describe(old_portal))
                        exchange('execute_old_after_failure',label,execute(old_portal))
                    query('recover',label,'ROLLBACK TO SAVEPOINT r304_recovery')
                    healthy=('r304_healthy_'+str(case)).encode()
                    exchange('healthy_execute',label,bind(statement,healthy,'45')+describe(healthy)+execute(healthy))
                    query('rollback',label,'ROLLBACK')
                    exchange('close_statement',label,r.client.typed(b'C',b'S'+statement+b'\0'))
    exchange('close_recovery',{},r.client.typed(b'C',b'S'+recovery+b'\0'))
    assert case==33 and ordinal==463,(case,ordinal)
    return ordinal
