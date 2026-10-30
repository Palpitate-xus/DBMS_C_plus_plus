#!/usr/bin/env python3
"""Frozen PG18.6 FLOAT8 output signatures, including all 489 requests.

Full private-relation origins are retained: mismatches are not silently
normalized away to claim a green suite. Data-only output bugs and these
independent origin differences are classified separately by the audit.
"""
from compat.numeric_type_review import Review
from pathlib import Path
import json
import struct

def signature(messages):
    states=[]
    for kind,body in messages:
        if kind!=b'E':continue
        assert body.endswith(b'\0\0')
        fields={}
        for field in body[:-1].split(b'\0'):
            if not field:continue
            assert field[:1] not in fields
            fields[field[:1]]=field[1:]
        assert fields.get(b'S')==b'ERROR' and fields.get(b'M')
        state=fields[b'C'].decode('ascii');assert len(state)==5
        fields[b'M'].decode('utf-8');states.append(state)
    assert all(body==b'' for kind,body in messages if kind in (b'1',b'2',b'3',b'n'))
    return dict(kinds=[kind.decode('ascii') for kind,body in messages],states=states,
        descriptions=[body.hex() for kind,body in messages if kind==b'T'],
        parameters=[body.hex() for kind,body in messages if kind==b't'],
        data=[body.hex() for kind,body in messages if kind==b'D'],
        commands=[body.hex() for kind,body in messages if kind==b'C'],
        ready=[body.hex() for kind,body in messages if kind==b'Z'])

def main():
    plan=json.loads((Path(__file__).parent/'compat/float8_output_pg18_expected.json').read_text())
    assert len(plan)==489 and [row['request']['index'] for row in plan]==list(range(1,490))
    review=Review('FLOAT8_OUTPUT')
    try:
        for row in plan:
            request=row['request'];sql=request['sql'];mode=request['mode']
            try:
                if mode=='simple':messages=review.client.simple_query(review.sock,sql)
                else:
                    inputs=request['inputs'];values=request['parameters'];binary_input=request['binaryInput']
                    parse=b'\0'+sql.encode('utf-8')+b'\0'+struct.pack('!H',len(inputs))+b''.join(struct.pack('!I',oid) for oid in inputs)
                    assert len(inputs)==len(values)
                    bind=b'\0\0'+struct.pack('!H',len(values))+b''.join(struct.pack('!H',int(binary_input)) for value in values)
                    bind+=struct.pack('!H',len(values))
                    for oid,value in zip(inputs,values):
                        raw=None if value is None else struct.pack('!d',float(value)) if binary_input and oid==701 else struct.pack('!i',int(value)) if binary_input and oid in (0,23) else str(value).encode('utf-8')
                        bind+=struct.pack('!i',-1 if raw is None else len(raw))+(raw or b'')
                    bind+=struct.pack('!HH',1,int(mode=='binary'))
                    review.sock.sendall(review.client.typed(b'P',parse)+review.client.typed(b'D',b'S\0')+
                        review.client.typed(b'B',bind)+review.client.typed(b'D',b'P\0')+
                        review.client.typed(b'E',b'\0'+struct.pack('!I',0))+review.client.typed(b'S'))
                    messages=review.client.read_until_ready(review.sock)
                actual=signature(messages);valid=actual==row['expected']
            except Exception as error:
                actual=dict(exception=type(error).__name__,message=str(error));valid=False
            review.record((request['index'],mode,sql,request['inputs'],request['parameters'],request['binaryInput']),
                valid,actual,row['expected'])
        review.complete(489)
    finally:review.close()

if __name__=='__main__':main()
