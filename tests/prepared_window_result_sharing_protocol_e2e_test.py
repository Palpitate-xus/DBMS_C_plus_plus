#!/usr/bin/env python3
"""Strict PG18.6 ordinary window result sharing and original-site demand.

Simple and valid text/binary Extended Execute0; no retained-plan, failed-Bind,
format2, portal-lifetime, grouped aggregate or interrupt-policy assertions.
The fixed PG fixture retains every full observation; no alternative oracle.
"""
import hashlib
import json
import os
from pathlib import Path
import struct
import sys
import tempfile
from compat.numeric_type_review import Review

LABEL='PREPARED_WINDOW_RESULT_SHARING_PROTOCOL'
EXPECTED_CONTROLS=220

def shapes(sequence):
    a="lag(nextval('"+sequence+"'),0) OVER(ORDER BY id)"
    pair="SELECT id,"+a+" AS a,"+a+" AS b FROM fws_src"
    return [
        ('final-source-id',pair+' ORDER BY id'),
        ('no-final-order',pair),
        ('final-other-source',pair+' ORDER BY v'),
        ('final-window-alias',pair+' ORDER BY a'),
        ('both-window-aliases',pair+' ORDER BY a,b'),
        ('final-window-ordinal',pair+' ORDER BY 2'),
        ('source-order-limit',pair+' ORDER BY id LIMIT 2'),
        ('source-order-offset',pair+' ORDER BY id LIMIT 2 OFFSET 2'),
        ('source-order-ties',pair+' ORDER BY id FETCH FIRST 2 ROWS WITH TIES'),
        ('wrapped-window-subtree',"SELECT id,"+a+" AS a,("+a+")+0 AS b FROM fws_src ORDER BY id"),
        ('different-offset',"SELECT id,"+a+" AS a,lag(nextval('"+sequence+"'),1) OVER(ORDER BY id) AS b FROM fws_src ORDER BY id"),
        ('separate-named-clauses',"SELECT id,lag(nextval('"+sequence+"'),0) OVER w1 AS a,lag(nextval('"+sequence+"'),0) OVER w2 AS b FROM fws_src WINDOW w1 AS(ORDER BY id),w2 AS(ORDER BY id) ORDER BY id"),
        ('default-vs-explicit-ASC',"SELECT id,"+a+" AS a,lag(nextval('"+sequence+"'),0) OVER(ORDER BY id ASC) AS b FROM fws_src ORDER BY id"),
        ('independent-SQL-child-sites',"SELECT id,lag((SELECT nextval('"+sequence+"')),0) OVER(ORDER BY id) AS a,lag((SELECT nextval('"+sequence+"')),0) OVER(ORDER BY id) AS b FROM fws_src ORDER BY id"),
        ('ordinary-volatile-not-CSE',"SELECT id,"+a+" AS a,"+a+" AS b,nextval('"+sequence+"') AS x,nextval('"+sequence+"') AS y FROM fws_src ORDER BY id"),
        ('window-aggregate-FILTER',"SELECT id,sum(nextval('"+sequence+"')) FILTER(WHERE id%2=0) OVER(ORDER BY id) AS a,sum(nextval('"+sequence+"')) FILTER(WHERE id%2=0) OVER(ORDER BY id) AS b FROM fws_src ORDER BY id"),
        ('DISTINCT-target-boundary',pair.replace('SELECT id,','SELECT DISTINCT id,',1)+' ORDER BY id'),
        ('hidden-sort-window-subtree',"SELECT id,"+a+" AS a FROM fws_src ORDER BY ("+a+")+0"),
        ('different-unused-defaults',"SELECT id,lag(nextval('"+sequence+"'),0,0::bigint) OVER(ORDER BY id) AS a,lag(nextval('"+sequence+"'),0,1::bigint) OVER(ORDER BY id) AS b FROM fws_src ORDER BY id"),
        ('conditional-projection-reference',"SELECT id,"+a+" AS a,CASE WHEN id<4 THEN "+a+" ELSE 0::bigint END AS b FROM fws_src ORDER BY id"),
        ('different-window-FILTER',"SELECT id,sum(nextval('"+sequence+"')) FILTER(WHERE id%2=0) OVER(ORDER BY id) AS a,sum(nextval('"+sequence+"')) FILTER(WHERE id%2=1) OVER(ORDER BY id) AS b FROM fws_src ORDER BY id"),
        ('different-stage-final-reference',"SELECT id,"+a+" AS a,"+a+" AS b,row_number() OVER(ORDER BY id DESC) AS n FROM fws_src ORDER BY id"),
    ]


def matrix():
    result=[]
    # Every shape is fixed in Simple; these eight add both valid format modes.
    extended={0,1,3,4,6,11,13,21}
    for mode in ['simple','text','binary']:
        for index in range(22):
            if mode!='simple' and index not in extended:
                continue
            sequence='fws_'+str(len(result))
            label,sql=shapes(sequence)[index]
            result.append(dict(label=label,mode=mode,sequence=sequence,sql=sql))
    assert len(result)==38
    return result


def extra_matrix():
    result=[]
    for index in range(15):
        seq='fws_extra_'+str(index)
        one="lag(nextval('"+seq+"'),0) OVER(ORDER BY id)"
        part="lag(nextval('"+seq+"'),0) OVER(PARTITION BY p ORDER BY v,id)"
        pure="lag(length(v::text),0) OVER(ORDER BY id)"
        agg="sum(nextval('"+seq+"')) FILTER(WHERE id%2=0) OVER(ORDER BY id)"
        function='fws_fn_'+str(index)
        call='lag('+function+'(),0) OVER(ORDER BY id)'
        definitions=[
            ('original143-partition-incompatible-order','SELECT id,'+part+' AS a,'+part+' AS b FROM fws_src ORDER BY id',[]),
            ('partition-compatible-order','SELECT id,'+part+' AS a,'+part+' AS b FROM fws_src ORDER BY p,v,id',[]),
            ('separate-named-real-sort',"SELECT id,lag(nextval('"+seq+"'),0) OVER w1 AS a,lag(nextval('"+seq+"'),0) OVER w2 AS b FROM fws_src WINDOW w1 AS(ORDER BY id),w2 AS(ORDER BY id) ORDER BY v",[]),
            ('default-ASC-real-sort','SELECT id,'+one+" AS a,lag(nextval('"+seq+"'),0) OVER(ORDER BY id ASC) AS b FROM fws_src ORDER BY v",[]),
            ('wrapper-real-sort','SELECT id,'+one+' AS a,('+one+')+0 AS b FROM fws_src ORDER BY v',[]),
            ('conditional-real-sort','SELECT id,'+one+' AS a,CASE WHEN id<4 THEN '+one+' ELSE 0::bigint END AS b FROM fws_src ORDER BY v',[]),
            ('window-aggregate-FILTER-real-sort','SELECT id,'+agg+' AS a,'+agg+' AS b FROM fws_src ORDER BY v',[]),
            ('three-aliases-retain-two','SELECT id,'+one+' AS a,'+one+' AS b,'+one+' AS c FROM fws_src ORDER BY a,b',[]),
            ('nonvolatile-LAG-compatible-order','SELECT id,'+pure+' AS a,'+pure+' AS b FROM fws_src ORDER BY id',[]),
            ('nonvolatile-SUM-no-final-order','SELECT id,sum(v) OVER(ORDER BY id) AS a,sum(v) OVER(ORDER BY id) AS b FROM fws_src',[]),
            ('declared-STABLE-no-final-order','SELECT id,'+call+' AS a,'+call+' AS b FROM fws_src',
             ["CREATE FUNCTION "+function+"() RETURNS BIGINT LANGUAGE SQL STABLE AS $$SELECT nextval('"+seq+"')$$"]),
            ('declared-STABLE-real-sort','SELECT id,'+call+' AS a,'+call+' AS b FROM fws_src ORDER BY v',
             ["CREATE FUNCTION "+function+"() RETURNS BIGINT LANGUAGE SQL STABLE AS $$SELECT nextval('"+seq+"')$$"]),
            ('declared-VOLATILE-no-final-order','SELECT id,'+call+' AS a,'+call+' AS b FROM fws_src',
             ["CREATE FUNCTION "+function+"() RETURNS BIGINT LANGUAGE SQL VOLATILE AS $$SELECT nextval('"+seq+"')$$"]),
            ('declared-IMMUTABLE-pure','SELECT id,'+call+' AS a,'+call+' AS b FROM fws_src ORDER BY id',
             ["CREATE FUNCTION "+function+"() RETURNS INTEGER LANGUAGE SQL IMMUTABLE AS $$SELECT 7$$"]),
            ('declared-STABLE-window-SUM','SELECT id,sum('+function+'()) OVER(ORDER BY id) AS a,sum('+function+'()) OVER(ORDER BY id) AS b FROM fws_src',
             ["CREATE FUNCTION "+function+"() RETURNS BIGINT LANGUAGE SQL STABLE AS $$SELECT nextval('"+seq+"')$$"]),
        ]
        label,sql,setup=definitions[index]
        result.append(dict(label=label,mode='simple',sequence=seq,sql=sql,setup=setup))
    assert len(result)==15
    return result


def clean(value):
    if isinstance(value,bytes):return {'hex':value.hex()}
    if isinstance(value,(tuple,list)):return [clean(item) for item in value]
    if isinstance(value,dict):return {str(k):clean(v) for k,v in value.items()}
    return value


def observe(sock,wire,sql,mode,index):
    if mode=='simple':return wire.simple_query(sock,sql)
    name=('fws_stmt_'+str(index)).encode()
    parse=name+b'\0'+sql.encode()+b'\0'+struct.pack('!H',0)
    bind=b'\0'+name+b'\0'+struct.pack('!HH',0,0)
    bind+=struct.pack('!HH',1,1) if mode=='binary' else struct.pack('!H',0)
    sock.sendall(wire.typed(b'P',parse)+wire.typed(b'D',b'S'+name+b'\0')+
        wire.typed(b'B',bind)+wire.typed(b'D',b'P\0')+
        wire.typed(b'E',b'\0'+struct.pack('!I',0))+wire.typed(b'S'))
    return wire.read_until_ready(sock)


def decode(wire,messages):
    return dict(rows=clean(wire.data_row_values(messages)),
        descriptions=clean([wire.row_description_fields([message]) for message in messages if message[0]==b'T']),
        errors=clean([wire.diagnostic_fields(body) for kind,body in messages if kind==b'E']),
        states=[wire.diagnostic_fields(body).get(b'C',b'').decode() for kind,body in messages if kind==b'E'],
        commands=[body.rstrip(b'\0').decode() for kind,body in messages if kind==b'C'],
        kinds=[kind.decode() for kind,body in messages],
        parameters=[body.hex() for kind,body in messages if kind==b't'],
        ready=[body.hex() for kind,body in messages if kind==b'Z'],raw=clean(messages))


def cases():
    result=[]
    def add(label,sql,mode='simple'):
        result.append(dict(ordinal=len(result)+1,label=label,sql=sql,mode=mode))
    add('physical source','CREATE TABLE fws_src(id INTEGER,p INTEGER,v BIGINT)')
    add('six physical rows','INSERT INTO fws_src VALUES(1,0,10),(2,0,20),(3,1,30),(4,1,40),(5,2,50),(6,2,60)')
    add('actual physical origin','SELECT id FROM fws_src LIMIT 0')
    for case in matrix():
        name=case['mode']+' '+case['label']
        add(name+' create','CREATE SEQUENCE '+case['sequence'])
        add(name+' seed',"SELECT nextval('"+case['sequence']+"')")
        add(name,case['sql'],case['mode'])
        add(name+' post',"SELECT nextval('"+case['sequence']+"')")
    assert len(result)==155
    for case in extra_matrix():
        name='extra '+case['label']
        add(name+' create','CREATE SEQUENCE '+case['sequence'])
        add(name+' seed',"SELECT nextval('"+case['sequence']+"')")
        for index,sql in enumerate(case['setup']):add(name+' declaration '+str(index),sql)
        add(name,case['sql'],case['mode'])
        add(name+' post',"SELECT nextval('"+case['sequence']+"')")
    assert len(result)==EXPECTED_CONTROLS
    return result


def normalize(observations):
    # Only the independent genuine physical relation OID is normalized,
    # anchored by its actual id LIMIT0 description. Never infer origin by
    # an output alias or hide a missing/wrong descriptor.
    fields=observations[2]['actual']['descriptions']
    origin=None
    if len(fields)==1 and len(fields[0])==1:
        field=fields[0][0]
        if field[1]>0 and field[2:]==[1,23,4,-1,0]:origin=field[1]
    normalized=[]
    for item in observations:
        actual=item['actual'];descriptions=[]
        for description in actual['descriptions']:
            fields=[]
            for raw in description:
                field=list(raw)
                if origin is not None and field[1]==origin:field[1]='actual:fws_src'
                fields.append(field)
            descriptions.append(fields)
        normalized.append(dict(rows=actual['rows'],descriptions=descriptions,errors=actual['errors'],
            states=actual['states'],commands=actual['commands'],kinds=actual['kinds'],
            parameters=actual['parameters'],ready=actual['ready']))
    return normalized


def main():
    fixed=json.loads((Path(__file__).with_name('compat')/'prepared_window_result_sharing_pg18_expected.json').read_text())
    matrix=cases();assert fixed['controls']==EXPECTED_CONTROLS
    assert [item['case'] for item in fixed['cases']]==matrix
    output=Path(os.environ['PREPARED_WINDOW_RESULT_SHARING_OUTPUT_DIR']) if os.environ.get('PREPARED_WINDOW_RESULT_SHARING_OUTPUT_DIR') else Path(tempfile.mkdtemp(prefix='dbms-window-result-sharing-'))
    output.mkdir(parents=True,exist_ok=True)
    review=Review(LABEL);observations=[]
    try:
        for case in matrix:
            actual=decode(review.client,observe(review.sock,review.client,case['sql'],case['mode'],case['ordinal']-1))
            observations.append(dict(case=case,actual=actual))
        normalized=normalize(observations)
        for item,actual,reference in zip(observations,normalized,fixed['cases']):
            item['normalized']=actual;item['match']=actual==reference['expected']
            review.record(item['case']['label'],item['match'],actual,reference['expected'])
        review.complete(EXPECTED_CONTROLS)
    finally:
        target=output/('reference-observations.json' if '--reference18' in sys.argv else 'observations.json')
        target.write_text(json.dumps(dict(controls=review.controls,failures=len(review.failures),
            source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),observations=observations),ensure_ascii=False,indent=2)+'\n')
        print(LABEL+'_OBSERVATIONS',str(target),flush=True)
        review.close()


if __name__=='__main__':main()
