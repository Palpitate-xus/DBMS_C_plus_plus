#!/usr/bin/env python3
"""Global count namespace, context diagnostics and genuine input declarations.

Fixed PG18.6 SQL/oracle; normal Simple and Parse/Describe metadata only.
No Bind/planner/portal/format2 policy assertions.
"""
import json,os,struct,sys,tempfile
from pathlib import Path
from compat.numeric_type_review import Review
LABEL='GLOBAL_COUNT_BINDING_PROTOCOL'
EXPECTED_CONTROLS=58
base='VALUES(3),(1),(2) UNION ALL VALUES(4) ORDER BY 1'
cases=[]
def q(label,sql,rows=None,state=None):cases.append(dict(mode='simple',label=label,sql=sql,rows=rows,state=state))
def p(label,sql,parameters=None,state=None):cases.append(dict(mode='describe',label=label,sql=sql,parameters=parameters,state=state))
for tail in ('LIMIT column1','LIMIT (SELECT column1)','LIMIT (SELECT (SELECT column1))','OFFSET column1','LIMIT 0 OFFSET column1'):
 q('set output invisible '+tail,base+' '+tail,state='42703')
q('set alias invisible','SELECT 3 AS k UNION ALL SELECT 1 ORDER BY k LIMIT k',state='42703')
q('branch FROM qualified invisible','SELECT p.x AS k FROM(VALUES(3))p(x) UNION ALL VALUES(1) ORDER BY k LIMIT p.x',state='42P01')
q('branch FROM child qualified invisible','SELECT p.x AS k FROM(VALUES(3))p(x) UNION ALL VALUES(1) ORDER BY k LIMIT (SELECT p.x)',state='42P01')
q('child local unqualified',base+' LIMIT (SELECT column1 FROM(VALUES(2))s(column1))',[["1"],["2"]])
q('child local qualified',base+' LIMIT (SELECT s.column1 FROM(VALUES(2))s(column1))',[["1"],["2"]])
q('child local missing exposes no root',base+' LIMIT (SELECT column1 FROM(VALUES(2))s(x))',state='42703')
q('CTE visible to scalar child','WITH q(n)AS(VALUES(2)) '+base+' LIMIT(SELECT n FROM q)',[["1"],["2"]])
outer='SELECT p.x,(VALUES(10),(20) UNION ALL VALUES(30) ORDER BY 1 LIMIT p.x%2) FROM(VALUES(2),(3))p(x) ORDER BY 1'
q('genuine outer direct',outer,[["2",None],["3","10"]])
q('genuine outer through child',outer.replace('LIMIT p.x%2','LIMIT (SELECT p.x%2)'),[["2",None],["3","10"]])
q('ancestor same named set output','SELECT p.column1,(VALUES(10),(20) UNION ALL VALUES(30) ORDER BY 1 LIMIT column1%2) FROM(VALUES(2),(3))p(column1) ORDER BY 1',[["2",None],["3","10"]])
q('grandparent genuine depth','SELECT p.x,(SELECT(VALUES(10),(20) UNION ALL VALUES(30) ORDER BY 1 LIMIT p.x%2)) FROM(VALUES(2),(3))p(x) ORDER BY 1',[["2",None],["3","10"]])
q('child local shadows real outer',outer.replace('LIMIT p.x%2','LIMIT (SELECT x FROM(VALUES(1))s(x))'),[["2","10"],["3","10"]])
q('VALUES local forbidden','VALUES(3),(1) LIMIT column1',state='42P10')
q('VALUES local through scalar forbidden','VALUES(3),(1) LIMIT(SELECT column1)',state='42P10')
q('VALUES child local allowed','VALUES(3),(1) LIMIT(SELECT column1 FROM(VALUES(1))s(column1))',[["3"]])
q('set direct count aggregate',base+' LIMIT sum(1)',state='42803')
q('set direct offset aggregate',base+' OFFSET count(*)',state='42803')
q('set direct count window',base+' LIMIT row_number() OVER()',state='42P20')
q('set direct count SRF',base+' LIMIT generate_series(1,2)',state='0A000')
q('unselected aggregate arm still analyzed',base+' LIMIT CASE WHEN false THEN sum(1) ELSE 1 END',state='42803')
q('child aggregate belongs child',base+' LIMIT (SELECT count(*) FROM(VALUES(1),(2))s(x))',[["1"],["2"]])
q('child window belongs child',base+' LIMIT (SELECT row_number() OVER())',[["1"]])
q('child SRF belongs child',base+' LIMIT (SELECT generate_series(1,1))',[["1"]])
q('coalesce direct SRF role',base+' LIMIT COALESCE(generate_series(1,2),1)',state='0A000')
for expression,state in (("true",'42804'),("'2'::text",'42804'),('ARRAY[1]','42804'),('ROW(1)','42804'),("'bad'",'22P02'),('(SELECT 1,2)','42601')):
 q('count type '+expression,base+' LIMIT '+expression,state=state)
q('unknown literal coerces bigint',base+" LIMIT '2'",[["1"],["2"]])
q('numeric implicit count rounding',base+' LIMIT 1.5::numeric',[["1"],["2"]])
q('float implicit count rounding',base+' LIMIT 1.5::double precision',[["1"],["2"]])
q('OFFSET analysis before LIMIT type',base+' LIMIT true OFFSET column1',state='42703')
q('OFFSET type before LIMIT name',base+' LIMIT column1 OFFSET true',state='42804')
q('OFFSET role before LIMIT role',base+' LIMIT sum(1) OFFSET row_number() OVER()',state='42P20')
q('VALUES agg before variable check','VALUES(3),(1) LIMIT sum(column1)',state='42803')
q('VALUES window role','VALUES(3),(1) LIMIT row_number() OVER(ORDER BY column1)',state='42P20')
q('VALUES SRF role','VALUES(3),(1) LIMIT generate_series(1,column1)',state='0A000')
p('bare unknown count',base+' LIMIT $1',[20])
p('unknown arithmetic selected input',base+' LIMIT ($1+0)',[23])
p('unknown explicit text input',base+' LIMIT ($1::text)::bigint',[25])
p('unknown COALESCE integer input',base+' LIMIT COALESCE($1,1)',[23])
p('two direct count inputs',base+' LIMIT $1 OFFSET $2',[20,20])
p('OFFSET arithmetic first chooses int4',base+' LIMIT $1 OFFSET ($1+0)',[23])
p('OFFSET direct first chooses int8',base+' LIMIT ($1+0) OFFSET $1',[20])
p('OFFSET text constraint rejects LIMIT',base+' LIMIT $1 OFFSET ($1::text)::bigint',state='42804')
p('output actual int4 constrains count','SELECT $1 AS k UNION ALL SELECT 2 ORDER BY k LIMIT $1',[23])
p('unknown both arithmetic ambiguous',base+' LIMIT ($1+$2)',state='42725')
p('same unknown child OFFSET finalized TEXT',base+' LIMIT $1 OFFSET (SELECT $1)',state='42804')
p('unknown CASE selected input',base+' LIMIT CASE WHEN true THEN $1 ELSE 1 END',[23])
p('unknown selected numeric input',base+' LIMIT ($1+1.0)',[1700])
p('unknown independent target finalized TEXT','SELECT $1 AS x UNION ALL SELECT $1 LIMIT $2',[25,20])


def normal(value):
 if isinstance(value,bytes):return {'hex':value.hex()}
 if isinstance(value,dict):return {k:normal(v) for k,v in value.items()}
 if isinstance(value,(tuple,list)):return [normal(v) for v in value]
 return value

assert len(cases)==EXPECTED_CONTROLS, len(cases)
def main():
    r=Review(LABEL);observations=[]
    try:
     for case in cases:
      if case['mode']=='simple':
       messages=r.client.simple_query(r.sock,case['sql']);actual=r.runner.decode_wire_result(messages,include_types=True)
       good=actual[1]==case['state'] and messages[-1]==(b'Z',b'I')
       if case['rows'] is not None:good=good and actual[0]==case['rows'] and actual[4]=='SELECT '+str(len(case['rows']))
       if case['state']:good=good and actual[0]==[] and actual[4] is None
       detail={'result':actual,'messages':messages}
      else:
       name=('binding_'+str(r.controls)).encode();body=name+b'\0'+case['sql'].encode()+b'\0'+struct.pack('!H',0)
       r.sock.sendall(r.client.typed(b'P',body)+r.client.typed(b'D',b'S'+name+b'\0')+r.client.typed(b'S'))
       messages=r.client.read_until_ready(r.sock);actual=r.runner.decode_wire_result(messages,include_types=True)
       params=[]
       for kind,body in messages:
        if kind==b't':
         n=struct.unpack('!H',body[:2])[0];params.append([struct.unpack('!I',body[2+4*i:6+4*i])[0] for i in range(n)])
       good=actual[1]==case['state'] and messages[-1]==(b'Z',b'I') and not any(k in (b'D',b'C') for k,_ in messages)
       if case['state']:good=good and not any(k==b'1' for k,_ in messages)
       else:
        descriptions=[r.client.row_description_fields([message]) for message in messages if message[0]==b'T']
        oid=25 if case['label']=='unknown independent target finalized TEXT' else 23
        header=b'x' if oid==25 else b'k' if case['label']=='output actual int4 constrains count' else b'column1'
        good=good and params==[case['parameters']] and sum(k==b'1' for k,_ in messages)==1
        good=good and len(descriptions)==1 and descriptions[0]==[(header,0,0,oid,-1 if oid==25 else 4,-1,0)]
       detail={'result':actual,'parameters':params,'messages':messages}
      r.record(case['label'],good,detail,case);observations.append(dict(case=case,actual=detail,match=good))
     r.complete(len(cases))
    finally:
     output=os.environ.get('GLOBAL_COUNT_BINDING_OUTPUT_DIR')
     output=Path(output) if output else Path(tempfile.mkdtemp(prefix='dbms-global-count-binding-'))
     output.mkdir(parents=True,exist_ok=True)
     destination=output/('reference-observations.json' if '--reference18' in sys.argv else 'observations.json')
     destination.write_text(json.dumps(normal(observations),indent=2)+'\n')
     print(LABEL+'_OBSERVATIONS',str(destination),flush=True);r.close()


if __name__ == "__main__":
    main()
