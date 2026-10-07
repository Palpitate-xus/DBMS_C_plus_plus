"""Real mutation-owned EXPLAIN/ANALYZE, strict180006 and unchanged deadlines."""
import importlib.util,json,os,socket,sys
from pathlib import Path
repo=Path(__file__).resolve().parent.parent
spec=importlib.util.spec_from_file_location('dml_explain_runner',repo/'tests/compat/pg_diff_runner.py')
r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);c=r.load_protocol_client()
reference=sys.argv[1:]==['--reference18'];assert not sys.argv[1:] or reference
server=None
if reference:
    h,p,u,d,pw=r._reference_connection_settings();sock=socket.create_connection((h,p),timeout=r.wire_timeout());c.startup_reference(sock,u,d,pw);r.verify_reference_version(c,sock)
else:server=r.start_ours(c);sock=server['sock']
failures=[];counter=1
def q(sql,rows=None,state=None):
    result=r.decode_wire_result(c.simple_query(sock,sql),include_types=True)
    print('DML_EXPLAIN',sql,result,flush=True)
    if result[1]!=state:failures.append((sql,'state',result[1],state))
    if rows is not None and result[0]!=rows:failures.append((sql,'rows',result[0],rows))
    if state and (result[0] or result[4] is not None):failures.append((sql,'partial',result[0],result[4]))
    return result
def plan(sql,operation,analyze,json_,state=None,returned=0):
    prefix='EXPLAIN ('+('ANALYZE TRUE,' if analyze else '')+'FORMAT '+('JSON' if json_ else 'TEXT')+',TIMING FALSE) '
    result=q(prefix+sql,state=state)
    if state or result[1]:return
    if not result[0] or result[4]!='EXPLAIN':failures.append((sql,'real plan/completion',result))
    text='\n'.join(row[0] for row in result[0])
    if json_:
        try:
            doc=json.loads(text);root=doc[0]['Plan'] if reference else doc['plan']
            node=root['Node Type'] if reference else root['nodeType']
            op=root['Operation'] if reference else root['operation']
            if node!='ModifyTable' or op!=operation:failures.append((sql,'mutation owner',node,op))
            if analyze:
                actual=root['Actual Rows'] if reference else root['actualRows']
                if actual!=returned:failures.append((sql,'actual returning rows',actual,returned))
        except (ValueError,KeyError,TypeError) as error:failures.append((sql,'plan shape',text,str(error)))
    elif operation.lower() not in text.lower():failures.append((sql,'actual operation',text,operation))

# sql, operation, runtime effects, runtime rows, analysis error, runtime error.
cases=[
 ('UPDATE dml_explain_rows SET v=7 WHERE id=1','Update',0,0,None,None),
 ('UPDATE dml_explain_rows SET v=dml_explain_writer(7) WHERE id=1 RETURNING id,v','Update',1,1,None,None),
 ('UPDATE dml_explain_rows SET v=dml_explain_writer(7) WHERE true RETURNING id,v','Update',2,2,None,None),
 ('UPDATE dml_explain_rows SET v=DEFAULT WHERE id=1 RETURNING v','Update',1,1,None,None),
 ('UPDATE dml_explain_rows SET v=DEFAULT WHERE false RETURNING v','Update',0,0,None,None),
 ('UPDATE dml_explain_rows SET v=DEFAULT WHERE id=99 RETURNING v','Update',0,0,None,None),
 ('UPDATE dml_explain_rows AS t SET v=s.v FROM dml_explain_source AS s WHERE t.id=s.id RETURNING t.id,t.v','Update',0,2,None,None),
 ('UPDATE dml_explain_rows SET v=(SELECT dml_explain_writer(s.id) FROM dml_explain_source AS s WHERE s.id=dml_explain_rows.id) WHERE id=1 RETURNING v','Update',1,1,None,None),
 ('UPDATE dml_explain_rows SET v=7 WHERE id=ANY(SELECT id FROM dml_explain_source) RETURNING id','Update',0,2,None,None),
 ('UPDATE dml_explain_rows SET v=7 WHERE id>ALL(SELECT id FROM dml_explain_source WHERE false) RETURNING id','Update',0,2,None,None),
 ('UPDATE dml_explain_rows SET v=CAST(tag AS INT) RETURNING v','Update',0,0,None,'22P02'),
 ('UPDATE dml_explain_rows SET v=dml_explain_fail() WHERE id=1 RETURNING id','Update',1,0,None,'P0001'),
 ('UPDATE dml_explain_rows SET v=dml_explain_writer(7) RETURNING missing_dml_explain(id)','Update',0,0,'42883',None),
 ('UPDATE dml_explain_rows SET v=1/0 WHERE false RETURNING v','Update',0,0,'22012',None),
 ('UPDATE dml_explain_rows SET v=CASE WHEN false THEN 1/0 ELSE 7 END WHERE id=1 RETURNING v','Update',0,1,None,None),
 ('UPDATE dml_explain_rows SET v=CAST(\'bad\' AS INT) WHERE false','Update',0,0,'22P02',None),
 ('DELETE FROM dml_explain_rows WHERE id=1','Delete',0,0,None,None),
 ('DELETE FROM dml_explain_rows WHERE id=1 RETURNING dml_explain_writer(id)','Delete',1,1,None,None),
 ('DELETE FROM dml_explain_rows WHERE false RETURNING dml_explain_writer(id)','Delete',0,0,None,None),
 ('DELETE FROM dml_explain_rows AS t USING dml_explain_source AS s WHERE t.id=s.id RETURNING t.id,s.v','Delete',0,2,None,None),
 ('DELETE FROM dml_explain_rows WHERE id=ANY(SELECT id FROM dml_explain_source) RETURNING id','Delete',0,2,None,None),
 ('DELETE FROM dml_explain_rows WHERE dml_explain_writer(id)>0 RETURNING id','Delete',2,2,None,None),
 ('DELETE FROM dml_explain_rows WHERE 1/0=1','Delete',0,0,'22012',None),
 ('DELETE FROM dml_explain_rows WHERE missing_dml_explain(id)=1','Delete',0,0,'42883',None),
 ('INSERT INTO dml_explain_rows(id,v,tag) VALUES(3,7,\'8\')','Insert',0,0,None,None),
 ('INSERT INTO dml_explain_rows(id,v,tag) VALUES(3,dml_explain_writer(7),\'8\') RETURNING id,v','Insert',1,1,None,None),
 ('INSERT INTO dml_explain_rows(id,tag) VALUES(3,\'8\') RETURNING v','Insert',1,1,None,None),
 ('INSERT INTO dml_explain_rows(id,v,tag) VALUES(3,DEFAULT,\'8\') RETURNING v','Insert',1,1,None,None),
 ('INSERT INTO dml_explain_rows(id,v,tag) SELECT id+10,dml_explain_writer(id),\'8\' FROM dml_explain_source RETURNING id,v','Insert',2,2,None,None),
 ('INSERT INTO dml_explain_rows(id,v,tag) SELECT id+10,dml_explain_writer(id),\'8\' FROM dml_explain_source WHERE false RETURNING v','Insert',0,0,None,None),
 ('INSERT INTO dml_explain_rows(id,v,tag) VALUES(3,dml_explain_writer(7),\'8\'),(1,dml_explain_writer(7),\'8\') RETURNING id','Insert',2,0,None,'23505'),
 ('INSERT INTO dml_explain_rows(id,v,tag) VALUES(3,1/0,\'8\')','Insert',0,0,'22012',None),
 ('INSERT INTO dml_explain_rows(id,v,tag) SELECT 3,1/0,\'8\' WHERE false','Insert',0,0,'22012',None),
]
try:
    for sql in ['BEGIN','CREATE TEMP SEQUENCE dml_explain_calls',
        "CREATE FUNCTION dml_explain_writer(p INT) RETURNS INT LANGUAGE plpgsql AS $$BEGIN PERFORM nextval('dml_explain_calls');RETURN p;END$$",
        "CREATE FUNCTION dml_explain_fail() RETURNS INT LANGUAGE plpgsql AS $$BEGIN PERFORM nextval('dml_explain_calls');RAISE EXCEPTION 'dml plan failure';END$$",
        'CREATE TEMP TABLE dml_explain_rows(id INT PRIMARY KEY,v INT DEFAULT dml_explain_writer(7),tag TEXT)',
        "INSERT INTO dml_explain_rows VALUES(1,10,'7'),(2,20,'bad')",
        'CREATE TEMP TABLE dml_explain_source(id INT,v INT)',
        'INSERT INTO dml_explain_source VALUES(1,30),(2,NULL)',
        "SELECT nextval('dml_explain_calls')"]:
        assert q(sql)[1] is None
    for analyze,json_ in [(False,False),(True,False),(False,True),(True,True)]:
        for sql,op,calls,returned,static_error,dynamic_error in cases:
            q('SAVEPOINT dml_explain_case')
            expected=static_error or (dynamic_error if analyze else None)
            plan(sql,op,analyze,json_,expected,returned)
            q('ROLLBACK TO SAVEPOINT dml_explain_case');q('RELEASE SAVEPOINT dml_explain_case')
            if analyze and not static_error:counter+=calls
            q("SELECT currval('dml_explain_calls')",rows=[[str(counter)]])
            q('SELECT id,v,tag FROM dml_explain_rows ORDER BY id',rows=[['1','10','7'],['2','20','bad']])
    q('ROLLBACK')
    print('DML EXPLAIN FAILURES',failures,flush=True)
    assert not failures,failures
    print('[DML EXPLAIN EXECUTION '+('STRICT18' if reference else 'PROTOCOL')+'] passed',flush=True)
finally:
    try:c.simple_query(sock,'ROLLBACK')
    finally:
        if server:r.stop_ours(server)
        else:sock.close()
