#!/usr/bin/env python3
"""Fixed successful enum metadata/valid-format observations; no phase-policy probes."""
import json,os,struct,sys,tempfile
from pathlib import Path
from compat.numeric_type_review import Review
LABEL='ENUM_BOUND_DESCRIPTOR_RAW'
EXPECTED=33

def main():
 review=Review(LABEL);oid=None;observations=[]
 def record(label,good,actual,expected):
  observations.append({'control':review.controls+1,'label':label,'actual':actual,'expected':expected,'match':good})
  review.record(label,good,actual,expected)
 def query(sql,wanted=None,tag=None):
  messages=review.client.simple_query(review.sock,sql)
  actual=review.runner.decode_wire_result(messages,include_types=True)
  good=actual[1] is None and messages[-1]==(b'Z',b'I')
  if wanted is not None:good=good and actual[0]==wanted
  if tag is not None:good=good and actual[4]==tag
  record(sql,good,actual,(wanted,tag))
 def metadata(sql,wanted,extended=False,binary=False,describe=True):
  if not extended:messages=review.client.simple_query(review.sock,sql)
  else:
   name=b'enum_descriptor_'+str(review.controls).encode();typed=review.client.typed
   parse=name+b'\0'+sql.encode()+b'\0'+struct.pack('!H',0)
   bind=b'\0'+name+b'\0'+struct.pack('!HHH',0,0,1 if binary else 0)+(struct.pack('!H',1) if binary else b'')
   wire=typed(b'P',parse)+(typed(b'D',b'S'+name+b'\0') if describe else b'')+typed(b'B',bind)
   wire+=(typed(b'D',b'P\0') if describe else b'')+typed(b'E',b'\0'+struct.pack('!I',0))+typed(b'S')
   review.sock.sendall(wire);messages=review.client.read_until_ready(review.sock)
  descriptions=[review.client.row_description_fields([message]) for message in messages if message[0]==b'T']
  rows=review.client.data_row_values(messages);commands=[body.rstrip(b'\0') for kind,body in messages if kind==b'C']
  errors=[review.client.diagnostic_fields(body) for kind,body in messages if kind==b'E']
  wanted_rows=[[None if x is None else (struct.pack('!i',int(x)) if binary and i==1 else x.encode()) for i,x in enumerate(row)] for row in wanted]
  good=rows==wanted_rows and not errors and commands==[('SELECT '+str(len(wanted))).encode()] and messages[-1]==(b'Z',b'I')
  good=good and len(descriptions)==(2 if extended and describe else 0 if extended else 1)
  for i,fields in enumerate(descriptions):
   expected_format=1 if extended and binary and i==1 else 0
   good=good and [(f[0],f[1],f[2],f[3],f[4],f[5],f[6]) for f in fields]==[(b'e',0,0,oid,4,-1,expected_format),(b'id',0,0,23,4,-1,expected_format)]
  record(('binary' if binary else 'text','extended' if extended else 'simple','describe' if describe else 'execute-only',sql),good,
         {'rows':rows,'descriptions':descriptions,'commands':commands,'errors':errors,'messages':messages},
         {'rows':wanted_rows,'enum_oid':oid,'enum_length':4,'enum_typmod':-1,'descriptions':2 if extended and describe else 0 if extended else 1})
 def physical(sql,wanted,table_oid,extended=False,binary=False):
  if not extended:messages=review.client.simple_query(review.sock,sql)
  else:
   name=b'enum_physical_'+str(review.controls).encode();typed=review.client.typed
   parse=name+b'\0'+sql.encode()+b'\0'+struct.pack('!H',0)
   bind=b'\0'+name+b'\0'+struct.pack('!HHH',0,0,1 if binary else 0)+(struct.pack('!H',1) if binary else b'')
   review.sock.sendall(typed(b'P',parse)+typed(b'D',b'S'+name+b'\0')+typed(b'B',bind)+typed(b'D',b'P\0')+typed(b'E',b'\0'+struct.pack('!I',0))+typed(b'S'))
   messages=review.client.read_until_ready(review.sock)
  descriptions=[review.client.row_description_fields([message]) for message in messages if message[0]==b'T']
  rows=review.client.data_row_values(messages);commands=[body.rstrip(b'\0') for kind,body in messages if kind==b'C']
  errors=[review.client.diagnostic_fields(body) for kind,body in messages if kind==b'E']
  expected_rows=[[None if x is None else (struct.pack('!i',int(x)) if binary and i==1 else x.encode()) for i,x in enumerate(row)] for row in wanted]
  good=rows==expected_rows and not errors and commands==[('SELECT '+str(len(wanted))).encode()] and messages[-1]==(b'Z',b'I') and len(descriptions)==(2 if extended else 1)
  for i,fields in enumerate(descriptions):
   fmt=1 if extended and binary and i==1 else 0
   expected_fields=[(b'e',table_oid,1,oid,4,-1,fmt)]
   if len(wanted[0])==2:expected_fields.append((b'id',table_oid,2,23,4,-1,fmt))
   good=good and fields==expected_fields
  record(('physical','binary' if binary else 'text','extended' if extended else 'simple',sql),good,
         {'rows':rows,'descriptions':descriptions,'commands':commands,'errors':errors,'messages':messages},
         {'rows':expected_rows,'actual_catalog_table_oid':table_oid,'enum_oid':oid,'enum_length':4,'enum_typmod':-1})
 try:
  query("CREATE TYPE enum_descriptor_mood AS ENUM ('zeta','','NULL','alpha','β')",tag='CREATE TYPE')
  query('CREATE TABLE enum_descriptor_left(e enum_descriptor_mood,id INTEGER)',tag='CREATE TABLE')
  query('CREATE TABLE enum_descriptor_right(e enum_descriptor_mood,id INTEGER)',tag='CREATE TABLE')
  query("INSERT INTO enum_descriptor_left VALUES('alpha',0),('',1),('zeta',2)",tag='INSERT 0 3')
  query("INSERT INTO enum_descriptor_right VALUES('NULL',3),(NULL,4),('β',5)",tag='INSERT 0 3')
  sql="SELECT oid FROM pg_type WHERE typname='enum_descriptor_mood'"
  actual=review.runner.decode_wire_result(review.client.simple_query(review.sock,sql),include_types=True)
  try:oid=int(actual[0][0][0])
  except (IndexError,TypeError,ValueError):oid=None
  record(sql,oid is not None and oid>0 and actual[1] is None and actual[5]==[26],actual,'one actual catalog enum OID')
  source='SELECT e,id FROM enum_descriptor_left UNION ALL SELECT e,id FROM enum_descriptor_right'
  wanted=[['zeta','2'],['','1'],['NULL','3'],['alpha','0'],['β','5'],[None,'4']]
  for memory in ('64kB','16MB'):
   query("SET work_mem='"+memory+"'",tag='SET')
   ordered=source+' ORDER BY e NULLS LAST,id'
   metadata(ordered,wanted)
   metadata(ordered+' LIMIT 0',[])
   metadata(ordered,wanted,extended=True)
   metadata(ordered,wanted,extended=True,binary=True)
   metadata(ordered,wanted,extended=True,binary=True,describe=False)
   metadata('SELECT e,id FROM enum_descriptor_left WHERE false UNION ALL SELECT e,id FROM enum_descriptor_right WHERE false ORDER BY e,id',[],extended=True,binary=True)
   # Explicit enum NULL preserves its descriptor without invoking a non-NULL codec.
   metadata('SELECT NULL::enum_descriptor_mood AS e,1 AS id UNION ALL SELECT NULL::enum_descriptor_mood,2 ORDER BY id',
            [[None,'1'],[None,'2']],extended=True,binary=True)
   query('SELECT 1',[['1']],tag='SELECT 1')
  # Append the six independently observed ordinary physical SQL shapes.
  # The original 24 controls above retain their SQL and exact expectations.
  query('CREATE TABLE enum_descriptor_physical(e enum_descriptor_mood,id INTEGER)',tag='CREATE TABLE')
  query("INSERT INTO enum_descriptor_physical VALUES('zeta',0),('',1),('NULL',2),('alpha',3),('β',4),(NULL,5)",tag='INSERT 0 6')
  sql="SELECT oid FROM pg_class WHERE relname='enum_descriptor_physical'"
  actual=review.runner.decode_wire_result(review.client.simple_query(review.sock,sql),include_types=True)
  try:table_oid=int(actual[0][0][0])
  except (IndexError,TypeError,ValueError):table_oid=None
  record(sql,table_oid is not None and table_oid>0 and actual[1] is None and actual[5]==[26],actual,'one actual catalog relation OID')
  wanted=[['zeta','0'],['','1'],['NULL','2'],['alpha','3'],['β','4'],[None,'5']]
  for sql,rows in [('SELECT e,id FROM enum_descriptor_physical ORDER BY id',wanted),
                   ('SELECT e FROM enum_descriptor_physical ORDER BY id',[[row[0]] for row in wanted])]:
   physical(sql,rows,table_oid)
   physical(sql,rows,table_oid,extended=True)
   physical(sql,rows,table_oid,extended=True,binary=True)
  review.complete(EXPECTED)
 finally:
  def encode(x):
   if isinstance(x,bytes):return {'hex':x.hex()}
   raise TypeError(type(x).__name__)
  output=os.environ.get('ENUM_RESULT_DESCRIPTOR_OUTPUT_DIR')
  output=Path(output) if output else Path(tempfile.mkdtemp(prefix='dbms-enum-descriptor-'))
  output.mkdir(parents=True,exist_ok=True)
  dest=output/('enum-raw-reference-observations.json' if '--reference18' in sys.argv else 'enum-raw-observations.json')
  def normalize(x):
   if isinstance(x,bytes):return encode(x)
   if isinstance(x,dict):return {(key.hex() if isinstance(key,bytes) else key):normalize(value) for key,value in x.items()}
   if isinstance(x,(list,tuple)):return [normalize(value) for value in x]
   return x
  dest.write_text(json.dumps(normalize(observations),indent=2)+'\n')
  print('ENUM_BOUND_DESCRIPTOR_OBSERVATIONS',str(dest),flush=True)
  review.close()
if __name__=='__main__':main()
