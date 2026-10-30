#!/usr/bin/env python3
"""Enum array catalog identities, generated names, NULL protocol and lifecycle."""
from compat.numeric_type_review import Review
import struct
import uuid


def ident(value):
    return '"'+value.replace('"','""')+'"'
def literal(value):
    return "'"+value.replace("'","''")+"'"
def display(value):
    return value if value and (value[0].islower() or value[0]=='_') and all(
        char.islower() or char.isdigit() or char=='_' for char in value) else ident(value)


def main():
    review=Review('ENUM_ARRAY_CATALOG_PROTOCOL')
    schema='enum_array_'+uuid.uuid4().hex[:12]
    other=schema+'_other'
    created=[]
    def decl(name,space=schema):
        return ident(space)+'.'+ident(name)
    def boolean(expression,wanted='t'):
        review.query('SELECT '+expression+' AS v',[[wanted]],[16])
    def oid(name):
        sql='SELECT '+literal(name)+'::regtype::oid AS v'
        result=review.runner.decode_wire_result(review.client.simple_query(review.sock,sql),include_types=True)
        good=result[1] is None and result[5]==[26] and result[4]=='SELECT 1' and len(result[0])==1
        value=int(result[0][0][0]) if good and result[0][0][0].isdigit() else 0
        review.record(sql,good and 0<value<2**32,result,'one actual OID, result OID26')
        return value
    def null_wire(array_type,array_oid,binary=True,parameter=False,empty=False):
        sql='SELECT '+('$1' if parameter else 'NULL::'+array_type)+' AS v'
        if empty:sql+=' WHERE false'
        inputs=[array_oid] if parameter else []
        parse=b'\0'+sql.encode()+b'\0'+struct.pack('!H',len(inputs))
        parse+=b''.join(struct.pack('!I',value) for value in inputs)
        bind=b'\0\0'+struct.pack('!HH',0,len(inputs))
        if parameter:bind+=struct.pack('!i',-1)
        bind+=struct.pack('!HH',1,int(binary))
        review.sock.sendall(review.client.typed(b'P',parse)+review.client.typed(b'D',b'S\0')+
            review.client.typed(b'B',bind)+review.client.typed(b'D',b'P\0')+
            review.client.typed(b'E',b'\0'+struct.pack('!I',0))+review.client.typed(b'S'))
        messages=review.client.read_until_ready(review.sock)
        errors=[review.client.diagnostic_fields(body).get(b'C',b'').decode()
                for kind,body in messages if kind==b'E']
        descriptions=[review.client.row_description_fields([message]) for message in messages if message[0]==b'T']
        rows=review.client.data_row_values(messages)
        tags=[body[:-1] for kind,body in messages if kind==b'C']
        good=not errors and rows==([] if empty else [[None]])
        good=good and tags==[b'SELECT 0' if empty else b'SELECT 1'] and len(descriptions)==2
        for index,fields in enumerate(descriptions):
            good=good and [(f[0],f[3],f[4],f[5],f[6]) for f in fields]==[
                (b'v',array_oid,-1,-1,int(binary and index==1))]
        good=good and messages[-1]==(b'Z',b'I')
        review.record((sql,binary,parameter,empty),good,(errors,rows,tags,descriptions,messages[-1]),
                      (array_oid,'actual array descriptor and NULL/zero rows'))
    try:
        review.query('CREATE SCHEMA '+ident(schema));created.append(schema)
        cases=[
            ('plain','_plain'),('_lead','__lead'),('Rank[]','_Rank[]'),
            ('Quote"Rank','_Quote"Rank'),('name with space','_name with space'),
            ('a'*63,'_'+'a'*62),('汉'*21,'_'+'汉'*20),
        ]
        for name,array_name in cases:
            base=decl(name);array_type=base+'[]'
            review.query('CREATE TYPE '+base+" AS ENUM('10','2','1')")
            array_oid=oid(array_type)
            review.query('SELECT pg_typeof(NULL::'+array_type+')::text AS v',
                         [[schema+'.'+display(name)+'[]']],[25])
            boolean(literal(array_type)+'::regtype::oid<>'+literal(base)+'::regtype::oid')
            boolean(literal(decl(array_name))+'::regtype::oid='+literal(array_type)+'::regtype::oid')
            review.describe('SELECT NULL::'+array_type+' AS v',[array_oid])
            review.query('SELECT NULL::'+array_type+' AS v',[[None]],[array_oid])
            null_wire(array_type,array_oid)
            null_wire(array_type,array_oid,binary=False,parameter=True)
            null_wire(array_type,array_oid,parameter=True)
            null_wire(array_type,array_oid,empty=True)
        for name in ('_collision','_collision_1','collision'):
            review.query('CREATE TYPE '+decl(name)+" AS ENUM('z')")
        boolean(literal(decl('collision')+'[]')+'::regtype::oid='+literal(decl('_collision_2'))+'::regtype::oid')
        review.query('SELECT pg_typeof(NULL::'+decl('collision')+'[])::text AS v',
                     [[schema+'.collision[]']],[25])
        boolean(literal(decl('_collision'))+'::regtype::oid<>'+literal(decl('collision')+'[]')+'::regtype::oid')
        review.query('CREATE TYPE '+decl('rank')+" AS ENUM('10','2','1')")
        rank_array=oid(decl('rank')+'[]')
        review.query('CREATE TYPE '+decl('_rank')+" AS ENUM('10','2','1')")
        boolean(literal(decl('rank')+'[]')+'::regtype::oid='+str(rank_array)+'::oid')
        boolean(literal(decl('rank')+'[]')+'::regtype::oid='+literal(decl('__rank'))+'::regtype::oid')
        boolean(literal(decl('_rank')+'[]')+'::regtype::oid='+literal(decl('__rank_1'))+'::regtype::oid')
        review.query('SELECT pg_typeof(NULL::'+decl('rank')+'[])::text AS v',[[schema+'.rank[]']],[25])
        review.query('CREATE SCHEMA '+ident(other));created.append(other)
        review.query('CREATE TYPE '+decl('rank',other)+" AS ENUM('10','2','1')")
        boolean(literal(decl('rank')+'[]')+'::regtype::oid<>'+literal(decl('rank',other)+'[]')+'::regtype::oid')
        review.query('SELECT pg_typeof(NULL::'+decl('rank',other)+'[])::text AS v',[[other+'.rank[]']],[25])
        review.query('DROP TYPE '+decl('_plain'),state='2BP01')
        review.query('DROP TYPE '+decl('_plain')+' CASCADE',state='2BP01')
        boolean(literal(decl('plain')+'[]')+'::regtype::oid='+literal(decl('_plain'))+'::regtype::oid')
        review.query('SELECT 42 AS v',[['42']],[23])
        review.query('DROP TYPE '+decl('rank'))
        review.query('SELECT '+literal(decl('rank'))+'::regtype',state='42704')
        review.query('SELECT '+literal(decl('__rank'))+'::regtype',state='42704')
        boolean(literal(decl('_rank')+'[]')+'::regtype::oid='+literal(decl('__rank_1'))+'::regtype::oid')
        review.complete(103)
    finally:
        for name in reversed(created):
            review.client.simple_query(review.sock,'DROP SCHEMA '+ident(name)+' CASCADE')
        review.close()


if __name__=='__main__':
    main()
