#!/usr/bin/env python3
"""A schema move preserves relation identity and its owned storage family."""
from compat.numeric_type_review import Review
import sys
import uuid


def ident(value):
    return '"'+value.replace('"','""')+'"'


def literal(value):
    return "'"+value.replace("'","''")+"'"


def main():
    if sys.argv[1:]==['--reference']:
        sys.argv[1:]=['--reference18']
    r=Review('TABLE_SCHEMA_MOVE_PROTOCOL')
    token=uuid.uuid4().hex[:8]
    a,b,c='m224_'+token,'M224_'+token,'dot.'+token
    schemas=[a,b,c]
    owned=[]

    def command(sql,state=None,ready=b'I'):
        messages=r.client.simple_query(r.sock,sql)
        result=r.runner.decode_wire_result(messages,include_types=True)
        valid=(result[1]==state and result[0]==[] and result[3]==[] and result[5]==[] and
               messages[-1]==(b'Z',ready) and
               sum(k==b'C' for k,_ in messages)==(0 if state else 1))
        r.record(sql,valid,result,('command',state,ready))

    def relation(schema,name):
        return ident(schema)+'.'+ident(name)

    def read(target,n):
        sql='SELECT n AS v FROM '+target
        r.query(sql,rows=[[str(n)]],oids=[23])
        r.describe(sql+' WHERE FALSE',oids=[23])
        r.parameter(sql+' WHERE n=$1',[str(n)],[23],[[str(n)]],[23])

    def oid(schema,name,expected=None):
        sql=('SELECT c.oid::bigint AS v FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace '
             'WHERE n.nspname='+literal(schema)+' AND c.relname='+literal(name))
        result=r.runner.decode_wire_result(r.client.simple_query(r.sock,sql),include_types=True)
        valid=(result[1] is None and len(result[0])==1 and len(result[0][0])==1 and
               result[0][0][0].isdigit() and int(result[0][0][0])>0 and result[5]==[20])
        value=result[0][0][0] if valid else 'invalid-original-identity'
        if expected is not None:
            valid=valid and value==expected
        r.record(('OID',schema,name),valid,result,expected or 'one live relation OID')
        return value

    try:
        for schema in schemas:
            command('CREATE SCHEMA '+ident(schema));owned.append(schema)
        for i,name in enumerate(['items','MiX','dot.t']):
            source,target=relation(a,name),relation(b,name)
            command('CREATE TABLE '+source+'(n integer)')
            command('INSERT INTO '+source+' VALUES('+str(10+i)+')')
            original=oid(a,name)
            command('ALTER TABLE '+source+' SET SCHEMA '+ident(b))
            r.query('SELECT n AS v FROM '+source,state='42P01')
            read(target,10+i)
            oid(b,name,original)
            command('SET search_path='+ident(b)+',public')
            read(ident(name),10+i)
            command('RESET search_path')
            # Same-namespace is a no-op rather than a duplicate error.
            command('ALTER TABLE '+target+' SET SCHEMA '+ident(b))
            oid(b,name,original)
            command('BEGIN',ready=b'T')
            command('ALTER TABLE '+target+' SET SCHEMA '+ident(c),ready=b'T')
            read(relation(c,name),10+i)
            command('ROLLBACK')
            read(target,10+i)
            r.query('SELECT n AS v FROM '+relation(c,name),state='42P01')
            command('BEGIN',ready=b'T')
            command('SAVEPOINT before_move',ready=b'T')
            command('ALTER TABLE '+target+' SET SCHEMA '+ident(c),ready=b'T')
            command('ROLLBACK TO SAVEPOINT before_move',ready=b'T')
            command('COMMIT')
            read(target,10+i)
            command('ALTER TABLE '+target+' SET SCHEMA public')
            read(relation('public',name),10+i)
            oid('public',name,original)
            command('ALTER TABLE '+relation('public',name)+' SET SCHEMA '+a.upper())
            read(source,10+i)
            oid(a,name,original)
            command('DROP TABLE '+source)

        source=relation(a,'conflict');target=relation(b,'conflict')
        command('CREATE TABLE '+source+'(n integer)')
        command('CREATE TABLE '+target+'(n integer)')
        command('INSERT INTO '+source+' VALUES(1)')
        command('INSERT INTO '+target+' VALUES(2)')
        command('ALTER TABLE '+source+' SET SCHEMA '+ident(b),'42P07')
        read(source,1);read(target,2)
        command('ALTER TABLE '+source+' SET SCHEMA missing224_'+token,'3F000')
        read(source,1)
        # The fixture owns an administrative connection; PG permits moving
        # this user-created relation into pg_catalog and back.
        command('ALTER TABLE '+source+' SET SCHEMA pg_catalog')
        read(relation('pg_catalog','conflict'),1)
        command('ALTER TABLE '+relation('pg_catalog','conflict')+' SET SCHEMA '+ident(a))
        read(source,1)
        for suffix in ['', 'x.y','SELECT',ident(b)+' trailing',ident(b)+', ADD COLUMN z integer']:
            command('ALTER TABLE '+source+' SET SCHEMA '+suffix,'42601')

        # Indexes and owned sequences move, ordinary referenced sequences do not.
        family=relation(a,'family');moved=relation(b,'family')
        child=relation(c,'child');external=relation(c,'external')
        command('CREATE SEQUENCE '+external)
        command('CREATE TABLE '+family+'(id serial PRIMARY KEY,n integer UNIQUE,'
                'ext bigint DEFAULT nextval('+literal(external)+'))')
        command('CREATE INDEX idx224 ON '+family+'(n)')
        command('CREATE TABLE '+child+'(n integer REFERENCES '+family+'(id))')
        command('INSERT INTO '+family+'(n) VALUES(10)')
        command('INSERT INTO '+child+' VALUES(1)')
        familyOid=oid(a,'family');indexOid=oid(a,'idx224');seqOid=oid(a,'family_id_seq')
        command('ALTER TABLE '+family+' SET SCHEMA '+ident(b))
        oid(b,'family',familyOid);oid(b,'idx224',indexOid);oid(b,'family_id_seq',seqOid)
        oid(c,'external')
        command('INSERT INTO '+moved+'(n) VALUES(20)')
        r.query('SELECT id AS v,n AS w,ext AS x FROM '+moved+' ORDER BY id',
                rows=[['1','10','1'],['2','20','2']],oids=[23,23,20])
        command('INSERT INTO '+child+' VALUES(2)')
        command('INSERT INTO '+child+' VALUES(9)','23503')
        command('INSERT INTO '+moved+'(n) VALUES(20)','23505')
        r.query('SELECT currval('+literal(relation(b,'family_id_seq'))+') AS v',rows=[['3']],oids=[20])
        command('DROP TABLE '+child)
        # Dropping the former schema must not delete the moved family.
        command('DROP TABLE '+source)
        command('DROP SCHEMA '+ident(a)+' CASCADE')
        owned.remove(a)
        command('INSERT INTO '+moved+'(n) VALUES(30)')
        r.query('SELECT id AS v FROM '+moved+' WHERE n=30',rows=[['4']],oids=[23])
        command('DROP TABLE '+moved)
        r.query('SELECT nextval('+literal(relation(b,'family_id_seq'))+') AS v',state='42P01')
        command('DROP SEQUENCE '+external)
    finally:
        for schema in reversed(owned):
            command('DROP SCHEMA IF EXISTS '+ident(schema)+' CASCADE')
        r.close()
    r.complete(196)


if __name__=='__main__':
    main()
