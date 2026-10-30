#!/usr/bin/env python3
"""Stored user relations in catalog namespaces retain their real DB identity."""
from compat.numeric_type_review import Review
import struct
import sys
import uuid


def ident(value):
    return '"'+value.replace('"','""')+'"'


def main():
    if sys.argv[1:]==['--reference']:
        sys.argv[1:]=['--reference18']
    r=Review('CATALOG_PHYSICAL_RELATION_PROTOCOL')
    token=uuid.uuid4().hex[:8]
    schema='u226_'+token
    tables=['p226_'+token,'Mix226_'+token,'dot.'+token]
    owned=[]

    def target(namespace,name,quoted=True):
        return (ident(namespace) if quoted else namespace)+'.'+ident(name)

    def command(sql,state=None):
        messages=r.client.simple_query(r.sock,sql)
        result=r.runner.decode_wire_result(messages,include_types=True)
        valid=(result[1]==state and result[0]==[] and result[3]==[] and result[5]==[] and
               messages[-1]==(b'Z',b'I') and sum(k==b'C' for k,_ in messages)==(0 if state else 1))
        r.record(sql,valid,result,('command',state))

    def read(source,value):
        sql='SELECT n AS v FROM '+source
        r.query(sql,rows=[[str(value)]],oids=[23])
        r.describe(sql,oids=[23])
        r.parameter(sql+' WHERE n=$1',[str(value)],[23],[[str(value)]],[23])

    def binary(source,value):
        sql='SELECT n AS v FROM '+source+' WHERE n=$1'
        parse=b'\0'+sql.encode()+b'\0'+struct.pack('!HI',1,23)
        bind=b'\0\0'+struct.pack('!HHH',1,1,1)+struct.pack('!ii',4,value)+struct.pack('!HH',1,1)
        r.sock.sendall(r.client.typed(b'P',parse)+r.client.typed(b'B',bind)+
                       r.client.typed(b'D',b'P\0')+r.client.typed(b'E',b'\0'+struct.pack('!I',0))+r.client.typed(b'S'))
        messages=r.client.read_until_ready(r.sock)
        fields=r.client.row_description_fields(messages)
        rows=[body for kind,body in messages if kind==b'D']
        expected=struct.pack('!Hii',1,4,value)
        valid=([kind for kind,_ in messages]==[b'1',b'2',b'T',b'D',b'C',b'Z'] and
               len(fields)==1 and fields[0][3]==23 and fields[0][-1]==1 and
               rows==[expected] and messages[-1]==(b'Z',b'I'))
        r.record(('BINARY',sql,value),valid,(messages,fields),expected)

    try:
        command('CREATE SCHEMA '+ident(schema))
        for i,name in enumerate(tables):
            for namespace,value in [('public',900+i),(schema,700+i)]:
                source=target(namespace,name)
                command('CREATE TABLE '+source+'(n integer)');owned.append(source)
                command('INSERT INTO '+source+' VALUES('+str(value)+')')
        for j,namespace in enumerate(['pg_catalog','information_schema']):
            for i,name in enumerate(tables):
                value=100*(j+1)+i
                source=target(namespace,name);ordinary=target(namespace,name,False)
                command('CREATE TABLE '+source+'(n integer)');owned.append(source)
                command('INSERT INTO '+source+' VALUES('+str(value)+')')
                read(source,value);read(ordinary,value)
                binary(source,value)
                r.query('SELECT q.n AS v FROM '+ordinary+' AS q',rows=[[str(value)]],oids=[23])
                r.query('SELECT '+ident(namespace)+'.'+ident(name)+'.n AS v FROM '+source,
                        rows=[[str(value)]],oids=[23])
                r.query('SELECT * FROM '+source,rows=[[str(value)]],oids=[23])
                r.query('SELECT n AS v FROM '+source+' WHERE FALSE',rows=[],oids=[23])
                r.describe('SELECT n AS v FROM '+source+' LIMIT 0',oids=[23])
                r.query('SELECT count(*) AS v FROM '+ordinary,rows=[['1']],oids=[20])
                command('UPDATE '+ordinary+' SET n=n+10')
                read(source,value+10)
                command('DELETE FROM '+source+' WHERE n='+str(value+10))
                r.query('SELECT n AS v FROM '+source,rows=[],oids=[23])
                command('INSERT INTO '+ordinary+' VALUES('+str(value)+')')
                # Explicit path ordering and implicit catalog visibility do
                # not change the configured GUC or effective role defaults.
                if namespace=='pg_catalog':
                    for path,expected in [('pg_catalog,public',value),('public,pg_catalog',900+i),
                                          (ident(schema)+',pg_catalog',700+i),('pg_catalog,'+ident(schema),value)]:
                        command('SET search_path='+path)
                        read(ident(name),expected)
                    command('RESET search_path')
                    read(ident(name),value)
                else:
                    command('SET search_path=information_schema,public')
                    # A genuine implicit pg_catalog relation wins first.
                    read(ident(name),100+i)
                    command('SET search_path=information_schema,pg_catalog,public')
                    read(ident(name),value)
                    command('RESET search_path')
                renamed='renamed_'+token+'_'+str(j)+'_'+str(i)
                command('ALTER TABLE '+source+' RENAME TO '+ident(renamed))
                r.query('SELECT n AS v FROM '+source,state='42P01')
                current=target(namespace,renamed)
                read(current,value)
                command('ALTER TABLE '+current+' RENAME TO '+ident(name))
                read(source,value)
                read(target('public',name),900+i)
                read(target(schema,name),700+i)
        for namespace in ['pg_catalog','information_schema']:
            missing=target(namespace,'missing226_'+token)
            r.query('SELECT n AS v FROM '+missing,state='42P01')
            r.describe('SELECT n AS v FROM '+missing,state='42P01')
    finally:
        command('RESET search_path')
        for source in reversed(owned):
            command('DROP TABLE IF EXISTS '+source)
        command('DROP SCHEMA IF EXISTS '+ident(schema)+' CASCADE')
        r.close()
    r.complete(350)


if __name__=='__main__':
    main()
