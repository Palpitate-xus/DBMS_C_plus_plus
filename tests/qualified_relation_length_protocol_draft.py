#!/usr/bin/env python3
"""Logical schema and table identifiers have independent byte budgets."""
from compat.numeric_type_review import Review
import sys
import uuid


def ident(value):
    return '"'+value.replace('"','""')+'"'


def main():
    if sys.argv[1:]==['--reference']:
        sys.argv[1:]=['--reference18']
    r=Review('QUALIFIED_RELATION_LENGTH_PROTOCOL')
    token=uuid.uuid4().hex[:8]
    # Full logical components are kept below NAMEDATALEN, including UTF-8.
    schemas=['s218_'+token+'a'*43, 'S218_'+token+'b'*50,
             '界'*17+token, 's218_'+token+'q"'+('c'*40)]
    tables=['t'+('a'*62), 'T'+('b'*62), '界'*18+token,
            't'+('c'*48)+'q"'+token]
    owned=[]

    def command(sql,state=None,ready=b'I'):
        messages=r.client.simple_query(r.sock,sql)
        result=r.runner.decode_wire_result(messages,include_types=True)
        valid=(result[1]==state and result[0]==[] and result[3]==[] and result[5]==[] and
               messages[-1]==(b'Z',ready) and
               sum(k==b'C' for k,_ in messages)==(0 if state else 1))
        r.record(sql,valid,result,('command',state))
        return valid

    def read(table,value):
        sql='SELECT n AS v FROM '+table+' ORDER BY n'
        r.query(sql,rows=[[str(value)]],oids=[23])
        r.describe(sql,oids=[23])
        r.parameter('SELECT n AS v FROM '+table+' WHERE n=$1',[str(value)],[23],[[str(value)]],[23])

    try:
        for schema in schemas:
            command('CREATE SCHEMA '+ident(schema))
            owned.append(schema)
        for i,schema in enumerate(schemas):
            target=ident(schema)+'.'+ident(tables[i])
            renamed=tables[i][:-1]+'z'
            moved=ident(schemas[(i+1)%len(schemas)])+'.'+ident(renamed)
            current=ident(schema)+'.'+ident(renamed)
            decoy='public.'+ident('r218_'+token+'_'+str(i))
            command('CREATE TABLE '+decoy+'(n integer)')
            command('INSERT INTO '+decoy+' VALUES(999)')
            command('CREATE TABLE '+target+'(n integer)')
            command('INSERT INTO '+target+' VALUES('+str(100+i)+')')
            read(target,100+i)
            command('CREATE TABLE '+target+'(n integer)','42P07')
            command('SET search_path='+ident(schema)+',public')
            read(ident(tables[i]),100+i)
            command('RESET search_path')
            command('BEGIN',ready=b'T')
            command('UPDATE '+target+' SET n=777',ready=b'T')
            command('ROLLBACK')
            read(target,100+i)
            command('ALTER TABLE '+target+' RENAME TO '+ident(renamed))
            r.query('SELECT n AS v FROM '+target,state='42P01')
            read(current,100+i)
            command('ALTER TABLE '+current+' SET SCHEMA '+ident(schemas[(i+1)%len(schemas)]))
            r.query('SELECT n AS v FROM '+current,state='42P01')
            read(moved,100+i)
            command('TRUNCATE '+moved)
            r.query('SELECT n AS v FROM '+moved,rows=[],oids=[23])
            command('INSERT INTO '+moved+' VALUES('+str(200+i)+')')
            read(moved,200+i)
            read(decoy,999)
            command('DROP TABLE '+moved)
            r.query('SELECT n AS v FROM '+moved,state='42P01')
            command('DROP TABLE '+decoy)
        # FK metadata must preserve the full physical parent identity.
        parent=ident(schemas[0])+'.'+ident(tables[0])
        child=ident(schemas[1])+'.'+ident(tables[1])
        command('CREATE TABLE '+parent+'(n integer PRIMARY KEY)')
        command('CREATE TABLE '+child+'(n integer,CONSTRAINT r218_fk FOREIGN KEY(n) REFERENCES '+parent+'(n))')
        command('INSERT INTO '+parent+' VALUES(41)')
        command('INSERT INTO '+child+' VALUES(41)')
        command('INSERT INTO '+child+' VALUES(42)','23503')
        read(parent,41)
        read(child,41)
        command('DROP TABLE '+child)
        command('DROP TABLE '+parent)
    finally:
        for schema in reversed(owned):
            command('DROP SCHEMA IF EXISTS '+ident(schema)+' CASCADE')
        r.close()
    r.complete(185)


if __name__=='__main__':
    main()
