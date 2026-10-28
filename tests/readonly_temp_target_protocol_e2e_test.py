#!/usr/bin/env python3
"""Read-only TEMP DML resolves the real target, including explicit search_path."""
from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import strict_data_rows
import struct


LABEL = 'READONLY_TEMP_TARGET'
PLANNED = 284


def plan():
    commands = []
    def add(sql, extended=False, tag=None, rows=None, state=None, ready=b'I'):
        commands.append(dict(sql=sql, extended=extended, tag=tag, rows=rows, state=state, ready=ready))
    for sql, tag in (
        ('CREATE TABLE r314_shadow(id integer,n integer)','CREATE TABLE'),
        ('CREATE TABLE r314_source(id integer,n integer)','CREATE TABLE'),
        ('CREATE TEMP TABLE r314_temp(id integer,n integer)','CREATE TABLE'),
        ('CREATE TEMP TABLE r314_shadow(id integer,n integer)','CREATE TABLE'),
        ('CREATE TEMP TABLE "R314.Dot"(id integer,n integer)','CREATE TABLE'),
        ('INSERT INTO public.r314_shadow VALUES(1,100)','INSERT 0 1'),
        ('INSERT INTO r314_source VALUES(1,9)','INSERT 0 1'),
        ('INSERT INTO r314_temp VALUES(1,1)','INSERT 0 1'),
        ('INSERT INTO pg_temp.r314_shadow VALUES(1,1)','INSERT 0 1'),
        ('INSERT INTO "pg_temp"."R314.Dot" VALUES(1,1)','INSERT 0 1')):
        add(sql,tag=tag)
    targets=('r314_temp','pg_temp.r314_temp','"pg_temp"."r314_temp"',
             '"pg_temp"."R314.Dot"','r314_shadow','public.r314_shadow')
    for extended in (False,True):
        for target in targets:
            permanent=target=='public.r314_shadow'
            baseline=[['1','100' if permanent else '1']]
            changes=(('INSERT INTO '+target+' VALUES(2,2)','INSERT 0 1',[['1','1'],['2','2']]),
                     ('UPDATE '+target+' SET n=n+1 WHERE id=1','UPDATE 1',[['1','2']]),
                     ('DELETE FROM '+target+' WHERE id=1','DELETE 1',[]),
                     ('MERGE INTO '+target+' AS t USING r314_source AS s ON t.id=s.id WHEN MATCHED THEN UPDATE SET n=s.n','MERGE 1',[['1','9']]))
            for mutation, tag, values in changes:
                add('BEGIN READ ONLY',extended,'BEGIN',ready=b'T')
                add(mutation,extended,None if permanent else tag,state='25006' if permanent else None,ready=b'E' if permanent else b'T')
                add('SELECT id,n FROM '+target+' ORDER BY id',extended,rows=None if permanent else values,
                    state='25P02' if permanent else None,ready=b'E' if permanent else b'T')
                add('ROLLBACK',extended,'ROLLBACK')
                add('SELECT id,n FROM '+target+' ORDER BY id',extended,rows=baseline)
        add('SET search_path=public,pg_temp',extended,'SET')
        for mutation in ('INSERT INTO r314_shadow VALUES(2,2)',
                         'UPDATE r314_shadow SET n=9 WHERE id=1',
                         'DELETE FROM r314_shadow WHERE id=1'):
            add('BEGIN READ ONLY',extended,'BEGIN',ready=b'T')
            add(mutation,extended,state='25006',ready=b'E')
            add('ROLLBACK',extended,'ROLLBACK')
            add('SELECT id,n FROM public.r314_shadow ORDER BY id',extended,rows=[['1','100']])
            add('SELECT id,n FROM pg_temp.r314_shadow ORDER BY id',extended,rows=[['1','1']])
        add('RESET search_path',extended,'RESET')
    assert len(commands)==PLANNED,len(commands)
    return commands


def main():
    review=Review(LABEL)
    c,sock=review.client,review.sock
    try:
        for operation in plan():
            sql,extended=operation['sql'],operation['extended']
            if extended:
                sock.sendall(c.typed(b'P',b'\0'+sql.encode('utf-8')+b'\0\0\0')+
                    c.typed(b'B',b'\0\0'+struct.pack('!HHH',0,0,0))+
                    c.typed(b'E',b'\0'+struct.pack('!I',0))+c.typed(b'S'))
                messages=c.read_until_ready(sock)
            else:messages=c.simple_query(sock,sql)
            decoded=review.runner.decode_wire_result(messages,include_types=True)
            rows,valid_rows=strict_data_rows(messages)
            state,values=operation['state'],operation['rows']
            expected_tag=operation['tag'] if values is None else 'SELECT '+str(len(values))
            good=decoded[1]==state and valid_rows and messages[-1:]==[(b'Z',operation['ready'])]
            good=good and all(not body for kind,body in messages if kind in (b'1',b'2'))
            kinds=[k for k,b in messages]
            if state:
                prefix=[b'1',b'2'] if extended and state=='25006' else []
                good=good and kinds==prefix+[b'E',b'Z'] and not rows
                diagnostics=[c.diagnostic_fields(b) for k,b in messages if k==b'E']
                good=good and len(diagnostics)==1 and diagnostics[0].get(b'C')==state.encode() and diagnostics[0].get(b'S')==b'ERROR'
            else:
                prefix=[b'1',b'2'] if extended else []
                if values is not None:
                    prefix+=[] if extended else [b'T']
                    wanted=[[v.encode('utf-8') for v in row] for row in values]
                    good=good and rows==wanted
                    if not extended:
                        fields=c.row_description_fields(messages) if any(k==b'T' for k,b in messages) else []
                        good=good and [(f[0],f[3],f[4],f[5],f[6]) for f in fields]==[(b'id',23,4,-1,0),(b'n',23,4,-1,0)]
                else:good=good and not rows
                statuses=[b for k,b in messages if k==b'S']
                status_allowed=sql in ('SET search_path=public,pg_temp','RESET search_path')
                if statuses:
                    good=good and status_allowed and len(statuses)==1
                    # RESET's role-dependent default is the separate, user-
                    # deferred R210 scope. Check framing/name, not that value.
                    good=good and statuses[0].startswith(b'search_path\0') and statuses[0].endswith(b'\0') and statuses[0].count(b'\0')==2
                    if sql.startswith('SET '):good=good and statuses==[b'search_path\0public, pg_temp\0']
                good=good and kinds==prefix+[b'D']*len(rows)+[b'C']+([b'S'] if statuses else [])+[b'Z']
                good=good and [b for k,b in messages if k==b'C']==[expected_tag.encode()+b'\0']
            review.record((extended,sql),good,decoded,(values,state,expected_tag,operation['ready']))
        review.complete(PLANNED)
    finally:
        review.close()


if __name__=='__main__':
    main()
