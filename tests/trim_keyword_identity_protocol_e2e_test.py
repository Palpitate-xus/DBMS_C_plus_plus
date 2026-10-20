#!/usr/bin/env python3
"""TRIM grammar must own direction, order and actual system-callee identity.

Overload completeness of btrim/ltrim/rtrim is a separate open issue: this
fixture does not turn existing permissive routine signatures into an oracle.
"""
from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import Wire as BaseWire, MODES, WIDTHS, identifier, strict_data_rows
import struct
import sys
import uuid

SAMPLES = (
    ('both','b','oth','oth','both'), ('leading','l','eading','eading','leading'),
    ('trailing','t','railing','railing','trailing'), ('xxbothxx','x','both','bothxx','xxboth'),
    ('','x','','',''), ('ééxé','é','x','xé','ééx'),
    ('😀界a界😀','😀界','a','a界😀','😀界a'), ('éxé','é','x','xé','éx'),
    (' x ','',' x ',' x ',' x '), ('NULL','N','ULL','ULL','NULL'))
DIRECTIONS = ('', 'both ', 'leading ', 'trailing ')
MALFORMED = (
    'trim()', 'trim(from)', 'trim(both)', 'trim(leading)', 'trim(trailing)',
    "trim('x' from)", "trim(both 'x' from)", "trim(from 'x',)", "trim('x',)",
    "trim('x' from 'y' from 'z')", "trim('x' 'y')", "trim(leading, 'x')",
    "trim(both from from 'x')", "trim(both leading from 'x')", "trim('x' from 'y'",
    "trim(both from 'x' 'y')")
WIDTHS[17] = -1


def literal(value):
    return 'NULL::text' if value is None else "'" + value.replace("'", "''") + "'"


def forms(direction, source, characters):
    return ('trim('+direction+characters+' from '+source+')',
            'trim('+direction+source+','+characters+')',
            'trim('+direction+'from '+source+','+characters+')')


def expected(source, characters, direction):
    if source is None or characters is None:
        return None
    if not characters:
        return source
    return (source.lstrip(characters) if direction == 'leading ' else
            source.rstrip(characters) if direction == 'trailing ' else source.strip(characters))


def cases():
    result = []
    for source, characters, both, leading, trailing in SAMPLES:
        for direction in DIRECTIONS:
            value = leading if direction == 'leading ' else trailing if direction == 'trailing ' else both
            result += [(sql,value,25) for sql in forms(direction,literal(source),literal(characters))]
    for source in ('','both','leading','trailing',' both ','  x','x  ','\t x \t'):
        for direction in DIRECTIONS:
            for separator in ('', 'from '):
                result.append(('trim('+direction+separator+literal(source)+')',expected(source,' ',direction),25))
    for source, characters in ((None,'x'),('xx',None),(None,None)):
        for direction in DIRECTIONS:
            result += [(sql,None,25) for sql in forms(direction,literal(source),literal(characters))]
    result += [(sql,value,25) for sql,value in (
        ("trim('x' from trim(leading 'y' from 'yyxxbothxxyy'))",'bothxxyy'),
        ("trim(leading 'x' from trim(trailing 'y' from 'xxbothyy'))",'both'),
        ("trim(substring('xx' from 1 for 1) from 'xxbothxx')",'both'),
        ("trim('x' from substring('xxbothxx' from 1 for 8))",'both'),
        ("trim(' both '::char(10))",'both'), ("trim('x' from 'xxbothxx'::char(10))",'both'),
        ("trim(leading from ' x '::char(6))",'x'), ("trim(trailing from ' x '::varchar)",' x'),
        ('trim(NULL)',None), ('trim(leading NULL)',None),
        ('trim(trailing from NULL)',None), ('trim(both from NULL)',None))]
    for direction in DIRECTIONS:
        value = '\\x796178' if direction == 'leading ' else '\\x787961' if direction == 'trailing ' else '\\x7961'
        result += [(sql,value,17) for sql in forms(direction,"'\\x78796178'::bytea","'\\x78'::bytea")]
    assert len(result) == 244
    return result


class Wire(BaseWire):
    name = 'v'

    def description(self, oid, binary=False):
        return struct.pack('!H',1)+self.name.encode()+b'\0'+struct.pack('!IhIhih',0,0,oid,WIDTHS[oid],-1,int(binary))

    def result_ok(self, messages, values, oid, state=None, binary=False,
                  parsed=False, bound=False, simple=False, inputs=()):
        if oid != 17 or state:
            return super().result_ok(messages,values,oid,state,binary,parsed,bound,simple,inputs)
        rows, valid = strict_data_rows(messages)
        prefix = [b'T'] if simple else [b'1',b't',b'T'] if parsed else []
        descriptions = [self.description(oid)] if parsed else []
        if simple or bound:
            descriptions.append(self.description(oid,binary))
        if bound:
            prefix += [b'2',b'T']
        wanted_rows = [[None if value is None else bytes.fromhex(value[2:]) if binary else value.encode()] for value in values]
        return (valid and not self.errors(messages) and rows == wanted_rows and
                [kind for kind,_ in messages] == prefix+[b'D']*len(values)+[b'C',b'Z'] and
                [body for kind,body in messages if kind == b'T'] == descriptions and
                [body for kind,body in messages if kind == b't'] == ([self.parameters(inputs)] if parsed else []) and
                [body for kind,body in messages if kind == b'C'] == [('SELECT '+str(len(values))).encode()+b'\0'] and
                all(body == b'' for kind,body in messages if kind in (b'1',b'2')) and messages[-1:] == [(b'Z',b'I')])


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review=Review('TRIM_KEYWORD_IDENTITY_PROTOCOL')
    wire=Wire(review)
    schema='trim_identity_'+uuid.uuid4().hex
    qualified=identifier(schema)
    table=qualified+'.rows_'
    sequence=qualified+'.effects_'
    owned=False
    try:
        for mode in MODES:
            for expression,value,oid in cases():
                wire.query(mode,'SELECT '+expression+' AS v;',(value,),oid)
            for word in ('both','leading','trailing'):
                for direction in DIRECTIONS:
                    wire.name='ltrim' if direction == 'leading ' else 'rtrim' if direction == 'trailing ' else 'btrim'
                    wire.query(mode,'SELECT trim('+direction+literal(word)+');',(word,),25)
            wire.name='v'
        parameter_pairs=(('both','b'),('leading','l'),(' xx ','x'),('😀x😀','😀'),('',None),(None,'x'))
        for index,direction in enumerate(DIRECTIONS):
            for form,sql in enumerate(forms(direction,'$1::text','$2::text')):
                statement=('trim_parameters_'+str(index)+'_'+str(form)).encode()
                wire.describe(statement,'SELECT '+sql+' AS v;',25,(25,25))
                wire.bind_describe(statement,25,(25,25),('both','b'),False,False)
                for binary in (False,True):
                    for source,characters in parameter_pairs:
                        wire.execute(statement,25,(expected(source,characters,direction),),(25,25),(source,characters),binary,binary)
                wire.close(statement)
        before=len(review.failures)
        wire.command('CREATE SCHEMA '+qualified+';','CREATE SCHEMA')
        owned=len(review.failures)==before
        assert owned,'private schema ownership not established'
        wire.command('CREATE TABLE '+table+'(id integer,s text,c text);','CREATE TABLE')
        rows=(('both','b'),('leading','l'),(None,'x'),('','x'),('ééxé','é'),(' x ',''))
        wire.command('INSERT INTO '+table+' VALUES'+','.join('('+str(i)+','+literal(source)+','+literal(characters)+')' for i,(source,characters) in enumerate(rows,1))+';','INSERT 0 6')
        wire.command('CREATE SEQUENCE '+sequence+';','CREATE SEQUENCE')
        for mode in MODES:
            for direction in DIRECTIONS:
                for sql in forms(direction,'s','c'):
                    prefix='SELECT '+sql+' AS v FROM '+table
                    wire.query(mode,prefix+' ORDER BY id;',tuple(expected(source,characters,direction) for source,characters in rows),25)
                    wire.query(mode,prefix+' WHERE false;',(),25)
                    wire.query(mode,prefix+' LIMIT 0;',(),25)
                for separator in ('','from '):
                    prefix='SELECT trim('+direction+separator+'s) AS v FROM '+table
                    wire.query(mode,prefix+' ORDER BY id;',tuple(expected(source,' ',direction) for source,_ in rows),25)
                    wire.query(mode,prefix+' WHERE false;',(),25)
                    wire.query(mode,prefix+' LIMIT 0;',(),25)
        effect="SELECT trim(nextval('"+sequence+"')::text from nextval('"+sequence+"')::text) AS v;"
        statement=b'trim_effects'
        wire.describe(statement,effect,25)
        wire.bind_describe(statement,25,(),(),False,False)
        wire.query('simple',"SELECT nextval('"+sequence+"') AS v;",(1,),20)
        for mode,value,following in zip(MODES,('2','5','8'),(4,7,10)):
            wire.query(mode,effect,(value,),25)
            wire.query('simple',"SELECT nextval('"+sequence+"') AS v;",(following,),20)
        wire.close(statement)
        for name in ('btrim','ltrim','rtrim','trim'):
            parameters='s text' if name == 'trim' else 's text,c text'
            wire.command('CREATE FUNCTION '+qualified+'.'+identifier(name)+'('+parameters+") RETURNS text LANGUAGE SQL IMMUTABLE AS $$SELECT 'shadow'::text$$;",'CREATE FUNCTION')
        path=qualified+', pg_catalog'
        wire.command('SET search_path='+path+';','SET',parameter_status=schema+', pg_catalog')
        for mode in MODES:
            for direction,name,value in (('both ','btrim','both'),('leading ','ltrim','bothxx'),('trailing ','rtrim','xxboth')):
                wire.query(mode,"SELECT trim("+direction+"'x' from 'xxbothxx') AS v;",(value,),25)
                wire.query(mode,'SELECT '+qualified+'.'+name+"('xxbothxx','x') AS v;",('shadow',),25)
            wire.query(mode,'SELECT '+qualified+'."trim"'+"('both') AS v;",('shadow',),25)
        wire.command('SET search_path=pg_catalog,public;','SET',parameter_status='pg_catalog, public')
        for mode in MODES:
            for expression in MALFORMED:
                wire.query(mode,'SELECT '+expression+';',(),25,'42601')
        wire.command('DROP SCHEMA '+qualified+' CASCADE;','DROP SCHEMA',notices=True)
        owned=False
        review.complete(1218)
    finally:
        if owned:
            review.client.simple_query(review.sock,'RESET search_path; DROP SCHEMA '+qualified+' CASCADE;')
        review.close()


if __name__ == '__main__':
    main()
