#!/usr/bin/env python3
"""Shared UTF-8 splitting: full values, argument roles, metadata and demand.

Timings are observations only; neither speed ratios nor thresholds are gates.
"""
from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import Wire, identifier
import time
import uuid

LABEL = 'UTF8_SPLIT_LINEAR_PROTOCOL'
CONTROLS = 329
MODES = ('simple', 'extended', 'binary')


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def cases():
    result = []
    for value, trim, both, left, right in (
        ('', 'é', '', '', ''), ('ééxé', 'é', 'x', 'xé', 'ééx'),
        ('丰x丰', '中估', '丰x丰', '丰x丰', '丰x丰'),
        ('😀界a界😀', '😀界', 'a', 'a界😀', '😀界a'),
        ('éxé', 'é', 'x', 'xé', 'éx'), (' x ', '', ' x ', ' x ', ' x ')):
        for name, expected in (('btrim', both), ('ltrim', left), ('rtrim', right)):
            result.append((name + '(' + literal(value) + ',' + literal(trim) + ')', expected))
    for value, count, fill, left, right in (
        ('é', 5, '😀界', '😀界😀界é', 'é😀界😀界'), ('é界😀', 2, 'x', 'é界', 'é界'),
        ('a', 5, '', 'a', 'a'), ('a', 0, '界', '', ''), ('', 3, 'é', 'ée', 'ée')):
        for name, expected in (('lpad', left), ('rpad', right)):
            result.append((name + '(' + literal(value) + ',' + str(count) + ',' + literal(fill) + ')', expected))
    for value, source, target, expected in (
        ('aé界😀', 'é界😀', 'XYZ', 'aXYZ'), ('aé界😀', 'é界😀', 'Z', 'aZ'),
        ('aé界😀', '', 'x', 'aé界😀'), ('éé', 'éé', 'ab', 'aa'), ('é', '́', '!', 'e!'), ('', 'é', 'a', '')):
        result.append(('translate(' + ','.join(map(literal, (value, source, target))) + ')', expected))
    result += [
        ("string_to_array('aé界😀',NULL)::text", '{a,é,界,😀}'),
        ("string_to_array('',NULL)::text", '{}'),
        ("string_to_array('é',NULL)::text", '{e,́}'),
        ("string_to_array('é界é',NULL,'é')::text", '{NULL,界,NULL}'),
        ("string_to_array('aé界','')::text", '{aé界}'),
        ("string_to_array('a界b界','界')::text", '{a,b,""}'),
        ("btrim(NULL::text,'é')", None), ("lpad(NULL::text,3,'é')", None),
        ("translate(NULL::text,'é','a')", None), ("string_to_array(NULL::text,NULL)::text", None)]
    assert len(result) == 44
    return result


def families(value):
    return ('btrim(' + value + ",'é')", 'lpad(' + value + ",5,'z')",
            'translate(' + value + ",'é','a')", 'string_to_array(' + value + ',NULL)::text')


def outputs(value):
    if value is None:
        return (None,) * 4
    return (value.strip('é'), value[:5] if len(value) >= 5 else 'z' * (5-len(value)) + value,
            value.replace('é', 'a'), '{}' if not value else '{' + ','.join(value) + '}')


def main():
    review = Review(LABEL)
    wire = Wire(review)
    schema = 'utf8_split_' + uuid.uuid4().hex
    table, sequence = identifier(schema) + '.rows_', identifier(schema) + '.effects_'
    owned = False
    try:
        for mode in MODES:
            for expression, expected in cases():
                wire.query(mode, 'SELECT ' + expression + ' AS v;', (expected,), 25)
            for count in (2048, 4096, 8192):
                value = "repeat('aé界😀'," + str(count) + ')'
                trials = (
                    ('trim-input', 'btrim(' + value + ",'!')=" + value),
                    ('trim-set', "btrim('z'," + value + ")='z'"),
                    ('lpad-fill', "lpad(''," + str(4*count) + ',' + value + ')=' + value),
                    ('rpad-fill', "rpad(''," + str(4*count) + ',' + value + ')=' + value),
                    ('translate-input', 'translate(' + value + ",'é界😀','XYZ')=repeat('aXYZ'," + str(count) + ')'),
                    ('translate-from', "translate('z'," + value + ",'X')='z'"),
                    ('translate-to', "translate('é','é'," + value + ")='a'"),
                    ('array-input', 'array_to_string(string_to_array(' + value + ',NULL),\'\')=' + value))
                for label, condition in trials:
                    sql = "SELECT CASE WHEN " + condition + " THEN 'same' ELSE 'different' END AS v;"
                    started = time.monotonic_ns()
                    wire.query(mode, sql, ('same',), 25)
                    print('UTF8_SPLIT_LINEAR_ELAPSED_NS', mode, label, count, time.monotonic_ns()-started, flush=True)

        for index, expression in enumerate(families('$1::text')):
            statement = ('utf8_split_parameter_' + str(index)).encode()
            wire.describe(statement, 'SELECT ' + expression + ' AS v;', 25, (25,))
            wire.bind_describe(statement, 25, (25,), ('é界😀',), False, False)
            for binary in (False, True):
                for value in ('é界😀', '', 'aé', None):
                    wire.execute(statement, 25, (outputs(value)[index],), (25,), (value,), binary_input=binary, binary_output=binary)
            wire.close(statement)

        previous = len(review.failures)
        wire.command('CREATE SCHEMA ' + identifier(schema) + ';', 'CREATE SCHEMA')
        owned = len(review.failures) == previous
        assert owned, 'private schema ownership not confirmed'
        wire.command('CREATE TABLE ' + table + '(id integer,s text);', 'CREATE TABLE')
        wire.command('INSERT INTO ' + table + " VALUES(1,'é界😀'),(2,NULL),(3,''),(4,'aé');", 'INSERT 0 4')
        for mode in MODES:
            for index, expression in enumerate(families('s')):
                wire.query(mode, 'SELECT ' + expression + ' AS v FROM ' + table + ' ORDER BY id;',
                           tuple(outputs(value)[index] for value in ('é界😀', None, '', 'aé')), 25)
                wire.query(mode, 'SELECT ' + expression + ' AS v FROM ' + table + ' WHERE false;', (), 25)
                wire.query(mode, 'SELECT ' + expression + ' AS v FROM ' + table + ' LIMIT 0;', (), 25)
        wire.command('CREATE SEQUENCE ' + sequence + ';', 'CREATE SEQUENCE')
        next_value = 1
        for index, expression in enumerate(families("nextval('" + sequence + "')::text")):
            statement = ('utf8_split_effect_' + str(index)).encode()
            sql = 'SELECT ' + expression + ' AS v;'
            wire.describe(statement, sql, 25)
            wire.bind_describe(statement, 25, (), (), False, False)
            wire.query('simple', "SELECT nextval('" + sequence + "') AS v;", (next_value,), 20)
            next_value += 1
            for mode in MODES:
                wire.query(mode, sql, (outputs(str(next_value))[index],), 25)
                next_value += 1
                wire.query('simple', "SELECT nextval('" + sequence + "') AS v;", (next_value,), 20)
                next_value += 1
            wire.close(statement)
        wire.command('DROP SCHEMA ' + identifier(schema) + ' CASCADE;', 'DROP SCHEMA', notices=True)
        owned = False
        review.complete(CONTROLS)
    finally:
        if owned:
            review.client.simple_query(review.sock, 'DROP SCHEMA ' + identifier(schema) + ' CASCADE;')
        review.close()


if __name__ == '__main__':
    main()
