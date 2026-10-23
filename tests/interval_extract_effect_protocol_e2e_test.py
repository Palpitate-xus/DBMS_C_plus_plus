#!/usr/bin/env python3
"""DATE_PART's field is a row value; NULL and effects must be evaluated once."""
from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import identifier
import sys
import uuid


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review = Review('INTERVAL_EXTRACT_EFFECT_PROTOCOL')
    client, sock = review.client, review.sock
    schema = 'extract_effect_' + uuid.uuid4().hex
    table, sequence = identifier(schema) + '.rows_', identifier(schema) + '.effects_'
    owned = False
    try:
        before = len(review.failures)
        review.query('CREATE SCHEMA ' + identifier(schema), rows=[], tag='CREATE SCHEMA')
        assert len(review.failures) == before, 'private schema ownership not confirmed'
        owned = True
        review.query('CREATE TABLE ' + table + '(id integer,i interval,f text)', rows=[], tag='CREATE TABLE')
        review.query('INSERT INTO ' + table + " VALUES(1,'1 year','epoch'),(2,'-1 month','quarter'),(3,NULL,'nonesuch')", rows=[], tag='INSERT 0 3')
        review.query('SELECT date_part(f,i) AS v FROM ' + table + ' ORDER BY id', rows=[['31557600'], ['-1'], [None]], oids=[701])
        review.query('CREATE SEQUENCE ' + sequence, rows=[], tag='CREATE SEQUENCE')
        sql = "SELECT date_part(CASE WHEN nextval('" + sequence + "')%2=1 THEN 'epoch' ELSE 'year' END,i) AS v FROM " + table + ' WHERE id=1'
        review.describe(sql, oids=[701])
        review.query("SELECT currval('" + sequence + "')", state='55000')
        review.query(sql, rows=[['31557600']], oids=[701])
        review.query("SELECT currval('" + sequence + "')", rows=[['1']], oids=[20])
        sql = "SELECT date_part(CASE WHEN nextval('" + sequence + "')>0 THEN NULL::text ELSE 'epoch' END,i) AS v FROM " + table + ' WHERE id=1'
        review.query(sql, rows=[[None]], oids=[701])
        review.query("SELECT currval('" + sequence + "')", rows=[['2']], oids=[20])
        review.query("SELECT date_part(f,INTERVAL '1 second') AS v FROM " + table + ' WHERE id=3', state='22023')
        review.query('DROP SCHEMA ' + identifier(schema) + ' CASCADE', rows=[], tag='DROP SCHEMA')
        owned = False
        review.complete(13)
    finally:
        if owned:
            client.simple_query(sock, 'DROP SCHEMA ' + identifier(schema) + ' CASCADE')
        review.close()


if __name__ == '__main__':
    main()
