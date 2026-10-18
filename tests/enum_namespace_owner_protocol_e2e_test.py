#!/usr/bin/env python3
"""DROP SCHEMA owns decoded enum identities, not re-parsed SQL spellings."""
from compat.numeric_type_review import Review
import uuid


def ident(value):
    return '"'+value.replace('"', '""')+'"'


def literal(value):
    return "'"+value.replace("'", "''")+"'"


def main():
    review=Review("ENUM_NAMESPACE_OWNER_PROTOCOL")
    prefix="r155_"+uuid.uuid4().hex[:8]
    first,other=prefix+"Owner",prefix+"_other"
    names=("plain",'Quote"Rank',"a.b","name with space","Rank[]","汉")
    outside=first+".public_survivor"
    def target(schema,name):
        return ident(schema)+"."+ident(name)
    def control(sql,rows=None,oids=None,state=None,tag=None,ready=b"I"):
        messages=review.client.simple_query(review.sock,sql)
        result=review.runner.decode_wire_result(messages,include_types=True)
        good=result[1]==state and messages[-1]==(b"Z",ready)
        if state:
            good=good and result[0]==[] and result[4] is None
            good=good and sum(kind==b"E" for kind,_ in messages)==1
        else:
            good=good and not any(kind==b"E" for kind,_ in messages)
            if rows is not None:
                good=good and result[0]==rows and result[5]==oids and result[4]=="SELECT 1"
                fields=review.client.row_description_fields(messages) if any(kind==b"T" for kind,_ in messages) else []
                good=good and [(f[3],f[4],f[5],f[6]) for f in fields]==[(16,1,-1,0)]
            else:
                good=good and result[0]==[] and result[4]==tag
        review.record(sql,good,result,(rows,oids,state,tag,ready))
    def exists(schema,name,ready=b"I",state=None):
        sql="SELECT ("+literal(target(schema,name))+"::regtype::oid > 0::oid) AS v"
        control(sql,None if state else [["t"]],None if state else [16],state=state,ready=ready)
    def all_in(schema,ready=b"I",state=None):
        for name in names:exists(schema,name,ready,state)
    try:
        for schema in (first,other):control("CREATE SCHEMA "+ident(schema),tag="CREATE SCHEMA")
        for schema in (first,other):
            for name in names:
                control("CREATE TYPE "+target(schema,name)+" AS ENUM('z','a')",tag="CREATE TYPE")
        control("CREATE TYPE "+target("public",outside)+" AS ENUM('z','a')",tag="CREATE TYPE")
        all_in(first);all_in(other);exists("public",outside)
        control("DROP SCHEMA "+ident(first)+" RESTRICT",state="2BP01")
        all_in(first);all_in(other);exists("public",outside)
        control("BEGIN",tag="BEGIN",ready=b"T")
        control("SAVEPOINT enum_owner",tag="SAVEPOINT",ready=b"T")
        control("DROP SCHEMA "+ident(first)+" CASCADE",tag="DROP SCHEMA",ready=b"T")
        all_in(other,ready=b"T");exists("public",outside,ready=b"T")
        control("ROLLBACK TO enum_owner",tag="ROLLBACK",ready=b"T")
        # ROLLBACK TO leaves the transaction active.
        # Its exact Ready state is checked by the control above.
        control("COMMIT",tag="COMMIT")
        all_in(first)
        control("DROP SCHEMA "+ident(first)+" CASCADE",tag="DROP SCHEMA")
        all_in(first,state="3F000");all_in(other);exists("public",outside)
        control("CREATE SCHEMA "+ident(first),tag="CREATE SCHEMA")
        control("CREATE TYPE "+target(first,"recreated")+" AS ENUM('z','a')",tag="CREATE TYPE")
        exists(first,"recreated")
        control("DROP SCHEMA "+ident(first)+" CASCADE",tag="DROP SCHEMA")
        control("DROP SCHEMA "+ident(other)+" CASCADE",tag="DROP SCHEMA")
        all_in(other,state="3F000");exists("public",outside)
        control("DROP TYPE "+target("public",outside),tag="DROP TYPE")
        review.complete(87)
    finally:
        try:
            review.client.simple_query(review.sock,"ROLLBACK")
            for schema in (first,other):
                review.client.simple_query(review.sock,"DROP SCHEMA IF EXISTS "+ident(schema)+" CASCADE")
            review.client.simple_query(review.sock,"DROP TYPE IF EXISTS "+target("public",outside))
        finally:
            review.close()


if __name__=="__main__":
    main()
