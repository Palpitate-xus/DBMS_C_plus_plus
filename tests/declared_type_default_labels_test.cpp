#include "catalog/CatalogService.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    struct Case {std::string sql,label;Oid oid;};
    const std::vector<Case> cases={
        {"SELECT \"char\" 'A'","char",18},
        {"SELECT CAST('A' AS \"char\")","char",18},
        {"SELECT 'A'::\"char\"","char",18},
        {"SELECT pg_catalog.\"char\" 'A'","char",18},
        {"SELECT CAST('A' AS pg_catalog.\"char\")","char",18},
        {"SELECT 'A'::pg_catalog.\"char\"","char",18},
        {"SELECT CHAR 'abc'","bpchar",1042},
        {"SELECT CAST('abc' AS CHAR)","bpchar",1042},
        {"SELECT 'abc'::CHAR","bpchar",1042},
        {"SELECT bpchar 'abc'","bpchar",1042},
        {"SELECT CAST('abc' AS bpchar)","bpchar",1042},
        {"SELECT 'abc'::bpchar","bpchar",1042},
        {"SELECT pg_catalog.bpchar 'abc'","bpchar",1042},
        {"SELECT CAST('abc' AS pg_catalog.bpchar)","bpchar",1042},
        {"SELECT 'abc'::pg_catalog.bpchar","bpchar",1042},
        {"SELECT BIT '01'","bit",1560},
        {"SELECT CAST('01' AS BIT)","bit",1560},
        {"SELECT '01'::BIT","bit",1560},
        {"SELECT pg_catalog.bit '01'","bit",1560},
        {"SELECT CAST('01' AS pg_catalog.bit)","bit",1560},
        {"SELECT '01'::pg_catalog.bit","bit",1560},
        {"SELECT VARCHAR 'abc'","varchar",1043},
        {"SELECT CAST('abc' AS VARCHAR)","varchar",1043},
        {"SELECT 'abc'::VARCHAR","varchar",1043},
        {"SELECT pg_catalog.varchar 'abc'","varchar",1043},
        {"SELECT CAST('abc' AS pg_catalog.varchar)","varchar",1043},
        {"SELECT 'abc'::pg_catalog.varchar","varchar",1043},
        {"SELECT FLOAT(24) '1.25'","float4",700},
        {"SELECT CAST('1.25' AS FLOAT(24))","float4",700},
        {"SELECT '1.25'::FLOAT(24)","float4",700},
        {"SELECT INTEGER '12'","int4",23},
        {"SELECT CAST('12' AS INTEGER)","int4",23},
        {"SELECT '12'::INTEGER","int4",23},
        {"SELECT int4 '12'","int4",23},
        {"SELECT CAST('12' AS int4)","int4",23},
        {"SELECT '12'::int4","int4",23},
        {"SELECT \"char\" 'A' WHERE false","char",18},
        {"SELECT CAST('A' AS \"char\") WHERE false","char",18},
        {"SELECT 'A'::\"char\" WHERE false","char",18},
        {"SELECT pg_catalog.\"char\" 'A' WHERE false","char",18},
        {"SELECT CAST('A' AS pg_catalog.\"char\") WHERE false","char",18},
        {"SELECT 'A'::pg_catalog.\"char\" WHERE false","char",18},
        {"SELECT CHAR 'abc' WHERE false","bpchar",1042},
        {"SELECT CAST('abc' AS CHAR) WHERE false","bpchar",1042},
        {"SELECT 'abc'::CHAR WHERE false","bpchar",1042},
        {"SELECT bpchar 'abc' WHERE false","bpchar",1042},
        {"SELECT CAST('abc' AS bpchar) WHERE false","bpchar",1042},
        {"SELECT 'abc'::bpchar WHERE false","bpchar",1042},
        {"SELECT pg_catalog.bpchar 'abc' WHERE false","bpchar",1042},
        {"SELECT CAST('abc' AS pg_catalog.bpchar) WHERE false","bpchar",1042},
        {"SELECT 'abc'::pg_catalog.bpchar WHERE false","bpchar",1042},
        {"SELECT BIT '01' WHERE false","bit",1560},
        {"SELECT CAST('01' AS BIT) WHERE false","bit",1560},
        {"SELECT '01'::BIT WHERE false","bit",1560},
        {"SELECT pg_catalog.bit '01' WHERE false","bit",1560},
        {"SELECT CAST('01' AS pg_catalog.bit) WHERE false","bit",1560},
        {"SELECT '01'::pg_catalog.bit WHERE false","bit",1560},
        {"SELECT VARCHAR 'abc' WHERE false","varchar",1043},
        {"SELECT CAST('abc' AS VARCHAR) WHERE false","varchar",1043},
        {"SELECT 'abc'::VARCHAR WHERE false","varchar",1043},
        {"SELECT pg_catalog.varchar 'abc' WHERE false","varchar",1043},
        {"SELECT CAST('abc' AS pg_catalog.varchar) WHERE false","varchar",1043},
        {"SELECT 'abc'::pg_catalog.varchar WHERE false","varchar",1043},
        {"SELECT FLOAT(24) '1.25' WHERE false","float4",700},
        {"SELECT CAST('1.25' AS FLOAT(24)) WHERE false","float4",700},
        {"SELECT '1.25'::FLOAT(24) WHERE false","float4",700},
        {"SELECT INTEGER '12' WHERE false","int4",23},
        {"SELECT CAST('12' AS INTEGER) WHERE false","int4",23},
        {"SELECT '12'::INTEGER WHERE false","int4",23},
        {"SELECT int4 '12' WHERE false","int4",23},
        {"SELECT CAST('12' AS int4) WHERE false","int4",23},
        {"SELECT '12'::int4 WHERE false","int4",23},
        {"SELECT CAST(lower('A') AS varchar)","lower",1043},
        {"SELECT lower('A')::varchar","lower",1043},
        {"SELECT CAST(ARRAY[1,2] AS bigint[])","array",1016},
        {"SELECT ARRAY[1,2]::bigint[]","array",1016},
        {"SELECT CAST(CAST('A' AS \"char\") AS text)","text",25},
        {"SELECT CAST(TEXT 'A' AS \"char\")","char",18},
    };
    size_t checked=0,failed=0;
    const auto require=[&](bool pass,const std::string& role){++checked;failed+=!pass;std::cout<<"DECLARED_TYPE_DEFAULT_LABEL "<<role<<" pass="<<pass<<'\n';};
    const auto database=testDbPath("declared_type_default_labels");
    require(g_engine.createDatabase(database)==DBStatus::OK,"create isolated database");
    (void)g_engine.catalogService().get(database);
    for(const auto& item:cases) {
        bool pass=false;
        try {
            const auto query=g_engine.prepareBoundQuery(database,item.sql);
            pass=query.output.size()==1 && query.output[0].name==item.label && query.output[0].typeOid==item.oid;
            if(!pass && !query.output.empty())std::cout<<"actual label "<<query.output[0].name<<" OID="<<query.output[0].typeOid<<'\n';
        }catch(const DbError& error){std::cout<<"actual error "<<error.sqlState()<<" "<<error.what()<<'\n';}
        require(pass,item.sql);
    }
    std::cout<<"DECLARED_TYPE_DEFAULT_LABEL_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    return checked==79 && !failed?0:1;
}
