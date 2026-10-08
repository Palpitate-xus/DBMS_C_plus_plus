#include "catalog/CatalogService.h"
#include "catalog/systables.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/prepared_query_execution.h"
#include "parser/ast.h"
#include "test_utils.h"
#include <filesystem>
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    struct Case {std::string sql,state;Oid oid;bool null;std::string value;};
    const Case cases[]={
        {"SELECT CASE WHEN true THEN 'abc' ELSE 'def' END AS value","",25,false,"abc"},
        {"SELECT CASE WHEN false THEN NULL END AS value","",25,true,""},
        {"SELECT CASE WHEN true THEN NULL ELSE CAST(1 AS BIGINT) END AS value","",20,true,""},
        {"SELECT CASE WHEN true THEN 2 ELSE CAST(1 AS REAL) END AS value","",700,false,"2"},
        {"SELECT CASE WHEN true THEN CAST(2 AS NUMERIC) ELSE CAST(1 AS BIGINT) END AS value","",1700,false,"2"},
        {"SELECT CASE WHEN true THEN CAST(2 AS REAL) ELSE CAST(1 AS DOUBLE PRECISION) END AS value","",701,false,"2"},
        {"SELECT CASE WHEN true THEN CAST('ab' AS VARCHAR) ELSE CAST('cd' AS TEXT) END AS value","",25,false,"ab"},
        {"SELECT CASE WHEN true THEN CAST('ab' AS TEXT) ELSE CAST('cd' AS VARCHAR) END AS value","",1043,false,"ab"},
        {"SELECT CASE WHEN true THEN CAST('abcd' AS VARCHAR) ELSE CAST('xy' AS CHAR(2)) END AS value","",1042,false,"abcd"},
        {"SELECT CASE WHEN true THEN CAST(B'0101' AS BIT VARYING) ELSE CAST(B'11' AS BIT(2)) END AS value","",1560,false,"0101"},
        {"SELECT CASE WHEN true THEN DATE '2026-10-06' ELSE TIMESTAMP '2026-10-07 00:00:00' END AS value","",1114,false,"2026-10-06 00:00:00"},
        {"SELECT CASE WHEN true THEN '1 us' ELSE INTERVAL '2 days' END AS value","",1186,false,"00:00:00.000001"},
        {"SELECT CASE WHEN false THEN 1/0 ELSE 2 END AS value","",23,false,"2"},
        {"SELECT CASE WHEN false THEN CAST(2147483648 AS INTEGER) ELSE 2 END AS value","",23,false,"2"},
        {"SELECT CASE WHEN false THEN 'bad' ELSE 1 END AS value WHERE false","22P02",0,false,""},
        {"SELECT CASE WHEN false THEN '1 fortnight' ELSE INTERVAL '1 day' END AS value WHERE false","22007",0,false,""},
        {"SELECT CASE WHEN false THEN '2147483648 months' ELSE INTERVAL '1 day' END AS value WHERE false","22015",0,false,""},
        {"SELECT CASE WHEN false THEN CAST('1 day' AS TEXT) ELSE INTERVAL '1 day' END AS value WHERE false","42804",0,false,""},
        {"SELECT CASE WHEN false THEN 1 ELSE INTERVAL '1 day' END AS value WHERE false","42804",0,false,""},
        {"SELECT CASE WHEN 1 THEN 1 ELSE 2 END AS value WHERE false","42804",0,false,""},
        {"SELECT CASE WHEN 'bad' THEN 1 ELSE 2 END AS value WHERE false","22P02",0,false,""},
        {"SELECT CASE 1 WHEN 'bad' THEN 1 ELSE 2 END AS value WHERE false","22P02",0,false,""},
        {"SELECT CASE WHEN true THEN NULL ELSE CAST('A' AS \"char\") END AS value","",18,true,""},
        {"SELECT CASE WHEN false THEN NULL ELSE CAST('A' AS \"char\") END AS value","",18,false,"A"},
        {"SELECT CASE WHEN true THEN CAST('A' AS \"char\") ELSE CAST('B' AS \"char\") END AS value","",18,false,"A"},
        {"SELECT CASE WHEN true THEN NULL ELSE ARRAY[1::bigint,2::bigint] END AS value","",1016,true,""},
        {"SELECT CASE WHEN false THEN NULL ELSE ARRAY[1::bigint,2::bigint] END AS value","",1016,false,"{1,2}"},
        {"SELECT CASE WHEN true THEN NULL ELSE CAST('0.125' AS numeric) END AS value","",1700,true,""},
    };
    size_t checked=0,failed=0;
    const auto require=[&](bool pass,const std::string& role) {
        ++checked;failed+=!pass;std::cout<<"COLD_OPERATOR "<<role<<" pass="<<pass<<'\n';
    };
    const auto database=testDbPath("cold_builtin_operator");
    require(g_engine.createDatabase(database)==DBStatus::OK,"create isolated database");
    const auto cold=g_engine.catalogService().metadataSnapshot(database);
    const auto catalogPath=g_engine.dbPath(database)/"pg_catalog";
    const auto directoryBefore=std::filesystem::exists(catalogPath);
    require(cold.types.empty() && cold.namespaces.empty() && !g_engine.catalogService().has(database),
        "actual cold catalog before CASE/operator lookup");
    for(bool warm:{false,true}) {
        if(warm) {
            (void)g_engine.catalogService().get(database);
            const auto metadata=g_engine.catalogService().metadataSnapshot(database);
            require(!metadata.types.empty(),"actual initialized catalog");
        }
        for(const auto& expected:cases) {
            std::string state;std::shared_ptr<PreparedQuery> query;
            try {query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,expected.sql));}
            catch(const DbError& error){state=error.sqlState();}
            const auto role=std::string(warm?"warm ":"cold ")+expected.sql;
            if(!expected.state.empty()) {require(state==expected.state,role+" actual error "+state);continue;}
            if(query && query->output.size()==1)
                std::cout<<"COLD_OPERATOR_DESCRIPTOR actual_name="<<query->output[0].name<<" actual_oid="<<query->output[0].typeOid
                    <<" expected_oid="<<expected.oid<<'\n';
            require(state.empty() && query && query->output.size()==1 &&
                query->output[0].name=="value" && query->output[0].typeOid==expected.oid,
                role+" exact descriptor/state "+state);
            ExprValue value;bool evaluated=false;
            if(query) {
                try {
                    const auto* select=dynamic_cast<const SelectStmt*>(query->ast.get());
                    if(select && select->selectList.size()==1) {
                        PreparedQueryExecution execution(query,&g_engine,database);
                        execution.prepareExpression(select->selectList.front().expr.get());
                        value=execution.evaluate(select->selectList.front().expr.get(),execution.context());
                        evaluated=true;
                    }
                } catch(const DbError& error){state=error.sqlState();}
            }
            require(evaluated && state.empty() && value.isNull==expected.null &&
                (expected.null || value.value==expected.value),role+" exact lazy value/null "+state);
            require(evaluated && state.empty() && mapBuiltinTypeNameToOid(value.typeName)==expected.oid,
                role+" actual evaluated type "+value.typeName);
        }
        if(!warm) {
            const auto after=g_engine.catalogService().metadataSnapshot(database);
            require(after.types.empty() && after.namespaces.empty() && !g_engine.catalogService().has(database) &&
                std::filesystem::exists(catalogPath)==directoryBefore,
                "cold lookup did not initialize catalog/cache/directory");
        }
    }
    std::cout<<"COLD_OPERATOR_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    return checked==140 && !failed?0:1;
}
