#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "executor/ExecutionPlan.h"
#include "common/SqlArrayText.h"
#include "test_utils.h"
#include <iostream>
extern dbms::StorageEngine g_engine;
using namespace dbms;
static size_t checked=0,failed=0;
static void check(bool pass,const std::string& label) {
    ++checked;if(!pass){++failed;std::cerr<<"FAIL "<<label<<'\n';}
}
int main() {
    TypeRegistry::instance().bootstrap();
    const auto database=testDbPath("array_concat_implicit_typmod");
    check(g_engine.createDatabase(database)==DBStatus::OK,"isolated database");
    (void)g_engine.catalogService().get(database);
    ExprEvaluator evaluator;evaluator.setCurrentDB(database);
    struct Case {std::string sql,state;uint32_t oid;int32_t modifier;bool isNull;std::string value;};
    // Actual PostgreSQL18.6 text values, physical type/modifier and errors,
    // frozen before the production change; full wire matrix is separate.
    const Case cases[]={
        {"SELECT ARRAY['abc'::character(4)] || ARRAY['xy'::character(4)] AS v","",1014,-1,false,"{\"abc \",\"xy  \"}"},
        {"SELECT ARRAY['abc'::character(5),'abcdef'::character(6),NULL] || ARRAY['xy'::character(3)] AS v","",1014,-1,false,"{\"abc  \",abcdef,NULL,\"xy \"}"},
        {"SELECT ARRAY['abc '::pg_catalog.bpchar] || ARRAY['xy'::pg_catalog.bpchar] AS v","",1014,-1,false,"{\"abc \",xy}"},
        {"SELECT ARRAY['abc'::character(4)] || 'xy'::character(4) AS v","",1014,-1,false,"{\"abc \",\"xy  \"}"},
        {"SELECT 'abc'::character(4) || ARRAY['xy'::character(4)] AS v","",1014,-1,false,"{\"abc \",\"xy  \"}"},
        {"SELECT ARRAY['abc'::character(4)] || NULL::character AS v","",1014,-1,false,"{\"abc \",NULL}"},
        {"SELECT NULL::character || ARRAY['xy'::character(4)] AS v","",1014,-1,false,"{NULL,\"xy  \"}"},
        {"SELECT NULL::character[] || ARRAY['xy'::character(4)] AS v","",1014,-1,false,"{\"xy  \"}"},
        {"SELECT ARRAY['abc'::character(4)] || NULL::character[] AS v","",1014,-1,false,"{\"abc \"}"},
        {"SELECT NULL::character[] || NULL::character[] AS v","",1014,-1,true,""},
        {"SELECT ARRAY[]::character[] || ARRAY['xy'::character(4)] AS v","",1014,-1,false,"{\"xy  \"}"},
        {"SELECT ARRAY['abc'::character(4)] || ARRAY[]::character[] AS v","",1014,-1,false,"{\"abc \"}"},
        {"SELECT ARRAY[''::pg_catalog.bpchar,'NULL'::pg_catalog.bpchar,NULL::character] || ARRAY['é你'::character(4)] AS v","",1014,-1,false,"{\"\",\"NULL\",NULL,\"é你  \"}"},
        {"SELECT ARRAY['a,b'::pg_catalog.bpchar,'a\"b'::pg_catalog.bpchar,'a\\b'::pg_catalog.bpchar] || ARRAY['{x}'::pg_catalog.bpchar] AS v","",1014,-1,false,"{\"a,b\",\"a\\\"b\",\"a\\\\b\",\"{x}\"}"},
        {"SELECT ARRAY[ARRAY['abc'::character(4)],ARRAY['xy'::character(3)]] || ARRAY[ARRAY['end'::character(5)]] AS v","",1014,-1,false,"{{\"abc \"},{\"xy \"},{\"end  \"}}"},
        {"SELECT '[0:1]={\"abc \",xy}'::pg_catalog.bpchar[] || '[4:5]={\"long  \",end}'::pg_catalog.bpchar[] AS v","",1014,-1,false,"[0:3]={\"abc \",xy,\"long  \",end}"},
        {"SELECT '[0:1]={\"abc \",xy}'::pg_catalog.bpchar[] || 'end'::character(4) AS v","",1014,-1,false,"[0:2]={\"abc \",xy,\"end \"}"},
        {"SELECT 'end'::character(4) || '[0:1]={\"abc \",xy}'::pg_catalog.bpchar[] AS v","",1014,-1,false,"[0:2]={\"end \",\"abc \",xy}"},
        {"SELECT ARRAY['abc '::character(4)] || ARRAY['xy'::varchar] AS v","",1014,-1,false,"{\"abc \",xy}"},
        {"SELECT ARRAY['xy'::varchar] || ARRAY['abc '::character(4)] AS v","",1015,-1,false,"{xy,abc}"},
        {"SELECT ARRAY['abc '::character(4)] || ARRAY['xy'::text] AS v","",1014,-1,false,"{\"abc \",xy}"},
        {"SELECT ARRAY['xy'::text] || ARRAY['abc '::character(4)] AS v","",1009,-1,false,"{xy,abc}"},
        {"SELECT CAST(ARRAY['abc'::character(4)] || ARRAY['xy'::character(4)] AS character[]) AS v","",1014,5,false,"{a,x}"},
        {"SELECT CAST(ARRAY['abc'::character(4)] || ARRAY['xy'::character(4)] AS character(2)[]) AS v","",1014,6,false,"{ab,xy}"},
        {"SELECT CAST(ARRAY['abc'::character(4)] || ARRAY['xy'::character(4)] AS pg_catalog.bpchar[]) AS v","",1014,-1,false,"{\"abc \",\"xy  \"}"},
        {"SELECT array_cat(ARRAY['abc'::character(4)],ARRAY['xy'::character(4)]) AS v","",1014,-1,false,"{\"abc \",\"xy  \"}"},
        {"SELECT array_append(ARRAY['abc'::character(4)],'xy'::character(4)) AS v","",1014,-1,false,"{\"abc \",\"xy  \"}"},
        {"SELECT array_prepend('abc'::character(4),ARRAY['xy'::character(4)]) AS v","",1014,-1,false,"{\"abc \",\"xy  \"}"},
        {"SELECT ARRAY[B'01',B'001',B'',NULL] || ARRAY[B'10',B'101'] AS v","",1561,-1,false,"{01,001,\"\",NULL,10,101}"},
        {"SELECT ARRAY[B'01'] || B'001' AS v","",1561,-1,false,"{01,001}"},
        {"SELECT B'001' || ARRAY[B'01'] AS v","",1561,-1,false,"{001,01}"},
        {"SELECT ARRAY[B'01'] || NULL::bit AS v","",1561,-1,false,"{01,NULL}"},
        {"SELECT NULL::bit || ARRAY[B'01'] AS v","",1561,-1,false,"{NULL,01}"},
        {"SELECT NULL::bit[] || ARRAY[B'01'] AS v","",1561,-1,false,"{01}"},
        {"SELECT ARRAY[B'01'] || NULL::bit[] AS v","",1561,-1,false,"{01}"},
        {"SELECT NULL::bit[] || NULL::bit[] AS v","",1561,-1,true,""},
        {"SELECT ARRAY[]::bit[] || ARRAY[B'01'] AS v","",1561,-1,false,"{01}"},
        {"SELECT ARRAY[ARRAY[B'01',B'001']] || ARRAY[ARRAY[B'10',B'101']] AS v","",1561,-1,false,"{{01,001},{10,101}}"},
        {"SELECT '[0:1]={01,001}'::pg_catalog.bit[] || '[4:5]={10,101}'::pg_catalog.bit[] AS v","",1561,-1,false,"[0:3]={01,001,10,101}"},
        {"SELECT ARRAY[B'01'] || ARRAY[B'001'::bit varying] AS v","",1561,-1,false,"{01,001}"},
        {"SELECT ARRAY[B'001'::bit varying] || ARRAY[B'01'] AS v","",1563,-1,false,"{001,01}"},
        {"SELECT CAST(ARRAY[B'01'] || ARRAY[B'001'] AS bit[]) AS v","",1561,1,false,"{0,0}"},
        {"SELECT CAST(ARRAY[B'01'] || ARRAY[B'001'] AS bit(2)[]) AS v","",1561,2,false,"{01,00}"},
        {"SELECT CAST(ARRAY[B'01'] || ARRAY[B'001'] AS pg_catalog.bit[]) AS v","",1561,-1,false,"{01,001}"},
        {"SELECT array_cat(ARRAY[B'01'],ARRAY[B'001']) AS v","",1561,-1,false,"{01,001}"},
        {"SELECT array_append(ARRAY[B'01'],B'001') AS v","",1561,-1,false,"{01,001}"},
        {"SELECT array_prepend(B'001',ARRAY[B'01']) AS v","",1561,-1,false,"{001,01}"},
        {"SELECT ARRAY[1] || ARRAY[2::bigint] AS v","",1016,-1,false,"{1,2}"},
        {"SELECT ARRAY[1] || 2.5::numeric AS v","",1231,-1,false,"{1,2.5}"},
        {"SELECT ARRAY['{x}'::text,'',NULL] || ARRAY['NULL'::text] AS v","",1009,-1,false,"{\"{x}\",\"\",NULL,\"NULL\"}"},
        {"SELECT B'01' || B'001' AS v","",1562,-1,false,"01001"},
        {"SELECT 'abc '::character(4) || 'xy'::character(4) AS v","",25,-1,false,"abcxy"},
        {"SELECT ARRAY[1] || ARRAY[TRUE] AS v","42883",0,-1,false,""},
        {"SELECT ARRAY[B'01'] || ARRAY[TRUE] AS v","42883",0,-1,false,""},
        {"SELECT ARRAY['abc'::character(4)] || ARRAY[ARRAY['x'::character(2),'y'::character(2)]] AS v","2202E",0,-1,false,""},
        {"SELECT ARRAY[ARRAY[B'01']] || B'1' AS v","22000",0,-1,false,""},
    };
    for(const auto& item:cases) {
        try {
            const auto query=g_engine.prepareBoundQuery(database,item.sql);
            const auto* select=dynamic_cast<const SelectStmt*>(query.ast.get());
            const auto value=evaluator.eval(select->selectList.at(0).expr.get(),RowContext{});
            check(item.state.empty(),item.sql+" expected SQLSTATE "+item.state);
            if(item.state.empty()) {
                check(query.output.size()==1 && query.output[0].typeOid==item.oid && query.output[0].typeMod==item.modifier,item.sql+" physical descriptor");
                check(value.isNull==item.isNull,item.sql+" SQL NULL flag");
                if(!item.isNull)check(value.value==item.value,item.sql+" value="+value.value+" expected="+item.value);
            }
        } catch(const DbError& error){check(!item.state.empty() && error.sqlState()==item.state,item.sql+" SQLSTATE="+error.sqlState());}
          catch(const std::exception& error){check(false,item.sql+" exception="+error.what());}
    }
    // Runtime cells, not statement constants: NULL's opaque payload must
    // never be serialized and implicit conversion has no length-one default.
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
        {"character[]","{\"abc \",NULL,\"\"}"},{"bit[]","{01,NULL,001}"}}) {
        BinaryOpExpr binary;binary.op="||";
        for(size_t slot=0;slot<2;++slot) {
            auto parameter=std::make_unique<ParameterExpr>();parameter->slot=slot;
            parameter->declaredType=item.first;parameter->origin=ParameterOrigin::RuntimeCell;
            (slot?binary.right:binary.left)=std::move(parameter);
        }
        binary.arrayConcat=ExprHelper::resolveArrayConcatTypes(item.first,item.first,true);
        for(bool leftNull:{false,true}) {
            RowContext row;row.setParameters({ExprValue(item.first,leftNull?"opaque NULL payload":item.second,leftNull),ExprValue(item.first,item.second)});
            const auto value=evaluator.eval(&binary,row);
            check(!value.isNull,item.first+" NULL receiver/non-NULL source");
            check(value.value==(leftNull?item.second:sql_array_text::concatenate(item.second,item.second)),item.first+" complete runtime bytes");
        }
    }
    size_t calls=0;
    evaluator.registerFunction("r329_tick",[&](const auto&){++calls;return ExprValue("character[]",calls==1?"{\"abc \"}":"{\"xy  \"}");},'v');
    BinaryOpExpr binary;binary.op="||";binary.arrayConcat=ExprHelper::resolveArrayConcatTypes("character[]","character[]",true);
    for(auto* target:{&binary.left,&binary.right}) {auto call=std::make_unique<FunctionCallExpr>();call->funcName="r329_tick";call->resolvedResultType="character[]";*target=std::move(call);}
    const auto value=evaluator.eval(&binary,RowContext{});
    check(calls==2,"each live argument evaluated exactly once");
    check(value.value=="{\"abc \",\"xy  \"}","volatile full-width values");
    std::cout<<"ARRAY_CONCAT_IMPLICIT_COMPLETE CHECKED="<<checked<<" FAILURES="<<failed<<'\n';
    return failed?1:0;
}
