#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "test_utils.h"
#include "expression/array_type.h"
#include "parser/parser.h"
#include <iostream>
extern dbms::StorageEngine g_engine;
using namespace dbms;
static size_t checked=0,failed=0;
static void check(bool pass,const std::string& label) {
    ++checked;if(!pass){++failed;std::cerr<<"FAIL "<<label<<'\n';}
}
int main() {
    TypeRegistry::instance().bootstrap();
    const auto database=testDbPath("bit_array_cast");
    check(g_engine.createDatabase(database)==DBStatus::OK,"isolated database");
    (void)g_engine.catalogService().get(database);
    ExprEvaluator evaluator;evaluator.setCurrentDB(database);
    struct Case {std::string sql,state;uint32_t oid;int32_t modifier;bool isNull;std::string value;};
    // Actual PostgreSQL18.6 results, frozen before the producer repair.
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
        {"SELECT ARRAY[B'01'] || ARRAY[B'001'::bit varying] AS v","",1561,-1,false,"{01,001}"},
        {"SELECT ARRAY[B'001'::bit varying] || ARRAY[B'01'] AS v","",1563,-1,false,"{001,01}"},
        {"SELECT ARRAY[B'01',NULL,B''] || ARRAY[B'001'::bit varying] AS v","",1561,-1,false,"{01,NULL,\"\",001}"},
        {"SELECT ARRAY[B'001'::bit varying,NULL] || ARRAY[B'01',B''] AS v","",1563,-1,false,"{001,NULL,01,\"\"}"},
        {"SELECT NULL::bit[] || ARRAY[B'001'::bit varying] AS v","",1561,-1,false,"{001}"},
        {"SELECT NULL::varbit[] || ARRAY[B'01'] AS v","",1563,-1,false,"{01}"},
        {"SELECT ARRAY[B'01'] || NULL::varbit[] AS v","",1561,-1,false,"{01}"},
        {"SELECT ARRAY[B'001'::varbit] || NULL::bit[] AS v","",1563,-1,false,"{001}"},
        {"SELECT NULL::bit[] || NULL::varbit[] AS v","",1561,-1,true,""},
        {"SELECT NULL::varbit[] || NULL::bit[] AS v","",1563,-1,true,""},
        {"SELECT ARRAY[]::bit[] || ARRAY[B'001'::varbit] AS v","",1561,-1,false,"{001}"},
        {"SELECT ARRAY[]::varbit[] || ARRAY[B'01'] AS v","",1563,-1,false,"{01}"},
        {"SELECT ARRAY[B'01'] || B'001'::varbit AS v","",1561,-1,false,"{01,001}"},
        {"SELECT B'01' || ARRAY[B'001'::varbit] AS v","",1561,-1,false,"{01,001}"},
        {"SELECT ARRAY[B'001'::varbit] || B'01' AS v","",1563,-1,false,"{001,01}"},
        {"SELECT B'001'::varbit || ARRAY[B'01'] AS v","",1563,-1,false,"{001,01}"},
        {"SELECT ARRAY[B'01',B'001'::varbit,NULL,B''] AS v","",1561,-1,false,"{01,001,NULL,\"\"}"},
        {"SELECT ARRAY[B'001'::varbit,B'01',NULL,B''] AS v","",1563,-1,false,"{001,01,NULL,\"\"}"},
        {"SELECT ARRAY[ARRAY[B'01'],ARRAY[B'001'::varbit]] AS v","",1561,-1,false,"{{01},{001}}"},
        {"SELECT ARRAY[ARRAY[B'001'::varbit],ARRAY[B'01']] AS v","",1563,-1,false,"{{001},{01}}"},
        {"SELECT '[0:1]={01,NULL}'::pg_catalog.bit[] || '[5:6]={001,101}'::pg_catalog.varbit[] AS v","",1561,-1,false,"[0:3]={01,NULL,001,101}"},
        {"SELECT '[0:1]={001,NULL}'::pg_catalog.varbit[] || '[5:6]={01,10}'::pg_catalog.bit[] AS v","",1563,-1,false,"[0:3]={001,NULL,01,10}"},
        {"SELECT array_cat(ARRAY[B'01'],ARRAY[B'001'::varbit]) AS v","",1561,-1,false,"{01,001}"},
        {"SELECT array_cat(ARRAY[B'001'::varbit],ARRAY[B'01']) AS v","",1563,-1,false,"{001,01}"},
        {"SELECT array_append(ARRAY[B'01'],B'001'::varbit) AS v","",1561,-1,false,"{01,001}"},
        {"SELECT array_append(ARRAY[B'001'::varbit],B'01') AS v","",1563,-1,false,"{001,01}"},
        {"SELECT array_prepend(B'01',ARRAY[B'001'::varbit]) AS v","",1561,-1,false,"{01,001}"},
        {"SELECT array_prepend(B'001'::varbit,ARRAY[B'01']) AS v","",1563,-1,false,"{001,01}"},
        {"SELECT ARRAY[B'01'] || ARRAY[TRUE] AS v","42883",0,-1,false,""},
        {"SELECT ARRAY[TRUE] || ARRAY[B'01'::varbit] AS v","42883",0,-1,false,""},
        {"SELECT ARRAY[B'01',TRUE] AS v","42804",0,-1,false,""},
        {"SELECT ARRAY[B'01'::varbit,1] AS v","42804",0,-1,false,""},
        {"SELECT ARRAY[B'101',NULL,B'1']::varbit[] AS v","",1563,-1,false,"{101,NULL,1}"},
        {"SELECT ARRAY[B'101'::varbit,NULL,B'1'::varbit]::bit[] AS v","",1561,1,false,"{1,NULL,1}"},
        {"SELECT ARRAY[B'101'::varbit,NULL,B'1'::varbit]::bit(4)[] AS v","",1561,4,false,"{1010,NULL,1000}"},
        {"SELECT ARRAY[B'101',NULL,B'1']::varbit(2)[] AS v","",1563,2,false,"{10,NULL,1}"},
        {"SELECT ARRAY[B'101',NULL,B'1']::pg_catalog.varbit[] AS v","",1563,-1,false,"{101,NULL,1}"},
        {"SELECT ARRAY[B'101'::varbit,NULL,B'1'::varbit]::\"pg_catalog\".\"bit\"[] AS v","",1561,-1,false,"{101,NULL,1}"},
        {"SELECT ARRAY[B'101',NULL]::\"pg_catalog\".\"varbit\"(2)[] AS v","",1563,2,false,"{10,NULL}"},
        {"SELECT ARRAY[ARRAY[B'101'::varbit,NULL],ARRAY[B'1'::varbit,B'']]::bit(4)[] AS v","",1561,4,false,"{{1010,NULL},{1000,0000}}"},
        {"SELECT ARRAY[ARRAY[B'101',NULL],ARRAY[B'1',B'']]::varbit(2)[] AS v","",1563,2,false,"{{10,NULL},{1,\"\"}}"},
        {"SELECT ARRAY[1,-1,NULL,2147483647]::bit(8)[] AS v","",1561,8,false,"{00000001,11111111,NULL,11111111}"},
        {"SELECT ARRAY[1::bigint,-1::bigint,NULL,9223372036854775807::bigint]::bit(65)[] AS v","",1561,65,false,"{00000000000000000000000000000000000000000000000000000000000000001,11111111111111111111111111111111111111111111111111111111111111111,NULL,00111111111111111111111111111111111111111111111111111111111111111}"},
        {"SELECT ARRAY[1,-1,NULL]::pg_catalog.bit[] AS v","",1561,-1,false,"{1,1,NULL}"},
        {"SELECT ARRAY[1::bigint,-1::bigint,NULL]::\"pg_catalog\".\"bit\"(8)[] AS v","",1561,8,false,"{00000001,11111111,NULL}"},
        {"SELECT ARRAY[B'101',NULL,B'1']::integer[] AS v","",1007,-1,false,"{5,NULL,1}"},
        {"SELECT ARRAY[B'101',NULL,B'1']::bigint[] AS v","",1016,-1,false,"{5,NULL,1}"},
        {"SELECT ARRAY[B'101',NULL]::\"pg_catalog\".\"int4\"[] AS v","",1007,-1,false,"{5,NULL}"},
        {"SELECT ARRAY[ARRAY[1,NULL],ARRAY[-1,2]]::bit(8)[] AS v","",1561,8,false,"{{00000001,NULL},{11111111,00000010}}"},
        {"SELECT ARRAY[ARRAY[B'101',NULL],ARRAY[B'1',B'']]::bigint[] AS v","",1016,-1,false,"{{5,NULL},{1,0}}"},
        {"SELECT ARRAY[NULL::bit,NULL::varbit]::varbit[] AS v","",1563,-1,false,"{NULL,NULL}"},
        {"SELECT ARRAY[NULL::varbit,NULL::bit]::bit(4)[] AS v","",1561,4,false,"{NULL,NULL}"},
        {"SELECT ARRAY[NULL::integer]::bit(8)[] AS v","",1561,8,false,"{NULL}"},
        {"SELECT ARRAY[NULL::bit]::bigint[] AS v","",1016,-1,false,"{NULL}"},
        {"SELECT ARRAY[NULL::smallint]::bit(8)[] AS v","42846",0,-1,false,""},
        {"SELECT ARRAY[NULL::integer]::varbit[] AS v","42846",0,-1,false,""},
        {"SELECT ARRAY[NULL::bigint]::varbit[] AS v","42846",0,-1,false,""},
        {"SELECT ARRAY[NULL::varbit]::integer[] AS v","42846",0,-1,false,""},
        {"SELECT ARRAY[NULL::varbit]::bigint[] AS v","42846",0,-1,false,""},
        {"SELECT ARRAY[NULL::bit]::smallint[] AS v","42846",0,-1,false,""},
        {"SELECT ARRAY[NULL::bit]::numeric[] AS v","42846",0,-1,false,""},
        {"SELECT ARRAY[NULL::numeric]::bit[] AS v","42846",0,-1,false,""},
        {"SELECT ARRAY[NULL::boolean]::bit[] AS v","42846",0,-1,false,""},
        {"SELECT ARRAY[NULL::bit]::boolean[] AS v","42846",0,-1,false,""},
        {"SELECT ARRAY[NULL::bytea]::bit[] AS v","42846",0,-1,false,""},
        {"SELECT ARRAY[NULL::bit]::bytea[] AS v","42846",0,-1,false,""},
        {"SELECT ARRAY[1::smallint]::bit(8)[] AS v","42846",0,-1,false,""},
        {"SELECT ARRAY[1]::varbit[] AS v","42846",0,-1,false,""},
        {"SELECT ARRAY[B'101'::varbit]::integer[] AS v","42846",0,-1,false,""},
        {"SELECT ARRAY[B'101']::smallint[] AS v","42846",0,-1,false,""},
        {"SELECT ARRAY[B'111111111111111111111111111111111']::integer[] AS v","22003",0,-1,false,""},
        {"SELECT ARRAY[B'11111111111111111111111111111111111111111111111111111111111111111']::bigint[] AS v","22003",0,-1,false,""},
        {"SELECT ARRAY['101'::text,NULL,'1'::varchar]::bit(4)[] AS v","",1561,4,false,"{1010,NULL,1000}"},
        {"SELECT ARRAY[B'101',NULL]::text[] AS v","",1009,-1,false,"{101,NULL}"},
        {"SELECT ARRAY[B'101',NULL]::varchar[] AS v","",1015,-1,false,"{101,NULL}"},
        {"SELECT ARRAY[1,NULL]::bigint[] AS v","",1016,-1,false,"{1,NULL}"},
        {"SELECT ARRAY[1.5,NULL]::integer[] AS v","",1007,-1,false,"{2,NULL}"},
    };
    for(const auto& item:cases) {
        try {
            const auto query=g_engine.prepareBoundQuery(database,item.sql);
            const auto* select=dynamic_cast<const SelectStmt*>(query.ast.get());
            evaluator.bindScalarFunctions(select->selectList.at(0).expr.get());
            const auto value=evaluator.eval(select->selectList.at(0).expr.get(),RowContext{});
            check(item.state.empty(),item.sql+" expected SQLSTATE "+item.state);
            if(item.state.empty()) {
                check(query.output.size()==1 && query.output[0].typeOid==item.oid && query.output[0].typeMod==item.modifier,item.sql+" declared descriptor");
                check(value.isNull==item.isNull,item.sql+" SQL NULL flag");
                if(!item.isNull)check(value.value==item.value,item.sql+" value="+value.value+" expected="+item.value);
            }
        } catch(const DbError& error){check(!item.state.empty() && error.sqlState()==item.state,item.sql+" SQLSTATE="+error.sqlState());}
          catch(const std::exception& error){check(false,item.sql+" exception="+error.what());}
    }
    struct Edge {std::string from,to;bool allowed;};
    const Edge edges[]={
        {"bit","varbit",true},{"varbit","bit",true},
        {"integer","bit",true},{"bigint","bit",true},
        {"bit","integer",true},{"bit","bigint",true},
        {"unknown","bit",true},{"unknown","varbit",true},
        {"smallint","bit",false},{"integer","varbit",false},
        {"bigint","varbit",false},{"varbit","integer",false},
        {"varbit","bigint",false},{"bit","smallint",false},
        {"bit","numeric",false},{"numeric","bit",false},
        {"boolean","bit",false},{"bit","boolean",false},
        {"bytea","bit",false},{"bit","bytea",false},
        {"text","bit",true},{"bit","text",true},
        {"pg_catalog.bit","pg_catalog.varbit",true},
        {"\"pg_catalog\".\"varbit\"","\"pg_catalog\".\"bit\"",true},
        {"pg_catalog.int4","pg_catalog.bit",true},
        {"pg_catalog.bit","\"pg_catalog\".\"int8\"",true},
        {"custom.bit","varbit",false},{"bit","custom.varbit",false},
        {"\"PG_CATALOG\".\"bit\"","varbit",false},
        {"bit","\"pg_catalog\".\"VARBIT\"",false},
    };
    for(const auto& edge:edges) {
        try {array_detail::checkExplicitElementCast(edge.from,edge.to);check(edge.allowed,"cast edge "+edge.from+" -> "+edge.to);}
        catch(const DbError& error){check(!edge.allowed && error.sqlState()=="42846","cast edge rejection "+edge.from+" -> "+edge.to+" "+error.sqlState());}
    }
    std::cout<<"BIT_ARRAY_CAST_COMPLETE CHECKED="<<checked<<" FAILURES="<<failed<<'\n';
    return failed?1:0;
}
