#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/expr_helper.h"
#include "expression/common_type.h"
#include "expression/prepared_query_execution.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    const std::string name="case_common_type",db=testDbPath(name);
    cleanupTestDb(name); assert(g_engine.createDatabase(db)==DBStatus::OK);
    for (const auto& item : std::vector<std::pair<std::string,std::string>>{
        {"CASE WHEN true THEN 'abc' ELSE 'def' END","text"},
        {"CASE WHEN false THEN NULL END","text"},
        {"CASE WHEN true THEN NULL ELSE CAST(1 AS BIGINT) END","bigint"},
        {"CASE WHEN true THEN 2 ELSE CAST(1 AS REAL) END","real"},
        {"CASE WHEN true THEN CAST(2 AS NUMERIC) ELSE CAST(1 AS BIGINT) END","numeric"},
        {"CASE WHEN true THEN CAST(2 AS REAL) ELSE CAST(1 AS DOUBLE PRECISION) END","double precision"},
        {"CASE WHEN true THEN CAST('ab' AS VARCHAR) ELSE CAST('cd' AS TEXT) END","text"},
        {"CASE WHEN true THEN CAST('ab' AS TEXT) ELSE CAST('cd' AS VARCHAR) END","varchar"},
        {"CASE WHEN true THEN CAST('abcd' AS VARCHAR) ELSE CAST('xy' AS CHAR(2)) END","bpchar"},
        {"CASE WHEN true THEN CAST(B'0101' AS BIT VARYING) ELSE CAST(B'11' AS BIT(2)) END","bit"},
        {"CASE WHEN true THEN DATE '2026-10-06' ELSE TIMESTAMP '2026-10-07 00:00:00' END","timestamp"},
        {"CASE WHEN true THEN '1 us' ELSE INTERVAL '2 days' END","interval"},
        {"CASE WHEN false THEN 1/0 ELSE 2 END","integer"},
        {"CASE WHEN false THEN CAST(2147483648 AS INTEGER) ELSE 2 END","integer"}}) {
        auto prepared=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,"SELECT "+item.first));
        const auto* select=dynamic_cast<const SelectStmt*>(prepared->ast.get()); assert(select);
        const auto type=common_type_detail::canonical(prepared->output.front().type);
        if(type!=item.second) std::cerr<<"CASE_TYPE "<<item.first<<" actual "<<type<<" expected "<<item.second<<'\n';
        assert(type==item.second);
        const auto inferred=common_type_detail::canonical(ExprHelper::inferParsedResultType(select->selectList.front().expr.get(),{},db,&g_engine));
        assert(inferred==item.second);
        PreparedQueryExecution execution(prepared,&g_engine,db);
        execution.prepareExpression(select->selectList.front().expr.get());
        const auto value=execution.evaluate(select->selectList.front().expr.get(),execution.context());
        assert(common_type_detail::canonical(value.typeName)==item.second);
        if(item.first.find("VARCHAR) ELSE CAST('xy'")!=std::string::npos) assert(value.value=="abcd");
        if(item.second=="bit") assert(value.value=="0101");
        if(item.first.find("THEN NULL")!=std::string::npos || item.first=="CASE WHEN false THEN NULL END") assert(value.isNull);
    }
    for (const auto& item : std::vector<std::pair<std::string,std::string>>{
        {"CASE WHEN false THEN 'bad' ELSE 1 END","22P02"},
        {"CASE WHEN false THEN '1 fortnight' ELSE INTERVAL '1 day' END","22007"},
        {"CASE WHEN false THEN '2147483648 months' ELSE INTERVAL '1 day' END","22015"},
        {"CASE WHEN false THEN CAST('1 day' AS TEXT) ELSE INTERVAL '1 day' END","42804"},
        {"CASE WHEN false THEN 1 ELSE INTERVAL '1 day' END","42804"},
        {"CASE WHEN 1 THEN 1 ELSE 2 END","42804"},
        {"CASE WHEN 'bad' THEN 1 ELSE 2 END","22P02"},
        {"CASE 1 WHEN 'bad' THEN 1 ELSE 2 END","22P02"}}) {
        std::string state;
        try{(void)g_engine.prepareBoundQuery(db,"SELECT "+item.first+" WHERE false");}
        catch(const DbError& error){state=error.sqlState();}
        if(state!=item.second) std::cerr<<"CASE_ERROR "<<item.first<<" actual "<<state<<" expected "<<item.second<<'\n';
        assert(state==item.second);
    }
    assert(g_engine.dropDatabase(db)==DBStatus::OK); cleanupTestDb(name);
    std::cout<<"[CASE COMMON TYPE] static descriptor/input, lazy prepared values and coercion-width controls passed\n";
}
