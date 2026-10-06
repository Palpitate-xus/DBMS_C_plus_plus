#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/prepared_query_execution.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    const std::string name="simple_case_equality",db=testDbPath(name);
    assert(g_engine.createDatabase(db)==DBStatus::OK);
    size_t failures=0;
    auto check=[&](const std::string& label,const std::string& actual,const std::string& expected) {
        if(actual!=expected) {++failures; std::cerr<<"SIMPLE_CASE_FAILURE "<<label<<" actual="<<actual<<" expected="<<expected<<std::endl;}
    };
    for(const auto& expression:std::vector<std::string>{
        "CASE 1 WHEN true THEN 1 ELSE 2 END",
        "CASE '1' WHEN 1 THEN 1 ELSE 2 END",
        "CASE 1 WHEN CAST('1' AS TEXT) THEN 1 ELSE 2 END",
        "CASE DATE '2026-10-06' WHEN INTERVAL '1 day' THEN 1 ELSE 2 END",
        "CASE CAST(NULL AS INT) WHEN true THEN 1 ELSE 2 END",
        "CASE 1 WHEN true THEN CAST('bad' AS INT) ELSE 2 END",
        "CASE 1 WHEN true THEN 1 ELSE CAST('bad' AS INT) END",
        "CASE 1 WHEN 1 THEN 1 WHEN true THEN 2 ELSE 3 END",
        "CASE true WHEN CAST('true' AS TEXT) THEN 1 ELSE 2 END"}) {
        std::string state;
        try {(void)g_engine.prepareBoundQuery(db,"SELECT "+expression+" WHERE false");}
        catch(const DbError& error) {state=error.sqlState();}
        check(expression,state,"42883");
    }
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
        {"CASE 1 WHEN '1' THEN 1 ELSE 2 END","1"},
        {"CASE '1' WHEN '1' THEN 1 ELSE 2 END","1"},
        {"CASE DATE '2026-10-06' WHEN '2026-10-06' THEN 1 ELSE 2 END","1"},
        {"CASE DATE '2026-10-06' WHEN TIMESTAMP '2026-10-06 00:00:00' THEN 1 ELSE 2 END","1"},
        {"CASE 1 WHEN CAST(1 AS BIGINT) THEN 1 ELSE 2 END","1"},
        {"CASE 16777217 WHEN CAST(16777216 AS REAL) THEN 1 ELSE 2 END","2"},
        {"CASE CAST(16777217 AS NUMERIC) WHEN CAST(16777216 AS REAL) THEN 1 ELSE 2 END","2"},
        {"CASE CAST(9007199254740993 AS BIGINT) WHEN CAST(9007199254740992 AS DOUBLE PRECISION) THEN 1 ELSE 2 END","1"},
        {"CASE CAST(1e20 AS REAL) WHEN CAST(1e20 AS DOUBLE PRECISION) THEN 1 ELSE 2 END","2"},
        {"CASE CAST('a ' AS CHAR(2)) WHEN CAST('a' AS TEXT) THEN 1 ELSE 2 END","1"},
        {"CASE CAST('a ' AS CHAR(2)) WHEN CAST('a ' AS TEXT) THEN 1 ELSE 2 END","2"},
        {"CASE CAST('ab' AS TEXT) WHEN CAST('ab' AS VARCHAR) THEN 1 ELSE 2 END","1"},
        {"CASE CAST('ab' AS CHAR(2)) WHEN CAST('ac' AS CHAR(2)) THEN 1 ELSE 2 END","2"},
        {"CASE CAST('ab' AS CHAR(2)) WHEN CAST('ac' AS VARCHAR) THEN 1 ELSE 2 END","2"},
        {"CASE CAST('1.40129846e-45' AS REAL) WHEN CAST('1.40129846e-45' AS REAL) THEN 1 ELSE 2 END","1"},
        {"CASE CAST(NULL AS BIGINT) WHEN CAST(NULL AS REAL) THEN 1 ELSE 2 END","2"},
        {"CASE CAST('NaN' AS REAL) WHEN CAST('NaN' AS DOUBLE PRECISION) THEN 1 ELSE 2 END","1"}}) {
        try {
            auto prepared=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,"SELECT "+item.first));
            auto* select=dynamic_cast<SelectStmt*>(prepared->ast.get()); assert(select);
            const auto* conditional=dynamic_cast<const CaseExpr*>(select->selectList.front().expr.get()); assert(conditional);
            assert(conditional->simpleComparisonTypes.size()==conditional->whenClauses.size());
            if(item.first.find("16777217")!=std::string::npos || item.first.find("NULL AS BIGINT")!=std::string::npos)
                assert(conditional->simpleComparisonTypes.front()==std::make_pair(std::string("double precision"),std::string("real")));
            PreparedQueryExecution execution(prepared,&g_engine,db);
            execution.prepareExpression(select->selectList.front().expr.get());
            const auto value=execution.evaluate(select->selectList.front().expr.get(),execution.context());
            check(item.first,value.isNull?"SQLNULL":value.value,item.second);
            check(item.first+"-type",value.typeName,"integer");
        } catch(const DbError& error) {check(item.first,error.sqlState(),"success");}
    }
    assert(g_engine.createSequence(db,"case_switch_sequence",1,1)==DBStatus::OK);
    auto prepared=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,
        "SELECT CASE CAST(nextval('case_switch_sequence') AS INTEGER) WHEN CAST(0 AS NUMERIC) THEN 0 WHEN CAST(1 AS BIGINT) THEN 1 ELSE 2 END"));
    std::string state;
    try {(void)g_engine.currval(db,"case_switch_sequence");} catch(const DbError& error) {state=error.sqlState();}
    check("pure preparation never executes switch",state,"55000");
    auto* select=dynamic_cast<SelectStmt*>(prepared->ast.get()); assert(select);
    PreparedQueryExecution execution(prepared,&g_engine,db);
    execution.prepareExpression(select->selectList.front().expr.get());
    const auto value=execution.evaluate(select->selectList.front().expr.get(),execution.context());
    check("switch evaluated once",value.value,"1");
    check("switch exact effects",std::to_string(g_engine.nextval(db,"case_switch_sequence")),"2");
    assert(g_engine.createSequence(db,"case_null_rhs_sequence",1,1)==DBStatus::OK);
    TableSchema nullSchema; nullSchema.len=1;
    nullSchema.cols[0].dataName="v"; nullSchema.cols[0].dataType="integer"; nullSchema.cols[0].dsize=4;
    assert(g_engine.createTable(db,"case_null_source",nullSchema)==DBStatus::OK);
    auto nullPrepared=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,
        "SELECT CASE v WHEN CAST(nextval('case_null_rhs_sequence') AS INTEGER) THEN 1 WHEN CAST(nextval('case_null_rhs_sequence') AS INTEGER) THEN 2 ELSE 3 END FROM case_null_source"));
    auto* nullSelect=dynamic_cast<SelectStmt*>(nullPrepared->ast.get()); assert(nullSelect);
    PreparedQueryExecution nullExecution(nullPrepared,&g_engine,db);
    nullExecution.prepareExpression(nullSelect->selectList.front().expr.get());
    auto nullRow=nullExecution.context();
    const auto* nullCase=dynamic_cast<const CaseExpr*>(nullSelect->selectList.front().expr.get()); assert(nullCase);
    const auto* nullColumn=dynamic_cast<const ColumnRefExpr*>(nullCase->switchExpr.get()); assert(nullColumn && nullColumn->binding);
    nullExecution.setSourceRow(nullRow,nullColumn->binding->sourceOrdinal,{{"integer","",true}});
    const auto nullValue=nullExecution.evaluate(nullSelect->selectList.front().expr.get(),nullRow);
    check("NULL switch retains demanded WHEN effects",nullValue.value,"3");
    check("each demanded WHEN executes once",std::to_string(g_engine.currval(db,"case_null_rhs_sequence")),"2");
    assert(g_engine.dropDatabase(db)==DBStatus::OK); cleanupTestDb(name);
    std::cerr<<"SIMPLE_CASE failures="<<failures<<std::endl;
    assert(failures==0);
    std::cout<<"[SIMPLE CASE EQUALITY BINDING] static operators, declared casts, NULL and switch-once passed\n";
}
