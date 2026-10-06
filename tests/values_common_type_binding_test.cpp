#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/prepared_query_execution.h"
#include "expression/expr_helper.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    const std::string name="values_common_type_binding",db=testDbPath(name);
    assert(g_engine.createDatabase(db)==DBStatus::OK);size_t failures=0;
    const auto check=[&](const std::string& label,const std::string& actual,const std::string& expected){
        if(actual!=expected){++failures;std::cerr<<"VALUES_COMMON_FAILURE "<<label<<" actual="<<actual<<" expected="<<expected<<std::endl;}
    };
    struct Case {std::string sql,type;std::vector<std::string> values;};
    for(const auto& item:std::vector<Case>{
        {"VALUES(CASE 1 WHEN 1 THEN 1 ELSE 2 END),(CAST(2147483648 AS BIGINT))","bigint",{"1","2147483648"}},
        {"VALUES(CAST(NULL AS INT)),(CAST(1 AS BIGINT))","bigint",{"SQLNULL","1"}},
        {"VALUES('1'),(CASE 1 WHEN 1 THEN 2 ELSE 3 END)","integer",{"1","2"}},
        {"VALUES(CASE 1 WHEN 1 THEN 1 ELSE 2 END),(CAST(1.5 AS NUMERIC))","numeric",{"1","1.5"}},
        {"VALUES(CASE 1 WHEN 1 THEN CAST(1 AS REAL) ELSE CAST(2 AS REAL) END),(3)","real",{"1","3"}},
        {"VALUES(NULL),('NULL')","text",{"SQLNULL","NULL"}}}) {
        auto prepared=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,item.sql));
        check(item.sql+" descriptor",prepared->output.at(0).type,item.type);
        auto* select=dynamic_cast<SelectStmt*>(prepared->ast.get());assert(select);
        PreparedQueryExecution execution(prepared,&g_engine,db);
        for(auto& row:select->valuesRows)for(auto& expr:row)execution.prepareExpression(expr.get());
        for(size_t row=0;row<select->valuesRows.size();++row){
            const auto value=execution.evaluate(select->valuesRows[row][0].get(),execution.context());
            check(item.sql+" rowtype"+std::to_string(row),ExprHelper::canonicalResultTypeName(value.typeName),item.type);
            check(item.sql+" value"+std::to_string(row),value.isNull?"SQLNULL":value.value,item.values.at(row));
        }
    }
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
        {"VALUES(CASE 1 WHEN 1 THEN 1 ELSE 2 END),(CAST('2' AS TEXT))","42804"},
        {"VALUES('bad'),(CASE 1 WHEN 1 THEN 1 ELSE 2 END)","22P02"}}){
        std::string state;try{(void)g_engine.prepareBoundQuery(db,item.first);}catch(const DbError& error){state=error.sqlState();}
        check(item.first+" state",state,item.second);
    }
    assert(g_engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb(name);
    std::cerr<<"VALUES_COMMON failures="<<failures<<std::endl;assert(!failures);
}
