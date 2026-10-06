#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/prepared_query_execution.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    const std::string name="simple_case_constant_demand",db=testDbPath(name);
    assert(g_engine.createDatabase(db)==DBStatus::OK);
    size_t failures=0,index=0;
    const auto check=[&](const std::string& label,const std::string& actual,const std::string& expected) {
        if(actual!=expected) {++failures;std::cerr<<"CASE_DEMAND_FAILURE "<<label<<" actual="<<actual<<" expected="<<expected<<std::endl;}
    };
    struct Case {std::string expression,value,state,calls;};
    for(const auto& item:std::vector<Case>{
        {"CASE CAST(NULL AS INT) WHEN CAST(nextval('SEQ') AS INT) THEN 1 WHEN CAST(nextval('SEQ') AS INT) THEN 2 ELSE 3 END","3","","55000"},
        {"CASE CAST(NULL AS INT) WHEN CAST(nextval('SEQ') AS INT)+1 THEN 1 ELSE 3 END","3","","55000"},
        {"CASE CAST(nextval('SEQ') AS INT) WHEN NULL THEN 1 ELSE 3 END","3","","55000"},
        {"CASE CAST(nextval('SEQ') AS INT) WHEN NULL THEN CAST(nextval('SEQ') AS INT) ELSE 3 END","3","","55000"},
        {"CASE CAST(nextval('SEQ') AS INT) WHEN NULL THEN 1 WHEN 1 THEN 2 ELSE 3 END","2","","1"},
        {"CASE CAST(NULL AS INT) WHEN 1 THEN 1/0 ELSE 3 END","3","","55000"},
        {"CASE CAST(NULL AS INT) WHEN 1 THEN CAST(2147483648 AS INT) ELSE 3 END","3","","55000"},
        {"CASE CAST(NULL AS INT) WHEN 1/0 THEN 1 ELSE 3 END","","22012","55000"},
        {"CASE CAST(NULL AS INT) WHEN CAST(2147483648 AS INT) THEN 1 ELSE 3 END","","22003","55000"},
        {"CASE CAST(NULL AS INT) WHEN 1 THEN 1 ELSE 1/0 END","","22012","55000"},
        {"CASE 0 WHEN 0 THEN 1 ELSE 1/0 END","1","","55000"},
        {"CASE 0 WHEN 1 THEN 1/0 WHEN 0 THEN 2 ELSE 3 END","2","","55000"},
        {"CASE WHEN false THEN 1/0 ELSE 3 END","3","","55000"},
        {"CASE WHEN true THEN 3 ELSE 1/0 END","3","","55000"},
        {"CASE WHEN false AND nextval('SEQ')=1 THEN 1 ELSE 3 END","3","","55000"}}) {
        const auto sequence="case_demand_"+std::to_string(index++);
        assert(g_engine.createSequence(db,sequence,1,1)==DBStatus::OK);
        auto sql=item.expression;
        for(size_t at=sql.find("SEQ");at!=std::string::npos;at=sql.find("SEQ",at+sequence.size()))sql.replace(at,3,sequence);
        std::string value,state;
        try {
            auto prepared=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,"SELECT "+sql));
            auto* select=dynamic_cast<SelectStmt*>(prepared->ast.get());assert(select);
            PreparedQueryExecution execution(prepared,&g_engine,db);
            execution.prepareExpression(select->selectList.front().expr.get());
            // Planning constants is pure: no sequence can be reached here.
            std::string prepareState;
            try {(void)g_engine.currval(db,sequence);}catch(const DbError& error){prepareState=error.sqlState();}
            check(sql+" preparation",prepareState,"55000");
            value=execution.evaluate(select->selectList.front().expr.get(),execution.context()).value;
        }catch(const DbError& error){state=error.sqlState();}
        check(sql+" value",value,item.value);check(sql+" state",state,item.state);
        std::string calls;
        try {calls=std::to_string(g_engine.currval(db,sequence));}catch(const DbError& error){calls=error.sqlState();}
        check(sql+" effects",calls,item.calls);
    }
    assert(g_engine.createSequence(db,"runtime_null_demand",1,1)==DBStatus::OK);
    TableSchema schema;schema.len=1;schema.cols[0].dataName="v";schema.cols[0].dataType="integer";schema.cols[0].dsize=4;
    assert(g_engine.createTable(db,"runtime_null_source",schema)==DBStatus::OK);
    auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,
        "SELECT CASE v WHEN CAST(nextval('runtime_null_demand') AS INT) THEN 1 WHEN CAST(nextval('runtime_null_demand') AS INT) THEN 2 ELSE 3 END FROM runtime_null_source"));
    auto* select=dynamic_cast<SelectStmt*>(query->ast.get());assert(select);
    auto* conditional=dynamic_cast<CaseExpr*>(select->selectList.front().expr.get());assert(conditional);
    auto* column=dynamic_cast<ColumnRefExpr*>(conditional->switchExpr.get());assert(column&&column->binding);
    PreparedQueryExecution execution(query,&g_engine,db);execution.prepareExpression(conditional);
    auto row=execution.context();execution.setSourceRow(row,column->binding->sourceOrdinal,{{"integer","",true}});
    check("runtime NULL value",execution.evaluate(conditional,row).value,"3");
    check("runtime NULL WHEN effects",std::to_string(g_engine.currval(db,"runtime_null_demand")),"2");
    assert(g_engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb(name);
    std::cerr<<"CASE_DEMAND failures="<<failures<<std::endl;assert(failures==0);
    std::cout<<"[CASE CONSTANT DEMAND] strict NULL planning and runtime NULL effects passed\n";
}
