#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/prepared_query_execution.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    StorageEngine engine;
    const std::string name="unary_builtin_engine_binding",db=testDbPath(name);
    assert(engine.createDatabase(db)==DBStatus::OK);
    TableSchema schema;schema.len=2;
    schema.cols[0].dataName="m";schema.cols[0].dataType="money";schema.cols[0].dsize=8;
    schema.cols[1].dataName="i";schema.cols[1].dataType="integer";schema.cols[1].dsize=4;
    assert(engine.createTable(db,"unary_rows",schema)==DBStatus::OK);
    assert(engine.createSequence(db,"unary_bind_effects",1,1)==DBStatus::OK);
    assert(engine.createUDF(db,"unary_money_writer",std::vector<std::string>{},
        std::vector<std::string>{},"BEGIN PERFORM nextval('unary_bind_effects'); RETURN CAST(1 AS MONEY); END;",
        'v',"plpgsql","money")==DBStatus::OK);
    size_t failures=0;
    const auto check=[&](const std::string& label,const std::string& actual,const std::string& expected) {
        if(actual!=expected){++failures;std::cerr<<"UNARY_ENGINE_FAILURE "<<label<<" actual="<<actual<<" expected="<<expected<<std::endl;}
    };
    for(const auto& sql:std::vector<std::string>{
        "SELECT -m FROM unary_rows WHERE false",
        "WITH r AS (SELECT m FROM unary_rows) SELECT +m FROM r WHERE false",
        "SELECT CASE true WHEN true THEN 1 ELSE -unary_money_writer() END",
        "SELECT -unary_money_writer() WHERE false",
        "WITH w AS (INSERT INTO unary_rows VALUES(CAST(1 AS MONEY),CAST(nextval('unary_bind_effects') AS INT)) RETURNING m) SELECT -m FROM w"}) {
        std::string state;
        try{(void)engine.prepareBoundQuery(db,sql);}catch(const DbError& error){state=error.sqlState();}
        check(sql,state,"42883");
        state.clear();
        try{(void)engine.currval(db,"unary_bind_effects");}catch(const DbError& error){state=error.sqlState();}
        check(sql+" has no sequence effects",state,"55000");
    }
    auto prepared=std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,"SELECT -i FROM unary_rows"));
    auto* select=dynamic_cast<SelectStmt*>(prepared->ast.get());assert(select);
    auto* unary=dynamic_cast<UnaryOpExpr*>(select->selectList.front().expr.get());assert(unary);
    auto* column=dynamic_cast<ColumnRefExpr*>(unary->operand.get());assert(column&&column->binding);
    PreparedQueryExecution execution(prepared,&engine,db);execution.prepareExpression(unary);
    auto row=execution.context();
    execution.setSourceRow(row,column->binding->sourceOrdinal,{{"money","",true},{"integer","3"}});
    auto value=execution.evaluate(unary,row);
    check("physical nullable source value",value.value,"-3");check("physical source type",value.typeName,"integer");
    execution.setSourceRow(row,column->binding->sourceOrdinal,{{"money","",true},{"integer","",true}});
    value=execution.evaluate(unary,row);assert(value.isNull&&value.typeName=="integer");
    assert(engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb(name);
    assert(failures==0);
    std::cout<<"[UNARY ENGINE BINDING] actual owner, physical/logical metadata and no effects passed\n";
}
