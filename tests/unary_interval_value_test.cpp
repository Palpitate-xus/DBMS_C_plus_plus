#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/prepared_query_execution.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    SQLParser parser;ExprEvaluator evaluator;
    size_t failures=0;
    const auto check=[&](const std::string& label,const std::string& actual,const std::string& expected){
        if(actual!=expected){++failures;std::cerr<<"UNARY_INTERVAL_FAILURE "<<label<<" actual="<<actual<<" expected="<<expected<<std::endl;}
    };
    struct Item{const char* expression;const char* value;const char* state;};
    for(const auto& item:std::vector<Item>{
        {"-INTERVAL '1 month 2 days 03:04:05.000001'","-1 mons -2 days -03:04:05.000001",""},
        {"-INTERVAL '-1 month 2 days -03:04:05.000001'","1 mon -2 days +03:04:05.000001",""},
        {"-INTERVAL '1.5 months'","-1 mons -15 days",""},
        {"-INTERVAL '1.25 seconds'","-00:00:01.25",""},
        {"-INTERVAL '-0.000001 seconds'","00:00:00.000001",""},
        {"-INTERVAL '0 days'","00:00:00",""},
        {"-INTERVAL '2147483647 months'","-178956970 years -7 mons",""},
        {"-INTERVAL '2147483647 days'","-2147483647 days",""},
        {"-INTERVAL '9223372036854775807 microseconds'","-2562047788:00:54.775807",""},
        {"-INTERVAL '-2147483647 months'","178956970 years 7 mons",""},
        {"-INTERVAL '-2147483647 days'","2147483647 days",""},
        {"-INTERVAL '-9223372036854775807 microseconds'","2562047788:00:54.775807",""},
        {"-INTERVAL '-2147483648 months'","","22008"},
        {"-INTERVAL '-2147483648 days'","","22008"},
        {"-INTERVAL '-9223372036854775808 microseconds'","","22008"}}) {
        auto parsed=parser.parse(std::string("SELECT ")+item.expression);assert(parsed.isValid());
        auto* select=dynamic_cast<SelectStmt*>(parsed.stmt.get());assert(select);
        std::string value,state;
        try{auto result=evaluator.eval(select->selectList.front().expr.get(),RowContext{});assert(result.typeName=="interval"&&!result.isNull);value=result.value;}
        catch(const DbError& error){state=error.sqlState();}
        check(std::string(item.expression)+" value",value,item.value);check(std::string(item.expression)+" state",state,item.state);
    }
    StorageEngine engine;const std::string name="unary_interval_value",db=testDbPath(name);
    assert(engine.createDatabase(db)==DBStatus::OK);
    TableSchema schema;schema.len=1;schema.cols[0].dataName="v";schema.cols[0].dataType="interval";schema.cols[0].dsize=128;
    assert(engine.createTable(db,"unary_interval_rows",schema)==DBStatus::OK);
    auto prepared=std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,"SELECT -v FROM unary_interval_rows"));
    auto* select=dynamic_cast<SelectStmt*>(prepared->ast.get());assert(select);
    auto* unary=dynamic_cast<UnaryOpExpr*>(select->selectList.front().expr.get());assert(unary);
    auto* column=dynamic_cast<ColumnRefExpr*>(unary->operand.get());assert(column&&column->binding);
    PreparedQueryExecution execution(prepared,&engine,db);execution.prepareExpression(unary);
    auto row=execution.context();execution.setSourceRow(row,column->binding->sourceOrdinal,{{"interval","1 mon -2 days 03:04:05"}});
    check("bound source mixed signs",execution.evaluate(unary,row).value,"-1 mons +2 days -03:04:05");
    execution.setSourceRow(row,column->binding->sourceOrdinal,{{"interval","",true}});
    const auto nullable=execution.evaluate(unary,row);assert(nullable.typeName=="interval"&&nullable.isNull);
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
        {"SELECT -TIME '01:02:03'","-01:02:03"},
        {"SELECT -v","-1 mons -2 days -03:04:05"}}) {
        auto query=std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,item.first,
            {{"unary:interval","v","interval",{},true,"1 mon 2 days 03:04:05",1}}));
        auto* statement=dynamic_cast<SelectStmt*>(query->ast.get());assert(statement);
        PreparedQueryExecution runtime(query,&engine,db);runtime.prepareExpression(statement->selectList.front().expr.get());
        const auto value=runtime.evaluate(statement->selectList.front().expr.get(),runtime.context());
        check(item.first+" prepared value",value.value,item.second);assert(value.typeName=="interval"&&!value.isNull);
    }
    assert(engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb(name);
    assert(failures==0);
    std::cout<<"[UNARY INTERVAL VALUE] fieldwise negation, boundaries and typed NULL passed\n";
}
