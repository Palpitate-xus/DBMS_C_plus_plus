#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "expression/prepared_query_execution.h"
#include "parser/parser.h"
#include "Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    cleanupTestDb("scientific_numeric_column");
    const auto database=testDbPath("scientific_numeric_column");
    assert(g_engine.createDatabase(database,"utf8")==DBStatus::OK);
    Session session;session.permission=1;session.username="testuser";session.currentDB=database;
    DdlExecutor ddl;assert(!ddl.executeSql("CREATE TABLE scientific_inputs(a NUMERIC,rate NUMERIC)",session));
    size_t controls=0,failures=0;
    const auto check=[&](const std::string& label,bool good){
        ++controls;if(!good)++failures;
        std::cout<<"SCIENTIFIC_COLUMN_NATIVE "<<label<<" match="<<good<<std::endl;
    };
    for(const auto& literal:std::vector<std::string>{"1e-40","1E+40",".5e-40","5.e-41"}) {
        check("single exponent token "+literal,SQLParser::tokenize(literal)==std::vector<std::string>{literal});
        for(const auto& column:std::vector<std::string>{"a","rate"}) {
            const auto tokens=SQLParser::tokenize(column+"+"+literal);
            check("column plus token "+column+"+"+literal,tokens==std::vector<std::string>{column,"+",literal});
            const auto bound=g_engine.prepareBoundQuery(database,"SELECT "+column+"+"+literal+" FROM scientific_inputs");
            check("empty table metadata "+column+"+"+literal,bound.output.size()==1 && bound.output[0].type=="numeric");
        }
    }
    check("identifier ending e is not exponent",SQLParser::tokenize("rate-1")==std::vector<std::string>{"rate","-","1"});
    assert(g_engine.insert(database,"scientific_inputs",{{"a","2e-40"},{"rate","2e-40"}})==DBStatus::OK);
    const auto tiny=[](char digit){return "0."+std::string(39,'0')+digit;};
    for(const auto& control:std::vector<std::pair<std::string,std::string>>{
        {"a+1e-40",tiny('3')},{"a-1e-40",tiny('1')},
        {"a*1e+40","2."+std::string(40,'0')},{"a/1e-40","2."+std::string(40,'0')},
        {"rate-1e-40",tiny('1')}}) {
        StorageEngine::SelectExpr legacy;
        legacy.displayName="n";legacy.isScalar=true;legacy.funcName="arith";
        legacy.funcArgs=SQLParser::tokenize(control.first);
        check("legacy exact token execution "+control.first,
              g_engine.queryExpr(database,"scientific_inputs",{}, {legacy})==std::vector<std::string>{control.second+" "});
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,"SELECT "+control.first+" FROM scientific_inputs"));
        auto* select=dynamic_cast<SelectStmt*>(query->ast.get());assert(select);
        PreparedQueryExecution execution(query,&g_engine,database);
        execution.prepareExpression(select->selectList[0].expr.get());
        auto row=execution.context();
        assert(query->sourceRanges.size()==1);
        execution.setSourceRow(row,query->sourceRanges.front().ordinal,{ExprValue("numeric","2e-40"),ExprValue("numeric","2e-40")});
        const auto value=execution.evaluate(select->selectList[0].expr.get(),row);
        check("prepared exact row "+control.first,!value.isNull && value.typeName=="numeric" && value.value==control.second);
        execution.setSourceRow(row,query->sourceRanges.front().ordinal,{ExprValue("numeric","",true),ExprValue("numeric","",true)});
        check("prepared NULL row "+control.first,execution.evaluate(select->selectList[0].expr.get(),row).isNull);
        execution.closeChildCursors();
    }
    assert(g_engine.dropDatabase(database)==DBStatus::OK);
    cleanupTestDb("scientific_numeric_column");finalCleanupTestData();
    assert(controls==36);
    std::cout<<"SCIENTIFIC_COLUMN_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<std::endl;
    assert(failures==0);
}
