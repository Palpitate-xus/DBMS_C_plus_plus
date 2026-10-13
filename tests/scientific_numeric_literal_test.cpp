#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/expr_helper.h"
#include "expression/prepared_query_execution.h"
#include "parser/parser.h"
#include "Session.h"
#include "test_utils.h"
#include "types/numeric.h"
#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="scientific_numeric_literal";
    cleanupTestDb(name);
    const auto database=testDbPath(name);
    assert(g_engine.createDatabase(database,"utf8")==DBStatus::OK);
    Session session; session.permission=1; session.username="testuser"; session.currentDB=database;
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE numeric_literals(id INT,n NUMERIC)",session));
    assert(g_engine.createSequence(database,"numeric_literal_calls",1,1)==DBStatus::OK);
    std::vector<std::string> failures;
    size_t controls=0;
    const auto check=[&](const std::string& label,bool good) {
        ++controls;
        std::cout<<"SCIENTIFIC_NUMERIC_NATIVE "<<label<<" match="<<good<<std::endl;
        if(!good)failures.push_back(label);
    };
    const std::vector<std::string> literals={
        "1e40","1E40","1e+40","1e-40","-1e40","+1e40",
        ".5e2","-.5E-2","5.e1","-5.E+1","1.25e2","1.250e-2",
        "0e0","0E+40","0e-40","1e500","1E-500","9.999e+3","9.999e-3"};
    for(const auto& literal:literals) {
        check("VALUES type "+literal,ExprHelper::inferValuesResultType(literal)=="numeric");
        SQLParser parser;
        auto parsed=parser.parseForBinding("SELECT "+literal);
        auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
        check("parser "+literal,select && select->selectList.size()==1);
        if(select && select->selectList.size()==1) {
            check("AST input type "+literal,ExprHelper::inferParsedInputType(select->selectList[0].expr.get(),{})=="numeric");
            check("AST result type "+literal,ExprHelper::inferParsedResultType(select->selectList[0].expr.get(),{})=="numeric");
        }
        for(const auto& sql:std::vector<std::string>{
            "SELECT "+literal,
            "INSERT INTO numeric_literals VALUES(1,"+literal+") RETURNING n",
            "INSERT INTO numeric_literals SELECT 1,"+literal+" WHERE false RETURNING n",
            "UPDATE numeric_literals SET n="+literal+" WHERE false RETURNING n"}) {
            try {
                const auto prepared=g_engine.prepareBoundQuery(database,sql);
                check("bound "+sql,prepared.output.size()==1 && prepared.output[0].type=="numeric");
            } catch(const DbError& error) {
                check("bound "+sql+" error="+error.sqlState(),false);
            }
        }
        try {
            auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,"SELECT "+literal));
            auto* statement=dynamic_cast<SelectStmt*>(query->ast.get());
            PreparedQueryExecution execution(query,&g_engine,database);
            execution.prepareExpression(statement->selectList[0].expr.get());
            const auto cell=execution.evaluate(statement->selectList[0].expr.get(),execution.context());
            check("prepared exact datum "+literal,!cell.isNull && cell.typeName=="numeric" && Numeric(cell.value)==Numeric(literal));
            execution.closeChildCursors();
        } catch(const DbError& error) {check("prepared exact datum "+literal+" error="+error.sqlState(),false);}
    }
    for(const auto& value:std::vector<std::string>{"1e","1e+","1e-","e40",".e2","1e2e3","1e2.3","1e2tail"}) {
        LiteralExpr literal;literal.value=value;
        check("not numeric token "+value,ExprHelper::inferParsedInputType(&literal,{})!="numeric");
    }
    for(const auto& value:std::vector<std::string>{"'1e40'","E'1e40'","'1e+'"})
        check("quoted UNKNOWN "+value,ExprHelper::inferValuesResultType(value)=="unknown");
    for(const auto& control:std::vector<std::pair<std::string,std::string>>{
        {"0","integer"},{"2147483647","integer"},{"2147483648","bigint"},
        {"-2147483648","integer"},{"9223372036854775807","bigint"},
        {"-9223372036854775808","bigint"},{"9223372036854775808","numeric"},
        {".5","numeric"},{"5.","numeric"},{"1.25","numeric"}})
        check("existing literal type "+control.first,ExprHelper::inferValuesResultType(control.first)==control.second);
    for(const auto& sql:std::vector<std::string>{
        "SELECT 1e40+nextval('numeric_literal_calls')",
        "INSERT INTO numeric_literals VALUES(nextval('numeric_literal_calls'),1e40) RETURNING n",
        "UPDATE numeric_literals SET n=1e40+nextval('numeric_literal_calls') RETURNING n",
        "UPDATE numeric_literals SET n=1e40/0 WHERE false RETURNING n"}) {
        try {
            const auto prepared=g_engine.prepareBoundQuery(database,sql);
            check("pure metadata "+sql,prepared.output.size()==1 && prepared.output[0].type=="numeric");
        } catch(const DbError& error) {check("pure metadata "+sql+" error="+error.sqlState(),false);}
    }
    const std::vector<std::pair<std::string,std::string>> errors={
        {"INSERT INTO numeric_literals VALUES(1,'1e40'::TEXT)","42804"},
        {"UPDATE numeric_literals SET n='1e40'::TEXT WHERE false","42804"},
        {"INSERT INTO numeric_literals SELECT 1,'1e40'::TEXT WHERE false","42804"},
        {"INSERT INTO numeric_literals VALUES(1,'1e+')","22P02"},
        {"UPDATE numeric_literals SET n='1e+' WHERE false","22P02"}};
    for(const auto& control:errors) {
        std::string actual;
        try{(void)g_engine.prepareBoundQuery(database,control.first);}
        catch(const DbError& error){actual=error.sqlState();}
        check("error phase "+control.first+" actual="+actual,actual==control.second);
    }
    check("binding never evaluates nextval",g_engine.nextval(database,"numeric_literal_calls")==1);
    check("binding never writes target rows",g_engine.query(database,"numeric_literals",{},{}).empty());
    assert(g_engine.dropDatabase(database)==DBStatus::OK);
    cleanupTestDb(name);
    finalCleanupTestData();
    assert(controls==203);
    std::cout<<"SCIENTIFIC_NUMERIC_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures.size()<<std::endl;
    assert(failures.empty());
}
