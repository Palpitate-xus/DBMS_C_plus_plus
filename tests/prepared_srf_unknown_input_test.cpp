#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="prepared_srf_unknown_input",db=testDbPath(name);
    cleanupTestDb(name); assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session;session.username="admin";session.permission=1;session.currentDB=db;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* value;~Restore(){setCurrentSession(value);}} restore{previous};
    DdlExecutor ddl;assert(!ddl.executeSql("CREATE SEQUENCE unknown_input_effects",session));
    for(const auto* sql:{"SELECT NULL","SELECT '{1,2}'"}) {
        SQLParser parser;auto parsed=parser.parseForBinding(sql);assert(parsed.isValid());
        auto* select=dynamic_cast<SelectStmt*>(parsed.stmt.get());assert(select);
        assert(ExprHelper::inferParsedInputType(select->selectList[0].expr.get(),{},db,&g_engine)=="unknown");
        assert(ExprHelper::inferParsedResultType(select->selectList[0].expr.get(),{},db,&g_engine)=="text");
    }
    size_t mismatches=0;
    for(const auto& control:std::vector<std::pair<std::string,std::string>>{
        {"SELECT unnest(NULL)","42725"},
        {"SELECT unnest('{1,2}') LIMIT 0","42725"},
        {"SELECT unnest(NULL) WHERE false","42725"},
        {"SELECT pg_catalog.unnest(NULL)","42725"},
        {"SELECT unnest(NULL::text)","42883"},
        {"SELECT unnest(COALESCE(NULL,NULL))","42883"},
        {"SELECT unnest(CASE WHEN true THEN NULL ELSE NULL END)","42883"},
        {"SELECT unnest(NULLIF(NULL,NULL))","42883"},
        {"SELECT unnest(missing_unknown_input_function(nextval('unknown_input_effects')))","42883"},
        {"SELECT unnest(NULL::int[])",""},
        {"SELECT unnest(ARRAY[1,2,NULL])",""},
        {"SELECT unnest((SELECT ARRAY[1,2]))",""}}) {
        std::string actual;
        try{(void)g_engine.prepareBoundQuery(db,control.first);}
        catch(const DbError& error){actual=error.sqlState();}
        std::cout<<"SRF UNKNOWN NATIVE "<<control.first<<" actual="<<actual<<" expected="<<control.second<<std::endl;
        mismatches+=actual!=control.second;
    }
    assert(g_engine.nextval(db,"unknown_input_effects")==1);
    assert(mismatches==0);
    auto contextual=g_engine.prepareBoundQuery(db,"SELECT NULLIF(NULL,NULL)");
    const auto* contextualSelect=dynamic_cast<const SelectStmt*>(contextual.ast.get());assert(contextualSelect);
    const auto* contextualCall=dynamic_cast<const FunctionCallExpr*>(contextualSelect->selectList[0].expr.get());assert(contextualCall);
    assert(contextualCall->resolvedResultType=="text");
    assert(ExprHelper::inferParsedInputType(contextualCall,{},db,&g_engine)=="text");
    auto typed=g_engine.prepareBoundQuery(db,"SELECT unnest($1)",{{"p","p","integer[]",{},true,"",1}});
    assert(typed.output.size()==1 && typed.output[0].type=="integer");
    assert(g_engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb(name);
    std::cout<<"[PREPARED SRF UNKNOWN INPUT] metadata-only input/output separation passed\n";
}
