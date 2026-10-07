#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="routine_namespace_declaration",db=testDbPath(name);
    cleanupTestDb(name);assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session;session.username="admin";session.permission=1;session.currentDB=db;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore{Session* session;~Restore(){setCurrentSession(session);}} restore{previous};
    DdlExecutor ddl;assert(!ddl.executeSql("CREATE SCHEMA \"Routine.Schema\"",session));
    SQLParser parser;
    auto parsed=parser.parse("CREATE FUNCTION \"Routine.Schema\".\"Odd.Name\"(p INT[]) RETURNS BIGINT LANGUAGE SQL AS $$SELECT 9::BIGINT$$");
    assert(parsed.isValid());
    const auto* declared=dynamic_cast<const CreateFunctionStmt*>(parsed.stmt.get());assert(declared);
    assert(declared->schema=="Routine.Schema" && declared->funcName=="Odd.Name");
    assert(declared->params.size()==1 && declared->returnType=="BIGINT");
    assert(!ddl.executeSql("CREATE FUNCTION \"Routine.Schema\".\"Odd.Name\"(p INT[]) RETURNS BIGINT LANGUAGE SQL AS $$SELECT 9::BIGINT$$",session));
    const auto stored=g_engine.getUDF(db,"Odd.Name","Routine.Schema");
    assert(!stored.expression.empty() && stored.returnType=="bigint" && stored.paramTypes==std::vector<std::string>{"integer[]"});
    assert(!g_engine.udfExists(db,"Odd.Name"));
    assert(g_engine.beginTransaction(db)==DBStatus::OK);
    assert(!ddl.executeSql("CREATE FUNCTION \"Routine.Schema\".rollback_function() RETURNS TEXT LANGUAGE SQL AS $$SELECT 'x'::TEXT$$",session));
    assert(g_engine.udfExists(db,"rollback_function","Routine.Schema"));
    assert(g_engine.rollbackTransaction()==DBStatus::OK);
    assert(!g_engine.udfExists(db,"rollback_function","Routine.Schema"));
    assert(g_engine.udfExists(db,"Odd.Name","Routine.Schema"));
    assert(g_engine.dropUDF(db,"Odd.Name","Routine.Schema")==DBStatus::OK);
    assert(g_engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb(name);
    std::cout<<"[ROUTINE NAMESPACE DECLARATION] quoted components, arrays, separate metadata and real undo passed\n";
}
