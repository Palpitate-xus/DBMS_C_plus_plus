#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    SQLParser parser;
    for(const auto* malformed:{"DROP FUNCTION public.","DROP FUNCTION public..f",
        "DROP PROCEDURE .p","DROP ROUTINE ()"})
        assert(!parser.parseForBinding(malformed).success);
    for(const auto* kind:{"FUNCTION","PROCEDURE","ROUTINE"}) {
        auto parsed=parser.parseForBinding(std::string("DROP ")+kind+" public.f");
        assert(parsed.success);
        const auto* drop=dynamic_cast<const DropStmt*>(parsed.stmt.get());
        assert(drop && drop->objectNames==std::vector<std::string>{"public.f"});
        parsed=parser.parseForBinding(std::string("DROP ")+kind+" IF EXISTS \"S.X\" . \"F.Y\" () RESTRICT");
        assert(parsed.success);drop=dynamic_cast<const DropStmt*>(parsed.stmt.get());
        assert(drop && drop->ifExists && !drop->cascade);
        assert((drop->objectNames==std::vector<std::string>{"\"S.X\".\"F.Y\"","(",")"}));
    }
    const auto database=testDbPath("drop_routine_qualified_name");
    assert(g_engine.createDatabase(database)==DBStatus::OK);
    Session backend;backend.username="admin";backend.permission=1;backend.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&backend);
    struct Restore {Session* previous;~Restore(){setCurrentSession(previous);}} restore{previous};
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA \"S.X\"",backend));
    assert(!ddl.executeSql("CREATE FUNCTION public.f() RETURNS INT LANGUAGE SQL AS $$SELECT 1$$",backend));
    assert(!ddl.executeSql("CREATE FUNCTION \"S.X\".\"F.Y\"() RETURNS INT LANGUAGE SQL AS $$SELECT 2$$",backend));
    assert(!ddl.executeSql("DROP FUNCTION \"S.X\" . \"F.Y\" ()",backend));
    assert(!g_engine.udfExists(database,"F.Y","S.X"));
    assert(g_engine.udfExists(database,"f","public"));
    assert(!ddl.executeSql("DROP ROUTINE public.f()",backend));
    assert(!g_engine.udfExists(database,"f","public"));
    assert(!ddl.executeSql("CREATE PROCEDURE public.p() LANGUAGE SQL AS $$SELECT 1$$",backend));
    assert(!ddl.executeSql("DROP PROCEDURE public.p()",backend));
    assert(!g_engine.procedureExists(database,"p","public"));
    assert(g_engine.dropDatabase(database)==DBStatus::OK);
    std::cout<<"[DROP ROUTINE QUALIFIED NAME] intact AST identity and actual function/procedure/routine deletion passed\n";
}
