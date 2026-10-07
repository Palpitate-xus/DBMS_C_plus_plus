#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="prepared_srf_host_ownership",db=testDbPath(name);
    cleanupTestDb(name);assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session;session.username="admin";session.permission=1;session.currentDB=db;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* value;~Restore(){setCurrentSession(value);}} restore{previous};
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SEQUENCE host_effects",session));
    const auto owns=[&](const std::string& expression) {
        SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+expression);
        const auto* select=parsed.isValid()?dynamic_cast<const SelectStmt*>(parsed.stmt.get()):nullptr;
        assert(select && select->selectList.size()==1);
        const auto* call=dynamic_cast<const FunctionCallExpr*>(select->selectList[0].expr.get());assert(call);
        return g_engine.ownsPreparedSetReturningCall(db,call);
    };
    assert(owns("unnest(ARRAY[1,2])"));
    assert(owns("pg_catalog.unnest(ARRAY[1,2])"));
    assert(owns("unnest(NULL)"));
    assert(owns("unnest(missing_host_function())"));
    assert(owns("pg_listening_channels()"));
    assert(owns("pg_catalog.pg_listening_channels()"));
    assert(!owns("public.pg_listening_channels()"));
    assert(!owns("upper('x')"));
    assert(!owns("missing_host_function(nextval('host_effects'))"));
    assert(!owns("\"Unnest\"(ARRAY[1,2])"));
    assert(!owns("public.unnest(ARRAY[1,2])"));
    assert(!ddl.executeSql("CREATE FUNCTION unnest(p TEXT) RETURNS TEXT VOLATILE LANGUAGE plpgsql AS $$BEGIN PERFORM nextval('host_effects'); RETURN p; END$$",session));
    assert(!owns("unnest('scalar')"));
    assert(!owns("public.unnest('scalar')"));
    assert(owns("pg_catalog.unnest(ARRAY[1,2])"));
    std::string state;
    try{(void)g_engine.prepareBoundQuery(db,"SELECT pg_catalog.unnest(missing_host_function())");}
    catch(const DbError& error){state=error.sqlState();}
    assert(state=="42883");
    assert(g_engine.nextval(db,"host_effects")==1);
    assert(!g_engine.inTransaction());
    assert(g_engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb(name);
    std::cout<<"[PREPARED SRF HOST OWNERSHIP] pure identity, routine shadowing and argument priority passed\\n";
}
