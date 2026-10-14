#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "test_utils.h"
#include <cassert>
#include <filesystem>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "schema_routine_namespace_identity";
    const std::string db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db) == DBStatus::OK);
    Session session;
    session.username = "schema_owner";
    session.permission = 1;
    session.currentDB = db;
    Session* previous = currentSession();
    setCurrentSession(&session);
    struct Restore { Session* previous; ~Restore() { setCurrentSession(previous); } } restore{previous};
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA app_aux", session));

    // Preserve the original low-level call. Its existing explicit schema
    // argument defaults to public; decoded dots remain name bytes.
    assert(g_engine.createUDF(db, "app_aux.bump", "x", "x + 1") == DBStatus::OK);
    assert(g_engine.udfExists(db, "app_aux.bump", "public"));
    assert(!g_engine.udfExists(db, "bump", "app_aux"));
    assert(std::filesystem::is_regular_file(g_engine.dbPath(db) / ".funcs" / "app_aux.bump.func"));
    assert(!std::filesystem::exists(g_engine.dbPath(db) / ".funcs" / ".namespaces" / "app_aux" / "bump.func"));
    // SQL public."app_aux.bump" targets that same actual file, not app_aux.bump.
    assert(ddl.executeSql("CREATE FUNCTION public.\"app_aux.bump\"(x TEXT) RETURNS TEXT LANGUAGE SQL AS $$SELECT x$$", session));
    assert(g_engine.getUDF(db, "app_aux.bump", "public").expression == "x + 1");
    assert(!ddl.executeSql("CREATE FUNCTION app_aux.bump(x INT) RETURNS INT LANGUAGE SQL AS $$SELECT x+1$$", session));
    assert(g_engine.udfExists(db, "bump", "app_aux"));
    assert(std::filesystem::is_regular_file(g_engine.dbPath(db) / ".funcs" / ".namespaces" / "app_aux" / "bump.func"));

    assert(g_engine.dropSchema(db, "app_aux", false) == DBStatus::INVALID_VALUE);
    assert(g_engine.udfExists(db, "app_aux.bump", "public"));
    assert(g_engine.udfExists(db, "bump", "app_aux"));
    assert(g_engine.dropSchema(db, "app_aux", true) == DBStatus::OK);
    assert(g_engine.udfExists(db, "app_aux.bump", "public"));
    assert(!g_engine.udfExists(db, "bump", "app_aux"));
    assert(!g_engine.schemaExists(db, "app_aux"));
    assert(!g_engine.inTransaction());
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[SCHEMA ROUTINE IDENTITY] canonical API/path and same SQL identity controls passed\n";
}
