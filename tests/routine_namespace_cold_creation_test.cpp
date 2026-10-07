#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <filesystem>
#include <iostream>
extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="routine_namespace_cold_creation",db=testDbPath(name);
    cleanupTestDb(name); assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session; session.username="admin"; session.permission=1; session.currentDB=db;
    DdlExecutor ddl;
    assert(!g_engine.catalogService().has(db));
    assert(std::filesystem::is_regular_file(g_engine.dbPath(db)/".schema_public"));
    std::string error;
    try {
        assert(!ddl.executeSql("CREATE FUNCTION cold_implicit() RETURNS INT LANGUAGE SQL AS $$SELECT 7$$",session));
        assert(!ddl.executeSql("CREATE FUNCTION public.cold_explicit() RETURNS INT LANGUAGE SQL AS $$SELECT 8$$",session));
    } catch(const DbError& failure) {error=failure.sqlState();}
    std::cerr<<"[COLD ROUTINE CREATION] real public marker actual state="<<error<<"\n";
    assert(error.empty());
    assert(g_engine.udfExists(db,"cold_implicit") && g_engine.udfExists(db,"cold_explicit"));
    // The declaration retains the existing namespace. This does not prohibit
    // future routine catalog publication during the actual CREATE operation.
    assert(std::filesystem::is_regular_file(g_engine.dbPath(db)/".schema_public"));
    try {(void)ddl.executeSql("CREATE FUNCTION cold_missing.rejected() RETURNS INT LANGUAGE SQL AS $$SELECT 9$$",session);}
    catch(const DbError& failure){error=failure.sqlState();}
    assert(error=="3F000" && !g_engine.udfExists(db,"rejected","cold_missing"));
    assert(g_engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb(name);

    const std::string emptyName="routine_namespace_cold_no_public",emptyDb=testDbPath(emptyName);
    cleanupTestDb(emptyName); assert(g_engine.createDatabase(emptyDb)==DBStatus::OK);
    session.currentDB=emptyDb;
    assert(g_engine.dropSchema(emptyDb,"public",false)==DBStatus::OK);
    assert(!g_engine.catalogService().has(emptyDb));
    for(const auto& spelling:{std::string{"cold_rejected"},std::string{"public.cold_rejected"}}) {
        error.clear();
        try {(void)ddl.executeSql("CREATE FUNCTION "+spelling+"() RETURNS INT LANGUAGE SQL AS $$SELECT 1$$",session);}
        catch(const DbError& failure){error=failure.sqlState();}
        assert(error=="3F000" && !g_engine.udfExists(emptyDb,"cold_rejected"));
        assert(!std::filesystem::exists(g_engine.dbPath(emptyDb)/".schema_public"));
    }
    // A directory with a marker's spelling is not a persisted namespace fact.
    assert(std::filesystem::create_directory(g_engine.dbPath(emptyDb)/".schema_public"));
    error.clear();
    try {(void)ddl.executeSql("CREATE FUNCTION public.cold_rejected() RETURNS INT LANGUAGE SQL AS $$SELECT 1$$",session);}
    catch(const DbError& failure){error=failure.sqlState();}
    assert(error=="3F000" && !g_engine.udfExists(emptyDb,"cold_rejected"));
    assert(g_engine.dropDatabase(emptyDb)==DBStatus::OK);cleanupTestDb(emptyName);
    std::cout<<"[COLD ROUTINE CREATION] actual marker, no bootstrap, missing/dropped/non-file controls passed\n";
}
