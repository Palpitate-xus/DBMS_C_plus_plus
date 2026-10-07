#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <sstream>

extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("domain_ddl_io_error");
    assert(g_engine.createDatabase(db,"utf8") == DBStatus::OK);
    Session session; session.currentDB=db; session.username="testuser";session.permission=1;
    auto* prior = currentSession();setCurrentSession(&session);
    struct Restore {Session* prior;~Restore(){setCurrentSession(prior);}} restore{prior};
    const auto metadata = g_engine.dbPath(db)/".domains";
    assert(std::filesystem::create_directory(metadata));
    DdlExecutor ddl;
    std::ostringstream output;
    auto* originalOutput = std::cout.rdbuf(output.rdbuf());
    bool failed=false;
    try { failed=ddl.executeSql("CREATE DOMAIN broken_domain AS INT",session); }
    catch (...) { std::cout.rdbuf(originalOutput);throw; }
    std::cout.rdbuf(originalOutput);
    assert(failed);
    assert(output.str().find("SQLSTATE 58030")!=std::string::npos);
    assert(!g_engine.inTransaction());
    assert(std::filesystem::is_directory(metadata));
    assert(std::filesystem::is_empty(metadata));
    assert(std::filesystem::remove(metadata));
    assert(!ddl.executeSql("CREATE DOMAIN usable_domain AS INT DEFAULT 7",session));
    assert(g_engine.resolveDomainAncestry(db,"usable_domain").defaultValue=="7");
    assert(!g_engine.inTransaction());
    std::string semanticState;
    try {(void)ddl.executeSql("CREATE DOMAIN absent_schema.not_created AS INT",session);}
    catch(const DbError& error){semanticState=error.sqlState();}
    assert(semanticState=="3F000");
    assert(!g_engine.inTransaction());
    assert(g_engine.dropDatabase(db)==DBStatus::OK);
    std::cout<<"[DOMAIN DDL IO ERROR] bool result, SQLSTATE and reusable owner passed\n";
}
