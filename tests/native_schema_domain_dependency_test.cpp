#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "common/DbError.h"
#include "utils/Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto name = "native_schema_external_domain";
    const auto database = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    Session session;
    session.username = "domain_namespace_owner";
    session.permission = 1;
    session.currentDB = database;
    session.searchPath = "public";
    auto* previous = currentSession();
    setCurrentSession(&session);
    struct Restore { Session* previous; ~Restore() { setCurrentSession(previous); } } restore{previous};
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA domain_owner", session));
    assert(!ddl.executeSql("CREATE DOMAIN domain_owner.owned_int AS INT CHECK (VALUE > 0)", session));
    assert(!ddl.executeSql("CREATE TABLE external_domain_table(id INT PRIMARY KEY, payload domain_owner.owned_int, keep_value TEXT)", session));
    assert(g_engine.insertRow(database, "external_domain_table", {{"id", "1"}, {"payload", "5"}, {"keep_value", "kept"}}) == DBStatus::OK);
    assert(g_engine.getTableSchema(database, "external_domain_table").len == 3);
    std::cout << "[SCHEMA DOMAIN CASCADE] setup domain/table/row accepted\n" << std::flush;
    assert(g_engine.dropSchema(database, "domain_owner", false) == DBStatus::INVALID_VALUE);
    assert(g_engine.getTableSchema(database, "external_domain_table").len == 3);
    DBStatus status = DBStatus::IO_ERROR;
    try { status = g_engine.dropSchema(database, "domain_owner", true); }
    catch (const DbError& error) {
        std::cout << "[SCHEMA DOMAIN CASCADE] native owner threw " << error.sqlState()
                  << " " << error.what() << " schema_after="
                  << g_engine.schemaExists(database, "domain_owner") << '\n' << std::flush;
        const auto restored = g_engine.getTableSchema(database, "external_domain_table");
        std::cout << "[SCHEMA DOMAIN CASCADE] after exception external_width=" << restored.len << '\n' << std::flush;
        assert(g_engine.dropDatabase(database) == DBStatus::OK);
        return 1;
    }
    std::cout << "[SCHEMA DOMAIN CASCADE] actual native CASCADE status="
              << static_cast<int>(status) << '\n' << std::flush;
    TableSchema table;
    try { table = g_engine.getTableSchema(database, "external_domain_table"); }
    catch (const DbError& error) {
        std::cout << "[SCHEMA DOMAIN CASCADE] surviving external table unreadable: "
                  << error.sqlState() << " " << error.what() << '\n' << std::flush;
        assert(g_engine.dropDatabase(database) == DBStatus::OK);
        return 1;
    }
    bool hasPayload = false;
    for (size_t i = 0; i < table.len; ++i) hasPayload = hasPayload || table.cols[i].dataName == "payload";
    const auto rows = g_engine.query(database, "external_domain_table", {}, {"id", "keep_value"});
    const bool valid = status == DBStatus::OK && table.len == 2 && !hasPayload &&
        rows == std::vector<std::string>{"1 kept "};
    std::cout << "[SCHEMA DOMAIN CASCADE] status=" << static_cast<int>(status)
              << " actual_width=" << table.len << " dangling_payload=" << hasPayload
              << " rows=" << rows.size() << " expected_width=2\n";
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    return valid ? 0 : 1;
}
