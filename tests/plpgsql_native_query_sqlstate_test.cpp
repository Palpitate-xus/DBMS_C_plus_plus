#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "plpgsql_native_query_sqlstate";
    const std::string db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "admin";
    session.permission = 1;
    session.currentDB = db;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE error_rows(id INT)", session));
    assert(g_engine.insertRow(db, "error_rows", {{"id", "1"}}) == dbms::DBStatus::OK);

    bool passed = true;
    for (const auto& failure : std::vector<std::pair<std::string, std::string>>{
        {"SELECT 1/0;", "22012"},
        {"SELECT CAST('abc' AS INTEGER);", "22P02"},
        {"SELECT CAST('2147483648' AS INTEGER);", "22003"},
        {"SELECT id FROM error_rows WHERE 1/0>0;", "22012"},
        {"SELECT id FROM error_rows ORDER BY 1/(id-id);", "22012"},
        {"SELECT id FROM error_rows WHERE CAST('abc' AS INTEGER)=id;", "22P02"}
    }) {
        const auto result = g_engine.plpgsqlQuery(db, failure.first);
        if (result.ok || result.sqlState != failure.second) {
            std::cerr << failure.first << " expected " << failure.second
                      << " got ok=" << result.ok << " state=" << result.sqlState
                      << " message=" << result.message << '\n';
            passed = false;
        }
    }
    assert(g_engine.createUDF(db, "native_into_division", {}, {},
        "DECLARE n INT; BEGIN SELECT 1/0 INTO n; RETURN n; END;", 'v', "plpgsql", "int")
        == dbms::DBStatus::OK);
    std::string value;
    bool isNull = false;
    try {
        g_engine.callUDF(db, "native_into_division", {}, value, &isNull);
        passed = false;
        std::cerr << "native INTO division did not throw\n";
    } catch (const dbms::DbError& error) {
        if (error.sqlState() != "22012") {
            std::cerr << "native INTO expected 22012 got " << error.sqlState() << '\n';
            passed = false;
        }
    }
    const auto recovery = g_engine.plpgsqlQuery(db, "SELECT 7,'',NULL;");
    assert(recovery.ok && recovery.rowCount == 1 && recovery.columnCount == 3);
    assert(recovery.firstRow == (std::vector<std::optional<std::string>>{
        "7", "", std::nullopt}));
    assert(g_engine.dropDatabase(db) == dbms::DBStatus::OK);
    cleanupTestDb(name);
    if (!passed) return 1;
    std::cout << "[PLPGSQL NATIVE QUERY SQLSTATE] passed\n";
}
