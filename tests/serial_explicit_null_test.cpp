#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "test_utils.h"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <sstream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "serial_explicit_null";
    const std::string database = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    dbms::SQLParser parser;
    for (const auto* type : {"SMALLSERIAL", "SERIAL", "BIGSERIAL",
                             "SERIAL2", "SERIAL4", "SERIAL8"}) {
        const std::string sql = "CREATE TABLE bad(id " + std::string(type) + " NULL)";
        const auto parsed = parser.parse(sql);
        assert(parsed.success);
        const auto* table = dynamic_cast<const dbms::CreateTableStmt*>(parsed.stmt.get());
        assert(table && table->columns.size() == 1);
        const auto& constraints = table->columns[0].constraints;
        assert(std::find(constraints.begin(), constraints.end(), "NULL") != constraints.end());
        std::ostringstream diagnostic;
        auto* old = std::cout.rdbuf(diagnostic.rdbuf());
        const bool failed = ddl.executeSql(sql, session);
        std::cout.rdbuf(old);
        assert(failed && diagnostic.str().find("42601") != std::string::npos);
        assert(!g_engine.tableExists(database, "bad"));
        assert(!g_engine.sequenceExists(database, "bad_id_seq"));
    }
    assert(!ddl.executeSql("CREATE TABLE good(id SERIAL NOT NULL)", session));
    assert(g_engine.insert(database, "good", {}) == dbms::DBStatus::OK);
    assert(g_engine.query(database, "good", {}, {"id"}) == std::vector<std::string>{"1 "});
    const auto defaultNull = parser.parse("CREATE TABLE nullable(id INT DEFAULT NULL)");
    assert(defaultNull.success);
    const auto* nullable = dynamic_cast<const dbms::CreateTableStmt*>(defaultNull.stmt.get());
    assert(nullable && nullable->columns[0].constraints.empty());
    assert(nullable->columns[0].defaultValue);
    assert(!ddl.executeSql("DROP TABLE good", session));
    cleanupTestDb(name);
    finalCleanupTestData();
    std::cout << "[SERIAL EXPLICIT NULL] passed\n";
}
