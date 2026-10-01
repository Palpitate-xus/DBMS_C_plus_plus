#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string database = testDbPath("quoted_column_predicate");
    cleanupTestDb("quoted_column_predicate");
    assert(g_engine.createDatabase(database) == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE quoted_columns(\"child id\" INT PRIMARY KEY,payload TEXT,\"nullable text\" TEXT,\"a\"\"b\" TEXT,\"key.name\" INT)", session));
    const auto run = [&](const std::string& sql) {
        bool handled = false;
        const bool error = dbms::tryDmlBridge(
            sql, dbms::SQLParser::classify(sql), session, handled);
        assert(handled && !error);
        return dbms::takeLastDmlResult();
    };
    run("INSERT INTO quoted_columns VALUES(1,'original',NULL,'has space',7),(2,'other','','plain',8)");
    auto result = run("UPDATE quoted_columns SET payload='changed' WHERE \"child id\"=1 RETURNING \"child id\",payload");
    assert((result.rows == std::vector<std::vector<std::string>>{{"1", "changed"}}));
    result = run("UPDATE quoted_columns SET payload='empty' WHERE \"nullable text\"='' RETURNING \"child id\"");
    assert((result.rows == std::vector<std::vector<std::string>>{{"2"}}));
    result = run("UPDATE quoted_columns SET payload='null' WHERE \"nullable text\" IS NULL RETURNING \"child id\"");
    assert((result.rows == std::vector<std::vector<std::string>>{{"1"}}));
    result = run("UPDATE quoted_columns SET payload='like' WHERE \"a\"\"b\" LIKE 'has space%' RETURNING \"child id\"");
    assert((result.rows == std::vector<std::vector<std::string>>{{"1"}}));
    result = run("DELETE FROM quoted_columns WHERE \"key.name\"=7 AND \"child id\"<>2 RETURNING \"child id\",payload");
    assert((result.rows == std::vector<std::vector<std::string>>{{"1", "like"}}));
    result = run("DELETE FROM quoted_columns WHERE \"nullable text\" IS NOT NULL RETURNING \"child id\"");
    assert((result.rows == std::vector<std::vector<std::string>>{{"2"}}));
    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    cleanupTestDb("quoted_column_predicate");
    std::cout << "[QUOTED COLUMN PREDICATE] passed" << std::endl;
}
