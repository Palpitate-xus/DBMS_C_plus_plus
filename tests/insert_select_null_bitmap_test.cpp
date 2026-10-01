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
    const std::string database = testDbPath("insert_select_null_bitmap");
    cleanupTestDb("insert_select_null_bitmap");
    assert(g_engine.createDatabase(database) == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    const auto run = [&](const std::string& sql, bool expectedError = false) {
        bool handled = false;
        const bool error = dbms::tryDmlBridge(
            sql, dbms::SQLParser::classify(sql), session, handled);
        assert(handled && error == expectedError);
        return dbms::takeLastDmlResult();
    };
    assert(!ddl.executeSql("CREATE TABLE source_rows(id INT,pid INT,content TEXT)", session));
    assert(!ddl.executeSql("CREATE TABLE target_rows(id INT PRIMARY KEY,pid INT,content TEXT DEFAULT 'fallback')", session));
    run("INSERT INTO source_rows VALUES(1,2,'NULL'),(2,NULL,NULL),(3,NULL,'')");
    auto result = run("INSERT INTO target_rows SELECT id,pid,content FROM source_rows RETURNING id,pid,content");
    assert(result.commandTag == "INSERT 0 3");
    assert((result.rows == std::vector<std::vector<std::string>>{{"1", "2", "NULL"}, {"2", "NULL", "NULL"}, {"3", "NULL", ""}}));
    assert((result.nulls == std::vector<std::vector<bool>>{{false, false, false}, {false, true, true}, {false, true, false}}));
    assert(!ddl.executeSql("CREATE TABLE star_rows(id INT,pid INT,content TEXT)", session));
    result = run("INSERT INTO star_rows SELECT * FROM source_rows RETURNING id,pid,content");
    assert((result.nulls == std::vector<std::vector<bool>>{{false, false, false}, {false, true, true}, {false, true, false}}));
    assert(!ddl.executeSql("CREATE TABLE projected_rows(id INT,pid INT,content TEXT)", session));
    result = run("INSERT INTO projected_rows SELECT id+10,pid*2,upper(content) FROM source_rows WHERE pid IS NULL RETURNING id,pid,content");
    assert((result.rows == std::vector<std::vector<std::string>>{{"12", "NULL", "NULL"}, {"13", "NULL", ""}}));
    assert((result.nulls == std::vector<std::vector<bool>>{{false, true, true}, {false, true, false}}));
    result = run("INSERT INTO projected_rows SELECT 30,NULL,'NULL' RETURNING pid,content");
    assert((result.rows == std::vector<std::vector<std::string>>{{"NULL", "NULL"}}));
    assert((result.nulls == std::vector<std::vector<bool>>{{true, false}}));
    assert(!ddl.executeSql("CREATE TABLE nonnull_rows(id INT,pid INT NOT NULL,content TEXT)", session));
    run("INSERT INTO nonnull_rows SELECT id,pid,content FROM source_rows", true);
    assert(g_engine.query(database, "nonnull_rows", {}, {"id"}).empty());
    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    cleanupTestDb("insert_select_null_bitmap");
    std::cout << "[INSERT SELECT NULL BITMAP] passed" << std::endl;
}
