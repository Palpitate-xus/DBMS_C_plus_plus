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
    const std::string name = "signed_dml_predicate";
    const std::string database = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(database) == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE signed_values(id INT PRIMARY KEY,"
                          "v BIGINT,\"signed count\" INT,payload TEXT)", session));
    const auto run = [&](const std::string& sql) {
        bool handled = false;
        const bool error = dbms::tryDmlBridge(
            sql, dbms::SQLParser::classify(sql), session, handled);
        assert(handled && !error);
        return dbms::takeLastDmlResult();
    };
    run("INSERT INTO signed_values VALUES(1,-2,-2,'old'),"
        "(2,-1,-1,'old'),(3,0,0,'old'),(4,2,2,'old'),(5,NULL,NULL,NULL)");
    auto result = run("UPDATE signed_values SET payload='changed' "
                      "WHERE v=-1 RETURNING id,v");
    assert(result.commandTag == "UPDATE 1");
    assert((result.rows == std::vector<std::vector<std::string>>{{"2", "-1"}}));
    result = run("UPDATE signed_values SET payload='plus' "
                 "WHERE v>=+2 RETURNING id");
    assert(result.commandTag == "UPDATE 1");
    assert((result.rows == std::vector<std::vector<std::string>>{{"4"}}));
    result = run("DELETE FROM signed_values WHERE \"signed count\"<-1 RETURNING id,v");
    assert(result.commandTag == "DELETE 1");
    assert((result.rows == std::vector<std::vector<std::string>>{{"1", "-2"}}));
    result = run("DELETE FROM signed_values WHERE v=-1 RETURNING id,payload");
    assert(result.commandTag == "DELETE 1");
    assert((result.rows == std::vector<std::vector<std::string>>{{"2", "changed"}}));
    assert(g_engine.query(database, "signed_values", {}, {"id"}).size() == 3);
    assert(g_engine.query(database, "signed_values", {"isnull v"}, {"id"}).size() == 1);
    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    cleanupTestDb(name);
    finalCleanupTestData();
    std::cout << "[SIGNED DML PREDICATE] passed\n";
}
