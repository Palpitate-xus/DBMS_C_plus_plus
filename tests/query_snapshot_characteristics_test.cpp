#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using dbms::DBStatus;
    using dbms::IsolationLevel;
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "query_snapshot_characteristics";
    const std::string database = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    for (const auto level : {IsolationLevel::READ_UNCOMMITTED, IsolationLevel::READ_COMMITTED,
                             IsolationLevel::REPEATABLE_READ, IsolationLevel::SERIALIZABLE}) {
        const auto other = level == IsolationLevel::READ_COMMITTED
            ? IsolationLevel::SERIALIZABLE : IsolationLevel::READ_COMMITTED;
        for (const bool rollbackChild : {false, true}) {
            assert(g_engine.setIsolationLevel(level));
            assert(g_engine.beginTransaction(database) == DBStatus::OK);
            assert(g_engine.setReadOnly(true));
            assert(g_engine.savepoint("child") == DBStatus::OK);
            g_engine.noteQuerySnapshot();
            assert(g_engine.setIsolationLevel(level));
            assert(!g_engine.setReadOnly(false));
            if (rollbackChild) assert(g_engine.rollbackToSavepoint("child") == DBStatus::OK);
            assert(g_engine.releaseSavepoint("child") == DBStatus::OK);
            assert(g_engine.setIsolationLevel(level));
            assert(!g_engine.setIsolationLevel(other));
            assert(!g_engine.setReadOnly(false));
            assert(g_engine.rollbackTransaction() == DBStatus::OK);
            assert(g_engine.beginTransaction(database) == DBStatus::OK);
            assert(g_engine.setIsolationLevel(other));
            assert(g_engine.setReadOnly(true));
            assert(g_engine.setReadOnly(false));
            assert(g_engine.commitTransaction() == DBStatus::OK);
        }
    }
    for (const std::string sql : {"SELECT 1;", "VALUES(1);", "/* comment */ SELECT NULL;",
                                 "WITH x AS (SELECT 1) SELECT * FROM x;", "INSERT INTO x VALUES(1);"}) {
        assert(dbms::SQLParser::requiresQuerySnapshot(sql));
    }
    for (const std::string sql : {"BEGIN;", "COMMIT;", "SAVEPOINT s;", "SHOW transaction_isolation;",
                                 "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;", "-- comment\n"}) {
        assert(!dbms::SQLParser::requiresQuerySnapshot(sql));
    }
    cleanupTestDb(name);
    finalCleanupTestData();
    std::cout << "[QUERY SNAPSHOT CHARACTERISTICS] passed\n";
}
