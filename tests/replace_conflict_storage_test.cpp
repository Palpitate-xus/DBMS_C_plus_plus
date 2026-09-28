#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using dbms::DBStatus;
    dbms::TypeRegistry::instance().bootstrap();
    const std::string db = testDbPath("replace_conflict_storage");
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = db;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE t (a INTEGER, b INTEGER, email TEXT UNIQUE, "
        "payload TEXT, PRIMARY KEY (a,b))", session));
    assert(g_engine.insert(db, "t", {{"a", "1"}, {"b", "2"},
        {"email", "first@example"}, {"payload", "old"}}) == DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"a", "3"}, {"b", "4"},
        {"email", "second@example"}, {"payload", "keep"}}) == DBStatus::OK);
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.beginSqlCommand());
    const std::map<std::string, std::string> replacement = {
        {"a", "1"}, {"b", "2"}, {"email", "second@example"},
        {"payload", "replacement"}};
    assert(g_engine.insert(db, "t", replacement) == DBStatus::DUPLICATE_KEY);
    assert(g_engine.remove(db, "t", {"=a 1", "=b 2"}) == DBStatus::OK);
    assert(g_engine.remove(db, "t", {"=email second@example"}) == DBStatus::OK);
    assert(g_engine.finishSqlCommand());
    assert(g_engine.beginSqlCommand());
    const auto remaining = g_engine.query(db, "t", {}, {"a", "b", "email"});
    assert(remaining.empty());
    assert(g_engine.insert(db, "t", replacement) == DBStatus::OK);
    assert(g_engine.finishSqlCommand());
    assert(g_engine.commitTransaction() == DBStatus::OK);
    const auto rows = g_engine.query(db, "t", {}, {"a", "b", "email", "payload"});
    assert(rows.size() == 1);
    assert(rows.front().find("replacement") != std::string::npos);
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    std::cout << "[REPLACE STORAGE] delete/reinsert conflicting keys OK\n";
}
