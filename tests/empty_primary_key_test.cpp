#include "Session.h"
#include "access/BPTree.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

void cleanup(const std::string& database) {
    if (std::filesystem::exists(database)) {
        std::filesystem::remove_all(database);
    }
}

Session makeSession(const std::string& database) {
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    return session;
}

void testInlineEmptyPrimaryKeyIsIndexed() {
    const std::string database = testDbPath("empty_inline_primary_key");
    cleanup(database);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session = makeSession(database);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE inline_key (code VARCHAR(20) PRIMARY KEY, payload INT)",
        session));

    assert(g_engine.insert(database, "inline_key",
                           {{"code", ""}, {"payload", "1"}}) ==
           dbms::DBStatus::OK);
    dbms::BPTree* primaryIndex =
        g_engine.getPKIndex(database, "inline_key");
    assert(primaryIndex != nullptr && primaryIndex->flush());
    primaryIndex->close();
    assert(primaryIndex->open());
    assert(g_engine.insert(database, "inline_key",
                           {{"code", ""}, {"payload", "2"}}) ==
           dbms::DBStatus::DUPLICATE_KEY);
    assert(g_engine.insert(database, "inline_key",
                           {{"code", "later"}, {"payload", "4"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.update(database, "inline_key", {{"code", ""}},
                           {"=code later"}) ==
           dbms::DBStatus::DUPLICATE_KEY);
    assert(g_engine.remove(database, "inline_key", {"=payload 1"}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "inline_key",
                           {{"code", ""}, {"payload", "5"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "inline_key",
                           {{"code", ""}, {"payload", "6"}}) ==
           dbms::DBStatus::DUPLICATE_KEY);
    assert(g_engine.insert(database, "inline_key",
                           {{"code", "NULL"}, {"payload", "3"}}) ==
           dbms::DBStatus::NULL_NOT_ALLOWED);
    cleanup(database);
}

void testAlteredEmptyPrimaryKeyRemainsIndexed() {
    const std::string database = testDbPath("empty_altered_primary_key");
    cleanup(database);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session = makeSession(database);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE altered_key (code VARCHAR(20), payload INT)", session));

    assert(g_engine.insert(database, "altered_key",
                           {{"code", ""}, {"payload", "1"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.alterTableAddPrimaryKey(
               database, "altered_key", "altered_key_pkey", {"code"}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "altered_key",
                           {{"code", ""}, {"payload", "2"}}) ==
           dbms::DBStatus::DUPLICATE_KEY);
    cleanup(database);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    testInlineEmptyPrimaryKeyIsIndexed();
    testAlteredEmptyPrimaryKeyRemainsIndexed();
    std::cout << "[EMPTY PRIMARY KEY] all passed" << std::endl;
    return 0;
}
