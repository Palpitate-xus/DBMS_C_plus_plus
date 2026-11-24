#include "access/BPTree.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>

extern dbms::StorageEngine g_engine;

int main() {
    const std::string testName = "index_predicate_membership";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    dbms::TypeRegistry::instance().bootstrap();
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "admin";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE demo (id integer PRIMARY KEY, gate integer, r integer, label varchar(16))",
                           session));
    assert(g_engine.insert(database, "demo", {{"id", "1"}, {"gate", "1"}, {"r", "9"}, {"label", "outside"}})
           == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "demo", {{"id", "2"}, {"gate", "3"}, {"r", "8"}, {"label", "inside"}})
           == dbms::DBStatus::OK);
    assert(!ddl.executeSql("CREATE INDEX grouped ON demo(label) WHERE gate > 2 AND (r = 8 OR r = 9)", session));

    const auto count = [&](const std::string& label) {
        auto* index = g_engine.getSecondaryIndex(database, "demo", "label");
        assert(index);
        return index->searchMulti(label).size();
    };
    assert(count("inside") == 1);
    assert(count("outside") == 0);

    assert(g_engine.insert(database, "demo", {{"id", "3"}, {"gate", "1"}, {"r", "9"}, {"label", "later"}})
           == dbms::DBStatus::OK);
    assert(count("later") == 0);
    assert(g_engine.update(database, "demo", {{"gate", "3"}}, {"=id 3"}) == dbms::DBStatus::OK);
    assert(count("later") == 1);
    cleanupTestDb(testName);
}
