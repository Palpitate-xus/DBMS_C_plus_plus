#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string database = testDbPath("truncate_owned_sequence_restart");
    cleanupTestDb("truncate_owned_sequence_restart");
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SEQUENCE owned_seq START 7 INCREMENT 3", session));
    assert(!ddl.executeSql("CREATE TABLE owned(id INT DEFAULT nextval('owned_seq'),code TEXT)", session));
    assert(!ddl.executeSql("ALTER SEQUENCE owned_seq OWNED BY owned.id", session));
    assert(g_engine.insert(database, "owned", {{"code", "before"}}) == dbms::DBStatus::OK);
    assert(!ddl.executeSql("TRUNCATE owned RESTART IDENTITY", session));
    assert(g_engine.insert(database, "owned", {{"code", "restart"}}) == dbms::DBStatus::OK);
    assert(g_engine.query(database, "owned", {}, {"id", "code"}) == std::vector<std::string>{"7 restart "});
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(!ddl.executeSql("TRUNCATE owned RESTART IDENTITY", session));
    assert(g_engine.insert(database, "owned", {{"code", "transaction"}}) == dbms::DBStatus::OK);
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(g_engine.query(database, "owned", {}, {"id", "code"}) == std::vector<std::string>{"7 restart "});
    assert(g_engine.nextval(database, "owned_seq") == 10);
    assert(!ddl.executeSql("DROP TABLE owned", session));
    assert(!ddl.executeSql("CREATE SEQUENCE descending_seq START -7 INCREMENT -3", session));
    assert(!ddl.executeSql("CREATE TABLE descending_owner(id INT DEFAULT nextval('descending_seq'))", session));
    assert(!ddl.executeSql("ALTER SEQUENCE descending_seq OWNED BY descending_owner.id", session));
    assert(g_engine.insert(database, "descending_owner", {}) == dbms::DBStatus::OK);
    assert(g_engine.nextval(database, "descending_seq") == -10);
    assert(!ddl.executeSql("TRUNCATE descending_owner RESTART IDENTITY", session));
    assert(g_engine.insert(database, "descending_owner", {}) == dbms::DBStatus::OK);
    assert(g_engine.query(database, "descending_owner", {}, {"id"}) == std::vector<std::string>{"-7 "});
    assert(!ddl.executeSql("DROP TABLE descending_owner", session));
    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    cleanupTestDb("truncate_owned_sequence_restart");
    std::cout << "[TRUNCATE OWNED SEQUENCE RESTART] passed" << std::endl;
}
