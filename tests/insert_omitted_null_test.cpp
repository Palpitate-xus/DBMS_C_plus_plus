#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "insert_omitted_null", db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    Session session;
    session.username = "admin";
    session.permission = 1;
    session.currentDB = db;
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE checked(id INT PRIMARY KEY,value INT)", session));
    assert(g_engine.insert(db, "checked", {{"id", "1"}, {"value", "1"}}) == DBStatus::OK);
    assert(!ddl.executeSql("ALTER TABLE checked ADD CONSTRAINT positive CHECK(value>0)", session));
    const auto missing = g_engine.insert(db, "checked", {{"id", "2"}});
    std::cout << "omitted nullable CHECK column: " << static_cast<int>(missing)
              << ", expected OK" << std::endl;
    assert(missing == DBStatus::OK);
    assert(g_engine.insertRow(db, "checked", {{"id", std::string("3")}, {"value", std::nullopt}}) == DBStatus::OK);
    assert(g_engine.insert(db, "checked", {{"id", "4"}, {"value", "-1"}}) == DBStatus::CHECK_VIOLATION);
    assert(g_engine.query(db, "checked", {"isnull value"}, {"id"}).size() == 2);

    assert(!ddl.executeSql("CREATE TABLE generated(id INT PRIMARY KEY,base INT,d INT DEFAULT 7,g INT GENERATED ALWAYS AS(base+1) STORED)", session));
    assert(g_engine.insert(db, "generated", {{"id", "1"}}) == DBStatus::OK);
    assert(g_engine.query(db, "generated", {"isnull base", "isnull g"}, {"id"}).size() == 1);
    assert(g_engine.query(db, "generated", {"=d 7"}, {"id"}).size() == 1);
    assert(g_engine.insertRow(db, "generated", {{"id", std::string("2")}, {"d", std::nullopt}}) == DBStatus::OK);
    assert(g_engine.query(db, "generated", {"isnull d"}, {"id"}).size() == 1);

    TableSchema text;
    text.append(makeIntColumn("id", false, 4, true));
    text.append(makeVarCharColumn("value", true, 20));
    assert(g_engine.createTable(db, "text_rows", text) == DBStatus::OK);
    assert(g_engine.insertRow(db, "text_rows", {{"id", std::string("1")}}) == DBStatus::OK);
    assert(g_engine.insertRow(db, "text_rows", {{"id", std::string("2")}, {"value", std::string{}}}) == DBStatus::OK);
    assert(g_engine.query(db, "text_rows", {"isnull value"}, {"id"}).size() == 1);
    assert(g_engine.query(db, "text_rows", {"isnotnull value"}, {"id"}).size() == 1);
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[INSERT OMITTED NULL] complete post-trigger NEW image passed\n";
}
