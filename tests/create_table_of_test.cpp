#include "commands/DdlExecutor.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include "table_schema.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static const dbms::Column* findCol(const dbms::TableSchema& t, const std::string& name) {
    for (size_t i = 0; i < t.len; ++i)
        if (t.cols[i].dataName == name) return &t.cols[i];
    return nullptr;
}

static void test_of_type_basic() {
    std::string db = testDbPath("of_basic");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TYPE person AS (name varchar(40), age int)", s));
    assert(!ddl.executeSql("CREATE TABLE people OF person", s));

    auto schema = g_engine.getTableSchema(db, "people");
    assert(schema.len == 2);
    const dbms::Column* name = findCol(schema, "name");
    const dbms::Column* age = findCol(schema, "age");
    assert(name && age);
    assert(name->isVariableLength);            // varchar(40)
    cleanup(db);
    std::cout << "[OF TYPE] basic OK" << std::endl;
}

static void test_of_type_numeric() {
    std::string db = testDbPath("of_numeric");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TYPE money_t AS (amount numeric(10,2), code varchar(3))", s));
    assert(!ddl.executeSql("CREATE TABLE wallet OF money_t", s));

    auto schema = g_engine.getTableSchema(db, "wallet");
    assert(schema.len == 2);
    assert(findCol(schema, "amount") && findCol(schema, "code"));
    cleanup(db);
    std::cout << "[OF TYPE] numeric(p,s) field OK" << std::endl;
}

static void test_of_type_missing() {
    std::string db = testDbPath("of_missing");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(ddl.executeSql("CREATE TABLE bad OF nonexistent_type", s));  // error
    assert(!g_engine.tableExists(db, "bad"));
    cleanup(db);
    std::cout << "[OF TYPE] missing type rejected OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_of_type_basic();
    test_of_type_numeric();
    test_of_type_missing();
    std::cout << "[OF TYPE] all passed" << std::endl;
    return 0;
}
