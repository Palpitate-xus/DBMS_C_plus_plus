#include "commands/DdlExecutor.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) {
    if (fs::exists(db)) fs::remove_all(db);
}

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static void test_composite_basic() {
    std::string db = "ctype_basic";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TYPE addr AS (street varchar(50), zip int)", s));

    auto ct = g_engine.getCompositeType(db, "addr");
    assert(ct.name == "addr");
    assert(ct.fields.size() == 2);
    assert(ct.fields[0].first == "street");
    assert(ct.fields[0].second == "varchar(50)");
    assert(ct.fields[1].first == "zip");
    assert(ct.fields[1].second == "int");

    cleanup(db);
    std::cout << "[CREATE TYPE] composite basic OK" << std::endl;
}

static void test_composite_numeric_modifier() {
    std::string db = "ctype_numeric";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    // numeric(10,2) contains a comma in its modifier — must survive parsing intact.
    assert(!ddl.executeSql("CREATE TYPE money_amt AS (amount numeric(10,2), code varchar(3))", s));

    auto ct = g_engine.getCompositeType(db, "money_amt");
    assert(ct.fields.size() == 2);
    assert(ct.fields[0].first == "amount");
    assert(ct.fields[0].second == "numeric(10,2)");
    assert(ct.fields[1].first == "code");
    assert(ct.fields[1].second == "varchar(3)");

    cleanup(db);
    std::cout << "[CREATE TYPE] composite numeric(p,s) modifier OK" << std::endl;
}

static void test_composite_multiword_type() {
    std::string db = "ctype_multiword";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TYPE evt AS (label varchar(20), ratio double precision)", s));

    auto ct = g_engine.getCompositeType(db, "evt");
    assert(ct.fields.size() == 2);
    assert(ct.fields[1].first == "ratio");
    assert(ct.fields[1].second == "double precision");

    cleanup(db);
    std::cout << "[CREATE TYPE] composite multi-word type OK" << std::endl;
}

static void test_drop_composite() {
    std::string db = "ctype_drop";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TYPE pt AS (x int, y int)", s));
    assert(g_engine.isCompositeType(db, "pt"));
    assert(!ddl.executeSql("DROP TYPE pt", s));
    assert(!g_engine.isCompositeType(db, "pt"));

    cleanup(db);
    std::cout << "[CREATE TYPE] drop composite OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_composite_basic();
    test_composite_numeric_modifier();
    test_composite_multiword_type();
    test_drop_composite();
    std::cout << "[CREATE TYPE] all passed" << std::endl;
    return 0;
}
