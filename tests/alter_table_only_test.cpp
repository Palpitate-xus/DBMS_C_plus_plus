#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>

extern dbms::StorageEngine g_engine;
namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (fs::exists(db)) fs::remove_all(db); }
static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser"; s.permission = 1; s.currentDB = db;
}

// Test ALTER TABLE ONLY parser support
static void test_only_parser() {
    dbms::SQLParser parser;
    auto r = parser.parse("ALTER TABLE ONLY t ADD COLUMN c INT");
    assert(r.success);
    auto* stmt = dynamic_cast<dbms::AlterTableStmt*>(r.stmt.get());
    assert(stmt);
    assert(stmt->only == true);
    assert(stmt->tableName == "t");
    std::cout << "[ALTER_ONLY] parser OK" << std::endl;
}

// Test ALTER TABLE SET TABLESPACE updates schema
static void test_set_tablespace() {
    std::string db = "alter_ts";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT)", s));

    // Alter tablespace via engine API
    auto res = g_engine.alterTableTablespace(db, "t", "my_space");
    assert(res == dbms::DBStatus::OK);

    // Verify it was persisted
    auto schema = g_engine.getTableSchema(db, "t");
    assert(schema.tablespace == "my_space");

    cleanup(db);
    std::cout << "[ALTER_ONLY] SET TABLESPACE OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_only_parser();
    test_set_tablespace();
    std::cout << "[ALTER_ONLY] all passed" << std::endl;
    return 0;
}
