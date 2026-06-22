#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
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

static void test_registry_aliases() {
    dbms::TypeRegistry::instance().bootstrap();
    assert(dbms::TypeRegistry::instance().normalizeTypeName("tinyint") == "smallint");
    assert(dbms::TypeRegistry::instance().normalizeTypeName("TINYINT") == "smallint");
    assert(dbms::TypeRegistry::instance().normalizeTypeName("datetime") == "timestamp");
    assert(dbms::TypeRegistry::instance().normalizeTypeName("blob") == "bytea");
    assert(dbms::TypeRegistry::instance().normalizeTypeName("nchar") == "character");
    assert(dbms::TypeRegistry::instance().normalizeTypeName("nvarchar") == "character varying");

    const auto* tinyint = dbms::TypeRegistry::instance().findType("tinyint");
    assert(tinyint != nullptr);
    assert(tinyint->canonicalName == "smallint");

    const auto* blob = dbms::TypeRegistry::instance().findType("blob");
    assert(blob != nullptr);
    assert(blob->canonicalName == "bytea");

    std::cout << "[TYPE-ALIAS] registry aliases OK" << std::endl;
}

static void test_ddl_alias_columns() {
    std::string db = "type_alias_t1";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql(
        "CREATE TABLE t (a TINYINT, b DATETIME, c BLOB, d NCHAR(10), e NVARCHAR(50))", s);
    assert(!err);
    assert(g_engine.tableExists(db, "t"));

    dbms::TableSchema schema = g_engine.getTableSchema(db, "t");
    assert(schema.len == 5);
    assert(schema.cols[0].dataType == "smallint" || schema.cols[0].dataType == "int2");
    assert(schema.cols[1].dataType == "timestamp");
    assert(schema.cols[2].dataType == "blob" || schema.cols[2].dataType == "bytea");
    assert(schema.cols[3].dataType == "char" || schema.cols[3].dataType == "character");
    assert(schema.cols[4].dataType == "varchar" || schema.cols[4].dataType == "character varying");

    cleanup(db);
    std::cout << "[TYPE-ALIAS] DDL alias columns OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_registry_aliases();
    test_ddl_alias_columns();
    std::cout << "[TYPE-ALIAS] all passed" << std::endl;
    return 0;
}
