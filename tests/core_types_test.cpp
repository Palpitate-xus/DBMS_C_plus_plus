// ============================================================================
// Core Type System Test — Phase 4 Wave 1
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
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

static const dbms::Column* findCol(const dbms::TableSchema& tbl, const std::string& name) {
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == name) return &tbl.cols[i];
    }
    return nullptr;
}

static void test_geometric_network_types() {
    std::string db = "core_types_t1";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql(
        "CREATE TABLE geo_tbl ("
        "  p point, ln line, ls lseg, b box, pt path, pg polygon, c circle,"
        "  ia inet, cd cidr, ma macaddr, ma8 macaddr8)", s);
    assert(!err);
    assert(g_engine.tableExists(db, "geo_tbl"));

    dbms::TableSchema tbl = g_engine.getTableSchema(db, "geo_tbl");
    const dbms::Column* c = findCol(tbl, "ln");
    assert(c && c->dataType == "line");
    c = findCol(tbl, "ls");
    assert(c && c->dataType == "lseg");
    c = findCol(tbl, "b");
    assert(c && c->dataType == "box");
    c = findCol(tbl, "pt");
    assert(c && c->dataType == "path");
    c = findCol(tbl, "pg");
    assert(c && c->dataType == "polygon");
    c = findCol(tbl, "c");
    assert(c && c->dataType == "circle");
    c = findCol(tbl, "ma");
    assert(c && c->dataType == "macaddr" && c->dsize == 6);
    c = findCol(tbl, "ma8");
    assert(c && c->dataType == "macaddr8" && c->dsize == 8);

    cleanup(db);
    std::cout << "[CORE-TYPES] geometric/network OK" << std::endl;
}

static void test_bit_and_jsonpath() {
    std::string db = "core_types_t2";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql(
        "CREATE TABLE bit_tbl (fixed BIT(8), varying VARBIT(32), jp JSONPATH)", s);
    assert(!err);

    dbms::TableSchema tbl = g_engine.getTableSchema(db, "bit_tbl");
    const dbms::Column* c = findCol(tbl, "fixed");
    assert(c && c->dataType == "bit" && c->dsize == 1);
    c = findCol(tbl, "varying");
    assert(c && c->dataType == "bit varying" && c->dsize == 4);
    c = findCol(tbl, "jp");
    assert(c && c->dataType == "jsonpath");

    cleanup(db);
    std::cout << "[CORE-TYPES] bit/jsonpath OK" << std::endl;
}

static void test_type_registry_has_new_types() {
    auto* reg = dbms::TypeRegistry::instance().findType("line");
    assert(reg != nullptr);
    assert(reg->category == dbms::TypeCategory::Geometric);
    reg = dbms::TypeRegistry::instance().findType("macaddr8");
    assert(reg != nullptr);
    assert(reg->category == dbms::TypeCategory::Network);
    reg = dbms::TypeRegistry::instance().findType("bit varying");
    assert(reg != nullptr);
    assert(reg->category == dbms::TypeCategory::BitString);
    std::cout << "[CORE-TYPES] registry OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_type_registry_has_new_types();
    test_geometric_network_types();
    test_bit_and_jsonpath();
    std::cout << "[CORE-TYPES] all passed" << std::endl;
    return 0;
}
