// ============================================================================
// Geometric type test — Phase 4 Wave 4.7
// line / lseg / box / path / polygon / circle: string-backed canonical text
// storage with structural validation (coordinate count, char set, brackets)
// and canonicalization (whitespace, box corner ordering, open/closed path).
// `point` keeps its packed-binary representation and is exercised separately.
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

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

static std::string fetchOne(const std::string& db, const std::string& tbl,
                            const std::vector<std::string>& conds,
                            const std::string& col) {
    auto rows = g_engine.query(db, tbl, conds, {col}, {});
    assert(rows.size() == 1);
    std::string r = rows[0];
    while (!r.empty() && (r.back() == ' ' || r.back() == '\n')) r.pop_back();
    return r;
}

static void test_line_lseg_circle() {
    std::string db = "geo_a";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE g (id INT PRIMARY KEY, ln LINE, ls LSEG, c CIRCLE)", s));

    // Whitespace normalized; canonical brackets emitted.
    assert(g_engine.insert(db, "g", {{"id","1"}, {"ln","{1, 2, 3}"}, {"ls","[ (0,0), (1,1) ]"}, {"c","<(0,0), 5>"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "g", {"=id 1"}, "ln") == "{1,2,3}");
    assert(fetchOne(db, "g", {"=id 1"}, "ls") == "[(0,0),(1,1)]");
    assert(fetchOne(db, "g", {"=id 1"}, "c") == "<(0,0),5>");

    // line with A=B=0 invalid; circle negative radius invalid; wrong count invalid.
    assert(g_engine.insert(db, "g", {{"id","2"}, {"ln","{0,0,3}"}, {"ls","[(0,0),(1,1)]"}, {"c","<(0,0),5>"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "g", {{"id","3"}, {"ln","{1,2,3}"}, {"ls","[(0,0),(1,1)]"}, {"c","<(0,0),-5>"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "g", {{"id","4"}, {"ln","{1,2,3}"}, {"ls","[(0,0),(1,1),(2,2)]"}, {"c","<(0,0),5>"}}) == dbms::DBStatus::INVALID_VALUE);
    // Garbage characters rejected.
    assert(g_engine.insert(db, "g", {{"id","5"}, {"ln","{1,2,x}"}, {"ls","[(0,0),(1,1)]"}, {"c","<(0,0),5>"}}) == dbms::DBStatus::INVALID_VALUE);

    auto rows = g_engine.query(db, "g", {}, {"id"}, {});
    assert(rows.size() == 1);
    cleanup(db);
    std::cout << "[GEO] line/lseg/circle OK" << std::endl;
}

static void test_box_corner_ordering() {
    std::string db = "geo_box";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE g (id INT PRIMARY KEY, b BOX)", s));

    // Corners reorder to (high-right),(low-left) regardless of input order.
    assert(g_engine.insert(db, "g", {{"id","1"}, {"b","(0,0),(2,3)"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "g", {"=id 1"}, "b") == "(2,3),(0,0)");
    assert(g_engine.insert(db, "g", {{"id","2"}, {"b","(2,3),(0,0)"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "g", {"=id 2"}, "b") == "(2,3),(0,0)");

    cleanup(db);
    std::cout << "[GEO] box corner ordering OK" << std::endl;
}

static void test_path_polygon() {
    std::string db = "geo_path";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE g (id INT PRIMARY KEY, po PATH, pc PATH, pg POLYGON)", s));

    // Open path preserves '[...]'; closed path and polygon use '(...)'.
    assert(g_engine.insert(db, "g", {{"id","1"},
        {"po","[(0,0),(1,1),(2,0)]"},
        {"pc","((0,0),(1,1),(2,0))"},
        {"pg","((0,0),(4,0),(2,3))"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "g", {"=id 1"}, "po") == "[(0,0),(1,1),(2,0)]");
    assert(fetchOne(db, "g", {"=id 1"}, "pc") == "((0,0),(1,1),(2,0))");
    assert(fetchOne(db, "g", {"=id 1"}, "pg") == "((0,0),(4,0),(2,3))");

    // Odd number of coordinates rejected.
    assert(g_engine.insert(db, "g", {{"id","2"},
        {"po","[(0,0),(1,1),(2)]"},
        {"pc","((0,0),(1,1))"},
        {"pg","((0,0),(1,1),(2,2))"}}) == dbms::DBStatus::INVALID_VALUE);

    cleanup(db);
    std::cout << "[GEO] path/polygon open/closed OK" << std::endl;
}

static void test_geo_update() {
    std::string db = "geo_upd";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE g (id INT PRIMARY KEY, b BOX)", s));

    assert(g_engine.insert(db, "g", {{"id","1"}, {"b","(0,0),(2,2)"}}) == dbms::DBStatus::OK);
    // Valid update canonicalizes.
    assert(g_engine.update(db, "g", {{"b","(1,1),(5,5)"}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "g", {"=id 1"}, "b") == "(5,5),(1,1)");
    // Invalid update rejected, row unchanged.
    assert(g_engine.update(db, "g", {{"b","(1,1)"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);
    assert(fetchOne(db, "g", {"=id 1"}, "b") == "(5,5),(1,1)");

    cleanup(db);
    std::cout << "[GEO] update enforce/canonicalize OK" << std::endl;
}

static void test_point_still_works() {
    std::string db = "geo_point";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE g (id INT PRIMARY KEY, p POINT)", s));

    // point keeps its binary representation; "x,y" round-trips.
    assert(g_engine.insert(db, "g", {{"id","1"}, {"p","3,4"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "g", {"=id 1"}, "p") == "3,4");

    cleanup(db);
    std::cout << "[GEO] point binary unaffected OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_line_lseg_circle();
    test_box_corner_ordering();
    test_path_polygon();
    test_geo_update();
    test_point_still_works();
    std::cout << "[GEO] all passed" << std::endl;
    return 0;
}
