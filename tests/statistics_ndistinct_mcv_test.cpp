// ============================================================================
// CREATE STATISTICS ndistinct / mcv test — Phase 4 Wave 4.32b
// Covers StorageEngine::computeNDistinct (per-column + full-group distinct
// counts) and computeMCVCombinations (most-common value combinations), the
// two extended-statistics kinds that complement computeFunctionalDependencies.
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

static void cleanup(const std::string& db) { if (fs::exists(db)) fs::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static void seed(const std::string& db) {
    // (NYC,10001) x2, (LA,90001), (SF,94101)
    assert(g_engine.insert(db, "t", {{"city", "NYC"}, {"zip", "10001"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"city", "NYC"}, {"zip", "10001"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"city", "LA"},  {"zip", "90001"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"city", "SF"},  {"zip", "94101"}}) == dbms::DBStatus::OK);
}

static void test_ndistinct() {
    std::string db = "stat_nd";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (city VARCHAR(10), zip VARCHAR(10))", s));
    seed(db);

    auto nd = g_engine.computeNDistinct(db, "t", {"city", "zip"});
    assert(nd["city"] == 3);            // NYC, LA, SF
    assert(nd["zip"] == 3);             // 10001, 90001, 94101
    assert(nd["city, zip"] == 3);       // the duplicate (NYC,10001) collapses

    // < 2 columns or unknown column -> empty.
    assert(g_engine.computeNDistinct(db, "t", {"city"}).empty());
    assert(g_engine.computeNDistinct(db, "t", {"city", "nope"}).empty());

    cleanup(db);
    std::cout << "[STATFN] ndistinct OK" << std::endl;
}

static void test_mcv() {
    std::string db = "stat_mcv";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (city VARCHAR(10), zip VARCHAR(10))", s));
    seed(db);

    auto mcv = g_engine.computeMCVCombinations(db, "t", {"city", "zip"}, 5);
    assert(mcv.size() == 3);                       // 3 distinct combinations
    assert(mcv[0].first == "(NYC, 10001)");        // most common
    assert(mcv[0].second == 2);
    // Remaining two each occur once; ties are ordered by display string.
    assert(mcv[1].second == 1 && mcv[2].second == 1);

    // topN truncation.
    auto top1 = g_engine.computeMCVCombinations(db, "t", {"city", "zip"}, 1);
    assert(top1.size() == 1 && top1[0].first == "(NYC, 10001)");

    cleanup(db);
    std::cout << "[STATFN] mcv OK" << std::endl;
}

static void test_guards() {
    std::string db = "stat_guard";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (city VARCHAR(10), zip VARCHAR(10))", s));
    // Empty table -> empty results.
    assert(g_engine.computeNDistinct(db, "t", {"city", "zip"}).empty());
    assert(g_engine.computeMCVCombinations(db, "t", {"city", "zip"}, 5).empty());
    // Missing table.
    assert(g_engine.computeNDistinct(db, "ghost", {"a", "b"}).empty());
    cleanup(db);
    std::cout << "[STATFN] guards OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_ndistinct();
    test_mcv();
    test_guards();
    std::cout << "[STATFN] all passed" << std::endl;
    return 0;
}
