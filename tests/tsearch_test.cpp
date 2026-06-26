// ============================================================================
// Full-text search type test — Phase 4 Wave 4.10 (tsvector / tsquery)
// tsvector: parse + canonicalize (sort lexemes length-first, dedup, merge/sort
//   positions, omit default weight D, single-quote lexemes).
// tsquery: validate the boolean grammar (! > <-> > & > |, parens, lexeme flags).
// INSERT and UPDATE enforce both; malformed input is rejected.
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

static void test_tsvector_canonical() {
    std::string db = "tsv";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, v TSVECTOR)", s));

    // Bare words quoted; sorted length-first then bytewise.
    assert(g_engine.insert(db, "t", {{"id","1"}, {"v","bb a ccc"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "v") == "'a' 'bb' 'ccc'");
    // Duplicate lexemes merged; positions sorted/deduped.
    assert(g_engine.insert(db, "t", {{"id","2"}, {"v","'cat':3,1 'cat':2 'cat':1"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 2"}, "v") == "'cat':1,2,3");
    // Default weight D omitted; explicit A/B kept.
    assert(g_engine.insert(db, "t", {{"id","3"}, {"v","'x':1A,2D 'y':3B"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 3"}, "v") == "'x':1A,2 'y':3B");
    // Lexeme with and without positions merges to positioned.
    assert(g_engine.insert(db, "t", {{"id","4"}, {"v","'w' 'w':5"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 4"}, "v") == "'w':5");

    // Invalid: position 0, bad weight, empty.
    assert(g_engine.insert(db, "t", {{"id","5"}, {"v","'a':0"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id","6"}, {"v","'a':1Z"}}) == dbms::DBStatus::INVALID_VALUE);

    cleanup(db);
    std::cout << "[TS] tsvector canonicalization OK" << std::endl;
}

static void test_tsquery_valid() {
    std::string db = "tsq";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, q TSQUERY)", s));

    // Valid boolean queries (stored verbatim).
    assert(g_engine.insert(db, "t", {{"id","1"}, {"q","cat & dog"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "q") == "cat & dog");
    assert(g_engine.insert(db, "t", {{"id","2"}, {"q","cat & (dog | !bird)"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id","3"}, {"q","'fat' <-> 'cat' & rat:*"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id","4"}, {"q","a <2> b"}}) == dbms::DBStatus::OK);

    // Invalid: trailing operator, empty parens, missing operand, unbalanced parens.
    assert(g_engine.insert(db, "t", {{"id","5"}, {"q","cat &"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id","6"}, {"q","cat dog"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id","7"}, {"q","(cat | dog"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id","8"}, {"q","()"}}) == dbms::DBStatus::INVALID_VALUE);

    cleanup(db);
    std::cout << "[TS] tsquery validation OK" << std::endl;
}

static void test_ts_update() {
    std::string db = "ts_upd";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, v TSVECTOR, q TSQUERY)", s));

    assert(g_engine.insert(db, "t", {{"id","1"}, {"v","'a':1"}, {"q","a & b"}}) == dbms::DBStatus::OK);
    // Valid update canonicalizes tsvector.
    assert(g_engine.update(db, "t", {{"v","z a"}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "v") == "'a' 'z'");
    // Invalid tsquery update rejected.
    assert(g_engine.update(db, "t", {{"q","a |"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);
    assert(fetchOne(db, "t", {"=id 1"}, "q") == "a & b");

    cleanup(db);
    std::cout << "[TS] update enforce OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_tsvector_canonical();
    test_tsquery_valid();
    test_ts_update();
    std::cout << "[TS] all passed" << std::endl;
    return 0;
}
