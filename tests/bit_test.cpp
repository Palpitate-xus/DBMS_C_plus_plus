// ============================================================================
// Bit string type test — Phase 4 Wave 4.9 (bit / bit varying)
// Verifies '0'/'1' string storage with length enforcement (bit(n) exact,
// bit varying(n) at most n), 0/1 validation, B'...' literal stripping, and
// canonical round-trip on INSERT/UPDATE.
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include "expression/ExprEvaluator.h"
#include "parser/parser.h"
#include "common/DbError.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

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

// bit(n) requires exactly n bits; only 0/1 allowed; B'...' wrapper stripped.
static void test_bit_fixed() {
    std::string db = testDbPath("bit_fixed");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE b (id INT PRIMARY KEY, v BIT(8))", s));

    // Exact 8 bits round-trips unchanged.
    assert(g_engine.insert(db, "b", {{"id", "1"}, {"v", "10101010"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "b", {"=id 1"}, "v") == "10101010");

    // B'...' literal wrapper is stripped to the bare string.
    assert(g_engine.insert(db, "b", {{"id", "2"}, {"v", "B'11110000'"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "b", {"=id 2"}, "v") == "11110000");

    // Wrong length (3 bits into bit(8)) is rejected.
    assert(g_engine.insert(db, "b", {{"id", "3"}, {"v", "101"}}) == dbms::DBStatus::INVALID_VALUE);
    // Non 0/1 digit is rejected.
    assert(g_engine.insert(db, "b", {{"id", "4"}, {"v", "10102010"}}) == dbms::DBStatus::INVALID_VALUE);

    // Only the two valid rows exist.
    auto rows = g_engine.query(db, "b", {}, {"id"}, {});
    assert(rows.size() == 2);

    cleanup(db);
    std::cout << "[BIT] bit(n) exact length OK" << std::endl;
}

// bit varying(n) permits up to n bits.
static void test_bit_varying() {
    std::string db = testDbPath("bit_var");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE b (id INT PRIMARY KEY, v BIT VARYING(8))", s));

    // Shorter than max is allowed (unlike fixed bit).
    assert(g_engine.insert(db, "b", {{"id", "1"}, {"v", "101"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "b", {"=id 1"}, "v") == "101");

    // Exactly max allowed.
    assert(g_engine.insert(db, "b", {{"id", "2"}, {"v", "11111111"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "b", {"=id 2"}, "v") == "11111111");

    // Longer than max rejected.
    assert(g_engine.insert(db, "b", {{"id", "3"}, {"v", "111111111"}}) == dbms::DBStatus::INVALID_VALUE);
    // Bad digit rejected.
    assert(g_engine.insert(db, "b", {{"id", "4"}, {"v", "12"}}) == dbms::DBStatus::INVALID_VALUE);

    cleanup(db);
    std::cout << "[BIT] bit varying(n) max length OK" << std::endl;
}

// VARBIT alias and the unconstrained varbit form.
static void test_varbit_alias() {
    std::string db = testDbPath("bit_alias");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE b (id INT PRIMARY KEY, v VARBIT(4))", s));

    dbms::TableSchema tbl = g_engine.getTableSchema(db, "b");
    bool found = false;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == "v") {
            found = true;
            assert(tbl.cols[i].dataType == "bit varying");
            assert(tbl.cols[i].dsize == 4);
        }
    }
    assert(found);

    assert(g_engine.insert(db, "b", {{"id", "1"}, {"v", "1010"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "b", {"=id 1"}, "v") == "1010");
    assert(g_engine.insert(db, "b", {{"id", "2"}, {"v", "10101"}}) == dbms::DBStatus::INVALID_VALUE);

    cleanup(db);
    std::cout << "[BIT] VARBIT alias OK" << std::endl;
}

// UPDATE enforces the same rules and canonicalizes.
static void test_bit_update() {
    std::string db = testDbPath("bit_upd");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE b (id INT PRIMARY KEY, v BIT(4))", s));

    assert(g_engine.insert(db, "b", {{"id", "1"}, {"v", "1010"}}) == dbms::DBStatus::OK);

    // Valid update with B'...' wrapper canonicalizes.
    assert(g_engine.update(db, "b", {{"v", "B'0011'"}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "b", {"=id 1"}, "v") == "0011");

    // Wrong length update rejected, row unchanged.
    assert(g_engine.update(db, "b", {{"v", "111"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);
    assert(fetchOne(db, "b", {"=id 1"}, "v") == "0011");

    cleanup(db);
    std::cout << "[BIT] update enforce/canonicalize OK" << std::endl;
}

static void test_default_typmods_and_empty_varbit() {
    std::string db = testDbPath("bit_defaults");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE defaults (id INT PRIMARY KEY, fixed BIT, free VARBIT)",
        s));
    const dbms::TableSchema table = g_engine.getTableSchema(db, "defaults");
    const auto findColumn = [&table](const std::string& name) {
        for (size_t i = 0; i < table.len; ++i) {
            if (table.cols[i].dataName == name) return &table.cols[i];
        }
        return static_cast<const dbms::Column*>(nullptr);
    };
    const dbms::Column* fixed = findColumn("fixed");
    const dbms::Column* free = findColumn("free");
    assert(fixed && fixed->dsize == 1);
    assert(free && free->dsize == 8388608);
    assert(g_engine.insert(
               db, "defaults", {{"id", "1"}, {"fixed", "1"}, {"free", ""}}) ==
           dbms::DBStatus::OK);
    assert(fetchOne(db, "defaults", {"=id 1"}, "free").empty());
    assert(g_engine.insert(
               db, "defaults", {{"id", "2"}, {"fixed", "10"}, {"free", "1"}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(ddl.executeSql("CREATE TABLE bad_zero (v BIT(0))", s));
    assert(ddl.executeSql("CREATE TABLE bad_many (v VARBIT(1,2))", s));
    assert(!g_engine.tableExists(db, "bad_zero"));
    assert(!g_engine.tableExists(db, "bad_many"));

    cleanup(db);
    std::cout << "[BIT] default typmods/empty varbit OK" << std::endl;
}

static void test_literals_casts_operators_and_functions() {
    dbms::SQLParser parser;
    const auto parsed = parser.parse(
        "SELECT B'1010', X'aF', B'1010' & B'1100', "
        "B'1010' | B'0101', B'1010' # B'1100', "
        "B'1010' << 2, B'1010' >> 2, ~B'0011', "
        "B'10'::bit(4), B'1010'::bit varying(2), "
        "get_bit(B'1010', 0), set_bit(B'1010', 1, 0), "
        "bit_count(B'101101'), bit_length(B'101'), octet_length(B'1011111011'), "
        "substring(B'10101', 2, 3)");
    assert(parsed.success);
    const auto* select = dynamic_cast<const dbms::SelectStmt*>(parsed.stmt.get());
    assert(select && select->selectList.size() == 16);
    const std::vector<std::string> expected = {
        "1010", "10101111", "1000", "1111", "0110",
        "1000", "0010", "1100", "1000", "10",
        "1", "1010", "4", "3", "2", "010"};
    dbms::ExprEvaluator evaluator;
    for (size_t i = 0; i < expected.size(); ++i) {
        const dbms::ExprValue value = evaluator.eval(
            select->selectList[i].expr.get(), {});
        assert(!value.isNull && value.value == expected[i]);
    }

    const auto mismatched = parser.parse("SELECT B'1' & B'00'");
    assert(mismatched.success);
    const auto* mismatchSelect =
        dynamic_cast<const dbms::SelectStmt*>(mismatched.stmt.get());
    bool rejected = false;
    try {
        (void)evaluator.eval(mismatchSelect->selectList[0].expr.get(), {});
    } catch (const dbms::DbError& error) {
        rejected = error.sqlState() == "22026";
    }
    assert(rejected);
    std::cout << "[BIT] literals/casts/operators/functions OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_bit_fixed();
    test_bit_varying();
    test_varbit_alias();
    test_bit_update();
    test_default_typmods_and_empty_varbit();
    test_literals_casts_operators_and_functions();
    std::cout << "[BIT] all passed" << std::endl;
    return 0;
}
