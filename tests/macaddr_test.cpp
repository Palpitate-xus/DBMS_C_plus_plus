// ============================================================================
// MAC address type test — Phase 4 Wave 4.8 (macaddr / macaddr8)
// Verifies binary fixed-width storage with normalization on INSERT/UPDATE,
// canonical lowercase colon output on read, and rejection of malformed input.
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
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

// Fetch the single value of column `col` for the (only) matching row.
static std::string fetchOne(const std::string& db, const std::string& tbl,
                            const std::vector<std::string>& conds,
                            const std::string& col) {
    auto rows = g_engine.query(db, tbl, conds, {col}, {});
    assert(rows.size() == 1);
    std::string r = rows[0];
    while (!r.empty() && (r.back() == ' ' || r.back() == '\n')) r.pop_back();
    return r;
}

// macaddr accepts colon / hyphen / dot / bare-hex forms; all canonicalize to
// lowercase colon-separated output.
static void test_macaddr_input_formats() {
    std::string db = testDbPath("macaddr_fmt");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE m (id INT PRIMARY KEY, a macaddr)", s));

    // id 1..5 carry the same address in five accepted notations.
    assert(g_engine.insert(db, "m", {{"id", "1"}, {"a", "08:00:2b:01:02:03"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "m", {{"id", "2"}, {"a", "08-00-2b-01-02-03"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "m", {{"id", "3"}, {"a", "0800.2b01.0203"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "m", {{"id", "4"}, {"a", "08002b010203"}}) == dbms::DBStatus::OK);
    // Mixed case input normalizes to lowercase.
    assert(g_engine.insert(db, "m", {{"id", "5"}, {"a", "08:00:2B:01:02:03"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "m", {{"id", "6"}, {"a", "00:00:00:00:00:00"}}) == dbms::DBStatus::OK);

    const std::string canon = "08:00:2b:01:02:03";
    for (int id = 1; id <= 5; ++id) {
        std::string got = fetchOne(db, "m", {"=id " + std::to_string(id)}, "a");
        assert(got == canon);
    }
    assert(fetchOne(db, "m", {"=id 6"}, "a") == "00:00:00:00:00:00");

    cleanup(db);
    std::cout << "[MACADDR] input formats normalize OK" << std::endl;
}

// Malformed input (bad hex digit, wrong length) is rejected at INSERT.
static void test_macaddr_invalid() {
    std::string db = testDbPath("macaddr_bad");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE m (id INT PRIMARY KEY, a macaddr)", s));

    // Non-hex digit.
    assert(g_engine.insert(db, "m", {{"id", "1"}, {"a", "zz:00:2b:01:02:03"}}) == dbms::DBStatus::INVALID_VALUE);
    // Too few bytes (5).
    assert(g_engine.insert(db, "m", {{"id", "2"}, {"a", "08:00:2b:01:02"}}) == dbms::DBStatus::INVALID_VALUE);
    // Too many bytes (7).
    assert(g_engine.insert(db, "m", {{"id", "3"}, {"a", "08:00:2b:01:02:03:04"}}) == dbms::DBStatus::INVALID_VALUE);
    // PostgreSQL permits whitespace before a complete hexadecimal octet.
    assert(g_engine.insert(db, "m", {{"id", "4"}, {"a", "08:00:2b: 01:02:03"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "m", {"=id 4"}, "a") == "08:00:2b:01:02:03");
    assert(g_engine.insert(db, "m", {{"id", "5"}, {"a", "08:00-2b:01:02:03"}}) == dbms::DBStatus::INVALID_VALUE);

    // Only the valid whitespace input was stored.
    auto rows = g_engine.query(db, "m", {}, {"id"}, {});
    assert(rows.size() == 1);

    cleanup(db);
    std::cout << "[MACADDR] invalid input rejected OK" << std::endl;
}

// macaddr8 stores 8 bytes and round-trips canonical form.
static void test_macaddr8() {
    std::string db = testDbPath("macaddr8_t");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE m (id INT PRIMARY KEY, a macaddr8)", s));

    assert(g_engine.insert(db, "m", {{"id", "1"}, {"a", "08-00-2b-01-02-03-04-05"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "m", {"=id 1"}, "a") == "08:00:2b:01:02:03:04:05");

    // A six-byte input is a genuine EUI-48 value and expands with FF:FE.
    assert(g_engine.insert(db, "m", {{"id", "2"}, {"a", "08:00:2b:01:02:03"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "m", {"=id 2"}, "a") == "08:00:2b:ff:fe:01:02:03");

    cleanup(db);
    std::cout << "[MACADDR] macaddr8 8-byte OK" << std::endl;
}

// UPDATE normalizes valid input and rejects malformed input.
static void test_macaddr_update() {
    std::string db = testDbPath("macaddr_upd");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE m (id INT PRIMARY KEY, a macaddr)", s));

    assert(g_engine.insert(db, "m", {{"id", "1"}, {"a", "08:00:2b:01:02:03"}}) == dbms::DBStatus::OK);

    // Valid update in hyphen form normalizes.
    assert(g_engine.update(db, "m", {{"a", "AA-BB-CC-DD-EE-FF"}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "m", {"=id 1"}, "a") == "aa:bb:cc:dd:ee:ff");

    // Invalid update is rejected and leaves the row unchanged.
    assert(g_engine.update(db, "m", {{"a", "nothex"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);
    assert(fetchOne(db, "m", {"=id 1"}, "a") == "aa:bb:cc:dd:ee:ff");

    cleanup(db);
    std::cout << "[MACADDR] update normalize/reject OK" << std::endl;
}

// A table mixing a variable-length column with macaddr exercises the
// mixed (fixedData) buildRowBuffer encode path.
static void test_macaddr_with_varlen() {
    std::string db = testDbPath("macaddr_var");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE m (id INT PRIMARY KEY, name VARCHAR(20), a macaddr)", s));

    assert(g_engine.insert(db, "m", {{"id", "1"}, {"name", "host"}, {"a", "DE:AD:BE:EF:00:01"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "m", {"=id 1"}, "a") == "de:ad:be:ef:00:01");
    assert(fetchOne(db, "m", {"=id 1"}, "name") == "host");

    cleanup(db);
    std::cout << "[MACADDR] mixed varlen encode OK" << std::endl;
}

static void test_typed_macaddr_casts() {
    struct Case {
        const char* source;
        const char* value;
        bool null;
        const char* target;
        const char* expected;
        const char* state;
    };
    const Case cases[] = {
        {"text","08:00:2b:01:02:03",false,"macaddr","08:00:2b:01:02:03",""},
        {"text","08:00:2b:01:02:03",false,"macaddr8","08:00:2b:ff:fe:01:02:03",""},
        {"macaddr","08:00:2b:01:02:03",false,"macaddr8","08:00:2b:ff:fe:01:02:03",""},
        {"macaddr8","08:00:2b:ff:fe:01:02:03",false,"macaddr","08:00:2b:01:02:03",""},
        {"macaddr8","08:00:2b:01:02:03:04:05",false,"macaddr","","22003"},
        {"text","100:00:2b:01:02:03",false,"macaddr","","22003"},
        {"text","08:00-2b:01:02:03",false,"macaddr","","22P02"},
        {"text","08:00:2b:01:02:03",false,"pg_catalog.\"macaddr8\"","08:00:2b:ff:fe:01:02:03",""},
        {"integer","1",false,"macaddr","","42846"},
        {"integer","",true,"macaddr8","","42846"},
        {"macaddr8","",true,"macaddr","",""},
        {"text","",true,"macaddr8","",""},
    };
    dbms::ExprEvaluator evaluator;
    for (const auto& item : cases) {
        dbms::CastExpr cast;
        cast.typeName=item.target;
        auto parameter=std::make_unique<dbms::ParameterExpr>();
        parameter->slot=0; parameter->declaredType=item.source;
        cast.operand=std::move(parameter);
        dbms::RowContext row;
        row.setParameters({dbms::ExprValue(item.source,item.value,item.null)});
        std::string state;
        try {
            const auto result=evaluator.eval(&cast,row);
            assert(result.isNull==item.null);
            assert(result.value==item.expected);
        } catch(const dbms::DbError& error) { state=error.sqlState(); }
        assert(state==item.state);
    }
    const auto same=dbms::ExprEvaluator::resolveComparison("=","macaddr","macaddr");
    assert(same.strict && !same.identity.empty());
    bool ambiguous=false;
    try { (void)dbms::ExprEvaluator::resolveComparison("=","macaddr","macaddr8"); }
    catch(const dbms::DbError& error) { ambiguous=error.sqlState()=="42725"; }
    assert(ambiguous);
    std::cout << "[MACADDR] typed CAST values/NULL/errors and binding OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_macaddr_input_formats();
    test_macaddr_invalid();
    test_macaddr8();
    test_macaddr_update();
    test_macaddr_with_varlen();
    test_typed_macaddr_casts();
    std::cout << "[MACADDR] all passed" << std::endl;
    return 0;
}
