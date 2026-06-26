// ============================================================================
// jsonpath type test — Phase 4 Wave 4.13
// jsonpath columns get pragmatic structural validation on INSERT/UPDATE:
// optional strict/lax mode, balanced () and [], no empty [] subscripts, no ".."
// or trailing ".", a valid starting token, no unterminated string. Stored
// verbatim (no canonicalization).
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

static void test_jsonpath_valid() {
    std::string db = "jp_ok";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, p JSONPATH)", s));

    const char* ok[] = {
        "$",
        "$.a.b",
        "$.a[0]",
        "$[*]",
        "$.store.book[*].title",
        "lax $.a.b",
        "strict $.\"key name\"",
        "$ ? (@.price < 10)",
        "$.a ? (@.x == 1 && @.y > 2)",
        "$.**.author",
    };
    int id = 1;
    for (const char* e : ok) {
        assert(g_engine.insert(db, "t", {{"id", std::to_string(id)}, {"p", e}}) == dbms::DBStatus::OK);
        assert(fetchOne(db, "t", {"=id " + std::to_string(id)}, "p") == std::string(e));
        ++id;
    }

    cleanup(db);
    std::cout << "[JSONPATH] valid accepted OK" << std::endl;
}

static void test_jsonpath_invalid() {
    std::string db = "jp_bad";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, p JSONPATH)", s));

    const char* bad[] = {
        ".a",            // no root / dangling accessor
        "$.a.",          // trailing dot
        "$..a",          // double dot
        "$.a[]",         // empty subscript
        "$ ? (@.x > 1",  // unbalanced paren
        "$.a[0",         // unbalanced bracket
        "$.\"oops",      // unterminated string
        ")bad",          // starts with operator
    };
    int id = 1;
    for (const char* e : bad) {
        assert(g_engine.insert(db, "t", {{"id", std::to_string(id)}, {"p", e}}) == dbms::DBStatus::INVALID_VALUE);
        ++id;
    }
    auto rows = g_engine.query(db, "t", {}, {"id"}, {});
    assert(rows.empty());

    cleanup(db);
    std::cout << "[JSONPATH] invalid rejected OK" << std::endl;
}

static void test_jsonpath_update() {
    std::string db = "jp_upd";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, p JSONPATH)", s));

    assert(g_engine.insert(db, "t", {{"id","1"}, {"p","$.a"}}) == dbms::DBStatus::OK);
    assert(g_engine.update(db, "t", {{"p","$.b[*]"}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "p") == "$.b[*]");
    assert(g_engine.update(db, "t", {{"p","$.b["}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);
    assert(fetchOne(db, "t", {"=id 1"}, "p") == "$.b[*]");

    cleanup(db);
    std::cout << "[JSONPATH] update enforce OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_jsonpath_valid();
    test_jsonpath_invalid();
    test_jsonpath_update();
    std::cout << "[JSONPATH] all passed" << std::endl;
    return 0;
}
