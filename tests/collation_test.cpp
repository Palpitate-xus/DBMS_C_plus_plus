#include "catalog/collation.h"
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

static void test_collation_provider() {
    using namespace dbms::collation;
    assert(normalizeName("C") == "c");
    assert(normalizeName("'POSIX'") == "posix");
    assert(normalizeName("en_US.UTF-8") == "en_us.utf8");
    assert(normalizeName("NOCASE") == "nocase");
    assert(isValid("C"));
    assert(isValid("POSIX"));
    assert(isValid("en_US.utf8"));
    assert(isValid("nocase"));
    assert(!isValid("nonexistent_collation"));
    assert(listBuiltins().size() >= 8);

    assert(compare("abc", "abc", "C") == 0);
    assert(compare("abc", "abc", "POSIX") == 0);
    assert(compare("abc", "abd", "C") < 0);
    assert(compare("abd", "abc", "C") > 0);
    // binary: 'B' < 'a'
    assert(compare("B", "a", "C") < 0);
    // nocase: 'B' == 'b'
    assert(compare("B", "b", "nocase") == 0);
    assert(compare("Apple", "apricot", "nocase") < 0);
    // reverse
    assert(compare("abc", "abd", "reverse") > 0);
    // Unknown collation falls back to binary
    assert(compare("abc", "abc", "zh_TW.utf8") == 0);
    std::cout << "[COLLATION] provider OK" << std::endl;
}

static void test_collate_persists_and_applies() {
    std::string db = "collation_t1";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    // Column 'a' uses C collation; 'b' uses nocase.
    bool err = ddl.executeSql(
        "CREATE TABLE t (a VARCHAR(50) COLLATE C, b VARCHAR(50) COLLATE nocase)", s);
    assert(!err);

    dbms::TableSchema schema = g_engine.getTableSchema(db, "t");
    assert(schema.len == 2);
    assert(schema.cols[0].collation == "C");
    assert(schema.cols[1].collation == "nocase" || schema.cols[1].collation == "NOCASE");

    assert(g_engine.insert(db, "t", {{"a", "hello"}, {"b", "World"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"a", "Hello"}, {"b", "world"}}) == dbms::DBStatus::OK);

    // nocase equality: b='world' should match both rows (collation-aware comparison).
    auto rows = g_engine.query(db, "t", {"=b world"}, {"a", "b"});
    assert(rows.size() == 2);
    std::cout << "[COLLATION] nocase where-as-equality OK" << std::endl;

    // collation-aware range
    rows = g_engine.query(db, "t", {"<b WORLD"}, {"a", "b"});
    assert(rows.empty());

    cleanup(db);
    std::cout << "[COLLATION] persists & applies OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_collation_provider();
    test_collate_persists_and_applies();
    std::cout << "[COLLATION] all passed" << std::endl;
    return 0;
}