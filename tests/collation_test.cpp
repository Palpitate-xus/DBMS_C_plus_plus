#include "catalog/collation.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <set>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

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
    std::string db = testDbPath("collation_t1");
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

static void test_collate_all_operators() {
    std::string db = testDbPath("collation_t2");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (c VARCHAR(50) COLLATE nocase)", s));
    assert(g_engine.insert(db, "t", {{"c", "Apple"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"c", "banana"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"c", "CHERRY"}}) == dbms::DBStatus::OK);

    // Equality under nocase.
    auto rows = g_engine.query(db, "t", {"=c apple"}, {"c"});
    assert(rows.size() == 1);

    // Inequality under nocase.
    rows = g_engine.query(db, "t", {"!=c apple"}, {"c"});
    assert(rows.size() == 2);

    // Less-than under nocase: order is apple < banana < cherry.
    rows = g_engine.query(db, "t", {"<c cherry"}, {"c"});
    assert(rows.size() == 2); // Apple and banana

    // Less-than-or-equal under nocase.
    rows = g_engine.query(db, "t", {"<=c cherry"}, {"c"});
    assert(rows.size() == 3); // Apple, banana and CHERRY

    // Greater-than under nocase.
    rows = g_engine.query(db, "t", {">c apple"}, {"c"});
    assert(rows.size() == 2); // banana and CHERRY

    // Greater-than-or-equal under nocase.
    rows = g_engine.query(db, "t", {">=c banana"}, {"c"});
    assert(rows.size() == 2); // banana and CHERRY

    cleanup(db);
    std::cout << "[COLLATION] all operators OK" << std::endl;
}

static void test_collate_mixed_columns() {
    std::string db = testDbPath("collation_t3");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    // 'bin' is binary (C); 'nc' is nocase; 'rev' is reverse.
    assert(!ddl.executeSql(
        "CREATE TABLE t (bin VARCHAR(50) COLLATE C, nc VARCHAR(50) COLLATE nocase, rev VARCHAR(50) COLLATE reverse)", s));

    assert(g_engine.insert(db, "t", {{"bin", "a"}, {"nc", "a"}, {"rev", "a"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"bin", "A"}, {"nc", "A"}, {"rev", "A"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"bin", "b"}, {"nc", "b"}, {"rev", "b"}}) == dbms::DBStatus::OK);

    // Binary column: 'A' is not 'a'.
    auto rows = g_engine.query(db, "t", {"=bin a"}, {"bin", "nc", "rev"});
    assert(rows.size() == 1);

    // Nocase column: 'A' and 'a' both match.
    rows = g_engine.query(db, "t", {"=nc a"}, {"bin", "nc", "rev"});
    assert(rows.size() == 2);

    // Reverse column: ordering is inverted. 'a' > 'b' under reverse, so 'b' < 'a'.
    rows = g_engine.query(db, "t", {"<rev a"}, {"bin", "nc", "rev"});
    assert(rows.size() == 1); // only 'b' is less than 'a' in reverse order

    cleanup(db);
    std::cout << "[COLLATION] mixed columns OK" << std::endl;
}

static void test_collate_binary_collations() {
    using namespace dbms::collation;
    assert(isBinary(""));
    assert(isBinary("default"));
    assert(isBinary("C"));
    assert(isBinary("POSIX"));
    assert(isBinary("ucs_basic"));
    assert(!isBinary("nocase"));
    assert(!isBinary("reverse"));
    assert(!isBinary("en_US.utf8"));

    std::string db = testDbPath("collation_t4");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE t (c_default VARCHAR(50), c_c VARCHAR(50) COLLATE C, c_posix VARCHAR(50) COLLATE POSIX, c_ucs VARCHAR(50) COLLATE ucs_basic)", s));

    dbms::TableSchema schema = g_engine.getTableSchema(db, "t");
    assert(schema.len == 4);

    // No explicit collation -> empty string -> binary.
    assert(schema.cols[0].collation.empty());
    assert(schema.cols[1].collation == "C");
    assert(schema.cols[2].collation == "POSIX" || schema.cols[2].collation == "posix");
    assert(schema.cols[3].collation == "ucs_basic" || schema.cols[3].collation == "UCS_BASIC");

    assert(g_engine.insert(db, "t", {{"c_default", "x"}, {"c_c", "x"}, {"c_posix", "x"}, {"c_ucs", "x"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"c_default", "X"}, {"c_c", "X"}, {"c_posix", "X"}, {"c_ucs", "X"}}) == dbms::DBStatus::OK);

    // All binary collations: lowercase 'x' is not uppercase 'X'.
    auto rows = g_engine.query(db, "t", {"=c_default x"}, {"c_default"});
    assert(rows.size() == 1);
    rows = g_engine.query(db, "t", {"=c_c x"}, {"c_c"});
    assert(rows.size() == 1);
    rows = g_engine.query(db, "t", {"=c_posix x"}, {"c_posix"});
    assert(rows.size() == 1);
    rows = g_engine.query(db, "t", {"=c_ucs x"}, {"c_ucs"});
    assert(rows.size() == 1);

    cleanup(db);
    std::cout << "[COLLATION] binary collations OK" << std::endl;
}

static void test_collate_with_index() {
    std::string db = testDbPath("collation_t5");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (c VARCHAR(50) COLLATE nocase)", s));
    assert(g_engine.insert(db, "t", {{"c", "Hello"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"c", "HELLO"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"c", "world"}}) == dbms::DBStatus::OK);

    // Create a B-tree index on the nocase column. The engine must skip it for
    // non-binary collations and fall back to a full scan, otherwise binary key
    // lookup would miss case-differing values.
    assert(!ddl.executeSql("CREATE INDEX idx ON t(c)", s));

    auto rows = g_engine.query(db, "t", {"=c hello"}, {"c"});
    assert(rows.size() == 2); // Hello + HELLO

    rows = g_engine.query(db, "t", {"=c world"}, {"c"});
    assert(rows.size() == 1);

    // Range on a nocase column must also be collation-aware.
    rows = g_engine.query(db, "t", {">c hello"}, {"c"});
    assert(rows.size() == 1); // world only (HELLO == hello)

    cleanup(db);
    std::cout << "[COLLATION] index fallback OK" << std::endl;
}

static void test_collate_schema_persistence() {
    std::string db = testDbPath("collation_t6");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE t (a VARCHAR(50) COLLATE nocase, b VARCHAR(50) COLLATE C)", s));

    // Re-read the schema through the engine; this exercises the on-disk
    // schema serialization/deserialization path.
    dbms::TableSchema schema = g_engine.getTableSchema(db, "t");
    assert(schema.cols[0].collation == "nocase" || schema.cols[0].collation == "NOCASE");
    assert(schema.cols[1].collation == "C");

    cleanup(db);
    std::cout << "[COLLATION] schema persistence OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_collation_provider();
    test_collate_persists_and_applies();
    test_collate_all_operators();
    test_collate_mixed_columns();
    test_collate_binary_collations();
    test_collate_with_index();
    test_collate_schema_persistence();
    std::cout << "[COLLATION] all passed" << std::endl;
    return 0;
}
