// ============================================================================
// XML type test — Phase 4 Wave 4.12
// xml columns validate well-formedness (CONTENT form) on INSERT/UPDATE:
// balanced/nested tags, quoted attributes, self-closing tags, comments, CDATA,
// processing instructions; mismatched/unclosed tags and unquoted attributes
// are rejected. Stored verbatim (no canonicalization).
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
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

static void test_xml_valid() {
    std::string db = testDbPath("xml_ok");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, x XML)", s));

    // Simple element round-trips verbatim.
    assert(g_engine.insert(db, "t", {{"id","1"}, {"x","<a>hello</a>"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "x") == "<a>hello</a>");
    // Nested + attributes + self-closing.
    assert(g_engine.insert(db, "t", {{"id","2"}, {"x","<a id=\"1\"><b/><c>x</c></a>"}}) == dbms::DBStatus::OK);
    // Content fragment (multiple roots) is allowed.
    assert(g_engine.insert(db, "t", {{"id","3"}, {"x","<a/><b/>"}}) == dbms::DBStatus::OK);
    // Plain text content is allowed.
    assert(g_engine.insert(db, "t", {{"id","4"}, {"x","just text"}}) == dbms::DBStatus::OK);
    // Comment, CDATA, processing instruction.
    assert(g_engine.insert(db, "t", {{"id","5"}, {"x","<a><!-- c --><![CDATA[ <x> ]]></a>"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id","6"}, {"x","<?xml version=\"1.0\"?><a>x</a>"}}) == dbms::DBStatus::OK);

    cleanup(db);
    std::cout << "[XML] well-formed accepted OK" << std::endl;
}

static void test_xml_invalid() {
    std::string db = testDbPath("xml_bad");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, x XML)", s));

    // Mismatched tags.
    assert(g_engine.insert(db, "t", {{"id","1"}, {"x","<a></b>"}}) == dbms::DBStatus::INVALID_VALUE);
    // Unclosed tag.
    assert(g_engine.insert(db, "t", {{"id","2"}, {"x","<a>text"}}) == dbms::DBStatus::INVALID_VALUE);
    // Unquoted attribute value.
    assert(g_engine.insert(db, "t", {{"id","3"}, {"x","<a id=1></a>"}}) == dbms::DBStatus::INVALID_VALUE);
    // Stray '<' in text.
    assert(g_engine.insert(db, "t", {{"id","4"}, {"x","a < b"}}) == dbms::DBStatus::INVALID_VALUE);
    // Unterminated comment.
    assert(g_engine.insert(db, "t", {{"id","5"}, {"x","<a><!-- oops</a>"}}) == dbms::DBStatus::INVALID_VALUE);

    auto rows = g_engine.query(db, "t", {}, {"id"}, {});
    assert(rows.empty());

    cleanup(db);
    std::cout << "[XML] malformed rejected OK" << std::endl;
}

static void test_xml_update() {
    std::string db = testDbPath("xml_upd");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, x XML)", s));

    assert(g_engine.insert(db, "t", {{"id","1"}, {"x","<a>1</a>"}}) == dbms::DBStatus::OK);
    // Valid update.
    assert(g_engine.update(db, "t", {{"x","<b>2</b>"}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "x") == "<b>2</b>");
    // Invalid update rejected, row unchanged.
    assert(g_engine.update(db, "t", {{"x","<b>2"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);
    assert(fetchOne(db, "t", {"=id 1"}, "x") == "<b>2</b>");

    cleanup(db);
    std::cout << "[XML] update enforce/reject OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_xml_valid();
    test_xml_invalid();
    test_xml_update();
    std::cout << "[XML] all passed" << std::endl;
    return 0;
}
