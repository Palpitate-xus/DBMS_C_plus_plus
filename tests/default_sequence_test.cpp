#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
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

static std::string trimRight(const std::string& s) {
    size_t end = s.size();
    while (end > 0 && (s[end - 1] == ' ' || s[end - 1] == '\t' || s[end - 1] == '\n' || s[end - 1] == '\r')) --end;
    return s.substr(0, end);
}

static void test_default_nextval() {
    std::string db = "seq_default";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::setCurrentSession(&s);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE SEQUENCE s1", s));
    assert(!ddl.executeSql("CREATE TABLE t1 (id INT DEFAULT nextval('s1'), name VARCHAR(20))", s));

    assert(g_engine.insert(db, "t1", {{"name", "a"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t1", {{"name", "b"}}) == dbms::DBStatus::OK);

    auto rows = g_engine.query(db, "t1", {}, {"id", "name"});
    assert(rows.size() == 2);
    assert(trimRight(rows[0]) == "1 a");
    assert(trimRight(rows[1]) == "2 b");

    // currval reflects the last nextval obtained in this session
    assert(s.sequenceLastValues["s1"] == 2);
    assert(g_engine.currval(db, "s1") == 2);

    cleanup(db);
    std::cout << "[DEFAULT_SEQUENCE] nextval default OK" << std::endl;
}

static void test_default_expression_with_parens() {
    std::string db = "seq_default_expr";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::setCurrentSession(&s);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t2 (id INT DEFAULT (10 + 5), name VARCHAR(20))", s));
    assert(g_engine.insert(db, "t2", {{"name", "x"}}) == dbms::DBStatus::OK);

    auto rows = g_engine.query(db, "t2", {}, {"id", "name"});
    assert(rows.size() == 1);
    assert(trimRight(rows[0]) == "15 x");

    cleanup(db);
    std::cout << "[DEFAULT_SEQUENCE] parenthesized default OK" << std::endl;
}

int main() {
    test_default_nextval();
    test_default_expression_with_parens();
    std::cout << "[DEFAULT_SEQUENCE] All tests passed" << std::endl;
    return 0;
}
