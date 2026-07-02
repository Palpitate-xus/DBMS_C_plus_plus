#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
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

static std::string trimRight(const std::string& s) {
    size_t end = s.size();
    while (end > 0 && (s[end - 1] == ' ' || s[end - 1] == '\t' || s[end - 1] == '\n' || s[end - 1] == '\r')) --end;
    return s.substr(0, end);
}

int main() {
    std::string db = testDbPath("seq_e2e_smoke");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::setCurrentSession(&s);
    dbms::DdlExecutor ddl;

    // E2E smoke: DEFAULT nextval end-to-end
    assert(!ddl.executeSql("CREATE SEQUENCE s", s));
    assert(!ddl.executeSql("CREATE TABLE t (id INT DEFAULT nextval('s'), name VARCHAR(20))", s));
    assert(g_engine.insert(db, "t", {{"name", "a"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"name", "b"}}) == dbms::DBStatus::OK);

    auto rows = g_engine.query(db, "t", {}, {"id", "name"});
    assert(rows.size() == 2);
    assert(trimRight(rows[0]) == "1 a");
    assert(trimRight(rows[1]) == "2 b");

    // currval reflects the last nextval obtained in this session
    assert(s.sequenceLastValues["s"] == 2);
    assert(g_engine.currval(db, "s") == 2);

    // Volatility persistence smoke
    assert(!ddl.executeSql("CREATE FUNCTION add_one(x INT) RETURNS INT IMMUTABLE AS 'x + 1' LANGUAGE sql", s));
    auto info = g_engine.getUDF(db, "add_one");
    assert(info.provolatile == 'i');

    // DROP SEQUENCE RESTRICT must fail due to dependent column default
    assert(ddl.executeSql("DROP SEQUENCE s RESTRICT", s));

    // DROP SEQUENCE CASCADE clears the default and succeeds
    assert(!ddl.executeSql("DROP SEQUENCE s CASCADE", s));

    // After cascade the sequence is gone
    assert(!g_engine.sequenceExists(db, "s"));

    // Inserting without id no longer uses the sequence (falls back to 0 sentinel)
    assert(g_engine.insert(db, "t", {{"name", "c"}}) == dbms::DBStatus::OK);
    rows = g_engine.query(db, "t", {}, {"id", "name"});
    assert(rows.size() == 3);
    assert(trimRight(rows[2]) == "0 c");

    cleanup(db);
    std::cout << "[DEFAULT_SEQUENCE_E2E] smoke passed" << std::endl;
    return 0;
}
