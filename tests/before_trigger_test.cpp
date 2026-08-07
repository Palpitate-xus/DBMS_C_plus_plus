// BEFORE row-level trigger test.
// Verifies:
//   1. BEFORE INSERT trigger can modify NEW column values via SET col = val
//   2. BEFORE UPDATE trigger can modify NEW column values (with OLD access)
//   3. BEFORE DELETE trigger fires with OLD values visible in action

#include "commands/TableManage.h"
#include "commands/DdlExecutor.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include "common/Config.h"
#include <cassert>
#include <cctype>
#include <filesystem>
#include <iostream>
#include <set>
#include "test_utils.h"

// Test-local engine instance (tests don't link main.cpp's global)
// Must be non-static so DdlExecutor.cpp's extern reference resolves
dbms::StorageEngine g_engine;

// Stubs for main.cpp helpers referenced by DdlExecutor.cpp / NetworkServer.cpp
bool checkAdmin(const Session& s) { return s.permission == 1; }
bool checkDB(const Session& s) { return !s.currentDB.empty(); }
std::string resolveTableName(Session&, const std::string& name) { return name; }

// Global current database context for trigger executor in tests
static std::string g_currentTestDB;

// Stubs for globals defined in main.cpp
dbms::Config g_config;
int g_slowQueryThresholdMs = 0;
int g_checkpointInterval = 0;

// Stubs for functions defined in main.cpp
bool execute(const std::string&, Session&) { return false; }
void logSlowQuery(const std::string&, double, const std::string&, const std::string&) {}
void recordSqlStat(const std::string&, double, const std::string&) {}
namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (fs::exists(db)) fs::remove_all(db); }
static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser"; s.permission = 1; s.currentDB = db;
}

// DdlExecutor intentionally handles DDL only.  The production trigger hook
// is wired to main.cpp's general SQL executor, so provide the small INSERT
// subset needed by this standalone test instead of pretending DdlExecutor
// can execute DML.
static bool executeTestTriggerAction(const std::string& sql) {
    std::string lower = sql;
    for (char& c : lower) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    const std::string prefix = "insert into ";
    if (lower.rfind(prefix, 0) != 0) return false;

    size_t tableStart = prefix.size();
    size_t tableEnd = lower.find_first_of(" (", tableStart);
    if (tableEnd == std::string::npos) return false;
    std::string table = sql.substr(tableStart, tableEnd - tableStart);

    size_t valuesPos = lower.find("values", tableEnd);
    if (valuesPos == std::string::npos) return false;
    size_t openQuote = sql.find('\'', valuesPos);
    if (openQuote == std::string::npos) return false;
    size_t closeQuote = sql.find('\'', openQuote + 1);
    if (closeQuote == std::string::npos) return false;
    std::string message = sql.substr(openQuote + 1, closeQuote - openQuote - 1);

    size_t concat = sql.find("||", closeQuote + 1);
    if (concat != std::string::npos) {
        size_t rhsStart = concat + 2;
        while (rhsStart < sql.size() && std::isspace(static_cast<unsigned char>(sql[rhsStart]))) ++rhsStart;
        size_t rhsEnd = sql.find_first_of(") ;", rhsStart);
        if (rhsEnd == std::string::npos) rhsEnd = sql.size();
        message += sql.substr(rhsStart, rhsEnd - rhsStart);
    }
    return g_engine.insert(g_currentTestDB, table, {{"msg", message}}) == dbms::DBStatus::OK;
}

// -------- Test 1: BEFORE INSERT trigger modifies NEW values --------
static void test_before_insert_modify() {
    std::string db = testDbPath("before_ins");
    cleanup(db);
    g_currentTestDB = db;
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, name VARCHAR(20), status VARCHAR(10))", s));

    // Create a BEFORE INSERT trigger that sets status = 'active' when not provided
    assert(g_engine.createTrigger(db, {
        "trg_set_status", "before", "insert", "t",
        "set status = 'active'",  // action: SET col = val form
        "", true, true
    }) == dbms::DBStatus::OK);

    // Insert without status — trigger should fill it
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"name", "alice"}}) == dbms::DBStatus::OK);

    // Query the row
    auto rows = g_engine.query(db, "t", {}, {});
    assert(rows.size() == 1);
    // The row should contain 'active' in the status column
    bool foundActive = false;
    for (const auto& r : rows) {
        if (r.find("active") != std::string::npos) foundActive = true;
    }
    assert(foundActive);

    cleanup(db);
    std::cout << "[BEFORE-TRIGGER] BEFORE INSERT modify OK" << std::endl;
}

// -------- Test 2: BEFORE INSERT trigger reads NEW.id and logs --------
static void test_before_insert_when() {
    std::string db = testDbPath("before_ins_when");
    cleanup(db);
    g_currentTestDB = db;
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, name VARCHAR(20))", s));
    assert(!ddl.executeSql("CREATE TABLE audit (msg VARCHAR(100))", s));

    // BEFORE INSERT trigger with WHEN: only fires when id > 10
    assert(g_engine.createTrigger(db, {
        "trg_audit", "before", "insert", "t",
        "insert into audit values ('inserted id=' || NEW.id)",
        "NEW.id > 10",  // WHEN condition
        true, true
    }) == dbms::DBStatus::OK);

    // Insert low id — trigger should NOT fire
    assert(g_engine.insert(db, "t", {{"id", "5"}, {"name", "low"}}) == dbms::DBStatus::OK);
    auto audit1 = g_engine.query(db, "audit", {}, {});
    assert(audit1.empty());

    // Insert high id — trigger SHOULD fire
    assert(g_engine.insert(db, "t", {{"id", "20"}, {"name", "high"}}) == dbms::DBStatus::OK);
    auto audit2 = g_engine.query(db, "audit", {}, {});
    assert(audit2.size() == 1);

    cleanup(db);
    std::cout << "[BEFORE-TRIGGER] BEFORE INSERT WHEN condition OK" << std::endl;
}

// -------- Test 3: BEFORE UPDATE trigger modifies NEW values --------
static void test_before_update_modify() {
    std::string db = testDbPath("before_upd");
    cleanup(db);
    g_currentTestDB = db;
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, name VARCHAR(20), version INT)", s));

    // Seed a row
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"name", "alice"}, {"version", "1"}}) == dbms::DBStatus::OK);

    // BEFORE UPDATE trigger: increment version automatically
    assert(g_engine.createTrigger(db, {
        "trg_version", "before", "update", "t",
        "set version = OLD.version + 1",
        "", true, true
    }) == dbms::DBStatus::OK);

    // Update name — trigger should bump version
    assert(g_engine.update(db, "t", {{"name", "alice2"}}, {"=id 1"}) == dbms::DBStatus::OK);

    // Verify version was incremented by trigger
    auto rows = g_engine.query(db, "t", {"=id 1"}, {});
    assert(rows.size() == 1);
    assert(rows[0].find(" 2") != std::string::npos);  // version should be 2

    cleanup(db);
    std::cout << "[BEFORE-TRIGGER] BEFORE UPDATE modify OK" << std::endl;
}

// -------- Test 4: BEFORE DELETE trigger fires --------
static void test_before_delete() {
    std::string db = testDbPath("before_del");
    cleanup(db);
    g_currentTestDB = db;
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, name VARCHAR(20))", s));
    assert(!ddl.executeSql("CREATE TABLE deleted_log (msg VARCHAR(100))", s));

    // Seed rows
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"name", "alice"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"name", "bob"}}) == dbms::DBStatus::OK);

    // BEFORE DELETE trigger: log deletion
    assert(g_engine.createTrigger(db, {
        "trg_log_del", "before", "delete", "t",
        "insert into deleted_log values ('deleted ' || OLD.name)",
        "", true, true
    }) == dbms::DBStatus::OK);

    // Delete one row
    assert(g_engine.remove(db, "t", {"=id 1"}) == dbms::DBStatus::OK);

    // Verify trigger fired
    auto logs = g_engine.query(db, "deleted_log", {}, {});
    assert(logs.size() == 1);
    assert(logs[0].find("alice") != std::string::npos);

    cleanup(db);
    std::cout << "[BEFORE-TRIGGER] BEFORE DELETE OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    cleanupAllTestData();

    // Set up trigger executor: executes action SQL via DdlExecutor with the
    // current test database context (mirrors main.cpp's capture of session s).
    g_engine.setTriggerExecutor([](const std::string& actionSql) -> bool {
        return executeTestTriggerAction(actionSql);
    });

    test_before_insert_modify();
    test_before_insert_when();
    test_before_update_modify();
    test_before_delete();
    std::cout << "[BEFORE-TRIGGER] all tests passed" << std::endl;
    return 0;
}
