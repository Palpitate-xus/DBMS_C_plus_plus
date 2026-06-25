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

static void test_create_trigger_before_insert() {
    std::string db = "trigger_basic";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY)", s));
    assert(!ddl.executeSql("CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION audit()", s));

    auto triggers = g_engine.getTriggers(db, "t", "before", "insert");
    assert(triggers.size() == 1);
    assert(triggers[0].name == "trg");
    assert(triggers[0].forEachRow);

    cleanup(db);
    std::cout << "[TRIGGER] before insert OK" << std::endl;
}

static void test_create_trigger_after_update() {
    std::string db = "trigger_update";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, score INT)", s));
    assert(!ddl.executeSql("CREATE TRIGGER trg AFTER UPDATE ON t FOR EACH ROW insert into log values (1)", s));

    auto triggers = g_engine.getTriggers(db, "t", "after", "update");
    assert(triggers.size() == 1);
    assert(triggers[0].action.find("insert") != std::string::npos);

    cleanup(db);
    std::cout << "[TRIGGER] after update OK" << std::endl;
}

static void test_create_trigger_when() {
    std::string db = "trigger_when";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, score INT)", s));
    assert(!ddl.executeSql("CREATE TRIGGER trg BEFORE DELETE ON t FOR EACH ROW WHEN (score > 0) EXECUTE FUNCTION check()", s));

    auto triggers = g_engine.getAllTriggers(db);
    assert(triggers.size() == 1);
    assert(triggers[0].whenCondition.find("score") != std::string::npos);

    cleanup(db);
    std::cout << "[TRIGGER] when condition OK" << std::endl;
}

static void test_create_trigger_statement_level() {
    std::string db = "trigger_stmt";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY)", s));
    assert(!ddl.executeSql("CREATE TRIGGER trg AFTER INSERT ON t FOR EACH STATEMENT EXECUTE FUNCTION stmt_func()", s));

    auto triggers = g_engine.getTriggers(db, "t", "after", "insert");
    assert(triggers.size() == 1);
    assert(!triggers[0].forEachRow);

    cleanup(db);
    std::cout << "[TRIGGER] statement level OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_create_trigger_before_insert();
    test_create_trigger_after_update();
    test_create_trigger_when();
    test_create_trigger_statement_level();
    std::cout << "[TRIGGER] all passed" << std::endl;
    return 0;
}
