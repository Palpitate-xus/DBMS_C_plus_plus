#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <atomic>
#include <cassert>
#include <filesystem>
#include <iostream>
#include <thread>
#include <vector>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static void test_create_trigger_before_insert() {
    std::string db = testDbPath("trigger_basic");
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
    std::string db = testDbPath("trigger_update");
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
    std::string db = testDbPath("trigger_when");
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
    std::string db = testDbPath("trigger_stmt");
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

static void test_trigger_metadata_failures_preserve_old_state() {
    std::string db = testDbPath("trigger_io_failure");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    const dbms::StorageEngine::Trigger first{
        "trg_first", "after", "insert", "t", "select 1", "", true, true, {}};
    const dbms::StorageEngine::Trigger second{
        "trg_second", "after", "insert", "t", "select 2", "", true, true, {}};
    assert(g_engine.createTrigger(db, first) == dbms::DBStatus::OK);

    const fs::perms readOnly = fs::perms::owner_read | fs::perms::owner_exec;
    fs::permissions(db, readOnly, fs::perm_options::replace);
    assert(g_engine.createTrigger(db, second) == dbms::DBStatus::IO_ERROR);
    assert(g_engine.disableTrigger(db, first.name) == dbms::DBStatus::IO_ERROR);
    assert(g_engine.dropTrigger(db, first.name) == dbms::DBStatus::IO_ERROR);
    fs::permissions(db, fs::perms::owner_all, fs::perm_options::replace);

    auto triggers = g_engine.getAllTriggers(db);
    assert(triggers.size() == 1);
    assert(triggers[0].name == first.name && triggers[0].enabled);

    assert(g_engine.disableTrigger(db, first.name) == dbms::DBStatus::OK);
    fs::permissions(db, readOnly, fs::perm_options::replace);
    assert(g_engine.enableTrigger(db, first.name) == dbms::DBStatus::IO_ERROR);
    fs::permissions(db, fs::perms::owner_all, fs::perm_options::replace);
    triggers = g_engine.getAllTriggers(db);
    assert(triggers.size() == 1 && !triggers[0].enabled);

    cleanup(db);
    std::cout << "[TRIGGER] metadata I/O failures preserve state OK" << std::endl;
}

static void test_concurrent_trigger_creates_do_not_lose_updates() {
    std::string db = testDbPath("trigger_concurrent");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    constexpr int workerCount = 24;
    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::atomic<bool> failed{false};
    std::vector<std::thread> workers;
    for (int worker = 0; worker < workerCount; ++worker) {
        workers.emplace_back([&, worker] {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
            dbms::StorageEngine::Trigger trigger{
                "trg_" + std::to_string(worker), "after", "insert", "t",
                "select " + std::to_string(worker), "", true, true, {}};
            if (g_engine.createTrigger(db, trigger) != dbms::DBStatus::OK)
                failed.store(true, std::memory_order_release);
        });
    }
    while (ready.load(std::memory_order_acquire) != workerCount)
        std::this_thread::yield();
    start.store(true, std::memory_order_release);
    for (auto& worker : workers) worker.join();

    assert(!failed.load(std::memory_order_acquire));
    assert(g_engine.getAllTriggers(db).size() == workerCount);
    cleanup(db);
    std::cout << "[TRIGGER] concurrent metadata updates serialized OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_create_trigger_before_insert();
    test_create_trigger_after_update();
    test_create_trigger_when();
    test_create_trigger_statement_level();
    test_trigger_metadata_failures_preserve_old_state();
    test_concurrent_trigger_creates_do_not_lose_updates();
    std::cout << "[TRIGGER] all passed" << std::endl;
    return 0;
}
