#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "Session.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include <atomic>
#include <cassert>
#include <cstring>
#include <filesystem>
#include <fstream>
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

static std::string readBytes(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    assert(in);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

static void writeBytes(const fs::path& path, const std::string& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    assert(out);
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    assert(out);
}

template <typename T>
static void appendNative(std::string& bytes, const T& value) {
    bytes.append(reinterpret_cast<const char*>(&value), sizeof(value));
}

static void appendLegacyString(std::string& bytes, const std::string& value) {
    const size_t length = value.size();
    appendNative(bytes, length);
    bytes.append(value);
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

    dbms::CatalogManager& initialCatalog =
        g_engine.catalogService().get(db);
    const auto* relation = initialCatalog.resolveRelation("t", {"public"});
    assert(relation != nullptr && relation->relhastriggers);
    const dbms::Oid relationOid = relation->oid;
    g_engine.catalogService().evict(db);
    dbms::CatalogManager& reloadedCatalog =
        g_engine.catalogService().get(db);
    relation = reloadedCatalog.findClass(relationOid);
    assert(relation != nullptr && relation->relhastriggers);

    g_engine.catalogService().evict(db);
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

static void test_drop_trigger_is_relation_scoped_and_updates_catalog() {
    const std::string db = testDbPath("trigger_drop_catalog");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE events (id INT)", s));
    assert(!ddl.executeSql("CREATE TABLE other_events (id INT)", s));
    assert(!ddl.executeSql(
        "CREATE TRIGGER audit_insert BEFORE INSERT ON events "
        "FOR EACH ROW EXECUTE FUNCTION audit()", s));
    assert(!ddl.executeSql(
        "CREATE TRIGGER audit_update AFTER UPDATE ON events "
        "FOR EACH ROW EXECUTE FUNCTION audit()", s));

    dbms::CatalogManager& catalog = g_engine.catalogService().get(db);
    const auto* relation = catalog.resolveRelation("events", {"public"});
    assert(relation != nullptr && relation->relhastriggers);
    const dbms::Oid relationOid = relation->oid;

    // PostgreSQL requires ON table, and a trigger on one table must not be
    // removed by naming a different table.
    assert(ddl.executeSql("DROP TRIGGER audit_insert", s));
    assert(ddl.executeSql(
        "DROP TRIGGER audit_insert ON other_events", s));
    assert(!ddl.executeSql(
        "DROP TRIGGER IF EXISTS audit_insert ON other_events", s));
    assert(g_engine.getAllTriggers(db).size() == 2);

    bool handled = false;
    const std::string firstDrop = "DROP TRIGGER audit_insert ON events";
    assert(!dbms::tryDdlBridge(
        firstDrop, dbms::SQLParser::classify(firstDrop), s, handled));
    assert(handled);
    assert(g_engine.getAllTriggers(db).size() == 1);
    relation = catalog.findClass(relationOid);
    assert(relation != nullptr && relation->relhastriggers);

    assert(!ddl.executeSql("DROP TRIGGER audit_update ON events", s));
    assert(g_engine.getAllTriggers(db).empty());
    relation = catalog.findClass(relationOid);
    assert(relation != nullptr && !relation->relhastriggers);

    g_engine.catalogService().evict(db);
    dbms::CatalogManager& reloaded = g_engine.catalogService().get(db);
    relation = reloaded.findClass(relationOid);
    assert(relation != nullptr && !relation->relhastriggers);

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[TRIGGER] relation-scoped DROP updates catalog OK"
              << std::endl;
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

static void test_legacy_trigger_metadata_is_migrated() {
    const std::string db = testDbPath("trigger_legacy");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    const dbms::StorageEngine::Trigger legacy{
        "legacy_trigger", "after", "insert", "t", "select 1", "",
        true, true, {"new new_rows"}};
    std::string bytes;
    const size_t count = 1;
    appendNative(bytes, count);
    appendLegacyString(bytes, legacy.name);
    appendLegacyString(bytes, legacy.timing);
    appendLegacyString(bytes, legacy.event);
    appendLegacyString(bytes, legacy.tableName);
    appendLegacyString(bytes, legacy.action);
    appendLegacyString(bytes, legacy.whenCondition);
    const uint8_t forEachRow = 1;
    const uint8_t enabled = 1;
    appendNative(bytes, forEachRow);
    appendNative(bytes, enabled);
    const size_t transitionCount = legacy.transitions.size();
    appendNative(bytes, transitionCount);
    appendLegacyString(bytes, legacy.transitions.front());
    const fs::path metadata = fs::path(db) / ".triggers";
    writeBytes(metadata, bytes);

    std::vector<dbms::StorageEngine::Trigger> loaded;
    assert(g_engine.tryGetAllTriggers(db, loaded));
    assert(loaded.size() == 1);
    assert(loaded[0].name == legacy.name);
    assert(loaded[0].transitions == legacy.transitions);

    // The first successful mutation rewrites legacy metadata to TRG2.
    assert(g_engine.disableTrigger(db, legacy.name) == dbms::DBStatus::OK);
    const std::string migrated = readBytes(metadata);
    assert(migrated.size() >= sizeof(uint32_t));
    uint32_t magic = 0;
    std::memcpy(&magic, migrated.data(), sizeof(magic));
    assert(magic == 0x32475254u);

    cleanup(db);
    std::cout << "[TRIGGER] legacy metadata migration OK" << std::endl;
}

static void test_corrupt_trigger_metadata_fails_closed() {
    const std::string db = testDbPath("trigger_corrupt");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, value INT)", s));
    const dbms::StorageEngine::Trigger trigger{
        "trg", "after", "insert", "t", "select 1", "", true, true, {}};
    assert(g_engine.createTrigger(db, trigger) == dbms::DBStatus::OK);

    const fs::path metadata = fs::path(db) / ".triggers";
    const std::string valid = readBytes(metadata);
    assert(valid.size() > sizeof(uint64_t));
    std::string corrupt = valid;
    corrupt.back() ^= static_cast<char>(0x5a);
    writeBytes(metadata, corrupt);

    std::vector<dbms::StorageEngine::Trigger> loaded{trigger};
    assert(!g_engine.tryGetAllTriggers(db, loaded));
    assert(loaded.empty());
    loaded.push_back(trigger);
    assert(!g_engine.tryGetTriggers(db, "t", "after", "insert", loaded));
    assert(loaded.empty());
    assert(g_engine.createTrigger(db, {
        "second", "after", "insert", "t", "select 2", "", true, true, {}}) ==
        dbms::DBStatus::CORRUPTED_DATA);
    assert(g_engine.dropTrigger(db, trigger.name) == dbms::DBStatus::CORRUPTED_DATA);
    assert(g_engine.disableTrigger(db, trigger.name) == dbms::DBStatus::CORRUPTED_DATA);
    assert(g_engine.alterTableRenameTable(db, "t", "renamed") ==
           dbms::DBStatus::CORRUPTED_DATA);
    assert(g_engine.tableExists(db, "t"));
    assert(!g_engine.tableExists(db, "renamed"));
    assert(readBytes(metadata) == corrupt);

    // All data-changing paths validate a single trigger snapshot before they
    // scan or modify rows, so corruption cannot cause partially applied DML.
    g_engine.setTriggerExecutor([](const std::string&) { return false; });
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"value", "10"}}) ==
           dbms::DBStatus::CORRUPTED_DATA);
    writeBytes(metadata, valid);
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"value", "10"}}) ==
           dbms::DBStatus::OK);
    writeBytes(metadata, corrupt);
    assert(g_engine.update(db, "t", {{"value", "11"}}, {"=id 1"}) ==
           dbms::DBStatus::CORRUPTED_DATA);
    assert(g_engine.remove(db, "t", {"=id 1"}) ==
           dbms::DBStatus::CORRUPTED_DATA);
    writeBytes(metadata, valid);
    const auto rows = g_engine.query(db, "t", {"=id 1"}, {});
    assert(rows.size() == 1);

    writeBytes(metadata, valid.substr(0, valid.size() - 1));
    assert(!g_engine.tryGetAllTriggers(db, loaded));
    assert(loaded.empty());
    writeBytes(metadata, valid);
    g_engine.setTriggerExecutor({});

    cleanup(db);
    std::cout << "[TRIGGER] corrupt metadata fails closed OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_create_trigger_before_insert();
    test_create_trigger_after_update();
    test_create_trigger_when();
    test_create_trigger_statement_level();
    test_drop_trigger_is_relation_scoped_and_updates_catalog();
    test_trigger_metadata_failures_preserve_old_state();
    test_concurrent_trigger_creates_do_not_lose_updates();
    test_legacy_trigger_metadata_is_migrated();
    test_corrupt_trigger_metadata_fails_closed();
    std::cout << "[TRIGGER] all passed" << std::endl;
    return 0;
}
