#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static std::string readBytes(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>()};
}

static void test_enum_basic() {
    std::string db = testDbPath("enum_basic");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql("CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')", s);
    assert(!err);

    err = ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, m mood)", s);
    assert(!err);

    assert(g_engine.insert(db, "t", {{"id", "1"}, {"m", "happy"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"m", "sad"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "3"}, {"m", "angry"}}) == dbms::DBStatus::INVALID_VALUE);

    auto rows = g_engine.query(db, "t", {}, {"id", "m"});
    assert(rows.size() == 2);

    cleanup(db);
    std::cout << "[ENUM] basic OK" << std::endl;
}

static void test_enum_update() {
    std::string db = testDbPath("enum_update");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql("CREATE TYPE color AS ENUM ('red', 'green', 'blue')", s);
    assert(!err);
    err = ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, c color)", s);
    assert(!err);

    assert(g_engine.insert(db, "t", {{"id", "1"}, {"c", "red"}}) == dbms::DBStatus::OK);
    assert(g_engine.update(db, "t", {{"c", "green"}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(g_engine.update(db, "t", {{"c", "yellow"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);

    cleanup(db);
    std::cout << "[ENUM] update OK" << std::endl;
}

static void test_enum_labels_round_trip_losslessly() {
    std::string db = testDbPath("enum_lossless_labels");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TYPE punctuation AS ENUM "
        "('comma,label', 'pipe|label', '', 'line\nlabel', 'it''s')", s));

    const std::vector<std::string> expected = {
        "comma,label", "pipe|label", "", "line\nlabel", "it's"};
    dbms::StorageEngine reopened;
    const auto stored = reopened.getEnumType(db, "punctuation");
    assert(stored.name == "punctuation");
    assert(stored.labels == expected);

    const std::string raw = readBytes(std::filesystem::path(db) / ".enums");
    assert(raw.rfind("DBMS_ENUM_V2:", 0) == 0);
    assert(raw.find("pipe|label") == std::string::npos);
    assert(raw.find("line\nlabel") == std::string::npos);

    cleanup(db);
    std::cout << "[ENUM] lossless labels OK" << std::endl;
}

static void test_enum_legacy_migration_and_atomic_rewrites() {
    namespace fs = std::filesystem;
    using dbms::DBStatus;

    std::string db = testDbPath("enum_atomic_metadata");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    const fs::path metadata = fs::path(db) / ".enums";
    {
        std::ofstream legacy(metadata, std::ios::binary);
        legacy << "legacy|one|two\nneighbor|keep\n";
        assert(legacy.good());
    }

    dbms::StorageEngine engine;
    auto legacy = engine.getEnumType(db, "legacy");
    assert((legacy.labels == std::vector<std::string>{"one", "two"}));

    legacy.labels = {"with|pipe", "line\nfeed", ""};
    assert(engine.updateEnumType(db, legacy) == DBStatus::OK);
    const std::string migrated = readBytes(metadata);
    assert(migrated.find("legacy|one|two") == std::string::npos);
    assert(migrated.find("DBMS_ENUM_V2:") == 0);

    dbms::StorageEngine reopened;
    assert(reopened.getEnumType(db, "legacy").labels == legacy.labels);
    assert((reopened.getEnumType(db, "neighbor").labels ==
            std::vector<std::string>{"keep"}));

    dbms::StorageEngine::EnumType invalid;
    invalid.name = "duplicate_labels";
    invalid.labels = {"same", "same"};
    assert(engine.createEnumType(db, invalid) == DBStatus::INVALID_ARGUMENT);
    invalid.name = "nul_label";
    invalid.labels = {std::string("bad\0label", 9)};
    assert(engine.createEnumType(db, invalid) == DBStatus::INVALID_ARGUMENT);
    invalid.name = "no_labels";
    invalid.labels.clear();
    assert(engine.createEnumType(db, invalid) == DBStatus::INVALID_ARGUMENT);
    assert(readBytes(metadata) == migrated);

    const fs::perms originalPermissions = fs::status(db).permissions();
    fs::permissions(db, fs::perms::owner_read | fs::perms::owner_exec,
                    fs::perm_options::replace);
    legacy.labels = {"replacement"};
    const DBStatus updateStatus = engine.updateEnumType(db, legacy);
    dbms::StorageEngine::EnumType added;
    added.name = "added";
    added.labels = {"value"};
    const DBStatus createStatus = engine.createEnumType(db, added);
    const DBStatus dropStatus = engine.dropEnumType(db, "legacy");
    fs::permissions(db, originalPermissions, fs::perm_options::replace);
    assert(updateStatus == DBStatus::IO_ERROR);
    assert(createStatus == DBStatus::IO_ERROR);
    assert(dropStatus == DBStatus::IO_ERROR);
    assert(readBytes(metadata) == migrated);

    assert(engine.dropEnumType(db, "legacy") == DBStatus::OK);
    dbms::StorageEngine afterDrop;
    assert(afterDrop.getEnumType(db, "legacy").name.empty());
    assert((afterDrop.getEnumType(db, "neighbor").labels ==
            std::vector<std::string>{"keep"}));

    cleanup(db);
    std::cout << "[ENUM] migration and atomic rewrites OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_enum_basic();
    test_enum_update();
    test_enum_labels_round_trip_losslessly();
    test_enum_legacy_migration_and_atomic_rewrites();
    std::cout << "[ENUM] all passed" << std::endl;
    return 0;
}
