#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <algorithm>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;
using dbms::DBStatus;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static bool hasLabel(const dbms::StorageEngine::EnumType& et, const std::string& l) {
    return std::find(et.labels.begin(), et.labels.end(), l) != et.labels.end();
}

static void test_update_append() {
    std::string db = testDbPath("enumalter_append");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);

    dbms::StorageEngine::EnumType et;
    et.name = "mood";
    et.labels = {"sad", "ok"};
    assert(g_engine.createEnumType(db, et) == DBStatus::OK);

    // Append a value.
    auto cur = g_engine.getEnumType(db, "mood");
    cur.labels.push_back("happy");
    assert(g_engine.updateEnumType(db, cur) == DBStatus::OK);

    auto after = g_engine.getEnumType(db, "mood");
    assert(after.labels.size() == 3);
    assert(hasLabel(after, "happy"));
    assert(after.labels.back() == "happy");
    cleanup(db);
    std::cout << "[ENUM-ALTER] append value OK" << std::endl;
}

static void test_update_insert_before() {
    std::string db = testDbPath("enumalter_before");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);

    dbms::StorageEngine::EnumType et;
    et.name = "sz";
    et.labels = {"small", "large"};
    assert(g_engine.createEnumType(db, et) == DBStatus::OK);

    auto cur = g_engine.getEnumType(db, "sz");
    auto it = std::find(cur.labels.begin(), cur.labels.end(), "large");
    cur.labels.insert(it, "medium");
    assert(g_engine.updateEnumType(db, cur) == DBStatus::OK);

    auto after = g_engine.getEnumType(db, "sz");
    assert(after.labels.size() == 3);
    assert(after.labels[0] == "small");
    assert(after.labels[1] == "medium");
    assert(after.labels[2] == "large");
    cleanup(db);
    std::cout << "[ENUM-ALTER] insert before OK" << std::endl;
}

static void test_update_rename() {
    std::string db = testDbPath("enumalter_rename");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);

    dbms::StorageEngine::EnumType et;
    et.name = "st";
    et.labels = {"new", "old"};
    assert(g_engine.createEnumType(db, et) == DBStatus::OK);

    auto cur = g_engine.getEnumType(db, "st");
    for (auto& l : cur.labels) if (l == "old") l = "archived";
    assert(g_engine.updateEnumType(db, cur) == DBStatus::OK);

    auto after = g_engine.getEnumType(db, "st");
    assert(hasLabel(after, "archived"));
    assert(!hasLabel(after, "old"));
    cleanup(db);
    std::cout << "[ENUM-ALTER] rename value OK" << std::endl;
}

static void test_update_missing_type() {
    std::string db = testDbPath("enumalter_missing");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);

    dbms::StorageEngine::EnumType et;
    et.name = "ghost";
    et.labels = {"a"};
    // No enum file at all -> TABLE_NOT_FOUND.
    assert(g_engine.updateEnumType(db, et) == DBStatus::TABLE_NOT_FOUND);
    cleanup(db);
    std::cout << "[ENUM-ALTER] missing type OK" << std::endl;
}

static void test_update_preserves_others() {
    std::string db = testDbPath("enumalter_others");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);

    dbms::StorageEngine::EnumType a; a.name = "color"; a.labels = {"red", "green"};
    dbms::StorageEngine::EnumType b; b.name = "shape"; b.labels = {"round"};
    assert(g_engine.createEnumType(db, a) == DBStatus::OK);
    assert(g_engine.createEnumType(db, b) == DBStatus::OK);

    auto cur = g_engine.getEnumType(db, "color");
    cur.labels.push_back("blue");
    assert(g_engine.updateEnumType(db, cur) == DBStatus::OK);

    // The other enum must be untouched.
    auto shape = g_engine.getEnumType(db, "shape");
    assert(shape.labels.size() == 1 && shape.labels[0] == "round");
    auto color = g_engine.getEnumType(db, "color");
    assert(color.labels.size() == 3);
    cleanup(db);
    std::cout << "[ENUM-ALTER] preserves other enums OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_update_append();
    test_update_insert_before();
    test_update_rename();
    test_update_missing_type();
    test_update_preserves_others();
    std::cout << "[ENUM-ALTER] all passed" << std::endl;
    return 0;
}
