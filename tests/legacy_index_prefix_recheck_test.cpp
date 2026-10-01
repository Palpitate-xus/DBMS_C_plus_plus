#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string database = testDbPath("legacy_index_prefix_recheck");
    std::filesystem::remove_all(database);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE legacy_prefix(id INT PRIMARY KEY,k TEXT COLLATE \"C\")", session));
    const std::string prefix = "12345678901234567890";
    for (const auto& item : std::vector<std::pair<std::string, std::string>>{
             {"1", prefix + "-alpha"}, {"2", prefix + "-beta"}, {"3", prefix}}) {
        assert(g_engine.insert(database, "legacy_prefix", {{"id", item.first}, {"k", item.second}}) == dbms::DBStatus::OK);
    }
    assert(!ddl.executeSql("CREATE INDEX legacy_prefix_k ON legacy_prefix(k)", session));
    for (const auto& key : {prefix + "-alpha", prefix + "-beta", prefix}) {
        const auto rows = g_engine.query(database, "legacy_prefix", {"=k '" + key + "'"}, {"id"});
        std::cerr << key << ": " << rows.size() << " rows" << std::endl;
        assert(rows.size() == 1);
    }
    assert(g_engine.query(database, "legacy_prefix", {"=k '" + prefix + "-missing'"}, {"id"}).empty());
    assert(g_engine.query(database, "legacy_prefix", {"=k '" + prefix + "-alpha'", "=id 2"}, {"id"}).empty());
    std::filesystem::remove_all(database);
    std::cout << "[LEGACY INDEX PREFIX RECHECK] passed" << std::endl;
}
