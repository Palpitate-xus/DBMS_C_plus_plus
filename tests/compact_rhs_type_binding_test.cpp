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
    const std::string database = testDbPath("compact_rhs_type_binding");
    std::filesystem::remove_all(database);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE rhs_values(id INT PRIMARY KEY,code TEXT,d DATE)", session));
    assert(g_engine.insert(database, "rhs_values",
        {{"id", "1"}, {"code", "1+2"}, {"d", "2024-01-01"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "rhs_values",
        {{"id", "2"}, {"code", "2*3"}, {"d", "2025-02-02"}}) == dbms::DBStatus::OK);
    assert(g_engine.query(database, "rhs_values", {"=code 1+2"}, {"id"}).size() == 1);
    assert(g_engine.query(database, "rhs_values", {"=code 2*3"}, {"id"}).size() == 1);
    assert(g_engine.query(database, "rhs_values", {"=d 2024-01-01"}, {"id"}).size() == 1);
    assert(g_engine.query(database, "rhs_values", {">d 2023-12-31"}, {"id"}).size() == 2);
    assert(g_engine.query(database, "rhs_values", {"=id 1+1"}, {"id"}).size() == 1);
    assert(g_engine.query(database, "rhs_values", {"=code '1+2'"}, {"id"}).size() == 1);
    assert(g_engine.query(database, "rhs_values", {"=code '1+' || '2'"}, {"id"}).size() == 1);
    assert(g_engine.query(database, "rhs_values", {"=d CAST('2024-01-01' AS date)"}, {"id"}).size() == 1);
    assert(g_engine.query(database, "rhs_values", {"=id CAST(2 AS integer)"}, {"id"}).size() == 1);
    assert(g_engine.query(database, "rhs_values", {"=id 2::numeric"}, {"id"}).size() == 1);
    const auto api = dbms::StorageEngine::parseConditions({"apicond =code 1+2"});
    assert(api.size() == 1 && api.front().op == "typedrhs =");
    const auto sql = dbms::StorageEngine::parseConditions({"=code 1+2"});
    assert(sql.size() == 1 && sql.front().op == "typedexpr");
    const auto taggedSql = dbms::StorageEngine::parseConditions(
        {"apicond typedexpr id = 1+1"});
    assert(taggedSql.size() == 1 && taggedSql.front().op == "typedexpr");
    std::filesystem::remove_all(database);
    std::cout << "[COMPACT RHS TYPE BINDING] passed" << std::endl;
}
