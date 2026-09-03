#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <map>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

std::string trimRight(const std::string& value) {
    const size_t end = value.find_last_not_of(" \t\n\r");
    return end == std::string::npos ? "" : value.substr(0, end + 1);
}

std::string selectedValue(const std::string& database, int id) {
    const auto rows = g_engine.query(
        database, "items", {"=id " + std::to_string(id)}, {"tag"});
    assert(rows.size() == 1);
    return trimRight(rows.front());
}

void test_autocommit_update_is_atomic() {
    const std::string database = testDbPath("update_atomicity");
    cleanupTestDb("update_atomicity");
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE items (id INT PRIMARY KEY, tag VARCHAR(20) UNIQUE)",
        session));

    assert(g_engine.insert(database, "items", {{"id", "1"}, {"tag", "a"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "items", {{"id", "2"}, {"tag", "b"}}) ==
           dbms::DBStatus::OK);

    // The first physical row can accept "duplicate", while the second then
    // conflicts with it. The whole SQL statement must roll the first row back.
    std::vector<std::map<std::string, std::string>> returnedRows;
    assert(g_engine.update(
               database, "items", {{"tag", "duplicate"}}, {},
               &returnedRows) ==
           dbms::DBStatus::DUPLICATE_KEY);
    assert(returnedRows.empty());
    assert(selectedValue(database, 1) == "a");
    assert(selectedValue(database, 2) == "b");

    cleanupTestDb("update_atomicity");
    std::cout << "[UPDATE ATOMICITY] failed autocommit statement rolled back OK"
              << std::endl;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_autocommit_update_is_atomic();
    return 0;
}
