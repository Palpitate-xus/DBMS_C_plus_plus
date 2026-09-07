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

static void cleanup(const std::string& database) {
    if (std::filesystem::exists(database))
        std::filesystem::remove_all(database);
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string database = testDbPath("json_type");
    cleanup(database);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY, j JSON, jb JSONB)", session));

    const std::string valid =
        R"({"s":"line\nbreak","unicode":"\u732b","slash":"\/"})";
    assert(g_engine.insert(database, "t",
                           {{"id", "1"}, {"j", valid}, {"jb", valid}}) ==
           dbms::DBStatus::OK);

    const std::vector<std::string> invalidStrings = {
        R"({"s":"\q"})",
        R"({"s":"\u12xz"})",
        std::string("{\"s\":\"line\nbreak\"}"),
        std::string("{\"s\":\"tab\there\"}"),
    };
    int id = 2;
    for (const auto& invalid : invalidStrings) {
        assert(g_engine.insert(database, "t",
                               {{"id", std::to_string(id++)},
                                {"j", invalid}}) ==
               dbms::DBStatus::INVALID_VALUE);
        assert(g_engine.insert(database, "t",
                               {{"id", std::to_string(id++)},
                                {"jb", invalid}}) ==
               dbms::DBStatus::INVALID_VALUE);
    }
    assert(g_engine.update(database, "t", {{"j", invalidStrings.front()}},
                           {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);

    cleanup(database);
    std::cout << "[JSON-TYPE] string validation passed" << std::endl;
    return 0;
}
