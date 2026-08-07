#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

static std::string trimRight(const std::string& value) {
    const size_t end = value.find_last_not_of(' ');
    return end == std::string::npos ? std::string() : value.substr(0, end + 1);
}

int main() {
    const std::string db = testDbPath("numeric_storage");
    if (std::filesystem::exists(db)) std::filesystem::remove_all(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = db;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (n NUMERIC)", session));

    const dbms::TableSchema schema = g_engine.getTableSchema(db, "t");
    assert(schema.len == 1);
    assert(schema.cols[0].dataType == "numeric");
    assert(schema.cols[0].isVariableLength);

    assert(g_engine.insert(db, "t", {{"n", "12345.67"}}) == dbms::DBStatus::OK);
    const auto rows = g_engine.query(db, "t", {}, {"n"}, {});
    assert(rows.size() == 1);
    assert(trimRight(rows[0]) == "12345.67");

    assert(g_engine.insert(db, "t", {{"n", "0.00000001"}}) == dbms::DBStatus::OK);
    assert(g_engine.query(db, "t", {}, {"n"}, {}).size() == 2);
    std::filesystem::remove_all(db);
    std::cout << "[NUMERIC STORAGE] exact decimal persistence OK" << std::endl;
    return 0;
}
