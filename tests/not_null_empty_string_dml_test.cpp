#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

void cleanup(const std::string& database) {
    if (std::filesystem::exists(database)) {
        std::filesystem::remove_all(database);
    }
}

Session makeSession(const std::string& database) {
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    return session;
}

void testNotNullEmptyValues() {
    const std::string database = testDbPath("not_null_empty_dml");
    cleanup(database);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session = makeSession(database);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE values_table ("
        "id INT PRIMARY KEY, required_text VARCHAR(20) NOT NULL, "
        "required_bytes BYTEA NOT NULL, required_number INT NOT NULL)",
        session));

    assert(g_engine.insert(
               database, "values_table",
               {{"id", "1"}, {"required_text", ""},
                {"required_bytes", ""}, {"required_number", "7"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "values_table",
               {{"id", "2"}, {"required_text", "value"},
                {"required_bytes", "\\x01"}, {"required_number", "8"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.update(
               database, "values_table",
               {{"required_text", ""}, {"required_bytes", ""}},
               {"=id 2"}) == dbms::DBStatus::OK);

    const dbms::TableSchema table =
        g_engine.getTableSchema(database, "values_table");
    size_t textColumn = table.len;
    size_t bytesColumn = table.len;
    for (size_t column = 0; column < table.len; ++column) {
        if (table.cols[column].dataName == "required_text") {
            textColumn = column;
        } else if (table.cols[column].dataName == "required_bytes") {
            bytesColumn = column;
        }
    }
    assert(textColumn < table.len && bytesColumn < table.len);
    size_t rowCount = 0;
    assert(g_engine.forEachRow(
        database, "values_table",
        [&](uint32_t pageId, uint16_t slotId, const char*, size_t) {
            const int64_t rid =
                dbms::StorageEngine::encodeRid(pageId, slotId);
            assert(!g_engine.isColumnNullByRid(
                database, "values_table", rid, textColumn));
            assert(!g_engine.isColumnNullByRid(
                database, "values_table", rid, bytesColumn));
            ++rowCount;
        }));
    assert(rowCount == 2);

    assert(g_engine.insert(
               database, "values_table",
               {{"id", "3"}, {"required_bytes", ""},
                {"required_number", "9"}}) ==
           dbms::DBStatus::NULL_NOT_ALLOWED);
    assert(g_engine.update(database, "values_table",
                           {{"required_text", "NULL"}}, {"=id 1"}) ==
           dbms::DBStatus::NULL_NOT_ALLOWED);
    assert(g_engine.insert(
               database, "values_table",
               {{"id", "4"}, {"required_text", ""},
                {"required_bytes", ""}, {"required_number", ""}}) ==
           dbms::DBStatus::INVALID_VALUE);

    cleanup(database);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    testNotNullEmptyValues();
    std::cout << "[NOT NULL EMPTY DML] all passed" << std::endl;
    return 0;
}
