#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

void assertNullAndEmptyRemainDistinct(dbms::StorageEngine& engine,
                                      const std::string& database) {
    const dbms::TableSchema table =
        engine.getTableSchema(database, "values_t");
    assert(table.len == 3);
    assert(table.cols[0].dataName == "id");
    assert(table.cols[1].dataName == "nullable_value");
    assert(table.cols[2].dataName == "empty_value");

    int64_t rid = -1;
    assert(engine.forEachRow(
        database, "values_t",
        [&](uint32_t pageId, uint16_t slotId, const char* data, size_t len) {
            const std::string row(data, len);
            if (engine.extractColumnValue(row, table, 0, database, true) ==
                "1") {
                rid = dbms::StorageEngine::encodeRid(pageId, slotId);
            }
        }));
    assert(rid >= 0);
    assert(engine.isColumnNullByRid(database, "values_t", rid, 1));
    assert(!engine.isColumnNullByRid(database, "values_t", rid, 2));
}

}  // namespace

int main() {
    cleanupAllTestData();
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "drop_column_null_preservation";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);

    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "values_t";
    table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    table.append(dbms::makeIntColumn("obsolete", false, 4));
    table.append(dbms::makeIntColumn("id", false, 4, true));
    table.append(dbms::makeVarCharColumn("nullable_value", true, 32));
    table.append(dbms::makeVarCharColumn("empty_value", true, 32));
    assert(g_engine.createTable(database, table) == dbms::DBStatus::OK);

    assert(g_engine.insertRow(
               database, "values_t",
               {{"obsolete", std::string("99")},
                {"id", std::string("1")},
                {"nullable_value", std::nullopt},
                {"empty_value", std::string()}}) == dbms::DBStatus::OK);
    assert(g_engine.alterTableDropColumn(
               database, "values_t", "obsolete") == dbms::DBStatus::OK);
    assertNullAndEmptyRemainDistinct(g_engine, database);
    assert(g_engine.insertRow(
               database, "values_t",
               {{"id", std::string("1")},
                {"nullable_value", std::string("duplicate")},
                {"empty_value", std::string("duplicate")}}) ==
           dbms::DBStatus::DUPLICATE_KEY);

    dbms::StorageEngine restarted;
    assertNullAndEmptyRemainDistinct(restarted, database);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[DROP COLUMN NULL PRESERVATION] all passed" << std::endl;
    return 0;
}
