#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "order_by_row_identity";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "sort_rows";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeIntColumn("sort_key", false, 4));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    constexpr int rowCount = 256;
    for (int id = 1; id <= rowCount; ++id) {
        assert(g_engine.insert(database, "sort_rows",
                   {{"id", std::to_string(id)},
                    {"sort_key", std::to_string(rowCount + 1 - id)}}) ==
               dbms::DBStatus::OK);
    }

    dbms::StorageEngine::OrderBySpec order;
    order.colName = "sort_key";
    order.ascending = true;
    const auto rows = g_engine.query(database, "sort_rows", {}, {"id"}, {order});
    assert(rows.size() == rowCount);
    for (int index = 0; index < rowCount; ++index)
        assert(rows[index] == std::to_string(rowCount - index) + " ");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[ORDER BY ROW IDENTITY] sorted each physical row exactly once\n";
    return 0;
}
