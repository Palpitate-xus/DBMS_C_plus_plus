#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

size_t rowCount(const std::string& database, const std::string& tableName) {
    size_t count = 0;
    assert(g_engine.forEachRow(
        database, tableName,
        [&](uint32_t, uint16_t, const char*, size_t) { ++count; }));
    return count;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    cleanupAllTestData();

    const std::string testName = "partition_column_dependency";
    const std::string database = testDbPath(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema table;
    table.tablename = "events";
    table.append(dbms::makeIntColumn("id", false, 4, true));
    table.append(dbms::makeIntColumn("bucket", false, 4));
    table.append(dbms::makeVarCharColumn("tag", false, 32));
    table.append(dbms::makeVarCharColumn("payload", true, 64));
    table.partitionType = dbms::TableSchema::PartitionType::Range;
    table.partitionKey = "bucket";
    table.rangePartitions = {{"low", "10"}, {"high", "20"}};
    table.defaultPartitionName = "overflow";
    table.subPartitionType = dbms::TableSchema::PartitionType::Hash;
    table.subPartitionKey = "tag";
    table.subHashPartitions = 2;
    assert(g_engine.createTable(database, table) == dbms::DBStatus::OK);

    assert(g_engine.insert(database, "events",
                           {{"id", "1"}, {"bucket", "5"},
                            {"tag", "left"}, {"payload", "one"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "events",
                           {{"id", "2"}, {"bucket", "25"},
                            {"tag", "right"}, {"payload", "two"}}) ==
           dbms::DBStatus::OK);
    assert(rowCount(database, "events") == 2);

    // DROP COLUMN has no CASCADE form. It must reject routing-key removal
    // before any heap files or schema metadata are rewritten.
    assert(g_engine.alterTableDropColumn(database, "events", "bucket") ==
           dbms::DBStatus::INVALID_VALUE);
    dbms::TableSchema unchanged =
        g_engine.getTableSchema(database, "events");
    assert(unchanged.len == 4);
    assert(unchanged.partitionKey == "bucket");
    assert(rowCount(database, "events") == 2);

    assert(g_engine.alterTableDropColumn(database, "events", "tag") ==
           dbms::DBStatus::INVALID_VALUE);
    unchanged = g_engine.getTableSchema(database, "events");
    assert(unchanged.len == 4);
    assert(unchanged.subPartitionKey == "tag");
    assert(rowCount(database, "events") == 2);

    assert(g_engine.alterTableRenameColumn(
               database, "events", "bucket", "route") ==
           dbms::DBStatus::OK);
    assert(g_engine.alterTableRenameColumn(
               database, "events", "tag", "label") ==
           dbms::DBStatus::OK);
    const dbms::TableSchema renamed =
        g_engine.getTableSchema(database, "events");
    assert(renamed.partitionKey == "route");
    assert(renamed.subPartitionKey == "label");
    assert(rowCount(database, "events") == 2);

    assert(g_engine.insert(database, "events",
                           {{"id", "3"}, {"route", "12"},
                            {"label", "middle"}, {"payload", "three"}}) ==
           dbms::DBStatus::OK);
    assert(rowCount(database, "events") == 3);

    // A permitted drop rebuilds partition heaps. This verifies that the
    // renamed routing metadata is used by the rewrite path too.
    assert(g_engine.alterTableDropColumn(
               database, "events", "payload") == dbms::DBStatus::OK);
    const size_t rewrittenRowCount = rowCount(database, "events");
    assert(rewrittenRowCount == 3);

    {
        dbms::StorageEngine restarted;
        const dbms::TableSchema reloaded =
            restarted.getTableSchema(database, "events");
        assert(reloaded.len == 3);
        assert(reloaded.partitionKey == "route");
        assert(reloaded.subPartitionKey == "label");
    }

    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    finalCleanupTestData();
    std::cout << "[PARTITION COLUMN] dependencies preserved OK" << std::endl;
    return 0;
}
