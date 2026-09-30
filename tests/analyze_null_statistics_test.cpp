#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <cmath>
#include <iostream>
#include <map>
#include <sstream>

extern dbms::StorageEngine g_engine;

static double nullFraction(const std::string& db, const std::string& table,
                           const std::string& column) {
    const auto rows = g_engine.getPgStatsRows(db, table, column);
    assert(rows.size() == 1);
    std::istringstream fields(rows.front());
    std::string database, relation, attribute;
    double fraction = -1;
    fields >> database >> relation >> attribute >> fraction;
    assert(attribute == column);
    return fraction;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("analyze_null_statistics");
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "items";
    table.append(dbms::makeVarCharColumn("text_value", true, 32));
    table.append(dbms::makeIntColumn("number_value", true, 4));
    table.append(dbms::makeVarCharColumn("all_null", true, 32));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    for (int i = 0; i < 20; ++i) {
        const bool isNull = i < 5;
        const std::string text = i < 10 ? "" : i < 15 ? "NULL" : "other";
        dbms::StorageEngine::SqlRow row{
            {"text_value", isNull ? std::nullopt : std::optional<std::string>(text)},
            {"number_value", isNull ? std::nullopt : std::optional<std::string>("7")},
            {"all_null", std::nullopt}};
        assert(g_engine.insertRow(db, "items", row) == dbms::DBStatus::OK);
    }
    assert(g_engine.analyzeTable(db, "items"));
    assert(g_engine.getTableRowCount(db, "items") == 20);
    const auto text = g_engine.getColumnStats(db, "items", "text_value");
    assert(text.nullCount == 5); // Empty text is data, not a sixth NULL.
    assert(text.cardinality == 3);
    const std::map<std::string, size_t> textMcv(text.mcv.begin(), text.mcv.end());
    assert((textMcv == std::map<std::string, size_t>{{"", 5}, {"NULL", 5}, {"other", 5}}));
    assert(text.histogram.empty()); // Only 15 non-NULL inputs, below the threshold.
    const auto number = g_engine.getColumnStats(db, "items", "number_value");
    assert(number.nullCount == 5 && number.cardinality == 1);
    assert(number.minVal == "7" && number.maxVal == "7");
    assert((number.mcv == std::vector<std::pair<std::string, size_t>>{{"7", 15}}));
    const auto allNull = g_engine.getColumnStats(db, "items", "all_null");
    assert(allNull.nullCount == 20 && allNull.cardinality == 0);
    assert(allNull.minVal.empty() && allNull.maxVal.empty());
    assert(allNull.histogram.empty() && allNull.mcv.empty());
    assert(std::abs(nullFraction(db, "items", "text_value") - 0.25) < 1e-9);
    assert(std::abs(nullFraction(db, "items", "number_value") - 0.25) < 1e-9);
    assert(nullFraction(db, "items", "all_null") == 1.0);
    table.tablename = "empty";
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    assert(g_engine.analyzeTable(db, "empty"));
    assert(nullFraction(db, "empty", "text_value") == 0.0);
    // Leaf page/slot numbers collide. NULL metadata must come from the
    // physical leaf tuple rather than a lookup in the parent heap.
    table.tablename = "partitioned";
    table.partitionType = dbms::TableSchema::PartitionType::Range;
    table.partitionKey = "number_value";
    table.rangePartitions = {{"low_values", "10"}, {"high_values", "MAXVALUE"}};
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    for (int i = 0; i < 4; ++i) {
        dbms::StorageEngine::SqlRow row{
            {"text_value", i % 2 == 0 ? std::nullopt
                : std::optional<std::string>(i == 1 ? "" : "other")},
            {"number_value", std::to_string(i < 2 ? i + 1 : i + 9)},
            {"all_null", std::nullopt}};
        assert(g_engine.insertRow(db, "partitioned", row) == dbms::DBStatus::OK);
    }
    assert(g_engine.analyzeTable(db, "partitioned"));
    const auto partitioned = g_engine.getColumnStats(db, "partitioned", "text_value");
    assert(partitioned.nullCount == 2 && partitioned.cardinality == 2);
    const std::map<std::string, size_t> partitionMcv(partitioned.mcv.begin(), partitioned.mcv.end());
    assert((partitionMcv == std::map<std::string, size_t>{{"", 1}, {"other", 1}}));
    assert(nullFraction(db, "partitioned", "text_value") == 0.5);
    assert(g_engine.dropDatabase(db) == dbms::DBStatus::OK);
    std::cout << "[ANALYZE NULL STATISTICS] passed\n";
}
