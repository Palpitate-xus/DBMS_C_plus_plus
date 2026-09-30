#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <iostream>
#include <set>

extern dbms::StorageEngine g_engine;

static void check(const std::string& db, const std::string& name,
                  dbms::Column column, std::vector<std::string> values,
                  const std::string& minimum, const std::string& maximum) {
    dbms::TableSchema table;
    table.tablename = name;
    table.append(column);
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    // Deliberately reverse insertion order to exercise typed sorting.
    std::reverse(values.begin(), values.end());
    for (const auto& value : values)
        assert(g_engine.insert(db, name, {{"v", value}}) == dbms::DBStatus::OK);
    assert(g_engine.analyzeTable(db, name));
    const auto stats = g_engine.getColumnStats(db, name, "v");
    if (stats.minVal != minimum || stats.maxVal != maximum)
        std::cerr << name << ": min=" << stats.minVal << " max=" << stats.maxVal << '\n';
    assert(stats.minVal == minimum && stats.maxVal == maximum);
    assert(stats.histogram.size() == (values.size() >= 20 ? 10 : 0));
    const std::set<std::string> original(values.begin(), values.end());
    std::string previous;
    bool first = true;
    for (const auto& bucket : stats.histogram) {
        assert(original.count(bucket.first) && original.count(bucket.second));
        assert(dbms::StorageEngine::compareValues(column, bucket.first, false,
            bucket.second, false, "<=") == dbms::StorageEngine::PredicateTruth::True);
        if (!first)
            assert(dbms::StorageEngine::compareValues(column, previous, false,
                bucket.first, false, "<=") == dbms::StorageEngine::PredicateTruth::True);
        previous = bucket.second;
        first = false;
    }
    if (!stats.histogram.empty()) {
        assert(stats.histogram.front().first == minimum);
        assert(stats.histogram.back().second == maximum);
    }
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("analyze_typed_statistics");
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    std::vector<std::string> fractional, largeIntegers, exactNumeric;
    for (int i = 1; i <= 20; ++i) {
        fractional.push_back(std::to_string(i) + ".25");
        largeIntegers.push_back(std::to_string(9007199254740992LL + i));
        exactNumeric.push_back("123456789012345678901234567890." + std::to_string(i + 10));
    }
    check(db, "double_values", dbms::makeDoubleColumn("v", false),
        fractional, "1.25", "20.25");
    check(db, "float_values", dbms::makeFloatColumn("v", false),
        fractional, "1.25", "20.25");
    check(db, "bigint_values", dbms::makeIntColumn("v", false, 8),
        largeIntegers, "9007199254740993", "9007199254741012");
    check(db, "numeric_values", dbms::makeDecimalColumn("v", false, 40, 2),
        exactNumeric, "123456789012345678901234567890.11", "123456789012345678901234567890.30");
    check(db, "signed_values", dbms::makeIntColumn("v", false, 8),
        {"-10", "-2", "0", "2", "10"}, "-10", "10");
    // Text digits remain lexically ordered rather than coerced to numbers.
    check(db, "text_values", dbms::makeVarCharColumn("v", false, 32),
        {"1", "2", "10", "20"}, "1", "20");
    check(db, "special_values", dbms::makeDoubleColumn("v", false),
        {"-Infinity", "-2.25", "1.25", "Infinity", "NaN"}, "-Infinity", "NaN");
    assert(g_engine.dropDatabase(db) == dbms::DBStatus::OK);
    std::cout << "[ANALYZE TYPED STATISTICS] passed\n";
}
