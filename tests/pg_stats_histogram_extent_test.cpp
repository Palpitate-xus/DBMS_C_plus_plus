#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <sstream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("pg_stats_histogram_extent");
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "fractional";
    table.append(dbms::makeDoubleColumn("v", false));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    for (int i = 1; i <= 20; ++i)
        assert(g_engine.insert(db, table.tablename, {{"v", std::to_string(i) + ".25"}})
            == dbms::DBStatus::OK);
    assert(g_engine.analyzeTable(db, table.tablename));
    const auto stats = g_engine.getColumnStats(db, table.tablename, "v");
    assert(stats.histogram.size() == 10);
    const auto rows = g_engine.getPgStatsRows(db, table.tablename, "v");
    assert(rows.size() == 1);
    std::istringstream fields(rows.front());
    std::string histogram;
    for (int i = 0; i < 8; ++i) fields >> histogram;
    std::string expected;
    for (const auto& bucket : stats.histogram) {
        if (!expected.empty()) expected += ',';
        expected += '{' + bucket.first + '}';
    }
    expected += ",{20.25}";
    if (histogram != expected) std::cerr << histogram << '\n';
    assert(histogram == expected);
    assert(histogram.find("{1.25}") == 0);
    assert(histogram.rfind("{20.25}") == histogram.size() - 7);
    table.tablename = "small";
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, table.tablename, {{"v", "0.25"}}) == dbms::DBStatus::OK);
    assert(g_engine.analyzeTable(db, table.tablename));
    const auto small = g_engine.getPgStatsRows(db, table.tablename, "v");
    assert(small.size() == 1 && small.front().find("{0.25}") != std::string::npos);
    assert(g_engine.getColumnStats(db, table.tablename, "v").histogram.empty());
    assert(g_engine.dropDatabase(db) == dbms::DBStatus::OK);
    std::cout << "[PG STATS HISTOGRAM EXTENT] passed\n";
}
