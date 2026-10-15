#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/Config.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;

using Cell = std::optional<std::string>;
using Rows = std::vector<std::vector<Cell>>;
using Engine = dbms::StorageEngine;

static Rows snapshot(const std::string& database, const dbms::TableSchema& table) {
    dbms::TableScanOp scan(&g_engine, database, table.tablename);
    assert(scan.open());
    Rows result;
    std::string raw;
    while (scan.next(raw)) {
        std::vector<Cell> row;
        for (size_t i = 0; i < table.len; ++i) {
            if (scan.lastColumnIsNull(i)) row.push_back(std::nullopt);
            else row.push_back(Engine::extractColumnValueStatic(raw, table, i));
        }
        result.push_back(std::move(row));
    }
    assert(!scan.hasError());
    scan.close();
    return result;
}

// Independent oracle: the pre-optimization comparator and statistics rules,
// applied to the actual stored values (not potentially normalized input text).
static Engine::ColumnStats reference(const dbms::Column& column,
                                     const Rows& rows, size_t ordinal) {
    Engine::ColumnStats expected;
    std::vector<std::string> values;
    for (const auto& row : rows) {
        if (!row[ordinal]) ++expected.nullCount;
        else values.push_back(*row[ordinal]);
    }
    std::sort(values.begin(), values.end(), [&column](const auto& left, const auto& right) {
        const bool leftComparable = Engine::compareValues(column, left, false,
            left, false, "=") != Engine::PredicateTruth::Unknown;
        const bool rightComparable = Engine::compareValues(column, right, false,
            right, false, "=") != Engine::PredicateTruth::Unknown;
        if (leftComparable != rightComparable) return leftComparable;
        if (!leftComparable) return left < right;
        return Engine::compareValues(column, left, false, right, false, "<")
            == Engine::PredicateTruth::True;
    });
    if (!values.empty()) {
        expected.minVal = values.front();
        expected.maxVal = values.back();
    }
    if (values.size() >= 20) {
        const size_t width = values.size() / 10;
        for (size_t i = 0; i < 10; ++i)
            expected.histogram.push_back({values[i * width],
                values[i == 9 ? values.size() - 1 : (i + 1) * width]});
    }
    for (const auto& value : values) {
        if (!expected.mcv.empty() && (expected.mcv.back().first == value ||
            Engine::compareValues(column, expected.mcv.back().first, false,
                value, false, "=") == Engine::PredicateTruth::True))
            ++expected.mcv.back().second;
        else expected.mcv.push_back({value, 1});
    }
    expected.cardinality = expected.mcv.size();
    std::stable_sort(expected.mcv.begin(), expected.mcv.end(),
        [](const auto& left, const auto& right) { return left.second > right.second; });
    if (expected.mcv.size() > 10) expected.mcv.resize(10);
    return expected;
}

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const bool savedAnalyze = g_config.autoAnalyzeEnabled;
    g_config.autoAnalyzeEnabled = false;
    struct Restore { bool value; ~Restore() { g_config.autoAnalyzeEnabled = value; } } restore{savedAnalyze};
    const auto database = testDbPath("analyze_sort_permutation");
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    size_t controls = 0;
    for (const size_t population : {0, 1, 2, 3, 17, 20, 33, 65}) {
        TableSchema table;
        table.tablename = "rows_" + std::to_string(population);
        table.append(makeIntColumn("big", true, 8));
        table.append(makeDecimalColumn("exact", true, 40, 3));
        table.append(makeDoubleColumn("floating", true));
        table.append(makeTextColumn("plain", true));
        auto folded = makeTextColumn("folded", true);
        folded.collation = "nocase";
        table.append(folded);
        table.append(makeTextColumn("all_null", true));
        assert(g_engine.createTable(database, table) == DBStatus::OK);
        assert(g_engine.beginTransaction(database) == DBStatus::OK);
        const std::vector<std::string> floats{"NaN", "-Infinity", "Infinity", "-2.25", "0", "1.25"};
        const std::vector<std::string> texts{"", "NULL", "Apple", "apple", "Zoo", "10", "2"};
        for (size_t row = 0; row < population; ++row) {
            // A nontrivial permutation, adjacent and separated duplicates,
            // exact integers beyond double precision and typed-equal spellings.
            const size_t key = (row * 23 + population * 7) % 29;
            Engine::SqlRow input{
                {"big", std::to_string(9007199254740992LL + static_cast<long long>(key))},
                {"exact", "123456789012345678901234567890." + std::to_string(key + 10)},
                {"floating", floats[key % floats.size()]},
                {"plain", texts[key % texts.size()]},
                {"folded", texts[(key + row / 3) % texts.size()]},
                {"all_null", std::nullopt}};
            if (row % 7 == 6) input["plain"] = std::nullopt;
            if (row % 11 == 10) input["floating"] = std::nullopt;
            if (row % 5 == 4) input["exact"] = row % 2 ? "0.50" : "0.500";
            assert(g_engine.insertRow(database, table.tablename, input) == DBStatus::OK);
        }
        assert(g_engine.commitTransaction() == DBStatus::OK);
        const auto stored = g_engine.getTableSchema(database, table.tablename);
        const auto before = snapshot(database, stored);
        assert(before.size() == population);
        std::vector<Engine::ColumnStats> expected;
        for (size_t ordinal = 0; ordinal < stored.len; ++ordinal)
            expected.push_back(reference(stored.cols[ordinal], before, ordinal));
        // Repeated ANALYZE must not retain indexes/flags from a previous run.
        for (int repeat = 0; repeat < 2; ++repeat) {
            assert(g_engine.analyzeTable(database, table.tablename));
            assert(g_engine.getTableRowCount(database, table.tablename) == population);
            assert(snapshot(database, stored) == before);
            for (size_t ordinal = 0; ordinal < stored.len; ++ordinal) {
                const auto actual = g_engine.getColumnStats(database,
                    table.tablename, stored.cols[ordinal].dataName);
                const auto& wanted = expected[ordinal];
                assert(actual.nullCount == wanted.nullCount);
                assert(actual.cardinality == wanted.cardinality);
                assert(actual.minVal == wanted.minVal && actual.maxVal == wanted.maxVal);
                assert(actual.histogram == wanted.histogram);
                assert(actual.mcv == wanted.mcv);
                ++controls;
            }
        }
    }
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    std::cout << "[ANALYZE SORT PERMUTATION] " << controls << " column/run controls passed\n";
}
