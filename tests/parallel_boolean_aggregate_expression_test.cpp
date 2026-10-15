// Real 512-row typed sources and actual two-worker execution; the independent
// population oracle and fixed result anchors also check serial/parallel parity.
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "expression/expr_helper.h"
#include "test_utils.h"

#include <charconv>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <tuple>
#include <unistd.h>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {
constexpr size_t kRows = 512;
constexpr size_t kChunkRows = 256;
constexpr int kWorkers = 2;

struct SourceRow {
    int64_t id = 0;
    int64_t group = 0;
    std::optional<int64_t> number;
    std::optional<bool> boolean;
};

struct Population {
    size_t rows = 0;
    size_t nulls = 0;
    size_t trues = 0;
    size_t falses = 0;
};

using Populations = std::map<int64_t, Population>;
using ResultRows = std::map<int64_t, std::optional<bool>>;
using RunKey = std::tuple<std::string, bool, std::string, std::string, bool>;

int64_t integer(const std::string& text) {
    int64_t value = 0;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
    if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size())
        throw std::runtime_error("source did not contain an exact integer datum");
    return value;
}

bool storedBoolean(const std::string& text) {
    if (text == "t" || text == "true" || text == "1") return true;
    if (text == "f" || text == "false" || text == "0") return false;
    throw std::runtime_error("source did not contain a boolean codec datum");
}

void add(Population& result, const std::optional<bool>& truth) {
    ++result.rows;
    if (!truth) ++result.nulls;
    else if (*truth) ++result.trues;
    else ++result.falses;
}

// Independent finite-population oracle: count actual typed numeric inputs,
// then apply the SQL aggregate truth table. Never evaluate an AST or invoke
// an aggregate to calculate its own expected result.
Populations populations(const std::vector<SourceRow>& rows, size_t begin, size_t end) {
    Populations result;
    for (size_t i = begin; i < end; ++i) {
        const auto& row = rows.at(i);
        const auto truth = row.number ? std::optional<bool>(*row.number > 5) : std::nullopt;
        add(result[0], truth); // Global result has no SQL group-key cell.
        add(result[row.group], truth);
    }
    return result;
}

bool populationIs(const Populations& populations, int64_t group,
                  size_t rows, size_t nulls, size_t trues, size_t falses) {
    const auto found = populations.find(group);
    if (found == populations.end()) return false;
    const auto& value = found->second;
    return value.rows == rows && value.nulls == nulls &&
        value.trues == trues && value.falses == falses &&
        value.rows == value.nulls + value.trues + value.falses;
}

ResultRows expectedRows(const Populations& populations, bool grouped,
                        const std::string& function) {
    ResultRows result;
    for (const auto& entry : populations) {
        if ((entry.first == 0) == grouped) continue;
        const auto& counts = entry.second;
        std::optional<bool> truth;
        if (counts.trues + counts.falses != 0)
            truth = function == "bool_or" ? counts.trues != 0 : counts.falses == 0;
        result.emplace(entry.first, truth);
    }
    return result;
}

struct Execution {
    bool ok = false;
    bool usedParallelWorkers = false;
    int requestedWorkers = 0;
    ResultRows rows;
    std::string sqlState;
    std::string error;
};

// Keep ownership through usedParallelWorkers(): executePlanChecked takes and
// destroys its unique_ptr, so retaining its former raw pointer would be wrong.
// This harness uses the actual Operator open/next/structured/close contract.
Execution execute(const std::string& database, const dbms::TableSchema& table,
                  bool grouped, bool parallel, const std::string& function,
                  const std::string& argument) {
    using namespace dbms;
    Execution result;
    OpPtr plan;
    try {
        auto scan = std::make_unique<TableScanOp>(&g_engine, database, table.tablename);
        const std::vector<std::string> keys = grouped
            ? std::vector<std::string>{"g"} : std::vector<std::string>{};
        const std::vector<StorageEngine::AggItem> items = {{function, argument, {}, {}}};
        ParallelGroupAggregateOp* parallelNode = nullptr;
        if (parallel) {
            auto concrete = std::make_unique<ParallelGroupAggregateOp>(
                std::move(scan), table, keys, items, std::vector<std::string>{}, kWorkers);
            parallelNode = concrete.get();
            plan = std::move(concrete);
        } else {
            plan = std::make_unique<GroupAggregateOp>(
                std::move(scan), table, keys, std::vector<std::vector<std::string>>{},
                items, std::vector<std::string>{});
        }
        if (!plan->open()) throw std::runtime_error("open failed: " + plan->errorMessage());
        if (parallelNode) {
            result.requestedWorkers = parallelNode->workers();
            result.usedParallelWorkers = parallelNode->usedParallelWorkers();
        }
        if (!plan->supportsStructuredRows())
            throw std::runtime_error("aggregate did not support actual structured rows");
        std::string display;
        while (plan->next(display)) {
            std::vector<std::string> cells;
            std::vector<bool> nulls;
            const size_t width = grouped ? 2 : 1;
            if (!plan->lastStructuredRow(cells, nulls) ||
                cells.size() != width || nulls.size() != width)
                throw std::runtime_error("aggregate lost cells or actual NULL bitmap");
            int64_t group = 0;
            if (grouped) {
                if (nulls[0]) throw std::runtime_error("non-NULL source group became NULL");
                group = integer(cells[0]);
                if (group != 1 && group != 2)
                    throw std::runtime_error("unexpected source group identity");
            }
            const size_t column = grouped ? 1 : 0;
            std::optional<bool> value;
            if (!nulls[column]) {
                // Check actual non-NULL output codec; never parse display or
                // accept an empty string/"NULL" as a boolean or SQL NULL.
                if (cells[column] != "t" && cells[column] != "f")
                    throw std::runtime_error("non-NULL boolean result was not t/f");
                value = cells[column] == "t";
            }
            if (!result.rows.emplace(group, value).second)
                throw std::runtime_error("aggregate emitted duplicate group rows");
        }
        if (plan->hasError()) throw std::runtime_error(plan->errorMessage());
        result.ok = true;
    } catch (const DbError& error) {
        result.sqlState = error.sqlState(); result.error = error.what();
    } catch (const std::exception& error) {
        result.sqlState = "XX000"; result.error = error.what();
    }
    if (plan) {
        try { plan->close(); }
        catch (const DbError& error) {
            result.ok = false; result.sqlState = error.sqlState(); result.error = error.what();
        } catch (const std::exception& error) {
            result.ok = false; result.sqlState = "XX000"; result.error = error.what();
        }
    }
    if (!result.ok) result.rows.clear();
    return result;
}

void printExecution(const Execution& result) {
    std::cout << " ok=" << result.ok << " configured_workers=" << result.requestedWorkers
              << " used_parallel_workers=" << result.usedParallelWorkers
              << " SQLSTATE=" << result.sqlState;
    for (const auto& entry : result.rows) {
        std::cout << " group[" << entry.first << "]=";
        if (!entry.second) std::cout << "<SQL-NULL>";
        else std::cout << (*entry.second ? "t" : "f");
    }
    if (!result.error.empty()) std::cout << " diagnostic=" << result.error;
    std::cout << '\n';
}

struct FixedControl {
    const char* table;
    bool grouped;
    const char* function;
    const char* argument;
    bool parallel;
    int64_t group;
    std::optional<bool> expected;
};
} // namespace

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string database = testDbPath("parallel_boolean_argument_" + std::to_string(::getpid()));
    if (std::filesystem::exists(database)) {
        std::cerr << "Refusing to reuse an existing fixture: " << database << '\n';
        return 2;
    }
    if (g_engine.createDatabase(database, "utf8") != DBStatus::OK) return 2;

    size_t controls = 0, failures = 0;
    const auto record = [&](bool good, const std::string& label) {
        ++controls;
        if (!good) ++failures;
        std::cout << "PARALLEL_BOOL_WORKER " << label << " PASS=" << good << '\n';
    };
    std::map<RunKey, Execution> runs;
    const std::vector<std::string> arguments = {
        "b", "v > 5", "v>5",
        "CASE WHEN v IS NULL THEN NULL ELSE v > 5 END",
        "CAST(b AS BOOLEAN)", "COALESCE(b,NULL::boolean)",
        "b AND true", "CASE WHEN b THEN true WHEN b IS NULL THEN NULL ELSE false END"};

    for (const bool mixed : {false, true}) {
        const std::string tableName = mixed ? "mixed" : "all_true";
        TableSchema definition;
        definition.tablename = tableName;
        definition.formatVersion = DATA_FILE_FORMAT_VERSION;
        definition.append(makeIntColumn("id", false, 2, true));
        definition.append(makeIntColumn("g", false, 2));
        definition.append(makeIntColumn("v", true, 2));
        definition.append(makeBooleanColumn("b", true));
        if (g_engine.createTable(database, definition) != DBStatus::OK) return 2;

        // Each physical 256-row chunk has both groups and real nullable
        // numeric/boolean fields. g2 is only-NULL in EACH worker, not merely
        // in one worker. The third row supplies a real false in mixed.
        const std::vector<SourceRow> pattern = {
            {0, 1, int64_t{6}, true}, {0, 2, std::nullopt, std::nullopt},
            {0, 1, mixed ? int64_t{1} : int64_t{9}, !mixed},
            {0, 1, std::nullopt, std::nullopt}};
        std::vector<SourceRow> authored;
        authored.reserve(kRows);
        for (size_t position = 0; position < kRows; ++position) {
            auto row = pattern[position % pattern.size()];
            row.id = static_cast<int64_t>(position + 1);
            const auto number = row.number
                ? std::optional<std::string>(std::to_string(*row.number)) : std::nullopt;
            const auto boolean = row.boolean
                ? std::optional<std::string>(*row.boolean ? "true" : "false") : std::nullopt;
            if (g_engine.insertRow(database, tableName,
                    {{"id", std::to_string(row.id)}, {"g", std::to_string(row.group)},
                     {"v", number}, {"b", boolean}}) != DBStatus::OK) return 2;
            authored.push_back(row);
        }

        const auto table = g_engine.getTableSchema(database, tableName);
        for (size_t ci = 0; ci < table.len; ++ci)
            std::cout << "SOURCE_COLUMN " << ci << " declaration=" << table.cols[ci].dataType
                      << " canonical=" << ExprHelper::canonicalResultTypeName(table.cols[ci].dataType)
                      << " bytes=" << table.cols[ci].dsize << " nullable=" << table.cols[ci].isNull << '\n';
        bool sourceOk = table.len == 4 &&
            table.cols[0].dsize == 4 && table.cols[1].dsize == 4 &&
            table.cols[2].dsize == 4 && table.cols[3].dsize == 1 &&
            ExprHelper::canonicalResultTypeName(table.cols[0].dataType) == "integer" &&
            ExprHelper::canonicalResultTypeName(table.cols[1].dataType) == "integer" &&
            ExprHelper::canonicalResultTypeName(table.cols[2].dataType) == "integer" &&
            ExprHelper::canonicalResultTypeName(table.cols[3].dataType) == "boolean" &&
            table.cols[2].isNull && table.cols[3].isNull;
        std::vector<SourceRow> observed;
        TableScanOp scan(&g_engine, database, tableName);
        try {
            if (!sourceOk) throw std::runtime_error("source schema did not preserve expected column types");
            if (!scan.open()) throw std::runtime_error("source open failed: " + scan.errorMessage());
            std::string raw;
            while (scan.next(raw)) {
                const size_t position = observed.size();
                if (position >= authored.size()) { sourceOk = false; break; }
                const auto& expected = authored[position];
                SourceRow row;
                row.id = integer(StorageEngine::extractColumnValueStatic(raw, table, 0));
                row.group = integer(StorageEngine::extractColumnValueStatic(raw, table, 1));
                if (!scan.lastColumnIsNull(2))
                    row.number = integer(StorageEngine::extractColumnValueStatic(raw, table, 2));
                if (!scan.lastColumnIsNull(3))
                    row.boolean = storedBoolean(StorageEngine::extractColumnValueStatic(raw, table, 3));
                sourceOk = sourceOk && !scan.lastColumnIsNull(0) && !scan.lastColumnIsNull(1) &&
                    row.id == expected.id && row.group == expected.group &&
                    row.number == expected.number && row.boolean == expected.boolean &&
                    scan.lastColumnIsNull(2) == !expected.number.has_value() &&
                    scan.lastColumnIsNull(3) == !expected.boolean.has_value() &&
                    row.boolean == (row.number ? std::optional<bool>(*row.number > 5) : std::nullopt);
                observed.push_back(row);
            }
            sourceOk = sourceOk && !scan.hasError() && observed.size() == kRows;
            scan.close();
        } catch (const std::exception& error) {
            sourceOk = false; std::cout << "SOURCE_EXCEPTION " << error.what() << '\n';
            try { scan.close(); } catch (...) {}
        }
        record(sourceOk, tableName + " actual-512-row-types-values-order-NULL-bitmap");
        if (!sourceOk) return 2; // Never run aggregate assertions on a false source premise.

        const auto all = populations(observed, 0, observed.size());
        const size_t trueCount = mixed ? 128 : 256;
        const size_t falseCount = mixed ? 128 : 0;
        record(populationIs(all, 0, 512, 256, trueCount, falseCount) &&
               populationIs(all, 1, 384, 128, trueCount, falseCount) &&
               populationIs(all, 2, 128, 128, 0, 0), tableName + " actual-whole-populations");
        for (size_t worker = 0; worker < 2; ++worker) {
            const auto chunk = populations(observed, worker * kChunkRows, (worker + 1) * kChunkRows);
            const size_t chunkTrue = mixed ? 64 : 128;
            const size_t chunkFalse = mixed ? 64 : 0;
            record(populationIs(chunk, 0, 256, 128, chunkTrue, chunkFalse) &&
                   populationIs(chunk, 1, 192, 64, chunkTrue, chunkFalse) &&
                   populationIs(chunk, 2, 64, 64, 0, 0),
                   tableName + " actual-worker-chunk-" + std::to_string(worker) + " typed-NULL-onlynull");
        }

        for (const bool grouped : {false, true}) {
            for (const std::string function : {"bool_and", "bool_or", "every"}) {
                const auto expected = expectedRows(all, grouped, function);
                for (const std::string& argument : arguments) {
                    const std::string label = tableName + " " + (grouped ? "grouped" : "global") +
                        " " + function + "(" + argument + ")";
                    const auto serial = execute(database, table, grouped, false, function, argument);
                    const auto parallel = execute(database, table, grouped, true, function, argument);
                    printExecution(serial); printExecution(parallel);
                    record(serial.ok && serial.sqlState.empty() && serial.rows == expected,
                           label + " serial-exact-typed-result");
                    record(parallel.ok && parallel.sqlState.empty() && parallel.rows == expected,
                           label + " parallel-exact-typed-result");
                    record(parallel.ok && parallel.requestedWorkers == kWorkers &&
                           parallel.usedParallelWorkers,
                           label + " actual-usedParallelWorkers-workers2-512rows");
                    record(serial.ok && parallel.ok && serial.rows == parallel.rows,
                           label + " serial-vs-actual-workers-agree");
                    runs.emplace(RunKey{tableName, grouped, function, argument, false}, serial);
                    runs.emplace(RunKey{tableName, grouped, function, argument, true}, parallel);
                }
            }
        }
    }

    // Sixteen independently written positive result anchors. These do not
    // call populations()/expectedRows(), so an oracle bug cannot make every
    // test agree with itself. record() stays active even under -DNDEBUG.
    const std::vector<FixedControl> fixed = {
        {"all_true", false, "bool_and", "b", false, 0, true},
        {"all_true", false, "bool_and", "b", true, 0, true},
        {"all_true", false, "bool_or", "v > 5", false, 0, true},
        {"all_true", false, "bool_or", "v > 5", true, 0, true},
        {"all_true", true, "every", "v > 5", false, 1, true},
        {"all_true", true, "every", "v > 5", true, 1, true},
        {"all_true", true, "bool_and", "b", false, 2, std::nullopt},
        {"all_true", true, "bool_and", "v > 5", true, 2, std::nullopt},
        {"mixed", false, "bool_and", "v > 5", false, 0, false},
        {"mixed", false, "bool_and", "v > 5", true, 0, false},
        {"mixed", false, "bool_or", "b", false, 0, true},
        {"mixed", false, "bool_or", "b", true, 0, true},
        {"mixed", true, "every", "b", false, 1, false},
        {"mixed", true, "every", "v > 5", true, 1, false},
        {"mixed", true, "bool_or", "v > 5", false, 2, std::nullopt},
        {"mixed", true, "every", "b", true, 2, std::nullopt},
    };
    for (size_t index = 0; index < fixed.size(); ++index) {
        const auto& control = fixed[index];
        const auto found = runs.find(RunKey{control.table, control.grouped,
            control.function, control.argument, control.parallel});
        bool good = found != runs.end() && found->second.ok;
        if (good) {
            const auto row = found->second.rows.find(control.group);
            good = row != found->second.rows.end() && row->second == control.expected;
        }
        record(good, "fixed-positive-result-anchor-" + std::to_string(index + 1));
    }

    // 8 source/whole/chunk proofs + 192 exact results + 96 worker flags
    // + 96 serial/parallel agreements + 16 fixed positive result anchors.
    if (controls != 408 || fixed.size() != 16) {
        std::cerr << "INCOMPLETE_MATRIX controls=" << controls << " expected=408\n";
        return 2;
    }
    std::cout << "PARALLEL_BOOL_WORKER_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
