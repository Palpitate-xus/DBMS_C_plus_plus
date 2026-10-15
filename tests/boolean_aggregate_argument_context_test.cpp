#include "catalog/type_registry.h"
#include "catalog/collation.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

struct Result {
    bool ok = false, usedWorkers = false;
    std::optional<bool> value;
    std::string state;
};

struct Population { bool ok = false; size_t rows = 0, nonNull = 0; };

static Population effects(dbms::StorageEngine& owner, const std::string& database) {
    Population result;
    result.ok = owner.forEachRow(database, "effects",
        [&](uint32_t page, uint16_t slot, const char*, size_t) {
            ++result.rows;
            if (!owner.isColumnNullByRid(database, "effects",
                    dbms::StorageEngine::encodeRid(page, slot), 0)) ++result.nonNull;
        });
    return result;
}

static Result aggregate(dbms::StorageEngine& owner, const std::string& database,
                        const dbms::TableSchema& schema, bool parallel,
                        const std::string& function, const std::string& argument,
                        const std::vector<std::string>& filter = {}) {
    using namespace dbms;
    auto child = std::make_unique<TableScanOp>(&owner, database, schema.tablename);
    const std::vector<StorageEngine::AggItem> items = {{function, argument, filter, {}}};
    OpPtr plan;
    ParallelGroupAggregateOp* workerNode = nullptr;
    if (parallel) {
        auto node = std::make_unique<ParallelGroupAggregateOp>(std::move(child), schema,
            std::vector<std::string>{}, items, std::vector<std::string>{}, 2);
        workerNode = node.get(); plan = std::move(node);
    } else plan = std::make_unique<GroupAggregateOp>(std::move(child), schema,
        std::vector<std::string>{}, std::vector<std::vector<std::string>>{},
        items, std::vector<std::string>{});
    Result result;
    try {
        if (!plan->open()) throw std::runtime_error("aggregate open failed");
        if (workerNode) result.usedWorkers = workerNode->usedParallelWorkers();
        std::string display;
        std::vector<std::string> cells; std::vector<bool> nulls;
        if (!plan->next(display) || !plan->lastStructuredRow(cells, nulls) ||
            cells.size() != 1 || nulls.size() != 1)
            throw std::runtime_error("aggregate lost its one typed result");
        if (!nulls[0]) {
            if (cells[0] != "t" && cells[0] != "f")
                throw std::runtime_error("aggregate result was not a boolean codec");
            result.value = cells[0] == "t";
        }
        if (plan->next(display) || plan->hasError())
            throw std::runtime_error("aggregate produced extra rows or an error");
        result.ok = true;
    } catch (const DbError& error) { result.state = error.sqlState(); }
      catch (const std::exception&) { result.state = "XX000"; }
    plan->close();
    return result;
}

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine owner;
    const auto database = testDbPath("boolean_aggregate_argument_context");
    assert(owner.createDatabase(database) == DBStatus::OK);
    TableSchema table; table.tablename = "source";
    table.append(makeIntColumn("id", false, 2, true));
    table.append(makeIntColumn("v", true, 2));
    auto text = makeVarCharColumn("t", true, 64); text.collation = "C";
    table.append(text);
    assert(owner.createTable(database, table) == DBStatus::OK);
    TableSchema sink; sink.tablename = "effects";
    sink.append(makeIntColumn("v", true, 2));
    assert(owner.createTable(database, sink) == DBStatus::OK);
    assert(owner.beginTransaction(database) == DBStatus::OK);
    for (size_t i = 0; i < 512; ++i) {
        const auto number = i % 4 == 0 ? std::optional<std::string>("6") :
            i % 4 == 2 ? std::optional<std::string>("1") : std::nullopt;
        const auto value = i % 4 == 0 ? std::optional<std::string>("") :
            i % 4 == 1 ? std::nullopt : std::optional<std::string>(i % 4 == 2 ? "NULL" : "zeta");
        assert(owner.insertRow(database, "source", {{"id", std::to_string(i + 1)},
            {"v", number}, {"t", value}}) == DBStatus::OK);
    }
    assert(owner.commitTransaction() == DBStatus::OK);
    const auto schema = owner.getTableSchema(database, "source");
    assert(schema.len == 3 && schema.cols[0].dsize == 4 && schema.cols[1].dsize == 4);
    std::cout << "BOOLEAN_CONTEXT_COLLATION declaration=" << schema.cols[2].collation
              << " resolved=" << schema.cols[2].resolvedCollation
              << " binary=" << schema.cols[2].resolvedCollationIsBinary << std::endl;
    const auto effectiveCollation = schema.cols[2].resolvedCollation.empty()
        ? schema.cols[2].collation : schema.cols[2].resolvedCollation;
    assert(schema.cols[2].collation == "C" &&
        collation::normalizeName(effectiveCollation) == "c" &&
        collation::isBinary(effectiveCollation));
    assert(owner.createUDF(database, "argument_truth", {"arg"}, {"int"},
        "BEGIN INSERT INTO effects(v) VALUES(arg); RETURN arg > 5; END;",
        'v', "plpgsql", "boolean") == DBStatus::OK);
    size_t controls = 0, failures = 0;
    const auto check = [&](bool good, const std::string& label) {
        ++controls; if (!good) ++failures;
        std::cout << "BOOLEAN_ARGUMENT_CONTEXT " << label << " pass=" << good << std::endl;
    };
    for (const bool parallel : {false, true}) {
        const std::string mode = parallel ? "parallel " : "serial ";
        for (const auto& control : std::vector<std::pair<std::string, std::optional<bool>>>{
            {"t = ''", true}, {"t = 'NULL'", true}, {"t = NULL::text", std::nullopt},
            {"t IS NULL", true}, {"CAST(t AS text) = ''", true},
            {"t COLLATE \"C\" = ''", true},
            {"CASE WHEN id % 4 = 3 THEN t < 'alpha' ELSE NULL END", true}}) {
            const auto result = aggregate(owner, database, schema, parallel,
                "bool_or", control.first);
            check(result.ok && result.state.empty() && result.value == control.second &&
                  (!parallel || result.usedWorkers), mode + control.first);
        }
        for (const auto& control : std::vector<std::pair<std::string, std::string>>{
            {"1 / (v - v) > 0", "22012"}, {"CAST(t AS integer) > 0", "22P02"}}) {
            const auto result = aggregate(owner, database, schema, parallel,
                "bool_and", control.first);
            check(!result.ok && result.state == control.second,
                mode + "actual-error " + control.second);
        }
        const auto filtered = aggregate(owner, database, schema, parallel,
            "bool_and", "1 / (v - v) > 0", {"=id 99999"});
        check(filtered.ok && !filtered.value && (!parallel || filtered.usedWorkers),
            mode + "FILTER-rejects-before-row-error");

        for (const auto* function : {"bool_and", "bool_or", "every"}) {
            for (const bool firstFour : {false, true}) {
                const std::string label = mode + function + (firstFour ? " filtered" : " all");
                assert(owner.beginTransaction(database) == DBStatus::OK && owner.beginSqlCommand());
                const auto result = aggregate(owner, database, schema, parallel, function,
                    "argument_truth(v)", firstFour ? std::vector<std::string>{"<=id 4"} :
                                                     std::vector<std::string>{});
                const bool expected = std::string(function) == "bool_or";
                check(result.ok && result.state.empty() && result.value == expected &&
                    (!parallel || result.usedWorkers), label + " result");
                check(owner.inTransaction() && !g_engine.inTransaction(), label + " actual-engine-owner");
                assert(owner.finishSqlCommand() && owner.beginSqlCommand());
                const auto counts = effects(owner, database);
                const auto observer = effects(g_engine, database);
                std::cout << "BOOLEAN_CONTEXT_EFFECTS " << label << " actual_rows=" << counts.rows
                          << " actual_nonnull=" << counts.nonNull << " outsider_rows=" << observer.rows << std::endl;
                check(counts.ok && counts.rows == (firstFour ? 4u : 512u) &&
                    counts.nonNull == (firstFour ? 2u : 256u) && observer.ok && observer.rows == 0,
                    label + " exact-effects-and-NULLs-still-private");
                const auto rolledBack = owner.rollbackTransaction();
                const auto empty = effects(owner, database);
                check(rolledBack == DBStatus::OK && empty.ok && empty.rows == 0, label + " rollback");
            }
        }
    }
    if (controls != 68) {
        std::cerr << "INCOMPLETE_BOOLEAN_ARGUMENT_CONTEXT controls=" << controls << '\n';
        return 2;
    }
    std::cout << "BOOLEAN_ARGUMENT_CONTEXT_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
