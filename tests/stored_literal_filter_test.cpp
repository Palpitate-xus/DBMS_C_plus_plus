#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("stored_literal_filter");
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);

    TableSchema definition;
    definition.tablename = "orders";
    definition.formatVersion = DATA_FILE_FORMAT_VERSION;
    definition.append(makeVarCharColumn("shop", true, 60));
    definition.append(makeIntColumn("number", false, 8));
    assert(g_engine.createTable(database, definition) == DBStatus::OK);
    const auto table = g_engine.getTableSchema(database, definition.tablename);
    for (const auto& row : std::vector<StorageEngine::SqlRow>{
             {{"shop", "O'Brien"}, {"number", "9007199254740993"}},
             {{"shop", "O'Brien"}, {"number", "9007199254740992"}},
             {{"shop", ""}, {"number", "-9223372036854775808"}},
             {{"shop", "NULL"}, {"number", "7"}},
             {{"shop", std::nullopt}, {"number", "8"}}}) {
        assert(g_engine.insertRow(database, table.tablename, row) == DBStatus::OK);
    }

    const auto condition = [&](const std::string& sql) {
        auto parsed = StorageEngine::parseConditions({"typedexpr " + sql});
        assert(parsed.size() == 1);
        auto predicate = std::move(parsed.front());
        StorageEngine::bindStoredComparison(predicate, table);
        return predicate;
    };
    const auto execute = [&](const StorageEngine::Condition& predicate,
                             const std::string& number) {
        assert(g_engine.beginTransaction(database) == DBStatus::OK);
        assert(g_engine.beginSqlCommand());
        PlanContext context;
        context.dbname = database;
        context.tablename = table.tablename;
        context.selectCols = {"number"};
        context.conds = {predicate};
        const auto result = QueryPlanner::executePlanChecked(QueryPlanner::buildSelectPlan(&g_engine, context));
        if (!result.ok || result.structuredRows != std::vector<std::vector<std::string>>{{number}}) {
            std::cerr << predicate.value << " expected " << number << ", error " << result.error
                      << ", rows " << result.rows.size() << '\n';
            for (const auto& row : result.rows) std::cerr << row << '\n';
        }
        assert(result.ok && result.structuredRows == std::vector<std::vector<std::string>>{{number}});
        assert(g_engine.finishSqlCommand());
        assert(g_engine.commitTransaction() == DBStatus::OK);
        bool scanFailed = true;
        bool indexReadFailed = true;
        assert(g_engine.anyRowMatches(database, table.tablename, {predicate}, &scanFailed, &indexReadFailed));
        assert(!scanFailed && !indexReadFailed);
    };

    const auto pair = condition(
        "shop = CAST('O''Brien' AS pg_catalog.text) AND "
        "CAST('9007199254740992' AS pg_catalog.int8) < number");
    assert(pair.literalComparisons && pair.literalComparisons->size() == 2);
    assert(pair.literalComparisons->at(1).operation == ">");
    assert(std::get<int64_t>(pair.literalComparisons->at(1).value) == 9007199254740992LL);
    execute(pair, "9007199254740993");

    const auto blank = condition("shop = CAST('' AS pg_catalog.varchar)");
    assert(blank.literalComparisons && std::get<std::string>(blank.literalComparisons->front().value).empty());
    execute(blank, "-9223372036854775808");
    const auto spelledNull = condition("CAST('NULL' AS pg_catalog.text) = shop");
    assert(spelledNull.literalComparisons);
    execute(spelledNull, "7");

    assert(!condition("shop = CAST(NULL AS pg_catalog.text)").literalComparisons);
    assert(!condition("number > CAST('bad' AS pg_catalog.int8)").literalComparisons);
    assert(!condition("number > CAST('4294967296' AS pg_catalog.int4)").literalComparisons);
    assert(!condition("shop = pg_catalog.lower('X')").literalComparisons);
    assert(!condition("shop COLLATE \"C\" = CAST('x' AS pg_catalog.text)").literalComparisons);
    assert(!condition("shop < CAST('x' AS pg_catalog.text)").literalComparisons);
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    std::cout << "[STORED LITERAL FILTER] exact integer, quoted text, NULL and fallback passed\n";
}
