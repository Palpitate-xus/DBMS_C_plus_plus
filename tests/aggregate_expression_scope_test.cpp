#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "aggregate_expression_scope", db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    TableSchema table;
    table.formatVersion = 2;
    table.append(makeIntColumn("v", false, 2));
    table.cols[0].isNull = true;
    for (const std::string relation : {"values_", "empty_", "null_"})
        assert(g_engine.createTable(db, relation, table) == DBStatus::OK);
    for (const auto& value : std::vector<std::optional<std::string>>{"1", std::nullopt, "2"})
        assert(g_engine.insertRow(db, "values_", {{"v", value}}) == DBStatus::OK);
    assert(g_engine.insertRow(db, "null_", {{"v", std::nullopt}}) == DBStatus::OK);
    const auto expression = [](const std::string& sql) {
        StorageEngine::SelectExpr result;
        result.isScalar = true;
        result.funcName = "arith";
        result.funcArgs = {sql};
        return result;
    };
    const auto expect = [&](const std::string& relation, const std::string& sql,
                            const std::vector<std::string>& conditions,
                            const std::optional<std::string>& expected) {
        std::vector<std::vector<std::string>> rows;
        std::vector<std::vector<bool>> nulls;
        g_engine.queryExpr(db, relation, conditions, {expression(sql)}, {}, &rows, &nulls);
        if (rows.size() != 1 || nulls.size() != 1 || nulls[0][0] != !expected ||
            (expected && rows[0][0] != *expected)) {
            std::cerr << relation << " " << sql << ": rows=" << rows.size()
                      << " first=" << (rows.empty() ? "absent" : rows[0][0]) << '\n';
            assert(false);
        }
    };
    expect("empty_", "count(*)-count(v)", {}, "0");
    expect("values_", "count(*)-count(v)", {}, "1");
    expect("values_", "sum(v)+1", {}, "4");
    expect("empty_", "sum(v)+1", {}, std::nullopt);
    expect("null_", "sum(v)+1", {}, std::nullopt);
    expect("values_", "sum(v)+1", {"=v 1"}, "2");
    expect("values_", "CAST(sum(v) AS BIGINT)+1", {}, "4");
    expect("empty_", "CAST(sum(v) AS BIGINT)+1", {}, std::nullopt);
    expect("empty_", "coalesce(sum(v),0)+1", {}, "1");
    expect("values_", "sum(DISTINCT v)+1", {}, "4");
    expect("values_", "sum(v) FILTER (WHERE v=1)+1", {}, "2");
    expect("values_", "count(*) FILTER (WHERE v IS NULL)-count(v) FILTER (WHERE v=2)", {}, "0");
    expect("values_", "count(*) FILTER (WHERE NULL)+1", {}, "1");
    expect("values_", "count(*) FILTER (WHERE 't')+1", {}, "4");
    expect("values_", "count(*) FILTER (WHERE 'f')+1", {}, "1");
    expect("values_", "(sum(v)+1)*2", {}, "8");
    expect("values_", "avg(v)+0", {}, "1.5000000000000000");
    TableSchema wide;
    wide.append(makeIntColumn("V", false, 8));
    assert(g_engine.createTable(db, "wide_", wide) == DBStatus::OK);
    assert(g_engine.insertRow(db, "wide_", {{"V", "9223372036854775807"}}) == DBStatus::OK);
    assert(g_engine.insertRow(db, "wide_", {{"V", "1"}}) == DBStatus::OK);
    expect("wide_", "sum(\"V\")+1", {}, "9223372036854775809");
    TableSchema text;
    text.formatVersion = 2;
    text.append(makeTextColumn("t", true));
    assert(g_engine.createTable(db, "text_", text) == DBStatus::OK);
    for (const auto& value : std::vector<std::optional<std::string>>{"", "NULL", std::nullopt})
        assert(g_engine.insertRow(db, "text_", {{"t", value}}) == DBStatus::OK);
    expect("text_", "min(t)||max(t)", {}, "NULL");
    expect("text_", "count(DISTINCT t)+1", {}, "3");
    const auto error = [&](const std::string& sql, const std::string& state) {
        bool rejected = false;
        std::string actual = "success";
        try { g_engine.queryExpr(db, "empty_", {}, {expression(sql)}); }
        catch (const DbError& failure) {
            actual = failure.sqlState();
            rejected = actual == state;
        }
        if (!rejected) std::cerr << sql << ": " << actual << ", expected " << state << '\n';
        assert(rejected);
        assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    };
    error("sum(missing)+1", "42703");
    error("sum(CAST($1 AS INT))+1", "42P02");
    error("sum($1)+1", "42P02");
    error("sum(sum(v))+1", "42803");
    error("sum(v)+v", "42803");
    error("sum(CAST(v AS TEXT))+1", "42883");
    error("sum('abc')+1", "42725");
    error("avg(NULL)+1", "42725");
    error("sum(v) FILTER (WHERE 1)+1", "42804");
    StorageEngine::QueryExprExecutionOptions noDemand;
    noDemand.maxProjectionRows = 0;
    std::vector<std::vector<std::string>> suppressed;
    g_engine.queryExpr(db, "values_", {}, {expression("sum(v)+1")}, {}, &suppressed,
                       nullptr, nullptr, noDemand);
    assert(suppressed.empty());
    TableSchema sink;
    sink.formatVersion = 2;
    sink.append(makeIntColumn("v", false, 2));
    sink.cols[0].isNull = true;
    assert(g_engine.createTable(db, "effects_", sink) == DBStatus::OK);
    assert(g_engine.createUDF(db, "aggregate_argument_writer", {"i"}, {"int"},
        "BEGIN INSERT INTO effects_ VALUES(i); RETURN i; END;", 'v', "plpgsql", "int") == DBStatus::OK);
    const auto effectCount = [&]() {
        StorageEngine::SelectExpr column;
        column.colName = "v";
        std::vector<std::vector<std::string>> rows;
        g_engine.queryExpr(db, "effects_", {}, {column}, {}, &rows, nullptr);
        return rows.size();
    };
    assert(g_engine.beginTransaction(db) == DBStatus::OK && g_engine.beginSqlCommand());
    expect("empty_", "sum(aggregate_argument_writer(v))+1", {}, std::nullopt);
    assert(effectCount() == 0);
    g_engine.queryExpr(db, "values_", {}, {expression("sum(aggregate_argument_writer(v))+1")},
                       {}, &suppressed, nullptr, nullptr, noDemand);
    assert(suppressed.empty() && effectCount() == 0);
    expect("values_", "sum(aggregate_argument_writer(v))+1", {}, "4");
    assert(g_engine.finishSqlCommand() && g_engine.beginSqlCommand());
    assert(effectCount() == 3);
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    assert(effectCount() == 0);
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[AGGREGATE EXPRESSION SCOPE] passed\n";
}
