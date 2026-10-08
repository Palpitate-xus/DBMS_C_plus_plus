#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "expression/expr_helper.h"
#include "expression/prepared_query_execution.h"
#include "test_utils.h"
#include <algorithm>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("correlated_true_integer_origin");
    if (g_engine.createDatabase(database, "utf8") != DBStatus::OK) return 2;
    size_t controls = 0, failures = 0, cases = 0;
    const auto require = [&](bool good, const std::string& label) {
        ++controls;
        if (!good) { ++failures; std::cerr << "[TRUE INTEGER CHILD FAIL] " << label << '\n'; }
    };
    // The original BIT/BIGINT24/25 fixture is untouched and still required.
    // Its scale=4 factory is an eight-byte BIGINT, not INTEGER width four.
    const auto legacy = makeIntColumn("legacy", true, 4);
    require(legacy.dataType == "bigint" && legacy.dsize == 8, "actual old factory contract");
    TableSchema input; input.tablename = "rows";
    input.append(makeIntColumn("id", false, 2, true));
    Column integer; integer.dataName = "i"; integer.isNull = true;
    if (!TypeRegistry::instance().resolveColumnType(integer, "integer", {}, false).empty()) return 2;
    require(ExprHelper::canonicalResultTypeName(integer.dataType) == "integer" && integer.dsize == 4,
            "real INTEGER registry descriptor and physical width");
    input.append(integer);
    TableSchema calls; calls.tablename = "calls"; calls.append(makeIntColumn("id", false, 2));
    if (g_engine.createTable(database, input) != DBStatus::OK ||
        g_engine.createTable(database, calls) != DBStatus::OK ||
        g_engine.createUDF(database, "integer_writer", {"p"}, {"integer"},
            "BEGIN INSERT INTO calls VALUES(1); RETURN p; END;", 'v', "plpgsql", "integer") != DBStatus::OK) return 2;
    const auto stored = g_engine.getTableSchema(database, "rows");
    require(stored.len == 2 && stored.cols[1].dataName == "i" &&
            ExprHelper::canonicalResultTypeName(stored.cols[1].dataType) == "integer" && stored.cols[1].dsize == 4,
            "persisted physical input is true INTEGER, not a labelled BIGINT datum");
    for (int id = 1; id <= 3; ++id)
        if (g_engine.insertRow(database, "rows", {{"id", std::to_string(id)},
            {"i", id == 1 ? std::optional<std::string>{} : std::optional<std::string>{std::to_string(id - 2)}}}) != DBStatus::OK) return 2;
    const auto count = [&] { return g_engine.query(database, "calls", {}, {"id"}, {}).size(); };
    for (int id = 1; id <= 3; ++id)
        for (const auto& operation : {" BETWEEN ", " NOT BETWEEN "})
            for (bool fallback : {false, true}) {
                ++cases; const auto before = count(); bool valid = false;
                const auto sql = "SELECT(SELECT o.i" + std::string(operation) +
                    "integer_writer(0) AND integer_writer(2)) AS value FROM rows o WHERE o.id=" + std::to_string(id);
                try {
                    auto query = std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database, sql));
                    const auto source = std::find_if(query->sourceRanges.begin(), query->sourceRanges.end(),
                        [&](const auto& value) { return value.owner == query->ast.get(); });
                    const bool typed = source != query->sourceRanges.end() && std::any_of(source->columns.begin(), source->columns.end(),
                        [&](const auto& value) { return value.name == "i" && ExprHelper::canonicalResultTypeName(value.type) == "integer"; });
                    require(typed && query->output.size() == 1 && query->output[0].type == "boolean" && count() == before,
                            "actual bound source/result metadata and pure admission");
                    if (!typed) return 2;
                    const auto good = [&](const ExprValue& value) {
                        return value.typeName == "boolean" && value.isNull == (id == 1) &&
                            (value.isNull || value.asBool() == (std::string(operation) == " BETWEEN "));
                    };
                    if (!fallback) {
                        auto cursor = QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedQueryPlan(
                            &g_engine, database, query, query->ast.get()), query->output);
                        std::vector<ExprValue> cells;
                        valid = cursor->next(cells) && cells.size() == 1 && good(cells[0]) && !cursor->next(cells);
                        cursor->close();
                    } else {
                        PreparedQueryExecution execution(query, &g_engine, database);
                        auto* select = dynamic_cast<SelectStmt*>(query->ast.get());
                        auto* child = select->selectList[0].expr.get(); execution.prepareExpression(child);
                        auto row = execution.context(); std::vector<ExprValue> cells;
                        for (const auto& column : source->columns)
                            cells.emplace_back(column.type, column.name == "id" ? std::to_string(id) : id == 1 ? "" : std::to_string(id - 2),
                                               column.name == "i" && id == 1);
                        execution.setSourceRow(row, source->ordinal, cells);
                        valid = good(execution.evaluate(child, row));
                    }
                } catch (const DbError& error) {
                    std::cerr << "[TRUE INTEGER CHILD ERROR] " << error.sqlState() << ' ' << error.what() << '\n';
                }
                require(valid && count() - before == 2, sql + (fallback ? " real native fallback" : " real typed cursor") +
                        " effects=" + std::to_string(count() - before));
            }
    require(cases == 12 && controls == 27, "all twelve genuine INTEGER cursor/fallback/NULL/operator cases reached");
    if (g_engine.dropDatabase(database) != DBStatus::OK) return 2;
    std::cout << "[TRUE INTEGER CORRELATED ORIGIN] complete controls=" << controls << " failures=" << failures << '\n';
    return failures ? 1 : 0;
}
