#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "expression/expr_helper.h"
#include "Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("typed_null_parameter_execution");
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    auto* previous = currentSession();
    struct Restore { Session* previous; ~Restore() { setCurrentSession(previous); } } restore{previous};
    Session session; session.username = "testuser"; session.permission = 1;
    session.currentDB = database; setCurrentSession(&session);
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE null_rows(id INTEGER PRIMARY KEY)", session));
    assert(!ddl.executeSql("CREATE SEQUENCE null_parameter_effects", session));
    assert(g_engine.createUDF(database, "null_parameter_writer", {"p"}, {"integer"},
        "BEGIN PERFORM nextval('null_parameter_effects'); RETURN p; END;",
        'v', "plpgsql", "integer") == DBStatus::OK);
    size_t controls = 0, failures = 0;
    const auto run = [&](const std::string& sql, const std::string& type, bool empty, size_t expectedUses = 1) {
        ++controls;
        std::cout << "TYPED_NULL_NATIVE " << sql << " declared=" << type << std::endl;
        std::vector<QueryBindingDatum> datums = {{"null-bind:1", "value", type, {}, false, std::nullopt, 1}};
        auto prepared = std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database, sql, datums));
        const auto canonical = ExprHelper::canonicalResultTypeName(type);
        assert(prepared->parameters.size() == 1);
        assert(prepared->parameters[0].typeName == canonical && prepared->parameters[0].isNull);
        assert(prepared->uses.size() == expectedUses);
        for (const auto& use : prepared->uses) assert(use.slot == 0);
        assert(prepared->output.size() == 1);
        if (ExprHelper::canonicalResultTypeName(prepared->output[0].type) != canonical) {
            ++failures;
            std::cout << "TYPED_NULL_NATIVE_FAILURE actual static type=" << prepared->output[0].type
                      << " expected=" << canonical << std::endl;
        }
        auto graph = QueryPlanner::buildPreparedQueryPlan(&g_engine, database, prepared, prepared->ast.get());
        auto stream = QueryPlanner::makePreparedCursor(std::move(graph), prepared->output);
        std::vector<ExprValue> cells;
        if (empty) assert(!stream->next(cells));
        else {
            assert(stream->next(cells));
            assert(cells.size() == 1 && cells[0].isNull);
            if (ExprHelper::canonicalResultTypeName(cells[0].typeName) != canonical) {
                ++failures;
                std::cout << "TYPED_NULL_NATIVE_FAILURE actual cell type=" << cells[0].typeName
                          << " expected=" << canonical << std::endl;
            }
            assert(!stream->next(cells));
        }
        stream->close();
    };
    for (bool populated : {false, true}) {
        if (populated) {
            assert(g_engine.insertRow(database, "null_rows", {{"id", "1"}}) == DBStatus::OK);
            assert(g_engine.insertRow(database, "null_rows", {{"id", "2"}}) == DBStatus::OK);
        }
        for (const auto& type : {"boolean", "bigint", "smallint", "integer", "text", "real",
                                 "double precision", "date", "timestamp", "timestamptz", "numeric"}) {
            const std::string body = "(SELECT $1 FROM null_rows ORDER BY id FETCH FIRST 1 ROW WITH TIES)";
            run("SELECT CAST(" + body + " AS " + type + ") AS value", type, false);
            run("SELECT CAST(" + body + " AS " + type + ") AS value WHERE FALSE", type, true);
            run("SELECT CAST(" + body + " AS " + type + ") AS value LIMIT 0", type, true);
        }
    }
    // The whole binder uses a true ParameterExpr and one typed frame slot even
    // across repeated actual parameter occurrences; it is not a NULL constant.
    run("SELECT CAST(CASE WHEN $1 IS NULL THEN $1 ELSE $1 END AS INTEGER) AS value", "integer", false, 3);
    std::vector<QueryBindingDatum> datums = {{"null-bind:writer", "value", "integer", {}, false, std::nullopt, 1}};
    auto writer = std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,
        "SELECT(SELECT null_parameter_writer($1) FROM null_rows ORDER BY id FETCH FIRST 1 ROW WITH TIES)", datums));
    auto graph = QueryPlanner::buildPreparedQueryPlan(&g_engine, database, writer, writer->ast.get());
    auto stream = QueryPlanner::makePreparedCursor(std::move(graph), writer->output);
    assert(g_engine.nextval(database, "null_parameter_effects") == 1);
    std::vector<ExprValue> cells;
    assert(stream->next(cells) && cells.size() == 1 && cells[0].typeName == "integer" && cells[0].isNull);
    assert(!stream->next(cells)); stream->close();
    assert(g_engine.nextval(database, "null_parameter_effects") == 4);
    ++controls;
    std::cout << "[TYPED NULL PARAMETER EXECUTION] complete " << controls << " failures=" << failures
              << " real declared NULL slots/empty/nonempty/parent demand/metadata/noeffects/exact ties lookahead\n";
    assert(failures == 0);
}
