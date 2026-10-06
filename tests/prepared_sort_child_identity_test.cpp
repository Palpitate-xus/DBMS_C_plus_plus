#include "commands/TableManage.h"
#include "expression/expr_helper.h"
#include "expression/prepared_query_execution.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine engine;
    const std::string name = "prepared_sort_child_identity";
    cleanupTestDb(name);
    const auto db = testDbPath(name);
    assert(engine.createDatabase(db, "utf8") == DBStatus::OK);
    TableSchema schema; schema.len = 2;
    schema.cols[0].dataName = "id"; schema.cols[0].dataType = "int"; schema.cols[0].dsize = 4;
    schema.cols[1].dataName = "ID"; schema.cols[1].dataType = "bigint"; schema.cols[1].dsize = 8;
    assert(engine.createTable(db, "sort_rows", schema) == DBStatus::OK);
    assert(engine.createUDF(db, "sort_identity_writer", {"arg"}, {"integer"},
        "BEGIN INSERT INTO sort_rows(id) VALUES(99); RETURN arg; END;", 'v',
        "plpgsql", "integer") == DBStatus::OK);
    const auto compare = [&](const std::string& sql, bool equal,
                             const std::vector<QueryBindingDatum>& parameters = {}) {
        auto query = std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db, sql, parameters));
        auto* select = dynamic_cast<SelectStmt*>(query->ast.get());
        assert(select && !select->selectList.empty() && !select->orderBy.empty());
        const auto column = [](const ColumnRefExpr& reference) {
            assert(reference.binding);
            const auto& binding = *reference.binding;
            return std::to_string(binding.scopeDepth) + ":" + std::to_string(binding.sourceOrdinal) + ":" +
                std::to_string(binding.columnOrdinal) + ":" + binding.declaredType;
        };
        const auto* target = select->selectList[0].expr.get();
        const auto* order = select->orderBy[0].expr.get();
        const auto first = ExprHelper::preparedSortExpressionIdentity(target, *query, column, db, &engine);
        const auto second = ExprHelper::preparedSortExpressionIdentity(order, *query, column, db, &engine);
        assert((first == second) == equal);
        // The general site identity and execution memo remain independent.
        assert(ExprHelper::scalarExpressionIdentity(target, column, db, &engine) !=
               ExprHelper::scalarExpressionIdentity(order, column, db, &engine));
        const auto parametersBefore = query->parameters;
        (void)ExprHelper::preparedSortExpressionIdentity(target, *query, column, db, &engine);
        assert(query->parameters.size() == parametersBefore.size());
        for (size_t i = 0; i < parametersBefore.size(); ++i) {
            assert(query->parameters[i].typeName == parametersBefore[i].typeName);
            assert(query->parameters[i].isNull == parametersBefore[i].isNull);
            assert(query->parameters[i].value == parametersBefore[i].value);
        }
    };
    compare("SELECT(SELECT 1) FROM sort_rows ORDER BY( SELECT 01 )", true);
    compare("SELECT(SELECT CAST(1 AS INT)) FROM sort_rows ORDER BY(SELECT CAST(01 AS INTEGER))", true);
    compare("SELECT(SELECT sort_identity_writer(1)) FROM sort_rows ORDER BY(SELECT sort_identity_writer(01))", true);
    compare("SELECT(SELECT id FROM sort_rows s LIMIT 1) FROM sort_rows ORDER BY(SELECT s.id FROM sort_rows s LIMIT 1)", true);
    compare("SELECT(SELECT s.id FROM sort_rows s LIMIT 1) FROM sort_rows ORDER BY(SELECT t.id FROM sort_rows t LIMIT 1)", false);
    compare("SELECT(SELECT id FROM sort_rows LIMIT 1) FROM sort_rows ORDER BY(SELECT id FROM sort_rows s LIMIT 1)", false);
    compare("SELECT(SELECT o.id) FROM sort_rows o ORDER BY(SELECT o.id)", true);
    compare("SELECT(SELECT o.id) FROM sort_rows o ORDER BY(SELECT o.\"ID\")", false);
    compare("SELECT(SELECT o.id) FROM sort_rows o ORDER BY(SELECT p.id FROM sort_rows p LIMIT 1)", false);
    compare("SELECT(SELECT 1 LIMIT 1) FROM sort_rows ORDER BY(SELECT 1 LIMIT 2)", false);
    compare("SELECT(SELECT id AS v FROM sort_rows s ORDER BY v LIMIT 1) FROM sort_rows ORDER BY(SELECT s.id AS v FROM sort_rows s ORDER BY v LIMIT 1)", true);
    compare("SELECT(SELECT (SELECT s.id) FROM sort_rows s LIMIT 1) FROM sort_rows ORDER BY(SELECT (SELECT s.id) FROM sort_rows s LIMIT 1)", true);
    compare("SELECT(SELECT c.v FROM(WITH q AS(SELECT id AS v FROM sort_rows) SELECT v FROM q)c LIMIT 1) FROM sort_rows ORDER BY(SELECT c.v FROM(WITH q AS(SELECT id AS v FROM sort_rows) SELECT v FROM q)c LIMIT 1)", true);
    compare("SELECT(SELECT COUNT(id) FROM sort_rows s) FROM sort_rows ORDER BY(SELECT COUNT(s.id) FROM sort_rows s)", true);
    compare("SELECT(SELECT SUM(id) FROM sort_rows s) FROM sort_rows ORDER BY(SELECT SUM(s.id) FROM sort_rows s)", true);
    const std::vector<QueryBindingDatum> parameters{
        {"param:p", "p", "integer", {}, true, std::nullopt, 1},
        {"param:q", "q", "bigint", {}, true, "1", 2}};
    compare("SELECT(SELECT p) FROM sort_rows ORDER BY(SELECT p)", true, parameters);
    compare("SELECT(SELECT p) FROM sort_rows ORDER BY(SELECT q)", false, parameters);
    compare("SELECT(SELECT p) FROM sort_rows ORDER BY(SELECT 1)", false, parameters);
    // Values never drive equivalence: two distinct datum slots remain
    // distinct even when both contain the same typed SQL NULL.
    compare("SELECT(SELECT p) FROM sort_rows ORDER BY(SELECT q)", false,
        {{"p", "p", "integer", {}, true, std::nullopt}, {"q", "q", "integer", {}, true, std::nullopt}});
    size_t rows = 0;
    assert(engine.forEachRow(db, "sort_rows", [&](uint32_t, uint16_t, const char*, size_t) { ++rows; }));
    assert(rows == 0); // preparing identities never invokes a writer
    engine.endBackendSession(); cleanupTestDb(name);
    std::cout << "[PREPARED SORT CHILD IDENTITY] passed\n";
}
