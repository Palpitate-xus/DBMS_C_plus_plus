#include "parser/query_binding.h"
#include "common/DbError.h"
#include "catalog/catalog.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <unistd.h>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    size_t descriptions = 0, routinePreparations = 0;
    QueryBindingMetadata metadata;
    metadata.relation = [&](const std::string& name) {
        ++descriptions;
        if (name == "t" || name == "side")
            return QueryRelationMetadata{"public", name, {{"id","integer"},{"ID","integer"}}, {}};
        throw DbError("42P01", "missing relation");
    };
    metadata.functionType = [&](const FunctionCallExpr*) { ++routinePreparations; return "bigint"; };
    const std::vector<QueryBindingDatum> datums{
        {"local:id", "id", "integer", {}, true, "99"},
        {"parameter:id", "id", "integer", {"f"}, false, "17", 1},
        {"local:wanted", "wanted", "text", {"b"}, true, "O'Brien"},
        {"local:empty", "empty", "integer", {}, true, std::nullopt}};
    const auto error = [&](const std::string& sql, const std::string& state) {
        bool seen = false;
        try { (void)prepareQuery(sql, datums, metadata); }
        catch (const DbError& e) { seen = true; assert(e.sqlState() == state); }
        assert(seen);
    };
    error("SELECT id FROM t WHERE FALSE", "42702");
    error("SELECT t.id FROM t WHERE id=1", "42702");
    error("SELECT missing.id FROM t WHERE FALSE", "42P01");
    error("SELECT t.missing FROM t WHERE FALSE", "42703");
    error("WITH ins AS(INSERT INTO side VALUES(nextval('s')) RETURNING side.id) SELECT id FROM t", "42702");
    assert(routinePreparations == 1); // metadata callback only; no executor exists
    error("WITH source AS(SELECT t.id FROM t) SELECT id FROM source WHERE FALSE", "42702");
    error("SELECT id FROM(SELECT t.id FROM t)source WHERE FALSE", "42702");
    error("SELECT t.id FROM t AS hidden", "42P01");
    error("SELECT t.id FROM t; INSERT INTO side VALUES(1)", "42601");
    for (const auto& invalid : {"SELECT FROM t", "SELECT 1, FROM t", "SELECT 1 WHERE", "SELECT 1 ORDER BY", "SELECT $0", "SELECT 1 x y"})
        error(invalid, "42601");
    auto prepared = prepareQuery("SELECT f.id,b.wanted,empty,t.\"ID\" FROM t WHERE t.id=1", datums, metadata);
    auto* select = dynamic_cast<SelectStmt*>(prepared.ast.get());
    assert(select && select->selectList.size() == 4 && prepared.parameters.size() == 3);
    assert(dynamic_cast<ParameterExpr*>(select->selectList[0].expr.get()));
    RowContext context; context.setParameters(prepared.parameters);
    ExprEvaluator evaluator;
    assert(evaluator.eval(select->selectList[0].expr, context).value == "17");
    assert(evaluator.eval(select->selectList[1].expr, context).value == "O'Brien");
    const auto null = evaluator.eval(select->selectList[2].expr, context);
    assert(null.isNull && null.typeName == "integer");
    assert(prepared.legacySql() == "SELECT CAST('17' AS integer) AS \"id\",CAST('O''Brien' AS text) AS \"wanted\",CAST(NULL AS integer) AS \"empty\",t.\"ID\" FROM t WHERE t.id=1");
    prepared = prepareQuery("WITH c AS(SELECT b.wanted AS v) SELECT c.v FROM c", datums, metadata);
    assert(prepared.parameters.size() == 1 && prepared.output[0].name == "v");
    prepared = prepareQuery("SELECT wanted,$1", datums, metadata);
    assert(prepared.parameters.size() == 2 && prepared.parameters[0].value == "O'Brien" && prepared.parameters[1].value == "17");
    prepared = prepareQuery("SELECT $1,wanted", datums, metadata);
    assert(prepared.parameters.size() == 2 && prepared.parameters[0].value == "17" && prepared.parameters[1].value == "O'Brien");
    error("SELECT wanted,$2", "42P02");
    prepared = prepareQuery("WITH c AS(SELECT wanted AS v) SELECT(SELECT c.v FROM c)", datums, metadata);
    assert(prepared.parameters.size() == 1 && prepared.output.size() == 1);
    select = dynamic_cast<SelectStmt*>(prepared.ast.get());
    bool needsQueryContext = false;
    try { evaluator.eval(select->selectList[0].expr, context); }
    catch (const DbError& e) { needsQueryContext = e.sqlState() == "0A000"; }
    assert(needsQueryContext);
    prepared = prepareQuery("SELECT(SELECT q.v FROM(SELECT t.id AS v)q) FROM t", datums, metadata);
    assert(prepared.output.size() == 1 && prepared.output[0].type == "integer");
    prepared = prepareQuery("SELECT(SELECT q.v FROM(WITH c AS(SELECT t.id AS v) SELECT c.v FROM c)q) FROM t", datums, metadata);
    assert(prepared.output.size() == 1 && prepared.output[0].type == "integer");
    error("SELECT q.v FROM t,(SELECT t.id AS v)q", "42P01");
    error("SELECT q.v FROM t,(WITH c AS(SELECT t.id AS v) SELECT c.v FROM c)q", "42P01");
    prepared = prepareQuery("SELECT EXISTS(SELECT t.id,t.\"ID\" FROM t)", datums, metadata);
    // EXISTS has its own boolean SQL grammar result even when the ordinary
    // routine metadata callback above deliberately returns bigint.
    assert(prepared.output[0].type == "boolean");
    prepared = prepareQuery("SELECT pg_catalog.extract(wanted,DATE '2026-10-06')", datums, metadata);
    assert(prepared.parameters.size() == 1 && prepared.parameters[0].value == "O'Brien");
    error("WITH ins AS(INSERT INTO side VALUES(nextval('s')) RETURNING side.id) SELECT pg_catalog.extract(missing_field,DATE '2026-10-06')", "42703");
    error("SELECT EXTRACT(t.id FROM DATE '2026-10-06') FROM t", "42601");
    prepared = prepareQuery("SELECT id FROM t a JOIN side b USING(id)", {}, metadata);
    assert(prepared.output.size() == 1 && prepared.output[0].name == "id");
    prepared = prepareQuery("SELECT a.* FROM t a JOIN side b USING(id)", {}, metadata);
    assert(prepared.output.size() == 2);
    for (const auto& sql : {"SELECT * FROM t a JOIN side b USING(id,\"ID\")", "SELECT * FROM t a NATURAL JOIN side b"}) {
        prepared = prepareQuery(sql, {}, metadata);
        assert(prepared.output.size() == 2 && prepared.output[0].name == "id" && prepared.output[1].name == "ID");
    }
    error("SELECT * FROM t a JOIN side b USING(id,id)", "42701");
    error("SELECT t.id FROM t,t", "42712");
    error("EXPLAIN ANALYZE WITH c AS(INSERT INTO side VALUES(nextval('s')) RETURNING side.id) SELECT id FROM t", "42702");
    error("WITH c AS(INSERT INTO side VALUES(nextval('s')) RETURNING side.id) SELECT 1,", "42601");
    prepared = prepareQuery("INSERT INTO side SELECT wanted RETURNING side.id", datums, metadata);
    assert(prepared.legacySql().find("INSERT INTO side SELECT CAST('O''Brien' AS text) AS \"wanted\"") == 0);
    prepared = prepareQuery("CREATE TEMP TABLE copy AS SELECT wanted", datums, metadata);
    assert(prepared.parameters.size() == 1 && prepared.legacySql().find("CAST('O''Brien' AS text) AS \"wanted\"") != std::string::npos);
    prepared = prepareQuery("SELECT r.v FROM(SELECT b.wanted AS v)r", datums, metadata);
    assert(prepared.parameters.size() == 1 && prepared.output[0].name == "v");
    assert(prepared.legacySql().find("CAST('O''Brien' AS text)") != std::string::npos);
    // No directory/OID/catalog persistence is permitted for a cold snapshot.
    const std::filesystem::path missing = std::filesystem::temp_directory_path() /
        ("dbms-query-binding-cold-metadata-nonexistent-" + std::to_string(getpid()));
    assert(!std::filesystem::exists(missing));
    const auto snapshot = CatalogManager::readMetadataSnapshot(missing.string());
    assert(snapshot.relations.empty() && !std::filesystem::exists(missing));
    std::cout << "[QUERY BINDING] passed\n";
}
