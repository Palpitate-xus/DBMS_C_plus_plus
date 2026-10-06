#include "executor/ExecutionPlan.h"
#include "common/DbError.h"
#include "parser/parser.h"
#include "catalog/CatalogService.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

namespace {
struct StartupTrace { size_t opened = 0, next = 0, closed = 0; };
class StartupRows final : public dbms::Operator {
    StartupTrace& trace_;
public:
    explicit StartupRows(StartupTrace& trace) : trace_(trace) {}
    bool open() override { ++trace_.opened; return true; }
    bool next(std::string&) override { ++trace_.next; return false; }
    void close() override { ++trace_.closed; }
};
}

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    SQLParser parser;
    for (const auto& sql : {"SELECT 1 OFFSET 2 LIMIT 1", "SELECT 1 LIMIT 1 OFFSET 2",
        "SELECT 1 OFFSET 2 ROWS FETCH FIRST 1 ROW ONLY"}) {
        auto parsed = parser.parseForBinding(sql);
        assert(parsed.success);
        const auto* select = dynamic_cast<const SelectStmt*>(parsed.stmt.get());
        assert(select && select->limit == 1 && select->offset == 2);
    }
    for (const auto& sql : {"SELECT 1 OFFSET 1 OFFSET 2", "SELECT 1 LIMIT ALL LIMIT 1",
        "SELECT 1 LIMIT 1 FETCH FIRST 1 ROW ONLY"})
        assert(!parser.parseForBinding(sql).success);
    StartupTrace startup;
    auto zero = std::make_unique<LimitOp>(std::make_unique<OffsetOp>(
        std::make_unique<StartupRows>(startup), 2), 0);
    assert(zero->open());
    std::string ignored;
    assert(!zero->next(ignored)); zero->close();
    assert(!startup.opened && !startup.next && !startup.closed);

    StorageEngine owner;
    const std::string db = testDbPath("explain_typed_execution");
    assert(!owner.databaseExists(db));
    assert(owner.createDatabase(db, "utf8") == DBStatus::OK);
    TableSchema schema;
    schema.len = 2;
    schema.cols[0].dataName = "id"; schema.cols[0].dataType = "int"; schema.cols[0].dsize = 4;
    schema.cols[1] = schema.cols[0]; schema.cols[1].dataName = "ID";
    assert(owner.createTable(db, "source", schema) == DBStatus::OK);
    assert(owner.insertRow(db, "source", {{"id","1"},{"ID","7"}}) == DBStatus::OK);
    assert(owner.insertRow(db, "source", {{"id","2"},{"ID","8"}}) == DBStatus::OK);
    schema.len = 1;
    assert(owner.createTable(db, "sink", schema) == DBStatus::OK);
    schema.cols[0].isNull = true;
    assert(owner.createTable(db, "caller_null", schema) == DBStatus::OK);
    assert(owner.insertRow(db, "caller_null",
        std::map<std::string, std::optional<std::string>>{{"id", std::nullopt}}) == DBStatus::OK);
    assert(owner.createUDF(db, "explain_native_writer", {"arg"}, {"int"},
        "BEGIN INSERT INTO sink VALUES(arg); RETURN -arg; END;", 'v', "plpgsql", "int") == DBStatus::OK);
    const auto plan = [&](const std::string& sql, std::vector<QueryBindingDatum> parameters = {}) {
        return QueryPlanner::buildPreparedSelectPlan(&owner, db, "source",
            owner.prepareBoundQuery(db, sql, parameters));
    };
    const auto execute = [&](OpPtr& tree) {
        std::vector<std::vector<std::string>> rows;
        std::string row;
        try {
            assert(tree->open());
            while (tree->next(row)) {
                std::vector<std::string> cells;
                std::vector<bool> nulls;
                assert(tree->lastStructuredRow(cells, nulls));
                rows.push_back(cells);
            }
            assert(!tree->hasError());
        } catch (...) { tree->close(); throw; }
        tree->close(); return rows;
    };
    std::string callerRaw;
    int64_t callerRid = 0;
    assert(owner.forEachRow(db, "caller_null", [&](uint32_t page, uint16_t slot, const char* data, size_t size) {
        callerRaw.assign(data, size); callerRid = StorageEngine::encodeRid(page, slot);
    }));
    const auto callerSchema = owner.getTableSchema(db, "caller_null");
    StorageEngine::bindNullRow(&owner, db, "caller_null", callerRid, 1);
    assert(owner.isColumnNullByRid(db, "caller_null", callerRid, 0));
    assert(StorageEngine::extractColumnValueStatic(callerRaw, callerSchema, 0).empty());
    auto quoted = plan("SELECT id,\"ID\",id+\"ID\" FROM source ORDER BY id DESC");
    assert(execute(quoted) == std::vector<std::vector<std::string>>({{"2","8","10"},{"1","7","8"}}));
    const auto bindingAfter = StorageEngine::captureNullRowBinding();
    assert(bindingAfter.engine == &owner && bindingAfter.database == db &&
        bindingAfter.table == "caller_null" && bindingAfter.rid == callerRid);
    assert(StorageEngine::extractColumnValueStatic(callerRaw, callerSchema, 0).empty());
    auto fail = plan("SELECT explain_native_writer(id),CAST('bad' AS INT) FROM source");
    assert(owner.beginTransaction(db) == DBStatus::OK); assert(owner.beginSqlCommand());
    bool castError = false;
    try { (void)execute(fail); }
    catch (const DbError& error) { castError = error.sqlState() == "22P02"; }
    catch (const std::runtime_error& error) {
        castError = std::string(error.what()).find("SQLSTATE 22P02") != std::string::npos;
    }
    assert(castError);
    const auto afterError = StorageEngine::captureNullRowBinding();
    assert(afterError.engine == &owner && afterError.table == "caller_null" && afterError.rid == callerRid);
    assert(owner.rollbackTransaction() == DBStatus::OK);
    StorageEngine::unbindNullRow();
    QueryPlanner::ExplainOptions analyzed; analyzed.analyze = true; analyzed.timing = false;
    const auto text = QueryPlanner::explain(quoted, &owner, db, analyzed);
    const auto json = QueryPlanner::explainJson(quoted, &owner, db, analyzed);
    assert(text.find("TypedProject") != std::string::npos && text.find("TypedSort") != std::string::npos);
    assert(text.find("actual rows=2 loops=3") != std::string::npos);
    assert(json.find("\"nodeType\":\"TypedProject\"") != std::string::npos);
    assert(json.find("\"actualRows\":2") != std::string::npos);
    TableSchema generated = owner.getTableSchema(db, "source");
    generated.cols[1].generatedKind = 'v'; generated.cols[1].generatedExpr = "id+1";
    generated.cols[1].isNull = true;
    assert(owner.createTable(db, "virtual_source", generated) == DBStatus::OK);
    assert(owner.insertRow(db, "virtual_source", {{"id","1"}}) == DBStatus::OK);
    auto generatedPlan = QueryPlanner::buildPreparedSelectPlan(&owner, db, "virtual_source",
        owner.prepareBoundQuery(db, "SELECT \"ID\"+0 FROM virtual_source ORDER BY id"));
    const auto generatedRows = execute(generatedPlan);
    if (generatedRows != std::vector<std::vector<std::string>>({{"2"}}))
        std::cerr << "VIRTUAL_TYPED_ROW=" << (generatedRows.empty() ? "NO_ROW" : generatedRows[0][0]) << '\n';
    assert(generatedRows == std::vector<std::vector<std::string>>({{"2"}}));
    auto nullDistinct = plan("SELECT DISTINCT NULL AS missing FROM source");
    assert(execute(nullDistinct) == std::vector<std::vector<std::string>>({{""}}));
    TableSchema arraySchema; arraySchema.len = 1;
    arraySchema.cols[0].dataName = "items";
    assert(TypeRegistry::instance().resolveColumnType(arraySchema.cols[0], "integer", {}, true).empty());
    assert(owner.createTable(db, "array_source", arraySchema) == DBStatus::OK);
    assert(owner.insertRow(db, "array_source", {{"items","{1,2}"}}) == DBStatus::OK);
    auto& catalog = owner.catalogService().get(db);
    const auto* publicNamespace = catalog.findNamespaceByName("public");
    assert(publicNamespace);
    PgClassRow relation; relation.relname = "array_source";
    relation.relnamespace = publicNamespace->oid; relation.relnatts = 1;
    const auto relationOid = catalog.createClass(relation);
    assert(relationOid != INVALID_OID);
    PgAttributeRow attribute; attribute.attrelid = relationOid;
    attribute.attnum = 1; attribute.attname = "items";
    attribute.atttypid = 23; attribute.attndims = 1;
    catalog.addAttribute(attribute);
    auto arrayPrepared = owner.prepareBoundQuery(db, "SELECT items,array_length(items,1) FROM array_source");
    assert(arrayPrepared.sourceRanges.size() == 1 &&
        arrayPrepared.sourceRanges[0].columns[0].type == "integer[]");
    auto arrayPlan = QueryPlanner::buildPreparedSelectPlan(&owner, db, "array_source", std::move(arrayPrepared));
    assert(execute(arrayPlan) == std::vector<std::vector<std::string>>({{"{1,2}","2"}}));

    auto bound = owner.prepareBoundQuery(db, "SELECT wanted,missing IS NULL", {
        {"v:wanted","wanted","bigint",{},true,"2147483648"},
        {"v:missing","missing","integer",{},true,std::nullopt}});
    auto* select = static_cast<SelectStmt*>(bound.ast.get());
    const auto* parameter = dynamic_cast<ParameterExpr*>(select->selectList[0].expr.get());
    assert(parameter && parameter->declaredType == "bigint");
    auto parameters = QueryPlanner::buildPreparedSelectPlan(&owner, db, "", std::move(bound));
    assert(execute(parameters) == std::vector<std::vector<std::string>>({{"2147483648","t"}}));
    auto childParameters = plan("SELECT (SELECT wanted)", {{"v:wanted","wanted","bigint",{},true,"2147483648"}});
    assert(execute(childParameters) == std::vector<std::vector<std::string>>({{"2147483648"}}));

    auto writer = plan("SELECT explain_native_writer(id) FROM source ORDER BY id DESC LIMIT 1");
    QueryPlanner::ExplainOptions plain;
    assert(QueryPlanner::explain(writer, &owner, db, plain).find("actual") == std::string::npos);
    assert(owner.plpgsqlQuery(db, "SELECT id FROM sink").rowCount == 0);
    assert(owner.beginTransaction(db) == DBStatus::OK); assert(owner.beginSqlCommand());
    assert(execute(writer) == std::vector<std::vector<std::string>>({{"-2"}}));
    assert(owner.inTransaction() && !g_engine.inTransaction());
    const auto observer = g_engine.plpgsqlQuery(db, "SELECT id FROM sink");
    assert(observer.ok && observer.rowCount == 0);
    assert(owner.rollbackTransaction() == DBStatus::OK);
    const auto rolledBack = g_engine.plpgsqlQuery(db, "SELECT id FROM sink");
    assert(rolledBack.ok && rolledBack.rowCount == 0);
    assert(owner.dropDatabase(db) == DBStatus::OK);
    std::cout << "[EXPLAIN TYPED EXECUTION] passed\n";
}
