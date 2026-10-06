#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "stored_function_order_metadata";
    const std::string db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    TableSchema table;
    table.len = 1;
    table.cols[0].dataName = "id";
    table.cols[0].dataType = "int";
    table.cols[0].dsize = 4;
    assert(g_engine.createTable(db, "metadata_writes", table) == DBStatus::OK);
    assert(g_engine.createUDF(db, "metawriter", {"arg"}, {"int"},
        "BEGIN INSERT INTO metadata_writes VALUES(arg); RETURN arg; END;",
        'v', "plpgsql", "int") == DBStatus::OK);
    assert(g_engine.createUDF(db, "metatext", {"arg"}, {"text"},
        "SELECT arg", 'i', "sql", "text") == DBStatus::OK);
    assert(g_engine.createUDF(db, "metawide", {"arg"}, {"bigint"},
        "SELECT arg", 'i', "sql", "bigint") == DBStatus::OK);
    assert(ExprHelper::inferResultType("metatext(payload)", {{"payload", "text"}}, db) == "text");
    assert(ExprHelper::inferResultType("metawriter(id)", {{"id", "int"}}, db) == "integer");
    assert(ExprHelper::inferResultType("metawide(id)", {{"id", "int"}}, db) == "bigint");
    SQLParser parser;
    auto parsed = parser.parse("SELECT metawriter(id)");
    auto* select = dynamic_cast<SelectStmt*>(parsed.stmt.get());
    auto* call = dynamic_cast<FunctionCallExpr*>(select->selectList.front().expr.get());
    ExprEvaluator evaluator;
    evaluator.setCurrentDB(db);
    assert(evaluator.scalarFunctionResultType(call) == "integer");
    const auto identity = evaluator.scalarFunctionIdentity(call);
    evaluator.bindScalarFunctions(call);
    const auto key = call->funcName;
    assert(evaluator.scalarFunctionResultType(call) == "integer");
    assert(evaluator.scalarFunctionIdentity(call) == identity);
    evaluator.bindScalarFunctions(call);
    assert(call->funcName == key && evaluator.scalarFunctionResultType(call) == "integer");
    assert(evaluator.scalarFunctionIdentity(call) == identity);
    const auto source = [](const ColumnRefExpr& column) {
        assert(column.column == "id");
        return std::string("source0-column0-integer-C");
    };
    const auto expressionIdentity = [&](const std::string& sql) {
        return ExprHelper::scalarExpressionIdentity(sql, source, db, &g_engine);
    };
    assert(expressionIdentity("metawriter(id)") == expressionIdentity("public.metawriter(d.id)"));
    assert(expressionIdentity("metawriter(id)+1") == expressionIdentity("metawriter(d.id)+01"));
    assert(expressionIdentity("metawriter(id)+1") != expressionIdentity("metawriter(d.id)+2"));
    assert(expressionIdentity("metatext(NULL)") != expressionIdentity("metatext('NULL')"));
    assert(expressionIdentity("CAST(NULL AS text)") != expressionIdentity("CAST('NULL' AS text)"));
    assert(expressionIdentity("metatext(NULL)") != expressionIdentity("metatext('null')"));
    assert(expressionIdentity("CAST(NULL AS text)") != expressionIdentity("CAST('null' AS text)"));
    assert(expressionIdentity("CAST(id AS integer)") == expressionIdentity("id::int"));
    assert(expressionIdentity("CAST(id AS varchar(1))") != expressionIdentity("CAST(id AS varchar(2))"));
    assert(expressionIdentity("id COLLATE \"C\"") != expressionIdentity("id COLLATE \"POSIX\""));
    StorageEngine differentOwner;
    assert(ExprHelper::scalarExpressionIdentity("abs(id)", source, db, &differentOwner) !=
           expressionIdentity("abs(id)"));
    evaluator.registerFunction(key, [](const std::vector<ExprValue>&) {
        return ExprValue("text", "override", false);
    });
    assert(evaluator.scalarFunctionResultType(call).empty());
    const auto writes = g_engine.plpgsqlQuery(db, "SELECT id FROM metadata_writes");
    assert(writes.ok && writes.rowCount == 0 && !g_engine.inTransaction());
    assert(g_engine.createUDF(db, "metastrict", {}, {},
        "DECLARE n INT; BEGIN SELECT 1 INTO STRICT n WHERE false; RETURN n; END;",
        'v', "plpgsql", "int") == DBStatus::OK);
    const auto failure = ExprHelper::evalString("metastrict()", {}, {}, db);
    assert(!failure.ok && failure.sqlState == "P0002");
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[STORED FUNCTION ORDER METADATA] passed\n";
}
