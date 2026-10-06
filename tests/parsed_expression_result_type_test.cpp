#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "expression/expr_helper.h"
#include "parser/ast.h"
#include "parser/parser.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    SQLParser parser;
    auto parsed = parser.parse("SELECT \"F\"+f");
    const auto* select = parsed.success ? dynamic_cast<const SelectStmt*>(parsed.stmt.get()) : nullptr;
    assert(select && select->selectList.size() == 1);
    const Expr* expression = select->selectList.front().expr.get();
    const std::map<std::string, std::string> hints{{"F", "bigint"}, {"f", "integer"}};
    const auto actual = ExprHelper::inferParsedResultType(expression, hints);
    std::cout << "parsed quoted expression actual type=" << actual << ", expected bigint" << std::endl;
    assert(actual == "bigint");
    const auto expect = [&](const std::string& sql, const std::string& expected,
                            const std::map<std::string, std::string>& types,
                            const std::string& db = "") {
        auto parsedExpression = parser.parse("SELECT " + sql);
        const auto* projection = parsedExpression.success
            ? dynamic_cast<const SelectStmt*>(parsedExpression.stmt.get()) : nullptr;
        assert(projection && projection->selectList.size() == 1);
        const auto type = ExprHelper::inferParsedResultType(
            projection->selectList.front().expr.get(), types, db, &g_engine);
        if (type != expected) std::cerr << sql << ": " << type << " expected " << expected << '\n';
        assert(type == expected);
    };
    expect("\"F\"", "bigint", hints);
    expect("F", "integer", hints);
    expect("CAST(\"F\" AS BIGINT)+f", "bigint", hints);
    expect("\"F\"::BIGINT+f", "bigint", hints);
    expect("coalesce(\"F\",f)", "bigint", hints);
    expect("CASE WHEN f=0 THEN f ELSE \"F\" END", "bigint", hints);
    expect("\"F\" IS NOT DISTINCT FROM f", "boolean", hints);
    expect("\"A\".\"F\"+\"A\".f", "bigint", {{"A.F", "bigint"}, {"A.f", "integer"}});
    expect("CAST(1 AS JSONB)->>'key'", "text", {});
    expect("CAST(1 AS JSONB)->'key'", "jsonb", {});
    expect("current_user", "name", {});
    for (const std::string type : {"bigint", "boolean", "numeric", "date", "text[]"}) {
        ParameterExpr parameter;
        parameter.slot = 2;
        parameter.declaredType = type;
        assert(ExprHelper::inferParsedResultType(&parameter) == type);
    }
    BinaryOpExpr parameterAddition;
    parameterAddition.op = "+";
    auto parameter = std::make_unique<ParameterExpr>();
    parameter->declaredType = "bigint";
    parameterAddition.left = std::move(parameter);
    auto literal = std::make_unique<LiteralExpr>();
    literal->value = "1";
    parameterAddition.right = std::move(literal);
    assert(ExprHelper::inferParsedResultType(&parameterAddition) == "bigint");

    const std::string name = "parsed_expression_result_type", db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    TableSchema table;
    table.append(makeIntColumn("id", false, 2));
    assert(g_engine.createTable(db, "metadata_calls", table) == DBStatus::OK);
    assert(g_engine.createUDF(db, "metawriter", {"arg"}, {"int"},
        "BEGIN INSERT INTO metadata_calls VALUES(arg); RETURN arg; END;",
        'v', "plpgsql", "int") == DBStatus::OK);
    expect("metawriter(f)+\"F\"", "bigint", hints, db);
    const std::string colliding = "\x01routine:.metawriter";
    expect("metawriter(1)+\"" + colliding + "\"", "bigint", {{colliding, "bigint"}}, db);
    const auto calls = g_engine.plpgsqlQuery(db, "SELECT id FROM metadata_calls");
    assert(calls.ok && calls.rowCount == 0 && !g_engine.inTransaction());
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[PARSED EXPRESSION RESULT TYPE] passed\n";
}
