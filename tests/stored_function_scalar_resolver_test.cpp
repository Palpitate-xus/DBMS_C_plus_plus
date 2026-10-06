#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "commands/DdlExecutor.h"
#include "common/DbError.h"
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
    const std::string name = "stored_function_scalar_resolver";
    const std::string db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    Session session;
    session.username = "admin";
    session.permission = 1;
    session.currentDB = db;
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE resolver_writes(id INT)", session));
    assert(g_engine.createUDF(db, "resolverwriter", {"arg"}, {"int"},
        "BEGIN INSERT INTO resolver_writes VALUES(arg); RETURN arg; END;",
        'v', "plpgsql", "int") == DBStatus::OK);
    const auto function = [&](const std::string& fn, const std::string& body,
                              const std::string& type, char volatility = 'v', bool strict = false) {
        assert(g_engine.createUDF(db, fn, {"arg"}, {type}, body,
            volatility, "sql", type, strict) == DBStatus::OK);
    };
    function("ResolverCase", "SELECT -arg", "int", 's');
    function("resolvercase", "SELECT arg", "int", 'i');
    function("resolvertext", "SELECT arg", "text");
    function("resolverstrict", "SELECT arg", "text", 'v', true);
    function("LENGTH", "SELECT -arg", "int", 'i');
    function("resolverwide", "SELECT arg", "bigint", 'i');
    auto result = ExprHelper::evalStringWithNulls(
        "\"ResolverCase\"(id)+public.resolvercase(id)", {{"id", "7"}}, {}, {{"id", "int"}}, db);
    assert(result.ok && !result.isNull && result.value == "0");
    result = ExprHelper::evalStringWithNulls(
        "\"LENGTH\"(id)+length('ab')", {{"id", "7"}}, {}, {{"id", "int"}}, db);
    assert(result.ok && result.value == "-5");
    result = ExprHelper::evalStringWithNulls(
        "resolverwide(id)", {{"id", "9223372036854775806"}}, {}, {{"id", "bigint"}}, db);
    assert(result.ok && result.value == "9223372036854775806" && result.typeName == "bigint");
    for (const std::string value : {"", "null", "a b", "O'Brien"}) {
        result = ExprHelper::evalStringWithNulls(
            "resolvertext(payload)", {{"payload", value}}, {}, {{"payload", "text"}}, db);
        assert(result.ok && !result.isNull && result.value == value);
    }
    result = ExprHelper::evalStringWithNulls(
        "resolverstrict(payload)", {{"payload", ""}}, {"payload"}, {{"payload", "text"}}, db);
    assert(result.ok && result.isNull);
    for (const std::string expression : {
        "CASE WHEN false THEN nosuchroutine(id) ELSE resolvercase(id) END",
        "coalesce(resolvercase(id),nosuchroutine(id))",
        "wrong_schema.resolvercase(id)", "pg_catalog.resolvercase(id)", "resolvercase()"}) {
        result = ExprHelper::evalStringWithNulls(expression, {{"id", "7"}}, {}, {{"id", "int"}}, db);
        assert(!result.ok && result.error.find("42883") != std::string::npos);
    }
    SQLParser parser;
    auto parsed = parser.parse("SELECT \"ResolverCase\"(id)");
    auto* select = dynamic_cast<SelectStmt*>(parsed.stmt.get());
    auto* call = dynamic_cast<FunctionCallExpr*>(select->selectList.front().expr.get());
    ExprEvaluator evaluator;
    evaluator.setCurrentDB(db);
    assert(evaluator.hasScalarFunction(call));
    assert(evaluator.scalarFunctionVolatility(call) == 's');
    evaluator.bindScalarFunctions(call);
    assert(evaluator.volatility(call->funcName) == 's');
    auto syntax = parser.parse("SELECT coalesce(1,2)");
    const auto* syntaxSelect = dynamic_cast<const SelectStmt*>(syntax.stmt.get());
    const auto* syntaxCall = dynamic_cast<const FunctionCallExpr*>(syntaxSelect->selectList.front().expr.get());
    assert(evaluator.scalarFunctionVolatility(syntaxCall) == 'i');
    auto combined = parser.parse("SELECT \"ResolverCase\"(id)+resolvercase(id)");
    auto* combinedSelect = dynamic_cast<SelectStmt*>(combined.stmt.get());
    auto* pair = dynamic_cast<BinaryOpExpr*>(combinedSelect->selectList.front().expr.get());
    evaluator.bindScalarFunctions(pair);
    const auto* leftCall = dynamic_cast<FunctionCallExpr*>(pair->left.get());
    const auto* rightCall = dynamic_cast<FunctionCallExpr*>(pair->right.get());
    const std::string leftKey = leftCall->funcName, rightKey = rightCall->funcName;
    assert(leftKey != rightKey);
    evaluator.bindScalarFunctions(pair);
    assert(leftCall->funcName == leftKey && rightCall->funcName == rightKey);
    // Binding is metadata-only: no function call may begin a transaction.
    assert(!g_engine.inTransaction());
    RowContext context;
    context.set("id", ExprValue("integer", "7"));
    assert(evaluator.eval(call, context).value == "-7");
    assert(evaluator.eval(pair, context).value == "0");
    result = ExprHelper::evalStringWithNulls(
        "coalesce(resolverwriter(id),nosuchroutine(id))", {{"id", "7"}}, {}, {{"id", "int"}}, db);
    assert(!result.ok && result.error.find("42883") != std::string::npos);
    const auto writes = g_engine.plpgsqlQuery(db, "SELECT id FROM resolver_writes");
    assert(writes.ok && writes.rowCount == 0 && !g_engine.inTransaction());
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[STORED SCALAR RESOLVER] passed\n";
}
