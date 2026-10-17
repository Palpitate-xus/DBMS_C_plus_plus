#include "catalog/type_registry.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"

#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    size_t controls = 0, failures = 0;
    const auto observe = [&](const std::string& label, const auto& action) {
        ++controls;
        try {
            if (action()) return;
            std::cerr << "NATIVE_R62_OWNERSHIP_FAILURE " << label << '\n';
        } catch (const std::exception& error) {
            std::cerr << "NATIVE_R62_OWNERSHIP_FAILURE " << label
                      << " exception=" << error.what() << '\n';
        }
        ++failures;
    };
    struct Input {
        std::string sql;
        std::vector<ExprValue> values;
    };
    const std::vector<Input> inputs = {
        {"", {}},
        {"1,2", {ExprValue("integer", "1"), ExprValue("integer", "2")}},
        {"'007'::text", {ExprValue("text", "007")}},
        {"NULL::smallint", {ExprValue("smallint", "", true)}},
        {"7::smallint", {ExprValue("smallint", "7")}},
        {"'[1,4)'::int4range", {ExprValue("int4range", "[1,4)")}}
    };
    const std::vector<std::tuple<std::string, uint32_t, std::string>> hints = {
        {"", 705, "7"}, {"smallint", 21, "7"}, {"integer[]", 1007, "{1,2}"}
    };
    for (const auto* name : {"abs", "length", "upper", "lower"})
        for (const auto& input : inputs)
            for (const auto& [hint, oid, payload] : hints)
                for (bool isNull : {false, true}) {
                    ExprEvaluator evaluator;
                    size_t effects = 0;
                    std::vector<ExprValue> received;
                    const std::string returnedType = hint.empty() ? "smallint" : hint;
                    ScalarFunction callback = [&](const std::vector<ExprValue>& args) {
                        ++effects;
                        received = args;
                        return ExprValue(returnedType, isNull ? "" : payload, isNull);
                    };
                    // Exercise both public registration overloads. Volatility
                    // does not grant ownership of a displaced SQL declaration.
                    if (isNull) evaluator.registerFunction(name, callback, 'i');
                    else evaluator.registerFunction(name, callback);
                    auto parsed = SQLParser().parseForBinding(
                        "SELECT pg_typeof(" + std::string(name) + "(" + input.sql + "))");
                    if (!parsed.success) throw std::runtime_error("native replacement fixture parse failed");
                    auto* statement = dynamic_cast<SelectStmt*>(parsed.stmt.get());
                    auto* outer = statement ? dynamic_cast<FunctionCallExpr*>(statement->selectList[0].expr.get()) : nullptr;
                    auto* inner = outer ? dynamic_cast<FunctionCallExpr*>(outer->args[0].get()) : nullptr;
                    if (!inner) throw std::runtime_error("native replacement fixture AST shape");
                    inner->resolvedResultType = hint;
                    const std::string expected = hint.empty() ? "unknown" : hint;
                    const std::string label = std::string(name) + "(" + input.sql +
                        ") hint=" + hint + (isNull ? " NULL" : " value");
                    observe(label + " actual callback exists", [&] {
                        return evaluator.hasScalarFunction(inner) && effects == 0;
                    });
                    observe(label + " no foreign callee result declaration", [&] {
                        return evaluator.scalarFunctionResultType(inner).empty() && effects == 0;
                    });
                    observe(label + " native ownership", [&] {
                        return evaluator.scalarFunctionIdentity(inner).rfind("native", 0) == 0 && effects == 0;
                    });
                    observe(label + " pure input declaration", [&] {
                        return ExprHelper::inferParsedInputType(inner, {}, "", nullptr, &evaluator) == expected && effects == 0;
                    });
                    observe(label + " pure result declaration", [&] {
                        return ExprHelper::inferParsedResultType(inner, {}, "", nullptr, &evaluator) ==
                            (hint.empty() ? "text" : hint) && effects == 0;
                    });
                    observe(label + " binding preserves caller declaration", [&] {
                        evaluator.bindScalarFunctions(outer);
                        return inner->resolvedResultType == hint && effects == 0;
                    });
                    observe(label + " bound metadata remains pure", [&] {
                        return ExprHelper::inferParsedInputType(inner, {}, "", nullptr, &evaluator) == expected && effects == 0;
                    });
                    observe(label + " declared OID exactly one callback execution", [&] {
                        const auto result = evaluator.eval(outer, {});
                        return !result.isNull && result.typeName == "regtype" &&
                            result.objectOid == oid && effects == 1;
                    });
                    observe(label + " uncoerced callback arguments", [&] {
                        if (effects != 1 || received.size() != input.values.size()) return false;
                        for (size_t i = 0; i < received.size(); ++i)
                            if (ExprHelper::canonicalResultTypeName(received[i].typeName) !=
                                    ExprHelper::canonicalResultTypeName(input.values[i].typeName) ||
                                received[i].isNull != input.values[i].isNull ||
                                (!received[i].isNull && received[i].value != input.values[i].value))
                                return false;
                        return true;
                    });
                }
    // Genuine builtin callbacks retain their overloads and selected output,
    // including range/geometry declarations, even with a conflicting AST hint.
    const std::vector<std::tuple<std::string, std::string, uint32_t>> builtins = {
        {"abs(-7::smallint)", "smallint", 21},
        {"abs(1::bigint)", "bigint", 20},
        {"length('abc')", "integer", 23},
        {"length('[(0,0),(3,4)]'::lseg)", "double precision", 701},
        {"upper('abc')", "text", 25},
        {"lower(NULL::text)", "text", 25},
        {"upper('[1,4)'::int4range)", "integer", 23},
        {"lower('[1,4)'::numrange)", "numeric", 1700}
    };
    for (const auto& [expression, expected, oid] : builtins) {
        ExprEvaluator evaluator;
        auto parsed = SQLParser().parseForBinding("SELECT pg_typeof(" + expression + ")");
        if (!parsed.success) throw std::runtime_error("genuine callback fixture parse failed");
        auto* outer = static_cast<FunctionCallExpr*>(static_cast<SelectStmt*>(parsed.stmt.get())->selectList[0].expr.get());
        auto* inner = static_cast<FunctionCallExpr*>(outer->args[0].get());
        inner->resolvedResultType = "integer[]";
        observe(expression + " genuine declared type", [&] {
            return ExprHelper::inferParsedInputType(inner, {}, "", nullptr, &evaluator) == expected;
        });
        observe(expression + " genuine bound type", [&] {
            evaluator.bindScalarFunctions(outer);
            return ExprHelper::inferParsedInputType(inner, {}, "", nullptr, &evaluator) == expected;
        });
        observe(expression + " genuine runtime OID", [&] {
            const auto result = evaluator.eval(outer, {});
            return !result.isNull && result.objectOid == oid;
        });
    }
    std::cout << "NATIVE_R62_OWNERSHIP_COMPLETE " << controls
              << " FAILURES " << failures << '\n';
    return controls == 1320 && failures == 0 ? 0 : 1;
}
