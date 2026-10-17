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
    const auto check = [&](bool good, const std::string& label) {
        ++controls;
        if (!good) {
            ++failures;
            std::cerr << "NATIVE_RESULT_HINT_FAILURE " << label << '\n';
        }
    };
    const auto native = [&](const std::string& name, const std::string& arguments,
                            const std::string& hint, const std::string& returnedType,
                            const std::string& payload, bool isNull,
                            const std::string& expectedType, uint32_t oid) {
        ExprEvaluator evaluator;
        size_t effects = 0;
        evaluator.registerFunction(name, [&](const std::vector<ExprValue>&) {
            ++effects;
            return ExprValue(returnedType, isNull ? "" : payload, isNull);
        });
        auto parsed = SQLParser().parseForBinding("SELECT pg_typeof(" + name + "(" + arguments + "))");
        if (!parsed.success) throw std::runtime_error("result-hint fixture parse failed");
        auto* outer = static_cast<FunctionCallExpr*>(static_cast<SelectStmt*>(parsed.stmt.get())->selectList[0].expr.get());
        auto* inner = static_cast<FunctionCallExpr*>(outer->args.front().get());
        inner->resolvedResultType = hint;
        const auto label = name + " hint=" + hint + (isNull ? " NULL" : " value");
        try {
            check(ExprHelper::inferParsedInputType(inner, {}, "", nullptr, &evaluator) == expectedType && effects == 0,
                  label + " pure input metadata");
            check(ExprHelper::inferParsedResultType(inner, {}, "", nullptr, &evaluator) ==
                      (expectedType == "unknown" ? "text" : expectedType) && effects == 0,
                  label + " pure result metadata");
            evaluator.bindScalarFunctions(outer);
            check(inner->resolvedResultType == hint && effects == 0, label + " binding preserves hint/effects");
            check(ExprHelper::inferParsedInputType(outer, {}, "", nullptr, &evaluator) == "regtype" && effects == 0,
                  label + " pure outer metadata");
            const auto result = evaluator.eval(outer, {});
            check(!result.isNull && result.typeName == "regtype" && result.objectOid == oid && effects == 1,
                  label + " declared OID and exactly one execution; got=" +
                      std::to_string(result.objectOid.value_or(0)) + "/" + std::to_string(effects));
        } catch (const std::exception& error) {
            check(false, label + " unexpected exception: " + error.what());
        }
    };

    const std::vector<std::tuple<std::string, uint32_t, std::string>> types = {
        {"smallint", 21, "7"}, {"bigint", 20, "2147483648"}, {"numeric", 1700, "1.25"},
        {"text", 25, "declared"}, {"name", 19, "named"}, {"integer[]", 1007, "{1,2}"},
        {"time", 1083, "12:34:56.1"}, {"boolean", 16, "t"}
    };
    for (const auto& [type, oid, payload] : types)
        for (bool isNull : {false, true})
            native("declared_native_result", "", type, type, payload, isNull, type, oid);
    // No declaration is learned by invoking a callback, even for a typed
    // non-NULL value. A NULL result must not lose an explicit declaration.
    for (bool isNull : {false, true})
        native("undeclared_native_result", "", "", "smallint", "7", isNull, "unknown", 705);
    for (const auto& name : {"char_length", "ceil", "mod", "sin", "array_position", "round"}) {
        native(name, "1,2", "text", "text", "native", true, "text", 25);
        // Preserve existing opaque math callbacks' input-derived legacy
        // fallback; length/float8/array replacements have UNKNOWN metadata.
        const bool legacy = std::string(name) == "ceil" || std::string(name) == "mod" || std::string(name) == "round";
        native(name, "1,2", "", "text", "native", false, legacy ? "integer" : "unknown", legacy ? 23 : 705);
    }
    // Real builtin declarations outrank a conflicting AST hint. The fix
    // must not let a hint attach arbitrary metadata to an owned callee.
    const std::vector<std::tuple<std::string, std::string, uint32_t>> builtins = {
        {"char_length('abc')", "integer", 23}, {"sin(4)", "double precision", 701},
        {"pi()", "double precision", 701}, {"array_position(ARRAY[1],1)", "integer", 23},
        {"pg_typeof(1)", "regtype", 2206}, {"version()", "text", 25}
    };
    for (const auto& [expression, expected, oid] : builtins) {
        ExprEvaluator evaluator;
        auto parsed = SQLParser().parseForBinding("SELECT pg_typeof(" + expression + ")");
        if (!parsed.success) throw std::runtime_error("builtin hint fixture parse failed");
        auto* outer = static_cast<FunctionCallExpr*>(static_cast<SelectStmt*>(parsed.stmt.get())->selectList[0].expr.get());
        auto* inner = static_cast<FunctionCallExpr*>(outer->args.front().get());
        inner->resolvedResultType = expected == "text" ? "smallint" : "text";
        check(ExprHelper::inferParsedInputType(inner, {}, "", nullptr, &evaluator) == expected,
              expression + " authoritative builtin metadata");
        evaluator.bindScalarFunctions(outer);
        check(ExprHelper::inferParsedInputType(inner, {}, "", nullptr, &evaluator) == expected,
              expression + " authoritative bound builtin metadata");
        const auto result = evaluator.eval(outer, {});
        check(result.objectOid == oid && !result.isNull, expression + " builtin runtime OID");
    }
    std::cout << "NATIVE_RESULT_HINT_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return controls == 168 && failures == 0 ? 0 : 1;
}
