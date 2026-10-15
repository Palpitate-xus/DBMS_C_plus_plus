#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "parser/parser.h"

#include <iostream>
#include <string>
#include <vector>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    struct Case {
        std::string expression, type, value;
        bool null;
    };
    const std::vector<Case> cases = {
        {"sign(NULL::numeric)", "numeric", "", true},
        {"sign(NULL::decimal)", "numeric", "", true},
        {"sign(CAST(NULL AS NUMERIC(12,2)))", "numeric", "", true},
        {"sign(sign(NULL::numeric))", "numeric", "", true},
        {"sign(NULL::smallint)", "double precision", "", true},
        {"sign(NULL::integer)", "double precision", "", true},
        {"sign(NULL::bigint)", "double precision", "", true},
        {"sign(NULL::real)", "double precision", "", true},
        {"sign(NULL::double precision)", "double precision", "", true},
        {"sign(NULL)", "double precision", "", true},
        {"sign(0::numeric)", "numeric", "0", false},
        {"sign(-1::numeric)", "numeric", "-1", false},
        {"sign('0.00000000000000001'::numeric)", "numeric", "1", false},
        {"sign('NaN'::numeric)", "numeric", "NaN", false},
        {"sign('Infinity'::numeric)", "numeric", "1", false},
        {"sign('-Infinity'::numeric)", "numeric", "-1", false},
        {"sign('NaN'::double precision)", "double precision", "0", false},
        {"sign(0)", "double precision", "0", false},
        {"sign(-1)", "double precision", "-1", false},
        {"sign(1)", "double precision", "1", false},
    };
    size_t controls = 0, failures = 0;
    for (const auto& test : cases) {
        ++controls;
        try {
            SQLParser parser;
            auto parsed = parser.parseForBinding("SELECT " + test.expression);
            auto* select = parsed.success ? dynamic_cast<SelectStmt*>(parsed.stmt.get()) : nullptr;
            if (!select || select->selectList.size() != 1) throw std::runtime_error("parse failed");
            ExprEvaluator evaluator;
            auto* expression = select->selectList.front().expr.get();
            evaluator.bindScalarFunctions(expression);
            const auto result = evaluator.eval(expression, RowContext{});
            if (result.typeName == test.type && result.isNull == test.null &&
                (test.null || result.value == test.value)) continue;
            std::cerr << "SIGN_NULL_FAIL " << test.expression << " actual="
                      << result.typeName << ':' << result.value << ':' << result.isNull << '\n';
        } catch (const std::exception& error) {
            std::cerr << "SIGN_NULL_FAIL " << test.expression << ' ' << error.what() << '\n';
        }
        ++failures;
    }
    // Native rows may carry the decimal alias without an SQL cast first
    // canonicalizing it to numeric. Nullability must not erase that domain.
    for (const auto* type : {"numeric", "decimal"}) for (const bool null : {false, true}) {
        ++controls;
        try {
            SQLParser parser;
            auto parsed = parser.parseForBinding("SELECT sign(n)");
            auto* select = parsed.success ? dynamic_cast<SelectStmt*>(parsed.stmt.get()) : nullptr;
            if (!select) throw std::runtime_error("parse failed");
            ExprEvaluator evaluator;
            auto* expression = select->selectList.front().expr.get();
            evaluator.bindScalarFunctions(expression);
            RowContext row;
            row.set("n", ExprValue(type, null ? "" : "0.00000000000000001", null));
            const auto result = evaluator.eval(expression, row);
            if (result.typeName != "numeric" || result.isNull != null || (!null && result.value != "1"))
                throw std::runtime_error("raw native numeric/decimal domain changed");
        } catch (const std::exception& error) {
            ++failures;
            std::cerr << "SIGN_NULL_RAW_ROW_FAIL " << type << ':' << null << ' ' << error.what() << '\n';
        }
    }
    // The fix is internal to the builtin callback; native replacements keep
    // their own result declaration and still must not run while binding.
    ++controls;
    try {
        SQLParser parser;
        auto parsed = parser.parseForBinding("SELECT sign(NULL::numeric)");
        auto* select = parsed.success ? dynamic_cast<SelectStmt*>(parsed.stmt.get()) : nullptr;
        if (!select) throw std::runtime_error("parse failed");
        size_t calls = 0;
        ExprEvaluator evaluator;
        evaluator.registerFunction("sign", [&](const std::vector<ExprValue>& args) {
            ++calls;
            if (args.size() != 1 || !args.front().isNull || args.front().typeName != "numeric")
                throw std::runtime_error("replacement input changed");
            return ExprValue("integer", "", true);
        });
        auto* expression = select->selectList.front().expr.get();
        evaluator.bindScalarFunctions(expression);
        if (calls) throw std::runtime_error("binding executed callback");
        const auto result = evaluator.eval(expression, RowContext{});
        if (calls != 1 || !result.isNull || result.typeName != "integer")
            throw std::runtime_error("replacement output changed");
    } catch (const std::exception& error) {
        ++failures;
        std::cerr << "SIGN_NULL_REPLACEMENT_FAIL " << error.what() << '\n';
    }
    std::cout << "SIGN_NULL_TYPE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
