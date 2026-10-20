#include "Session.h"
#include "commands/TableManage.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <limits>
#include <optional>
#include <set>

extern dbms::StorageEngine g_engine;

namespace {
using namespace dbms;
using Input = std::optional<std::string>;
void require(bool good, const std::string& label) {
    if (!good) throw std::runtime_error(label);
}
double number(const std::string& text) {
    char* end = nullptr;
    const double value = std::strtod(text.c_str(), &end);
    require(end != text.c_str() && end && *end == '\0', "invalid float8 output: " + text);
    return value;
}
bool same(double a, double b) {
    return (std::isnan(a) && std::isnan(b)) || std::memcmp(&a, &b, sizeof(a)) == 0;
}
std::string literal(const Input& value) {
    return value ? "'" + *value + "'::double precision" : "NULL::double precision";
}
ExprValue cell(const Input& value) { return {"double precision", value.value_or(""), !value}; }
FunctionCallExpr call(const std::string& name, size_t argc) {
    FunctionCallExpr node; node.funcName = name;
    for (size_t i = 0; i < argc; ++i) {
        auto parameter = std::make_unique<ParameterExpr>();
        parameter->slot = i; parameter->declaredType = "double precision";
        node.args.push_back(std::move(parameter));
    }
    return node;
}
void matches(const ExprValue& value, const std::optional<double>& expected) {
    require(value.typeName == "double precision" && value.isNull == !expected,
            "wrong result type or NULL bit");
    if (expected) require(same(number(value.value), *expected), "different float8 bits: " + value.value);
}
void matches(const ExprEvalResult& value, const std::optional<double>& expected) {
    require(value.ok, "helper failed: " + value.error);
    matches(ExprValue(value.typeName, value.value, value.isNull), expected);
}
} // namespace

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    size_t controls = 0, failures = 0;
    const auto check = [&](const std::string& label, const std::function<void()>& action) {
        ++controls;
        try { action(); }
        catch (const std::exception& e) {
            ++failures; std::cerr << "TRIG_SUBNORMAL_FAIL " << label << " : " << e.what() << '\n';
        }
    };
    check("IEEE binary64 platform", [] {
        require(sizeof(double) == 8 && std::numeric_limits<double>::is_iec559 &&
                std::numeric_limits<double>::has_denorm == std::denorm_present, "unsupported platform");
    });
    const std::string name = "trigonometric_subnormal", database = testDbPath(name);
    check("create private database", [&] {
        cleanupTestDb(name); require(g_engine.createDatabase(database) == DBStatus::OK, "create database");
    });
    Session session; session.username = "testuser"; session.permission = 1; session.currentDB = database;
    auto* previous = currentSession(); setCurrentSession(&session);
    struct Restore { Session* previous; ~Restore() { setCurrentSession(previous); } } restore{previous};
    const std::vector<Input> inputs = {"0", "-0", "1e-300", "-1e-300", "1e-310", "-1e-310",
                                       "5e-324", "-5e-324", "NaN", std::nullopt};
    const std::vector<std::pair<std::string, double (*)(double)>> unary = {
        {"sin", std::sin}, {"cos", std::cos}, {"tan", std::tan}, {"asin", std::asin},
        {"acos", std::acos}, {"atan", std::atan}, {"sinh", std::sinh}, {"cosh", std::cosh},
        {"tanh", std::tanh}, {"asinh", std::asinh}, {"atanh", std::atanh}, {"cbrt", std::cbrt}};
    for (const auto& [fn, oracle] : unary) for (const auto& input : inputs) {
        const std::optional<double> expected = input ? std::optional<double>(oracle(number(*input))) : std::nullopt;
        const auto expression = fn + "(" + literal(input) + ")";
        for (bool bind : {false, true}) check(expression + (bind ? " bound" : " raw"), [&] {
            ExprEvaluator evaluator; evaluator.setCurrentDB(database); auto node = call(fn, 1);
            RowContext row; row.setParameters({cell(input)});
            if (bind) evaluator.bindScalarFunctions(&node, &g_engine);
            matches(evaluator.eval(&node, row), expected);
        });
        check(expression + " parsed", [&] {
            auto parsed = SQLParser().parseForBinding("SELECT " + expression);
            require(parsed.success, parsed.error); auto* select = dynamic_cast<SelectStmt*>(parsed.stmt.get());
            require(select && select->selectList.size() == 1, "missing SELECT target");
            ExprEvaluator evaluator; evaluator.setCurrentDB(database);
            auto* node = select->selectList.front().expr.get(); evaluator.bindScalarFunctions(node, &g_engine);
            matches(evaluator.eval(node, RowContext{}), expected);
        });
        check(expression + " literal helper", [&] {
            matches(ExprHelper::evalString(expression, {}, {}, database, "", &g_engine), expected);
        });
        check(expression + " row helper", [&] {
            matches(ExprHelper::evalStringWithNulls(fn + "(x)", {{"x", input.value_or("")}},
                input ? std::set<std::string>{} : std::set<std::string>{"x"},
                {{"x", "double precision"}}, database, "", &g_engine), expected);
        });
    }
    const std::vector<std::pair<Input, Input>> pairs = {
        {"5e-324", "5e-324"}, {"-5e-324", "5e-324"}, {"5e-324", "-5e-324"},
        {"-5e-324", "-5e-324"}, {"5e-324", "1"}, {"1", "5e-324"},
        {"1e-310", "-1e-310"}, {"-0", "1"}, {"-0", "-0"},
        {"NaN", "5e-324"}, {std::nullopt, "1"}, {"1", std::nullopt}};
    for (const auto& [a, b] : pairs) {
        const std::optional<double> expected = a && b ? std::optional<double>(std::atan2(number(*a), number(*b))) : std::nullopt;
        const auto expression = "atan2(" + literal(a) + "," + literal(b) + ")";
        for (bool bind : {false, true}) check(expression + (bind ? " bound" : " raw"), [&] {
            ExprEvaluator evaluator; evaluator.setCurrentDB(database); auto node = call("atan2", 2);
            RowContext row; row.setParameters({cell(a), cell(b)});
            if (bind) evaluator.bindScalarFunctions(&node, &g_engine);
            matches(evaluator.eval(&node, row), expected);
        });
        check(expression + " literal helper", [&] {
            matches(ExprHelper::evalString(expression, {}, {}, database, "", &g_engine), expected);
        });
        check(expression + " row helper", [&] {
            std::set<std::string> nulls; if (!a) nulls.insert("a"); if (!b) nulls.insert("b");
            matches(ExprHelper::evalStringWithNulls("atan2(a,b)", {{"a", a.value_or("")}, {"b", b.value_or("")}},
                nulls, {{"a", "double precision"}, {"b", "double precision"}}, database, "", &g_engine), expected);
        });
    }
    if (controls != 650) { ++failures; std::cerr << "TRIG_SUBNORMAL_REACH " << controls << '\n'; }
    if (g_engine.dropDatabase(database) != DBStatus::OK) return 2;
    cleanupTestDb(name);
    std::cout << "TRIG_SUBNORMAL_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
