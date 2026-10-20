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
#include <stdexcept>

extern dbms::StorageEngine g_engine;

namespace {
using namespace dbms;
using Input = std::optional<std::string>;
void require(bool good, const std::string& label) { if (!good) throw std::runtime_error(label); }
double number(const std::string& text) {
    char* end = nullptr; const double value = std::strtod(text.c_str(), &end);
    require(end && end != text.c_str() && *end == '\0', "invalid float8 output: " + text);
    return value;
}
bool same(double a, double b) {
    return (std::isnan(a) && std::isnan(b)) || std::memcmp(&a, &b, sizeof(a)) == 0;
}
std::string literal(const Input& value) {
    return value ? "'" + *value + "'::double precision" : "NULL::double precision";
}
FunctionCallExpr call(const std::string& name) {
    FunctionCallExpr node; node.funcName = name;
    auto parameter = std::make_unique<ParameterExpr>();
    parameter->slot = 0; parameter->declaredType = "double precision";
    node.args.push_back(std::move(parameter)); return node;
}
void matches(const ExprValue& value, const std::optional<double>& expected) {
    require(value.typeName == "double precision" && value.isNull == !expected, "wrong type or NULL bit");
    if (expected) require(same(number(value.value), *expected), "different float8 bits: " + value.value);
}
void matches(const ExprEvalResult& value, const std::optional<double>& expected) {
    require(value.ok, "helper failed: " + value.error);
    matches(ExprValue(value.typeName, value.value, value.isNull), expected);
}
double signValue(double n) { return n > 0 ? 1 : n < 0 ? -1 : 0; }
} // namespace

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap(); size_t controls = 0, failures = 0;
    const auto check = [&](const std::string& label, const std::function<void()>& action) {
        ++controls;
        try { action(); }
        catch (const std::exception& e) {
            ++failures; std::cerr << "FLOAT_VALUE_SUBNORMAL_FAIL " << label << " : " << e.what() << '\n';
        }
    };
    check("IEEE binary64 platform", [] {
        require(sizeof(double) == 8 && std::numeric_limits<double>::is_iec559 &&
                std::numeric_limits<double>::has_denorm == std::denorm_present, "unsupported platform");
    });
    const std::string name = "floating_value_subnormal", database = testDbPath(name);
    check("create private database", [&] {
        cleanupTestDb(name); require(g_engine.createDatabase(database) == DBStatus::OK, "create database");
    });
    Session session; session.username = "testuser"; session.permission = 1; session.currentDB = database;
    auto* previous = currentSession(); setCurrentSession(&session);
    struct Restore { Session* previous; ~Restore() { setCurrentSession(previous); } } restore{previous};
    const double tiny = std::numeric_limits<double>::denorm_min();
    const double nan = std::numeric_limits<double>::quiet_NaN();
    const double inf = std::numeric_limits<double>::infinity();
    const std::vector<std::pair<std::string, double>> direct = {
        {"",0}, {" ",0}, {"bad",0}, {"abc12",0}, {"true",0}, {"NULL",0},
        {"0",0}, {"-0",-0.0}, {"+0",0}, {" 12.5",12.5}, {"-1.25tail",-1.25},
        {"12tail",12}, {"0x1p2suffix",4}, {"1e2x",100}, {"1e+bad",1},
        {"Infinity",inf}, {"-Infinity",-inf}, {"NaN",nan}, {"nan(payload)junk",nan},
        {"+Inf",inf}, {"1e309",0}, {"-1e309",0}, {"1e-999",0}, {"-1e-999",0},
        {"\t3.25\n",3.25}, {"1e-310",number("1e-310")}, {"-1e-310",number("-1e-310")},
        {"5e-324",tiny}, {"-5e-324",-tiny}, {"1e-310tail",number("1e-310")},
        {"  -5e-324junk",-tiny}, {"0x0.0000000000001p-1022",tiny},
        {"-0x0.0000000000001p-1022",-tiny},
        {"2.2250738585072014e-308",std::numeric_limits<double>::min()},
        {"2.225073858507201e-308",number("2.225073858507201e-308")},
        {"1.7976931348623157e308",std::numeric_limits<double>::max()},
        {"-1.7976931348623157e308",-std::numeric_limits<double>::max()},
        {"2e-324",0}, {"4e-324",tiny}, {std::string("1\0tail",6),1}};
    for (const auto& [text, expected] : direct)
        for (const std::string type : {"double precision", "real", "numeric", "integer", "text", "unknown"})
            check("native conversion " + type + " " + text, [&] {
                require(same(ExprValue(type,text,false).asDouble(), expected), "non-NULL conversion changed");
                require(same(ExprValue(type,text,true).asDouble(), 0.0), "NULL conversion changed");
            });
    const std::vector<Input> inputs = {"0", "-0", "1.25", "-1.25", "2.5", "-2.5", "3.5", "-3.5",
        "1e-300", "-1e-300", "2.2250738585072014e-308", "-2.2250738585072014e-308",
        "1e-310", "-1e-310", "5e-324", "-5e-324", "Infinity", "-Infinity", "NaN", std::nullopt};
    const std::vector<std::pair<std::string, double (*)(double)>> functions = {
        {"ceil",std::ceil}, {"ceiling",std::ceil}, {"floor",std::floor},
        {"round",std::nearbyint}, {"trunc",std::trunc}, {"abs",std::fabs}, {"sign",signValue}};
    for (const auto& [fn, oracle] : functions) for (const auto& input : inputs) {
        const std::optional<double> expected = input ? std::optional<double>(oracle(number(*input))) : std::nullopt;
        const auto expression = fn + "(" + literal(input) + ")";
        for (bool bind : {false, true}) check(expression + (bind ? " bound" : " raw"), [&] {
            ExprEvaluator evaluator; evaluator.setCurrentDB(database); auto node = call(fn);
            RowContext row; row.setParameters({ExprValue("double precision",input.value_or(""),!input)});
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
            matches(ExprHelper::evalStringWithNulls(fn + "(x)", {{"x",input.value_or("")}},
                input ? std::set<std::string>{} : std::set<std::string>{"x"},
                {{"x","double precision"}}, database, "", &g_engine), expected);
        });
    }
    if (direct.size() != 40 || inputs.size() != 20 || controls != 942) {
        ++failures; std::cerr << "FLOAT_VALUE_SUBNORMAL_REACH " << controls << '\n';
    }
    if (g_engine.dropDatabase(database) != DBStatus::OK) return 2;
    cleanupTestDb(name);
    std::cout << "FLOAT_VALUE_SUBNORMAL_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
