#include "Session.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cerrno>
#include <cmath>
#include <cstdlib>
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
struct Target { std::string name; int bits; };
struct Expected { bool isNull = false; std::string value, state; };
void require(bool good, const std::string& label) { if (!good) throw std::runtime_error(label); }
Expected expected(bool real, const Input& input, int bits) {
    if (!input) return {true, "", ""};
    errno = 0; char* end = nullptr;
    const double number = real ? static_cast<double>(std::strtof(input->c_str(), &end))
                               : std::strtod(input->c_str(), &end);
    const int error = errno;
    if (error == ERANGE && (number == 0 || std::isinf(number))) return {false,"","22003"};
    if (end == input->c_str() || end != input->c_str() + input->size()) return {false,"","22P02"};
    const long double rounded = std::nearbyint(static_cast<long double>(number));
    const long double bound = std::ldexp(1.0L, bits - 1);
    if (!std::isfinite(rounded) || rounded < -bound || rounded >= bound) return {false,"","22003"};
    return {false, std::to_string(static_cast<int64_t>(rounded)), ""};
}
std::string literal(const Input& input, const std::string& source) {
    return input ? "'" + *input + "'::" + source : "NULL::" + source;
}
CastExpr node(const std::string& source, const std::string& target) {
    CastExpr cast; cast.typeName = target;
    auto parameter = std::make_unique<ParameterExpr>(); parameter->slot = 0; parameter->declaredType = source;
    cast.operand = std::move(parameter); return cast;
}
void matches(const ExprValue& value, const Expected& wanted, const std::string& type) {
    require(value.typeName == type && value.isNull == wanted.isNull, "wrong result type/NULL bit");
    if (!wanted.isNull) require(value.value == wanted.value, "expected " + wanted.value + ", got " + value.value);
}
void outcome(const Expected& wanted, const std::string& type, const std::function<ExprValue()>& action) {
    ExprValue value; std::string state;
    try { value = action(); } catch (const DbError& error) { state = error.sqlState(); }
    require(state == wanted.state, "expected SQLSTATE " + wanted.state + ", got " + state);
    if (state.empty()) matches(value, wanted, type);
}
ExprValue helper(const ExprEvalResult& value) {
    if (!value.ok) throw DbError(value.sqlState.empty() ? "XX000" : value.sqlState, value.error);
    return ExprValue(value.typeName, value.value, value.isNull);
}
} // namespace

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap(); size_t controls = 0, failures = 0;
    const auto check = [&](const std::string& label, const std::function<void()>& action) {
        ++controls;
        try { action(); } catch (const std::exception& error) {
            ++failures; std::cerr << "FLOAT_INTEGER_CAST_FAIL " << label << " : " << error.what() << '\n';
        }
    };
    check("IEEE floating platform", [] {
        require(sizeof(float) == 4 && sizeof(double) == 8 && std::numeric_limits<float>::is_iec559 &&
                std::numeric_limits<double>::is_iec559, "unsupported platform");
    });
    const std::string name = "floating_integer_cast", database = testDbPath(name);
    check("create private database", [&] {
        cleanupTestDb(name); require(g_engine.createDatabase(database) == DBStatus::OK, "create database");
    });
    Session session; session.username = "testuser"; session.permission = 1; session.currentDB = database;
    auto* previous = currentSession(); setCurrentSession(&session);
    struct Restore { Session* previous; ~Restore() { setCurrentSession(previous); } } restore{previous};
    const std::vector<Input> inputs = {std::nullopt,"0","-0","0.5","-0.5","1.5","-1.5","2.5","-2.5","3.5","-3.5",
        "0.49999999999999994","-0.49999999999999994","32767","32767.5","-32768","-32768.5","-32768.6",
        "2147483647","2147483647.5","-2147483648","-2147483648.5","123456789","16777217",
        "9223372036854775807","-9223372036854775808","9223372036854774784","-9223372036854777856",
        "5e-324","-5e-324","1e-310","-1e-310","1e-45","-1e-45","Infinity","-Infinity","NaN",
        "bad","1tail","1e999tail","1e999","1e-999"};
    const std::vector<Target> targets = {{"smallint",16},{"integer",32},{"bigint",64}};
    for (const std::string source : {"real", "double precision"}) for (const auto& target : targets)
        for (const auto& input : inputs) {
            const auto wanted = expected(source == "real", input, target.bits);
            const auto expression = "CAST(" + literal(input, source) + " AS " + target.name + ")";
            for (bool bind : {false, true}) check(expression + (bind ? " bound" : " raw"), [&] {
                ExprEvaluator evaluator; evaluator.setCurrentDB(database); auto cast = node(source, target.name);
                RowContext row; row.setParameters({ExprValue(source,input.value_or(""),!input)});
                if (bind) evaluator.bindScalarFunctions(&cast, &g_engine);
                outcome(wanted,target.name,[&] { return evaluator.eval(&cast,row); });
            });
            check(expression + " parsed", [&] {
                auto parsed = SQLParser().parseForBinding("SELECT " + expression); require(parsed.success,parsed.error);
                auto* select = dynamic_cast<SelectStmt*>(parsed.stmt.get()); require(select && select->selectList.size() == 1,"missing target");
                ExprEvaluator evaluator; evaluator.setCurrentDB(database);
                auto* cast = select->selectList.front().expr.get(); evaluator.bindScalarFunctions(cast,&g_engine);
                outcome(wanted,target.name,[&] { return evaluator.eval(cast,RowContext{}); });
            });
            check(expression + " literal helper", [&] {
                outcome(wanted,target.name,[&] { return helper(ExprHelper::evalString(expression,{},{},database,"",&g_engine)); });
            });
            check(expression + " row helper", [&] {
                outcome(wanted,target.name,[&] { return helper(ExprHelper::evalStringWithNulls("CAST(x AS " + target.name + ")",
                    {{"x",input.value_or("")}},input ? std::set<std::string>{} : std::set<std::string>{"x"},
                    {{"x",source}},database,"",&g_engine)); });
            });
        }
    const std::vector<std::pair<std::string,bool>> aliases = {{"real",true},{"float4",true},{"float",true},
        {"double precision",false},{"float8",false},{"double",false}};
    for (const auto& [source, real] : aliases) for (const auto& target : targets)
        for (const Input input : {Input("1e-310"),Input("123456789"),Input("-9223372036854775808"),Input{}})
            check("native alias " + source + " to " + target.name, [&] {
                ExprEvaluator evaluator; evaluator.setCurrentDB(database); auto cast = node(source,target.name);
                RowContext row; row.setParameters({ExprValue(source,input.value_or(""),!input)});
                outcome(expected(real,input,target.bits),target.name,[&] { return evaluator.eval(&cast,row); });
            });
    for (const std::string source : {"real","double precision"}) for (const auto& target : targets)
        for (bool bind : {false,true}) check("native embedded NUL " + source + " to " + target.name, [&] {
            ExprEvaluator evaluator; evaluator.setCurrentDB(database); auto cast = node(source,target.name);
            RowContext row; row.setParameters({ExprValue(source,std::string("1\0tail",6),false)});
            if (bind) evaluator.bindScalarFunctions(&cast,&g_engine);
            outcome({false,"","22P02"},target.name,[&] { return evaluator.eval(&cast,row); });
        });
    for (const std::string target : {"real","double precision"}) for (bool bind : {false,true})
        check("native floating input embedded NUL to " + target, [&] {
            ExprEvaluator evaluator; evaluator.setCurrentDB(database); auto cast = node("text",target);
            RowContext row; row.setParameters({ExprValue("text",std::string("1\0tail",6),false)});
            if (bind) evaluator.bindScalarFunctions(&cast,&g_engine);
            outcome({false,"","22P02"},target,[&] { return evaluator.eval(&cast,row); });
        });
    if (inputs.size() != 42 || controls != 1350) { ++failures; std::cerr << "FLOAT_INTEGER_CAST_REACH " << controls << '\n'; }
    if (g_engine.dropDatabase(database) != DBStatus::OK) return 2;
    cleanupTestDb(name);
    std::cout << "FLOAT_INTEGER_CAST_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
