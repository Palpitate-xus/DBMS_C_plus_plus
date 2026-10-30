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
struct Expected { bool null = false; double value = 0; std::string state; };
void require(bool good,const std::string& label) { if (!good) throw std::runtime_error(label); }
std::string stateOf(const std::exception& error) {
    if (const auto* db = dynamic_cast<const DbError*>(&error)) return db->sqlState();
    const std::string message = error.what(); const auto at = message.find("SQLSTATE ");
    return at == std::string::npos ? "" : message.substr(at+9,5);
}
bool same(double a,double b) { return (std::isnan(a) && std::isnan(b)) || std::memcmp(&a,&b,sizeof(a)) == 0; }
Expected expected(const std::string& fn,const Input& input) {
    if (!input) return {true,0,""};
    errno = 0; char* end = nullptr; const float physical = std::strtof(input->c_str(),&end); const int err = errno;
    if (err == ERANGE && (physical == 0 || std::isinf(physical))) return {false,0,"22003"};
    if (end == input->c_str() || end != input->c_str()+input->size()) return {false,0,"22P02"};
    const double n = physical;
    if ((fn == "sin" || fn == "cos" || fn == "tan") && std::isinf(n)) return {false,0,"22003"};
    if (fn == "sqrt" && n < 0) return {false,0,"2201F"};
    if ((fn == "asin" || fn == "acos") && (n < -1 || n > 1)) return {false,0,"22003"};
    double result = n;
    if (fn == "sin") result = std::sin(n);
    else if (fn == "cos") result = std::cos(n);
    else if (fn == "tan") result = std::tan(n);
    else if (fn == "atan") result = std::atan(n);
    else if (fn == "sqrt") result = std::sqrt(n);
    else if (fn == "cbrt") result = std::cbrt(n);
    else if (fn == "degrees") result = n / 0.0174532925199432957692;
    else if (fn == "radians") result = n * 0.0174532925199432957692;
    else if (fn == "round") result = std::nearbyint(n);
    else if (fn == "ceil" || fn == "ceiling") result = std::ceil(n);
    else if (fn == "floor") result = std::floor(n);
    else if (fn == "trunc") result = std::trunc(n);
    else if (fn == "sign") result = n > 0 ? 1 : n < 0 ? -1 : 0;
    else if (fn == "abs") result = std::fabs(n);
    else if (fn == "atan2") result = std::atan2(n,1.0);
    else if (fn == "asin") result = std::asin(n);
    else if (fn == "acos") result = std::acos(n);
    // CAST and power(x,1)/pow(x,1) preserve the promoted physical datum,
    // including NaN, infinities and the sign of zero.
    return {false,result,""};
}
std::string expression(const std::string& fn,const std::string& value) {
    if (fn == "cast") return "CAST("+value+" AS double precision)";
    const auto call = fn+"("+value+((fn == "power" || fn == "pow" || fn == "atan2") ? ",1::double precision)" : ")");
    return fn == "abs" ? "CAST("+call+" AS double precision)" : call;
}
ExprPtr node(const std::string& fn) {
    auto parameter = std::make_unique<ParameterExpr>(); parameter->slot = 0; parameter->declaredType = "real";
    ExprPtr root = std::move(parameter);
    if (fn != "cast") {
        auto call = std::make_unique<FunctionCallExpr>(); call->funcName = fn; call->args.push_back(std::move(root));
        if (fn == "power" || fn == "pow" || fn == "atan2") {
            auto second = std::make_unique<ParameterExpr>(); second->slot = 1; second->declaredType = "double precision";
            call->args.push_back(std::move(second));
        }
        root = std::move(call);
    }
    if (fn == "cast" || fn == "abs") {
        auto cast = std::make_unique<CastExpr>(); cast->typeName = "double precision"; cast->operand = std::move(root); root = std::move(cast);
    }
    return root;
}
ExprValue helper(const ExprEvalResult& result) {
    if (!result.ok) throw DbError(result.sqlState.empty() ? "XX000" : result.sqlState,result.error);
    return ExprValue(result.typeName,result.value,result.isNull);
}
void outcome(const Expected& wanted,const std::function<ExprValue()>& action) {
    ExprValue value; std::string state;
    try { value = action(); } catch (const std::exception& error) { state = stateOf(error); if (state.empty()) throw; }
    require(state == wanted.state,"expected SQLSTATE "+wanted.state+", got "+state);
    if (!state.empty()) return;
    require(value.typeName == "double precision" && value.isNull == wanted.null,"wrong type/NULL bit");
    if (!wanted.null) {
        char* end = nullptr; const double actual = std::strtod(value.value.c_str(),&end);
        require(end && end != value.value.c_str() && *end == '\0' && same(actual,wanted.value),"different float8 bits: "+value.value);
    }
}
} // namespace

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap(); size_t controls = 0,failures = 0;
    const auto check = [&](const std::string& label,const std::function<void()>& action) {
        ++controls; try { action(); } catch (const std::exception& error) {
            ++failures; std::cerr << "REAL_WIDEN_FAIL " << label << " : " << error.what() << '\n';
        }
    };
    check("IEEE platform",[]{ require(sizeof(float) == 4 && sizeof(double) == 8 &&
        std::numeric_limits<float>::is_iec559 && std::numeric_limits<double>::is_iec559,"unsupported platform"); });
    const std::string name = "real_widening",database = testDbPath(name);
    check("private database",[&]{ cleanupTestDb(name); require(g_engine.createDatabase(database) == DBStatus::OK,"create database"); });
    Session session; session.username = "testuser"; session.permission = 1; session.currentDB = database;
    auto* previous = currentSession(); setCurrentSession(&session);
    struct Restore { Session* previous; ~Restore(){ setCurrentSession(previous); } } restore{previous};
    const std::vector<std::string> functions = {"cast","sin","cos","tan","atan","sqrt","cbrt","degrees","radians",
        "round","ceil","ceiling","floor","trunc","sign","abs","power","pow","atan2","asin","acos"};
    const std::vector<Input> inputs = {"0","-0","1.25","-1.25","0.1","-0.1","123456789","-123456789",
        "16777217","-16777217","2147483647","-2147483648","1e-45","-1e-45","1.1754944e-38",
        "-1.1754944e-38","3.4028235e38","-3.4028235e38","Infinity","-Infinity","NaN",std::nullopt};
    for (const auto& fn : functions) for (const auto& input : inputs) {
        const auto wanted = expected(fn,input);
        const auto sql = expression(fn,input ? "'"+*input+"'::real" : "NULL::real");
        for (bool bind : {false,true}) check(sql+(bind ? " bound" : " raw"),[&]{
            ExprEvaluator evaluator; evaluator.setCurrentDB(database); auto root = node(fn);
            RowContext row; row.setParameters({ExprValue("real",input.value_or(""),!input),ExprValue("double precision","1",false)});
            if (bind) evaluator.bindScalarFunctions(root.get(),&g_engine);
            outcome(wanted,[&]{ return evaluator.eval(root.get(),row); });
        });
        check(sql+" parsed",[&]{
            auto parsed = SQLParser().parseForBinding("SELECT "+sql); require(parsed.success,parsed.error);
            auto* select = dynamic_cast<SelectStmt*>(parsed.stmt.get()); require(select && select->selectList.size() == 1,"missing target");
            ExprEvaluator evaluator; evaluator.setCurrentDB(database); auto* root = select->selectList.front().expr.get();
            evaluator.bindScalarFunctions(root,&g_engine); outcome(wanted,[&]{ return evaluator.eval(root,RowContext{}); });
        });
        check(sql+" literal helper",[&]{ outcome(wanted,[&]{ return helper(ExprHelper::evalString(sql,{},{},database,"",&g_engine)); }); });
        check(sql+" row helper",[&]{ outcome(wanted,[&]{ return helper(ExprHelper::evalStringWithNulls(expression(fn,"x"),
            {{"x",input.value_or("")}},input ? std::set<std::string>{} : std::set<std::string>{"x"},{{"x","real"}},database,"",&g_engine)); }); });
    }
    const std::vector<std::string> invalid = {"1e39","1e-50","bad","",std::string("1\0tail",6)};
    for (const auto& fn : functions) for (const auto& text : invalid) for (bool bind : {false,true})
        check("invalid REAL "+fn,[&]{
            ExprEvaluator evaluator; evaluator.setCurrentDB(database); auto root = node(fn); RowContext row;
            row.setParameters({ExprValue("real",text,false),ExprValue("double precision","1",false)});
            if (bind) evaluator.bindScalarFunctions(root.get(),&g_engine);
            outcome(expected(fn,Input(text)),[&]{ return evaluator.eval(root.get(),row); });
        });
    for (const auto& fn : functions) if (fn != "cast") for (const auto& context : {std::string{},database})
        for (bool immutable : {false,true}) check("opaque owner "+fn,[&]{
            ExprEvaluator evaluator; evaluator.setCurrentDB(context); size_t calls = 0; bool preserved = false;
            const auto callback = [&](const std::vector<ExprValue>& args) {
                ++calls;
                const bool two = fn == "power" || fn == "pow" || fn == "atan2";
                preserved = args.size() == (two ? 2u : 1u) && args[0].typeName == "real" &&
                    args[0].value == "123456789" && !args[0].isNull;
                if (two && args.size() == 2) preserved = preserved && args[1].typeName == "double precision" &&
                    args[1].value == "1" && !args[1].isNull;
                return ExprValue("text","opaque",false);
            };
            if (immutable) evaluator.registerFunction(fn,callback,'i'); else evaluator.registerFunction(fn,callback);
            auto root = node(fn);
            if (fn == "abs") {
                auto inner = std::move(static_cast<CastExpr*>(root.get())->operand);
                root = std::move(inner);
            }
            evaluator.bindScalarFunctions(root.get(),&g_engine); require(calls == 0,"binding evaluated callback");
            RowContext row; row.setParameters({ExprValue("real","123456789",false),ExprValue("double precision","1",false)});
            const auto value = evaluator.eval(root.get(),row);
            require(calls == 1 && preserved && value.typeName == "text" && value.value == "opaque" && !value.isNull,"opaque callback changed");
        });
    if (functions.size() != 21 || inputs.size() != 22 || controls != 2602) { ++failures; std::cerr << "REAL_WIDEN_REACH " << controls << '\n'; }
    if (g_engine.dropDatabase(database) != DBStatus::OK) return 2;
    cleanupTestDb(name);
    std::cout << "REAL_WIDEN_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
