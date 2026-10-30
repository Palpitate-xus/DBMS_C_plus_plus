#include "Session.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "test_utils.h"
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <limits>
#include <optional>
#include <stdexcept>

extern dbms::StorageEngine g_engine;

namespace {
using namespace dbms;
struct Expected { bool null = false; double value = 0; std::string state; };
double number(const std::string& value,bool real = false) { return real ? static_cast<double>(std::strtof(value.c_str(),nullptr)) : std::strtod(value.c_str(),nullptr); }
Expected powerValue(double base,double exponent) {
    if (std::isnan(base) || std::isnan(exponent))
        return {false,exponent == 0 || base == 1 ? 1 : std::numeric_limits<double>::quiet_NaN(),""};
    if ((base == 0 && exponent < 0) || (base < 0 && std::isfinite(exponent) && std::floor(exponent) != exponent)) return {false,0,"2201F"};
    errno = 0; const double value = std::pow(base,exponent); const int error = errno;
    if (std::isfinite(base) && std::isfinite(exponent) &&
        (error == ERANGE || !std::isfinite(value) || (value == 0 && base != 0))) return {false,0,"22003"};
    return {false,value,""};
}
void require(bool good,const std::string& label) { if (!good) throw std::runtime_error(label); }
void outcome(const Expected& wanted,const std::function<ExprValue()>& action) {
    ExprValue value; std::string state;
    try { value = action(); } catch (const DbError& e) { state = e.sqlState(); }
    catch (const std::exception& e) { const std::string message = e.what(); const auto at = message.find("SQLSTATE "); if (at == std::string::npos) throw; state = message.substr(at+9,5); }
    require(state == wanted.state,"expected SQLSTATE "+wanted.state+", got "+state);
    if (!state.empty()) return;
    require(value.typeName == "double precision" && value.isNull == wanted.null,"wrong type/NULL");
    if (!wanted.null) {
        char* end = nullptr; const double actual = std::strtod(value.value.c_str(),&end);
        require(end && end != value.value.c_str() && *end == '\0' &&
                ((std::isnan(actual) && std::isnan(wanted.value)) || std::memcmp(&actual,&wanted.value,sizeof(actual)) == 0),"different float8 bits: "+value.value);
    }
}
FunctionCallExpr call(const std::string& fn,const std::string& left,const std::string& right) {
    FunctionCallExpr node; node.funcName = fn;
    for (size_t i = 0; i < 2; ++i) {
        auto p = std::make_unique<ParameterExpr>(); p->slot = i; p->declaredType = i ? right : left; node.args.push_back(std::move(p));
    }
    return node;
}
} // namespace

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap(); size_t controls = 0,failures = 0;
    const auto check = [&](const std::string& label,const std::function<void()>& action) {
        ++controls; try { action(); } catch (const std::exception& error) { ++failures; std::cerr << "REAL_POWER_FAIL " << label << " : " << error.what() << '\n'; }
    };
    check("IEEE platform",[]{ require(sizeof(float) == 4 && sizeof(double) == 8 && std::numeric_limits<double>::is_iec559,"unsupported platform"); });
    const std::string name = "real_widening_power",database = testDbPath(name);
    check("private database",[&]{ cleanupTestDb(name); require(g_engine.createDatabase(database) == DBStatus::OK,"create database"); });
    Session session; session.username = "testuser"; session.permission = 1; session.currentDB = database;
    auto* previous = currentSession(); setCurrentSession(&session);
    struct Restore { Session* previous; ~Restore(){ setCurrentSession(previous); } } restore{previous};
    const std::vector<std::pair<std::string,std::string>> pairs = {{"NaN","0"},{"NaN","1"},{"1","NaN"},{"-1","NaN"},
        {"0","-1"},{"0","-Infinity"},{"-2","0.5"},{"-Infinity","0.5"},{"-Infinity","3"},{"-Infinity","-3"},
        {"-Infinity","2"},{"2","Infinity"},{"0.5","Infinity"},{"-1","-Infinity"},{"2","1024"},{"2","-1075"},
        {"2","-1074"},{"0.5","2"},{"-0","3"},{"-0","2"},{"1.1","3"},{"2","-1022"},{"Infinity","-1"},{"-1","1e20"}};
    for (const std::string source : {"real","double precision"}) for (const std::string fn : {"power","pow"})
        for (const auto& [base,exponent] : pairs) for (bool bind : {false,true}) check(source+" "+fn+"("+base+","+exponent+")",[&]{
            const auto wanted = powerValue(number(base,source == "real"),number(exponent));
            ExprEvaluator evaluator; evaluator.setCurrentDB(database); auto node = call(fn,source,"double precision"); RowContext row;
            row.setParameters({ExprValue(source,base,false),ExprValue("double precision",exponent,false)});
            if (bind) evaluator.bindScalarFunctions(&node,&g_engine);
            outcome(wanted,[&]{ return evaluator.eval(&node,row); });
        });
    const std::vector<std::optional<std::string>> inputs = {"0","-0","1.25","-1.25","0.1","-0.1","123456789","-123456789",
        "16777217","-16777217","2147483647","-2147483648","1e-45","-1e-45","1.1754944e-38",
        "-1.1754944e-38","3.4028235e38","-3.4028235e38","Infinity","-Infinity","NaN",std::nullopt};
    for (const auto& input : inputs) for (bool bind : {false,true}) check("reverse REAL atan2",[&]{
        ExprEvaluator evaluator; evaluator.setCurrentDB(database); auto node = call("atan2","double precision","real"); RowContext row;
        row.setParameters({ExprValue("double precision","1",false),ExprValue("real",input.value_or(""),!input)});
        if (bind) evaluator.bindScalarFunctions(&node,&g_engine);
        const Expected wanted = input ? Expected{false,std::atan2(1.0,number(*input,true)),""} : Expected{true,0,""};
        outcome(wanted,[&]{ return evaluator.eval(&node,row); });
    });
    if (pairs.size() != 24 || inputs.size() != 22 || controls != 238) { ++failures; std::cerr << "REAL_POWER_REACH " << controls << '\n'; }
    if (g_engine.dropDatabase(database) != DBStatus::OK) return 2;
    cleanupTestDb(name);
    std::cout << "REAL_POWER_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
