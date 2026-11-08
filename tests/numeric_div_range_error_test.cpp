#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/expr_helper.h"
#include "types/numeric.h"
#include "test_utils.h"
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;
using namespace dbms;
namespace {
unsigned checks = 0;
void require(bool ok, const std::string& message) {
    ++checks;
    if (!ok) throw std::runtime_error(message);
}
}
int main() {
    try {
        // Test the Numeric owner directly, independent of SQL error mapping.
        const auto nativeQuotient = [](const char* a, const char* b, const char* want) {
            require(Numeric(a).truncDivide(Numeric(b)).toString() == want,
                    std::string("native truncDivide ") + a + " / " + b);
        };
        nativeQuotient("12.75", "0.5", "25");
        nativeQuotient("-12.75", "0.5", "-25");
        nativeQuotient("12.75", "-0.5", "-25");
        nativeQuotient("-12.75", "-0.5", "25");
        nativeQuotient("0", "-3", "0");
        nativeQuotient("-0.0001", "100", "0");
        nativeQuotient("1e-1000", "1e999", "0");
        nativeQuotient("999999999999999999999999999999", "1000000000000000000000000000000", "0");
        nativeQuotient("-999999999999999999999999999999", "1000000000000000000000000000000", "0");
        nativeQuotient("1999999999999999999999999999999", "1000000000000000000000000000000", "1");
        const auto maximum = Numeric("1e998").truncDivide(Numeric("0.1")).toString();
        require(maximum.size() == 1000 && maximum[0] == '1' &&
                maximum.find_first_not_of('0', 1) == std::string::npos,
                "1000-digit quotient is representable");
        // Products can have huge scale while retaining one significant digit.
        // Never stringify the huge operand; the quotient paths must decide by
        // effective magnitude before allocating a billion zeros.
        Numeric tiny("0.1");
        for (int i = 0; i < 30; ++i) tiny = tiny * tiny;
        require(tiny.scale() == (1 << 30), "native tiny scale reaches 2^30");
        require(tiny.truncDivide(Numeric(1)).toString() == "0", "huge-scale quotient below one");
        require(Numeric(0).truncDivide(tiny).toString() == "0", "huge-scale zero dividend");
        require(tiny.truncDivide(tiny).toString() == "1", "equal huge scales cancel");
        bool hugeQuotient = false;
        try { (void)Numeric(1).truncDivide(tiny); }
        catch (const NumericRangeError&) { hugeQuotient = true; }
        require(hugeQuotient, "huge-scale quotient rejected before allocating");
        bool scaleOverflow = false;
        try { (void)(tiny * tiny); }
        catch (const NumericRangeError&) { scaleOverflow = true; }
        require(scaleOverflow, "multiply scale sum beyond INT_MAX is rejected");
        // Billion-scale native inputs must be decided without allocating
        // the aligned zeros or stringifying the huge-scale operands.
        const Numeric tinyRemainder = tiny.exactRemainder(Numeric(1));
        require(tinyRemainder.scale() == tiny.scale() && tinyRemainder.precision() == 1 && tinyRemainder.sign() == 1,
                "tiny remainder one is the original tiny value");
        const Numeric selfRemainder = tiny.exactRemainder(tiny);
        require(selfRemainder.sign() == 0 && selfRemainder.scale() == tiny.scale(), "huge equal scales return zero without padding");
        const Numeric zeroRemainder = Numeric(0).exactRemainder(tiny);
        require(zeroRemainder.sign() == 0 && zeroRemainder.scale() == tiny.scale(), "zero dividend avoids huge alignment");
        bool hugeRemainder = false;
        try { (void)Numeric(1).exactRemainder(tiny); }
        catch (const NumericRangeError&) { hugeRemainder = true; }
        require(hugeRemainder, "huge native alignment rejected before allocation");
        bool nativeZero = false;
        try { (void)Numeric(1).exactRemainder(Numeric(0)); }
        catch (const std::domain_error&) { nativeZero = true; }
        require(nativeZero, "native remainder zero divisor");
        require(Numeric("NaN").exactRemainder(Numeric(0)).isNaN(), "native NaN precedence");
        require(Numeric("Infinity").exactRemainder(Numeric(1)).isNaN(), "native infinite dividend");
        require(Numeric("12.75").exactRemainder(Numeric("Infinity")).toString() == "12.75", "native infinite divisor");
        bool range = false;
        try { (void)Numeric("1e999").truncDivide(Numeric("0.1")); }
        catch (const NumericRangeError&) { range = true; }
        require(range, "1001-digit quotient retains implementation limit");
        for (const auto& a : {"0", "1", "-1", "NaN", "Infinity", "-Infinity"}) {
            for (const auto& b : {"0", "1", "-1", "NaN", "Infinity", "-Infinity"}) {
                const Numeric left(a), right(b);
                if (!left.isFinite() || !right.isFinite() || right.sign() == 0)
                    require(left.truncDivide(right).toString() == (left / right).toString(),
                            std::string("special values agree with previous Numeric /") + a + "/" + b);
            }
        }
        TypeRegistry::instance().bootstrap();
        g_engine.setBackgroundIntervals(60000, 60000);
        const auto database = testDbPath("numeric_div_range_error");
        cleanupTestDb("numeric_div_range_error");
        require(g_engine.createDatabase(database, "utf8") == DBStatus::OK, "database");
        Session session; session.username = "admin"; session.permission = 1; session.currentDB = database;
        struct Scope {
            Session* previous = currentSession();
            explicit Scope(Session& s) { setCurrentSession(&s); }
            ~Scope() { setCurrentSession(previous); }
        } scope(session);
        const auto eval = [&](const std::string& sql) {
            return ExprHelper::evalString(sql, {}, {}, database, "", &g_engine);
        };
        const auto exact = [&](const std::string& sql, const std::string& expected) {
            auto result = eval(sql);
            require(result.ok && !result.isNull && Numeric(result.value) == Numeric(expected),
                    "exact result " + sql + ": " + result.error);
        };
        const std::string overflowing = "div('1e999'::numeric, '0.1'::numeric)";
        const auto bad = eval(overflowing);
        require(!bad.ok && bad.sqlState == "22003", "div overflow must have structured 22003");
        exact("div('12'::numeric, '5'::numeric)", "2");
        exact("div('-12'::numeric, '5'::numeric)", "-2");
        exact("div('12'::numeric, '-5'::numeric)", "-2");
        exact("div('1e998'::numeric, '0.1'::numeric)", "1e999");
        auto nullValue = eval("div(NULL::numeric, '5'::numeric)");
        require(nullValue.ok && nullValue.isNull, "div strict NULL");
        auto zero = eval("div('12'::numeric, '0'::numeric)");
        require(!zero.ok && zero.sqlState.empty() && zero.error.find("SQLSTATE 22012") != std::string::npos,
                "div zero keeps existing marker and exception contract");
        // Prior 22003 was an intermediate-overflow artifact; both exact
        // remainder paths now return PostgreSQL's representable 0.0 result.
        for (const auto& sql : {"mod('1e999'::numeric, '0.1'::numeric)",
                                "mod('1e999'::numeric, '0.5'::numeric)",
                                "('1e999'::numeric % '0.1'::numeric)",
                                "('1e999'::numeric % '0.5'::numeric)"}) {
            const auto result = eval(sql);
            require(result.ok && !result.isNull && result.value == "0.0", std::string("large exact remainder ")+sql);
        }
        auto nanMod=eval("mod('NaN'::numeric, '0'::numeric)");
        require(nanMod.ok && !nanMod.isNull && nanMod.value == "NaN", "mod NaN retains priority over zero");
        auto nanOp=eval("('NaN'::numeric % '0'::numeric)");
        require(nanOp.ok && !nanOp.isNull && nanOp.value == "NaN", "operator NaN retains priority over zero");
        exact("mod('12'::numeric, '5'::numeric)", "2");
        // Both SQL remainder paths retain signed and scaled results.
        exact("mod('-12.75'::numeric, '0.5'::numeric)", "-0.25");
        exact("mod('12.75'::numeric, '-0.5'::numeric)", "0.25");
        exact("mod('-12.75'::numeric, '-0.5'::numeric)", "-0.25");
        exact("('12.75'::numeric % '-0.5'::numeric)", "0.25");
        exact("('-12.75'::numeric % '-0.5'::numeric)", "-0.25");
        exact("('-12.75'::numeric % '0.5'::numeric)", "-0.25");
        exact("mod('999999999999999999999999999999'::numeric, '1000000000000000000000000000000'::numeric)", "999999999999999999999999999999");
        exact("('999999999999999999999999999999'::numeric % '1000000000000000000000000000000'::numeric)", "999999999999999999999999999999");
        exact("div('999999999999999999999999999999'::numeric, '1000000000000000000000000000000'::numeric)", "0");
        exact("div('-999999999999999999999999999999'::numeric, '1000000000000000000000000000000'::numeric)", "0");
        auto modZero = eval("mod('12'::numeric, '0'::numeric)");
        require(!modZero.ok && modZero.sqlState.empty() && modZero.error.find("SQLSTATE 22012") != std::string::npos,
                "mod zero keeps pre-existing error marker");
        exact("CASE WHEN false THEN " + overflowing + " ELSE 7 END", "7");
        exact("CASE WHEN true THEN 8 ELSE " + overflowing + " END", "8");

        DdlExecutor ddl;
        require(!ddl.executeSql("CREATE TABLE div_range_default(v text DEFAULT ((" + overflowing + ")::text))", session), "create DEFAULT table");
        std::vector<StorageEngine::SqlRow> returning{{{"v", "sentinel"}}};
        bool rejected = false;
        try { (void)g_engine.insertRow(database, "div_range_default", {}, &returning); }
        catch (const DbError& error) { rejected = error.sqlState() == "22003"; }
        require(rejected, "overflowing DEFAULT must fail with structured 22003");
        require(returning == std::vector<StorageEngine::SqlRow>{{{"v", "sentinel"}}}, "no returning row");
        require(g_engine.query(database, "div_range_default", {}, {"v"}).empty(), "no SQL text inserted");
        require(!g_engine.inTransaction() && g_engine.getLockManager().lockedTables().empty(), "transaction and locks released");
        std::cout << "NUMERIC_DIV_RANGE_CHECKED=" << checks << " FAILED=0\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "NUMERIC_DIV_RANGE_CHECKED=" << checks << " FAIL=" << error.what() << '\n';
        return 1;
    }
}
