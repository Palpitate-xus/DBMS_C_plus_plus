#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/expr_helper.h"
#include "test_utils.h"
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;
using namespace dbms;
namespace {
unsigned checks = 0;
std::vector<std::string> failures;
void check(bool value, const std::string& name) {
    ++checks;
    if (!value) failures.push_back(name);
}
}
int main() {
    try {
        TypeRegistry::instance().bootstrap();
        g_engine.setBackgroundIntervals(60000, 60000);
        const auto database = testDbPath("numeric_div_nan_precedence");
        cleanupTestDb("numeric_div_nan_precedence");
        if (g_engine.createDatabase(database, "utf8") != DBStatus::OK)
            throw std::runtime_error("create database");
        Session session; session.username = "admin"; session.permission = 1;
        session.currentDB = database;
        struct Scope {
            Session* previous = currentSession();
            explicit Scope(Session& s) { setCurrentSession(&s); }
            ~Scope() { setCurrentSession(previous); }
        } scope(session);
        const auto eval = [&](const std::string& expr) {
            return ExprHelper::evalString(expr, {}, {}, database, "", &g_engine);
        };
        const auto value = [&](const std::string& expr, const std::string& want) {
            auto r = eval(expr);
            check(r.ok && !r.isNull && r.typeName == "numeric" && r.value == want,
                  expr + " => " + (r.ok ? r.value : r.error));
        };
        const auto nullValue = [&](const std::string& expr) {
            auto r = eval(expr);
            check(r.ok && r.isNull && r.typeName == "numeric", expr + " => NULL");
        };
        const auto zeroError = [&](const std::string& expr) {
            auto r = eval(expr);
            check(!r.ok && (r.sqlState == "22012" ||
                            r.error.find("SQLSTATE 22012") != std::string::npos),
                  expr + " => 22012");
        };
        value("div('NaN'::numeric, 0::numeric)", "NaN");
        value("div('NaN'::numeric, '-0.00'::numeric)", "NaN");
        value("div(0::numeric, 'NaN'::numeric)", "NaN");
        value("div('NaN'::numeric, 'NaN'::numeric)", "NaN");
        value("div('Infinity'::numeric, 'NaN'::numeric)", "NaN");
        value("div('-Infinity'::numeric, 'NaN'::numeric)", "NaN");
        value("div('NaN'::numeric, 'Infinity'::numeric)", "NaN");
        value("div('Infinity'::numeric, '-Infinity'::numeric)", "NaN");
        value("div('Infinity'::numeric, '-2'::numeric)", "-Infinity");
        value("div('-Infinity'::numeric, '-2'::numeric)", "Infinity");
        value("div('2'::numeric, 'Infinity'::numeric)", "0");
        value("div('-12.75'::numeric, '0.5'::numeric)", "-25");
        nullValue("div(NULL::numeric, 0::numeric)");
        nullValue("div('NaN'::numeric, NULL::numeric)");
        zeroError("div('Infinity'::numeric, 0::numeric)");
        zeroError("div('-Infinity'::numeric, 0::numeric)");
        zeroError("div(0::numeric, 0::numeric)");
        zeroError("div('1'::numeric, '-0.00'::numeric)");

        DdlExecutor ddl;
        if (ddl.executeSql("CREATE TABLE nan_default(v text DEFAULT (div('NaN'::numeric,0::numeric)::text))", session))
            throw std::runtime_error("create NaN DEFAULT table");
        if (g_engine.insertRow(database, "nan_default", {{"v", "keep"}}) != DBStatus::OK)
            throw std::runtime_error("insert existing row");
        bool inserted = false;
        try { inserted = g_engine.insertRow(database, "nan_default", {}) == DBStatus::OK; }
        catch (const std::exception&) { }
        check(inserted, "NaN / zero DEFAULT succeeds");
        const auto nanRows = g_engine.query(database, "nan_default", {}, {"v"});
        check(nanRows == std::vector<std::string>{"keep ", "NaN "},
              "NaN DEFAULT stores value alongside original row");
        check(!g_engine.inTransaction() && g_engine.getLockManager().lockedTables().empty(),
              "NaN DEFAULT leaves no transaction or lock");

        if (ddl.executeSql("CREATE TABLE div_zero_default(v text DEFAULT (div(1::numeric,0::numeric)::text))", session))
            throw std::runtime_error("create zero DEFAULT table");
        if (g_engine.insertRow(database, "div_zero_default", {{"v", "keep"}}) != DBStatus::OK)
            throw std::runtime_error("insert zero control existing row");
        std::vector<StorageEngine::SqlRow> returning{{{"v", "sentinel"}}};
        bool rejected = false;
        try { (void)g_engine.insertRow(database, "div_zero_default", {}, &returning); }
        catch (const DbError& e) { rejected = e.sqlState() == "22012"; }
        check(rejected, "finite / zero DEFAULT rejects with 22012");
        check(returning == std::vector<StorageEngine::SqlRow>{{{"v", "sentinel"}}},
              "failed DEFAULT does not publish RETURNING");
        check(g_engine.query(database, "div_zero_default", {}, {"v"}) == std::vector<std::string>{"keep "},
              "failed DEFAULT keeps existing row");
        check(!g_engine.inTransaction() && g_engine.getLockManager().lockedTables().empty(),
              "failed DEFAULT leaves no transaction or lock");
        for (const auto& failure : failures) std::cerr << "FAIL " << failure << '\n';
        std::cout << "NUMERIC_DIV_NAN_PRECEDENCE_CHECKED=" << checks
                  << " FAILED=" << failures.size() << '\n';
        return failures.empty() ? 0 : 1;
    } catch (const std::exception& e) {
        std::cerr << "NUMERIC_DIV_NAN_PRECEDENCE_SETUP_ERROR=" << e.what() << '\n';
        return 2;
    }
}
