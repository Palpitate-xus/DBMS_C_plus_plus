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
unsigned checked = 0;
std::vector<std::string> failures;
void check(bool ok, const std::string& name) {
    ++checked;
    if (!ok) failures.push_back(name);
}
}
int main() {
    try {
        TypeRegistry::instance().bootstrap();
        g_engine.setBackgroundIntervals(60000, 60000);
        const auto database = testDbPath("numeric_tochar_default_range");
        cleanupTestDb("numeric_tochar_default_range");
        if (g_engine.createDatabase(database, "utf8") != DBStatus::OK)
            throw std::runtime_error("create database");
        Session session; session.username="admin"; session.permission=1; session.currentDB=database;
        struct Scope {
            Session* previous = currentSession();
            explicit Scope(Session& value) { setCurrentSession(&value); }
            ~Scope() { setCurrentSession(previous); }
        } scope(session);
        const auto eval = [&](const std::string& expression) {
            return ExprHelper::evalString(expression, {}, {}, database, "", &g_engine);
        };
        const std::string large = "to_char('1e999'::numeric,'9.9')";
        const auto normal = eval("to_char('1.25'::numeric,'FM99.00')");
        check(normal.ok && !normal.isNull && normal.value == "1.25", "ordinary numeric formatting");
        const auto direct = eval(large);
        check(!direct.ok && direct.sqlState == "22003", "direct formatting exposes structured 22003");
        const auto safeCase = eval("CASE WHEN true THEN 'safe' ELSE " + large + " END");
        check(safeCase.ok && !safeCase.isNull && safeCase.value == "safe", "CASE does not evaluate unused branch");
        const auto selectedCase = eval("CASE WHEN false THEN 'safe' ELSE " + large + " END");
        check(!selectedCase.ok && selectedCase.sqlState == "22003", "CASE selected branch propagates 22003");
        const auto safeCoalesce = eval("coalesce('safe'," + large + ")");
        check(safeCoalesce.ok && !safeCoalesce.isNull && safeCoalesce.value == "safe", "COALESCE does not evaluate unused branch");
        const auto selectedCoalesce = eval("coalesce(NULL::text," + large + ")");
        check(!selectedCoalesce.ok && selectedCoalesce.sqlState == "22003", "COALESCE selected branch propagates 22003");
        DdlExecutor ddl;
        if (ddl.executeSql("CREATE TABLE tochar_default(v text DEFAULT " + large + ")", session))
            throw std::runtime_error("create default table");
        if (g_engine.insertRow(database, "tochar_default", {{"v", "keep"}}) != DBStatus::OK)
            throw std::runtime_error("insert existing row");
        const auto before = g_engine.query(database, "tochar_default", {}, {"v"});
        check(before == std::vector<std::string>{"keep "}, "existing row is present");
        std::vector<StorageEngine::SqlRow> returning{{{"v", "sentinel"}}};
        bool rejected = false;
        try { (void)g_engine.insertRow(database, "tochar_default", {}, &returning); }
        catch (const DbError& error) { rejected = error.sqlState() == "22003"; }
        check(rejected, "DEFAULT insert returns 22003");
        check(returning == std::vector<StorageEngine::SqlRow>{{{"v", "sentinel"}}}, "failed statement does not publish RETURNING");
        check(g_engine.query(database, "tochar_default", {}, {"v"}) == before, "failed statement preserves existing rows and stores no SQL");
        check(!g_engine.inTransaction() && g_engine.getLockManager().lockedTables().empty(), "failed statement releases transaction and locks");
        TableSchema literal; literal.tablename="native_legacy_literal";
        auto column = makeTextColumn("v", true); column.defaultValue="[one, two"; literal.append(column);
        if (g_engine.createTable(database, literal) != DBStatus::OK) throw std::runtime_error("create native literal table");
        check(g_engine.insert(database, "native_legacy_literal", {}) == DBStatus::OK, "legacy native literal still inserts");
        check(g_engine.query(database, "native_legacy_literal", {}, {"v"}) == std::vector<std::string>{"[one, two "}, "legacy native literal bytes remain intact");
        for (const auto& failure : failures) std::cerr << "FAIL " << failure << '\n';
        std::cout << "NUMERIC_TOCHAR_DEFAULT_RANGE_CHECKED=" << checked << " FAILED=" << failures.size() << '\n';
        return failures.empty() ? 0 : 1;
    } catch (const std::exception& error) {
        std::cerr << "NUMERIC_TOCHAR_DEFAULT_RANGE_SETUP_ERROR=" << error.what() << '\n';
        return 2;
    }
}
