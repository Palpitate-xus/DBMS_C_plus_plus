#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/expr_helper.h"
#include "types/numeric.h"
#include "test_utils.h"
#include <iostream>
#include <stdexcept>
#include <type_traits>
#include <vector>

extern dbms::StorageEngine g_engine;
using namespace dbms;
namespace {
unsigned checks = 0;
void need(bool value, const std::string& reason) {
    ++checks;
    if (!value) throw std::runtime_error(reason);
}
}
int main() {
    try {
        static_assert(std::is_base_of_v<std::invalid_argument, NumericRangeError>);
        bool range = false;
        try { (void)Numeric("1e1000"); }
        catch (const NumericRangeError&) { range = true; }
        need(range, "numeric range has its own typed exception");
        bool oldContract = false;
        try { (void)Numeric("1e1000"); }
        catch (const std::invalid_argument&) { oldContract = true; }
        need(oldContract, "existing invalid_argument callers still catch range failures");
        bool malformed = false;
        try { (void)Numeric("not-a-number"); }
        catch (const NumericRangeError&) { throw std::runtime_error("malformed input is not a range failure"); }
        catch (const std::invalid_argument&) { malformed = true; }
        need(malformed, "malformed input retains its prior exception category");

        TypeRegistry::instance().bootstrap();
        g_engine.setBackgroundIntervals(60000, 60000);
        const auto database = testDbPath("numeric_arithmetic_range_error");
        cleanupTestDb("numeric_arithmetic_range_error");
        need(g_engine.createDatabase(database, "utf8") == DBStatus::OK, "actual database");
        Session session; session.username = "admin"; session.permission = 1; session.currentDB = database;
        struct Scope {
            Session* previous = currentSession();
            explicit Scope(Session& s) { setCurrentSession(&s); }
            ~Scope() { setCurrentSession(previous); }
        } scope(session);
        const auto eval = [&](const std::string& expression) {
            return ExprHelper::evalString(expression, {}, {}, database, "", &g_engine);
        };
        const auto state = [&](const std::string& expression, const std::string& expected) {
            const auto result = eval(expression);
            need(!result.ok && result.sqlState == expected,
                 "expression state " + expression + ": " + result.sqlState + " " + result.error);
        };
        const std::vector<std::pair<std::string, std::string>> overflow = {
            {"range_add", "('1e999'::numeric + '9e999'::numeric)"},
            {"range_sub", "('-1e999'::numeric - '9e999'::numeric)"},
            {"range_mul", "('1e500'::numeric * '1e500'::numeric)"},
            {"range_div", "('1e999'::numeric / '0.1'::numeric)"},
            {"range_pow", "('10'::numeric ^ '1000'::numeric)"}
        };
        DdlExecutor ddl;
        for (const auto& [table, expression] : overflow) {
            state(expression, "22003");
            const auto sql = "CREATE TABLE " + table + "(v text DEFAULT (" + expression + "::text))";
            need(!ddl.executeSql(sql, session), "create " + table);
            std::vector<StorageEngine::SqlRow> returning{{{"v", "sentinel"}}};
            bool rejected = false;
            try { (void)g_engine.insertRow(database, table, {}, &returning); }
            catch (const DbError& error) { rejected = error.sqlState() == "22003"; }
            need(rejected, "DEFAULT propagates range error: " + table);
            need(returning == std::vector<StorageEngine::SqlRow>{{{"v", "sentinel"}}},
                 "failed DEFAULT does not publish a RETURNING row: " + table);
            need(g_engine.query(database, table, {}, {"v"}).empty(),
                 "failed DEFAULT stores no SQL expression text: " + table);
            need(!g_engine.inTransaction() && g_engine.getLockManager().lockedTables().empty(),
                 "failed statement releases its transaction and locks: " + table);
        }
        // This formerly overflowed while multiplying the quotient back.
        // It is now an explicitly verified successful DEFAULT, not discarded.
        const auto exactMod = eval("('1e999'::numeric % '0.1'::numeric)");
        need(exactMod.ok && !exactMod.isNull && exactMod.value == "0.0", "representable large remainder");
        need(!ddl.executeSql("CREATE TABLE range_mod(v text DEFAULT ((('1e999'::numeric % '0.1'::numeric))::text))", session), "create range_mod");
        need(g_engine.insertRow(database, "range_mod", {}, nullptr) == DBStatus::OK, "representable DEFAULT remainder inserted");
        need(g_engine.query(database, "range_mod", {}, {"v"}) == std::vector<std::string>{"0.0 "}, "DEFAULT stores remainder, not SQL source");
        const std::vector<std::pair<std::string, std::string>> ordinary = {
            {"('12'::numeric + '3'::numeric)", "15"},
            {"('12'::numeric - '3'::numeric)", "9"},
            {"('12'::numeric * '3'::numeric)", "36"},
            {"('12'::numeric / '3'::numeric)", "4"},
            {"('12'::numeric % '5'::numeric)", "2"},
            {"('10'::numeric ^ '3'::numeric)", "1000"}
        };
        for (const auto& [expression, expected] : ordinary) {
            const auto result = eval(expression);
            need(result.ok && !result.isNull && Numeric(result.value) == Numeric(expected),
                 "ordinary exact arithmetic: " + expression);
        }
        state("('12'::numeric / '0'::numeric)", "22012");
        state("('12'::numeric % '0'::numeric)", "22012");
        const auto nan = eval("('NaN'::numeric + '1'::numeric)");
        need(nan.ok && !nan.isNull && nan.value == "NaN", "NaN arithmetic remains special");
        const auto infinity = eval("('Infinity'::numeric + '1'::numeric)");
        need(infinity.ok && !infinity.isNull && infinity.value == "Infinity", "Infinity arithmetic remains special");

        // Legacy native defaults do not record whether their bytes are a
        // literal or an expression. An unparseable raw literal still uses
        // the existing fallback, while expression-shaped bytes that actually
        // raise the known range error now fail rather than being stored.
        TableSchema literalTable; literalTable.tablename = "native_raw_literal";
        auto literalColumn = makeTextColumn("v", true);
        literalColumn.defaultValue = "[one, two";
        literalTable.append(literalColumn);
        need(g_engine.createTable(database, literalTable) == DBStatus::OK, "native literal table");
        need(g_engine.insert(database, "native_raw_literal", {}) == DBStatus::OK, "legacy raw literal fallback");
        need(g_engine.query(database, "native_raw_literal", {}, {"v"}) ==
                 std::vector<std::string>{"[one, two "}, "legacy raw literal bytes remain intact");
        TableSchema ambiguousTable; ambiguousTable.tablename = "native_expression_bytes";
        auto ambiguousColumn = makeTextColumn("v", true);
        ambiguousColumn.defaultValue = "(('1e500'::numeric * '1e500'::numeric)::text)";
        ambiguousTable.append(ambiguousColumn);
        need(g_engine.createTable(database, ambiguousTable) == DBStatus::OK, "native expression-bytes table");
        bool nativeRejected = false;
        try { (void)g_engine.insertRow(database, "native_expression_bytes", {}); }
        catch (const DbError& error) { nativeRejected = error.sqlState() == "22003"; }
        need(nativeRejected, "legacy expression-shaped native bytes now produce the range failure");
        need(g_engine.query(database, "native_expression_bytes", {}, {"v"}).empty(),
             "native expression-shaped bytes were not stored as a result");
        need(g_engine.checkpoint(database), "checkpoint after rejected defaults");
        std::cout << "NUMERIC_ARITHMETIC_RANGE_ERROR_CHECKED=" << checks << " FAILED=0\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "NUMERIC_ARITHMETIC_RANGE_ERROR_CHECKED=" << checks << " FAIL=" << error.what() << '\n';
        return 1;
    }
}
