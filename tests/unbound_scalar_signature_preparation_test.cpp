#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include "utils/Session.h"
#include "test_utils.h"

#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("unbound_scalar_signature");
    if (g_engine.createDatabase(database) != DBStatus::OK) return 2;
    Session session; session.username = "testuser"; session.permission = 1;
    session.currentDB = database;
    const auto prior = currentSession(); setCurrentSession(&session);
    struct Restore { Session* value; ~Restore() { setCurrentSession(value); } } restore{prior};
    TableSchema table; table.tablename = "inputs";
    table.append(makeIntColumn("id", false, 4));
    table.append(makeTextColumn("txt", true));
    if (g_engine.createTable(database, table) != DBStatus::OK) return 2;

    size_t checks = 0, failures = 0;
    const auto check = [&](bool valid, const std::string& label) {
        ++checks;
        if (!valid) { ++failures; std::cerr << "UNBOUND_SIGNATURE_FAILURE " << label << '\n'; }
    };
    const auto identity = [&](const std::string& sql) {
        return ExprHelper::scalarExpressionIdentity(sql, [](const ColumnRefExpr& column) {
            if (column.column != "id") throw DbError("42703", "unknown source column");
            return std::string("source-0-column-0-integer");
        }, database, &g_engine);
    };
    for (const auto* expression : {"abs(id-5)", "abs(id+1)", "abs(-(id-5))", "abs(abs(id)-5)"}) {
        try {
            const auto key = identity(expression);
            check(!key.empty(), std::string("unbound identity ") + expression);
        } catch (const DbError& error) {
            check(false, std::string(expression) + " SQLSTATE=" + error.sqlState());
        }
    }
    try { check(identity("abs(id-5)") == identity("abs(d.id-5)"), "validated source qualification"); }
    catch (const DbError& error) { check(false, error.what()); }

    // The legacy context is genuinely typed. Binding must defer inference,
    // not execute an argument or manufacture TEXT before that context exists.
    SQLParser parser; ExprEvaluator evaluator; evaluator.setCurrentDB(database);
    RowContext row; row.set("id", ExprValue("integer", "3"));
    for (const auto& item : std::vector<std::pair<std::string, std::string>>{
             {"abs(id-5)", "2"}, {"abs(abs(id)-5)", "2"}, {"abs(id+1)", "4"}}) {
        try {
            auto parsed = parser.parseForBinding("SELECT " + item.first);
            auto* select = parsed.success ? dynamic_cast<SelectStmt*>(parsed.stmt.get()) : nullptr;
            if (!select) { check(false, "parse " + item.first); continue; }
            auto* expression = select->selectList.front().expr.get();
            evaluator.bindScalarFunctions(expression, &g_engine);
            const auto value = evaluator.eval(expression, row);
            check(!value.isNull && value.typeName == "integer" && value.value == item.second,
                  "typed legacy evaluation " + item.first);
        } catch (const DbError& error) { check(false, item.first + " SQLSTATE=" + error.sqlState()); }
    }
    for (const auto* expression : {"abs(txt-5)", "abs(txt)", "upper(id)", "length(id)"}) {
        for (const auto* tail : {" LIMIT 0", " WHERE false"}) {
            std::string state;
            try { (void)g_engine.prepareBoundQuery(database, std::string("SELECT ") + expression + " FROM inputs" + tail); }
            catch (const DbError& error) { state = error.sqlState(); }
            check(state == "42883", std::string("strict declared rejection ") + expression + tail);
        }
    }
    check(checks == 16, "complete matrix");
    if (g_engine.dropDatabase(database) != DBStatus::OK) return 2;
    std::cout << "UNBOUND_SIGNATURE_COMPLETE " << checks << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
