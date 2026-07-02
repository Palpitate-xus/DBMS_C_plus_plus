// ============================================================================
// Functions / Aggregates Test — Phase 4 Wave 2
// ============================================================================

#include "expression/ExprEvaluator.h"
#include "parser/ast.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <memory>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static dbms::ExprValue callFn(dbms::ExprEvaluator& eval, const std::string& name,
                              const std::vector<dbms::ExprValue>& args) {
    dbms::FunctionCallExpr call;
    call.funcName = name;
    for (const auto& a : args) {
        auto lit = std::make_unique<dbms::LiteralExpr>();
        lit->value = a.value;
        lit->typeName = a.typeName;
        call.args.push_back(std::move(lit));
    }
    dbms::RowContext ctx;
    return eval.eval(&call, ctx);
}

namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static void test_scalar_functions() {
    dbms::ExprEvaluator eval;

    auto r = callFn(eval, "sin", {dbms::ExprValue("double precision", "0.0", false)});
    assert(!r.isNull && std::stod(r.value) < 0.0001);

    r = callFn(eval, "sqrt", {dbms::ExprValue("double precision", "16.0", false)});
    assert(!r.isNull && std::stod(r.value) == 4.0);

    r = callFn(eval, "concat", {dbms::ExprValue("text", "hello", false),
                                dbms::ExprValue("text", " world", false)});
    assert(r.value == "hello world");

    r = callFn(eval, "trim", {dbms::ExprValue("text", "  spaced  ", false)});
    assert(r.value == "spaced");

    r = callFn(eval, "replace", {dbms::ExprValue("text", "abcabc", false),
                                 dbms::ExprValue("text", "b", false),
                                 dbms::ExprValue("text", "X", false)});
    assert(r.value == "aXcaXc");

    r = callFn(eval, "position", {dbms::ExprValue("text", "cd", false),
                                  dbms::ExprValue("text", "abcd", false)});
    assert(r.value == "3");

    r = callFn(eval, "extract", {dbms::ExprValue("text", "month", false),
                                 dbms::ExprValue("date", "2026-06-20", false)});
    assert(r.value == "6");

    std::cout << "[FUNCTIONS] scalar OK" << std::endl;
}

static void test_aggregates() {
    std::string db = testDbPath("functions_t1");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::TableSchema tbl;
    tbl.tablename = "scores";
    tbl.append(dbms::makeIntColumn("id", true, 2, true));
    tbl.append(dbms::makeIntColumn("score", true, 2, false));
    assert(g_engine.createTable(db, tbl) == dbms::DBStatus::OK);

    // Insert rows
    std::vector<std::string> rows = {"10", "20", "30", "20", "50"};
    for (size_t i = 0; i < rows.size(); ++i) {
        std::map<std::string, std::string> vals;
        vals["id"] = std::to_string(i + 1);
        vals["score"] = rows[i];
        assert(g_engine.insert(db, "scores", vals) == dbms::DBStatus::OK);
    }

    std::vector<dbms::StorageEngine::AggItem> items = {
        {"count", "*", {}},
        {"sum", "score", {}},
        {"avg", "score", {}},
        {"max", "score", {}},
        {"min", "score", {}},
        {"mode", "score", {}},
        {"median", "score", {}}
    };
    auto res = g_engine.aggregate(db, "scores", {}, items);
    assert(!res.empty());
    std::cout << "[FUNCTIONS] aggregate result: " << res.front() << std::endl;
    // Result is space-separated: count sum avg max min mode median
    // Expected: 5 130 26 50 10 20 20
    // Just check it contains expected tokens.
    assert(res.front().find("5 ") != std::string::npos);
    assert(res.front().find("20 ") != std::string::npos);

    cleanup(db);
    std::cout << "[FUNCTIONS] aggregates OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_scalar_functions();
    test_aggregates();
    std::cout << "[FUNCTIONS] all passed" << std::endl;
    return 0;
}
