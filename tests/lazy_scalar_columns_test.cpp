// Helper-local declarations must not create value demand or erase NULL bits.
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include <iostream>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>

namespace {
using namespace dbms;
unsigned checked = 0;
void need(bool value, const std::string& reason) {
    ++checked;
    if (!value) throw std::runtime_error(reason);
}
void result(const ExprEvalResult& value, const std::optional<std::string>& expected) {
    need(value.ok && value.isNull == !expected && (!expected || value.value == *expected),
         "actual helper value/NULL: " + value.error);
}
} // namespace

int main() {
    try {
        TypeRegistry::instance().bootstrap();
        const std::map<std::string, std::string> declarations = {
            {"t", "text"}, {"empty", "text"}, {"nil", "text"}, {"null_text", "text"},
            {"unread", "text"}, {"flag", "boolean"}, {"F", "text"}, {"f", "text"}};
        std::map<std::string, std::optional<std::string>> values = {
            {"t", "a'b"}, {"empty", ""}, {"nil", std::nullopt}, {"null_text", "NULL"},
            {"flag", "f"}, {"F", "UP"}, {"f", "down"}};
        std::map<std::string, unsigned> calls;
        const auto read = [&](const std::string& name) -> std::optional<std::string> {
            ++calls[name];
            if (name == "unread") throw DbError("XX001", "intentionally unavailable unselected input");
            return values.at(name);
        };
        const auto evaluate = [&](const std::string& sql) {
            calls.clear();
            return ExprHelper::evalStringWithLazyColumns(sql, declarations, read);
        };
        result(evaluate("upper('we''ird')"), "WE'IRD");
        need(calls.empty(), "constant expression reads no row fields");
        result(evaluate("replace('a'||'','x','y')"), "a");
        need(calls.empty(), "complete literal concatenation stays a real expression");
        result(evaluate("upper('prefix:' || t)"), "PREFIX:A'B");
        need(calls.size() == 1 && calls["t"] == 1, "only referenced source demanded");
        result(evaluate("upper(t)||lower(alias.t)"), "A'Ba'b");
        need(calls.size() == 1 && calls["t"] == 1, "qualified fallback and repeated reads share one physical input");
        result(evaluate("upper(empty)"), "");
        need(calls.size() == 1 && calls["empty"] == 1, "empty string keeps a non-NULL datum");
        result(evaluate("upper(nil)"), std::nullopt);
        need(calls.size() == 1 && calls["nil"] == 1, "NULL datum fetched exactly once");
        result(evaluate("upper(null_text)"), "NULL");
        result(evaluate("CASE WHEN flag THEN upper(unread) ELSE upper(t) END"), "A'B");
        need(calls.size() == 2 && calls["flag"] == 1 && calls["t"] == 1,
             "CASE preserves runtime branch demand after type analysis");
        result(evaluate("coalesce(t,unread)"), "a'b");
        need(calls.size() == 1 && calls["t"] == 1, "COALESCE preserves unused source laziness");
        result(evaluate("upper(\"F\")||upper(f)"), "UPDOWN");
        need(calls.size() == 2 && calls["F"] == 1 && calls["f"] == 1,
             "quoted and unquoted source identities stay separate");
        auto failure = evaluate("upper(missing)");
        need(!failure.ok && failure.sqlState == "42703" && calls.empty(),
             "unknown declaration fails before any value read");
        failure = evaluate("upper(t,t)");
        need(!failure.ok && failure.sqlState == "42883" && calls.size() == 1 && calls["t"] == 1,
             "existing runtime arity failure is preserved and repeated arguments read the source once");
        failure = evaluate("upper(unread)");
        need(!failure.ok && failure.sqlState == "XX001" && calls.size() == 1 && calls["unread"] == 1,
             "source DbError survives without fallback or a second read");
        values["t"] = "next row";
        result(evaluate("upper(t)"), "NEXT ROW");
        need(calls.size() == 1 && calls["t"] == 1, "no value cache escapes a helper invocation");

        const std::map<std::string, std::string> eagerRow{{"t", "next row"}};
        const std::map<std::string, std::string> collations{{"t", "C"}};
        const auto eager = ExprHelper::evalStringWithNulls("upper(t)", eagerRow, {}, declarations,
            "", "", nullptr, collations);
        const auto lazy = ExprHelper::evalStringWithLazyColumns("upper(t)", declarations, read,
            "", "", nullptr, collations);
        need(eager.ok && lazy.ok && eager.value == lazy.value && eager.typeName == lazy.typeName &&
             eager.isNull == lazy.isNull && eager.collation == lazy.collation,
             "lazy input preserves eager type and collation semantics");

        RowContext context;
        context.set("fallback", ExprValue("integer", "7", false));
        ExprEvaluator evaluator;
        unsigned readCalls = 0;
        evaluator.setNamedColumnReader([&](const std::string&) -> std::optional<ExprValue> {
            ++readCalls;
            return std::nullopt;
        });
        need(context.has("fallback") && context.get("fallback")->typeName == "integer" && readCalls == 0,
             "RowContext presence and type probes never invoke a runtime reader");
        const auto fallback = evaluator.readNamedColumn("fallback", context);
        need(!fallback.isNull && fallback.value == "7" && readCalls == 1,
             "unhandled runtime name retains the normal eager fallback");
        std::cout << "LAZY_SCALAR_COLUMNS_PASS checks=" << checked << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "LAZY_SCALAR_COLUMNS_FAIL checks=" << checked << " " << error.what() << '\n';
        return 1;
    }
}
