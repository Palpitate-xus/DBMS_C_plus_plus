// Genuine array_position(empty, needle, NULL-start) returns INTEGER NULL.
// Preserve argument errors, bounds, typed native cells and callback ownership.
#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "expression/prepared_query_execution.h"
#include "parser/parser.h"
#include "test_utils.h"

#include <exception>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {
struct Control {
    std::string expression;
    std::optional<std::string> value;
    std::string state;
};

std::string legacyState(const std::exception& error) {
    if (const auto* structured = dynamic_cast<const dbms::DbError*>(&error))
        return structured->sqlState();
    const std::string message = error.what();
    const auto at = message.find("SQLSTATE ");
    return at == std::string::npos ? std::string{} : message.substr(at + 9, 5);
}

dbms::FunctionCallExpr cellCall(const std::vector<dbms::ExprValue>& cells) {
    dbms::FunctionCallExpr call;
    call.funcName = "array_position";
    for (size_t i = 0; i < cells.size(); ++i) {
        auto parameter = std::make_unique<dbms::ParameterExpr>();
        parameter->slot = i;
        parameter->declaredType = cells[i].typeName;
        call.args.push_back(std::move(parameter));
    }
    return call;
}

std::vector<Control> sqlControls() {
    std::vector<Control> result;
    const auto value = [&](std::string sql, std::optional<std::string> expected = std::nullopt) {
        result.push_back({std::move(sql), std::move(expected), {}});
    };
    const auto error = [&](std::string sql, const std::string& state) {
        result.push_back({std::move(sql), std::nullopt, state});
    };
    for (const std::string type : {"integer", "text", "numeric"}) {
        const auto needle = type == "text" ? "'needle'::text" : "1::" + type;
        for (const auto& array : {"'{}'::" + type + "[]", "ARRAY[]::" + type + "[]"}) {
            value("array_position(" + array + ',' + needle + ",NULL::integer)");
            value("array_position(" + array + ",NULL::" + type + ",NULL::integer)");
            value("array_position(" + array + ',' + needle + ')');
            value("array_position(" + array + ',' + needle + ",1)");
            value("array_position(" + array + ",NULL::" + type + ",-2147483648)");
            error("array_position(" + array + ',' + needle + ",'bad')", "22P02");
        }
        for (const auto& search : {needle, "NULL::" + type}) {
            value("array_position(NULL::" + type + "[]," + search + ",NULL::integer)");
            value("array_position(NULL::" + type + "[]," + search + ",1)");
        }
        const auto source = type == "text" ? "ARRAY['needle'::text" : "ARRAY[1::" + type;
        error("array_position(" + source + "]," + needle + ",NULL::integer)", "22004");
        // A NULL needle with no NULL element is a separate existing gap.
        // Include NULL in the array so the PG 22004 expectation is correct.
        error("array_position(" + source + ",NULL],NULL::" + type + ",NULL::integer)", "22004");
    }
    for (const auto& control : std::vector<Control>{
        {"array_position(ARRAY[1,NULL,1,3],1)", "1", {}},
        {"array_position(ARRAY[1,NULL,1,3],1,2)", "3", {}},
        {"array_position(ARRAY[1,NULL,1,3],1,4)", std::nullopt, {}},
        {"array_position(ARRAY[1,NULL,1,3],NULL,1)", "2", {}},
        {"array_position(ARRAY[1,NULL,1,3],NULL,3)", std::nullopt, {}},
        {"array_position(ARRAY[NULL,NULL]::integer[],NULL,2)", "2", {}},
        {"array_position('[0:3]={1,NULL,1,3}'::integer[],1,0)", "0", {}},
        {"array_position('[0:3]={1,NULL,1,3}'::integer[],1,1)", "2", {}},
        {"array_position('[0:3]={1,NULL,1,3}'::integer[],NULL,0)", "1", {}},
        {"array_position('[-3:-1]={7,NULL,7}'::integer[],7,-99)", "-3", {}},
        {"array_position('[-3:-1]={7,NULL,7}'::integer[],7,-2)", "-1", {}},
        {"array_position('[-3:-1]={7,NULL,7}'::integer[],7,1)", std::nullopt, {}},
        {"array_position('[-3:-1]={7,NULL,7}'::integer[],NULL,-3)", "-2", {}},
        {"array_position('{\"NULL\",NULL,\"\"}'::text[],'NULL'::text)", "1", {}},
        {"array_position('{\"NULL\",NULL,\"\"}'::text[],NULL::text)", "2", {}},
        {"array_position('{\"NULL\",NULL,\"\"}'::text[],''::text)", "3", {}},
        {"array_position('{1,NULL,2}'::numeric[],2::numeric)", "3", {}},
        {"array_position('{1,NULL,2}'::numeric[],NULL::numeric)", "2", {}},
        {"array_position(ARRAY[1],1,-2147483648)", "1", {}},
        {"array_position(ARRAY[1],1,2147483647)", std::nullopt, {}},
        {"array_position(ARRAY[1,NULL,1],1,'2')", "3", {}},
        {"pg_catalog.array_position('{}'::integer[],1,NULL::integer)", std::nullopt, {}},
        {"array_position('{}'::integer[],1,'2147483648')", std::nullopt, "22003"},
        {"array_position(ARRAY[]::integer[],1,'-2147483649')", std::nullopt, "22003"},
        {"array_position('{}'::integer[],1,'bad'::integer)", std::nullopt, "22P02"},
        {"array_position(ARRAY[ARRAY[1,2]],1,NULL::integer)", std::nullopt, "0A000"},
    }) result.push_back(control);
    return result;
}
} // namespace

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string testName = "array_position_empty_null_start";
    const auto database = testDbPath(testName);
    cleanupTestDb(testName);
    if (g_engine.createDatabase(database) != DBStatus::OK) return 2;
    Session session; session.username = "testuser"; session.permission = 1; session.currentDB = database;
    auto* previous = currentSession(); setCurrentSession(&session);
    struct Restore {Session* value; ~Restore() {setCurrentSession(value);}} restore{previous};
    size_t checks = 0, failures = 0;
    const auto check = [&](bool good, const std::string& label) {
        ++checks;
        if (!good) {++failures; std::cerr << "ARRAY_POSITION_EMPTY_NULL_START_FAIL " << label << '\n';}
    };
    const auto controls = sqlControls();
    for (const auto& control : controls) for (const bool prepared : {false, true}) {
        const auto label = std::string(prepared ? "prepared " : "parsed native ") + control.expression;
        try {
            ExprValue actual;
            bool descriptor = true;
            if (prepared) {
                auto query = std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(
                    database, "SELECT " + control.expression));
                descriptor = query->output.size() == 1 && query->output[0].type == "integer" &&
                    query->output[0].typeOid == 23;
                auto* select = dynamic_cast<SelectStmt*>(query->ast.get());
                if (!select || select->selectList.size() != 1) return 2;
                PreparedQueryExecution execution(query, &g_engine, database);
                auto* expression = select->selectList.front().expr.get();
                execution.prepareExpression(expression);
                actual = execution.evaluate(expression, execution.context());
            } else {
                SQLParser parser; auto parsed = parser.parseForBinding("SELECT " + control.expression);
                auto* select = parsed.success ? dynamic_cast<SelectStmt*>(parsed.stmt.get()) : nullptr;
                if (!select || select->selectList.size() != 1) return 2;
                ExprEvaluator evaluator; evaluator.setCurrentDB(database);
                auto* expression = select->selectList.front().expr.get();
                // The native API requires array-context preparation before
                // evaluating an explicitly typed empty ARRAY constructor.
                ExprHelper::prepareArrayTypes(expression, {}, database, &g_engine);
                evaluator.bindScalarFunctions(expression, &g_engine);
                actual = evaluator.eval(expression, RowContext{});
            }
            check(control.state.empty() && descriptor && actual.typeName == "integer" &&
                actual.isNull == !control.value && (!control.value || actual.value == *control.value), label);
        } catch (const std::exception& error) {
            check(!control.state.empty() && legacyState(error) == control.state,
                label + " SQLSTATE=" + legacyState(error) + " " + error.what());
        }
    }

    // Parameter-backed cells preserve the actual public native datum domain,
    // rather than reconstructing typed NULLs as untyped literal sentinels.
    ExprEvaluator builtin;
    const auto native = [&](const std::string& label, const std::vector<ExprValue>& cells,
                            const std::string& state = {}) {
        auto call = cellCall(cells); RowContext row; row.setParameters(cells);
        try {
            const auto actual = builtin.eval(&call, row);
            check(state.empty() && actual.typeName == "integer" && actual.isNull, "native cells " + label);
        } catch (const std::exception& error) {
            check(!state.empty() && legacyState(error) == state,
                "native cells " + label + " SQLSTATE=" + legacyState(error));
        }
    };
    for (const std::string type : {"integer", "text", "numeric"}) {
        const auto needle = type == "text" ? "needle" : "1";
        const auto populated = type == "text" ? "{needle,NULL,needle}" : "{1,NULL,1}";
        for (const bool nullNeedle : {false, true}) {
            const ExprValue search(type, nullNeedle ? "" : needle, nullNeedle);
            native(type + " empty/null start", {{type + "[]", "{}"}, search, {"integer", "", true}});
            native(type + " populated/null start", {{type + "[]", populated}, search, {"integer", "", true}}, "22004");
            native(type + " NULL array/null start", {{type + "[]", "", true}, search, {"integer", "", true}});
        }
        native(type + " empty/bad start", {{type + "[]", "{}"}, {type, needle}, {"text", "bad"}}, "22P02");
        native(type + " empty/valid start", {{type + "[]", "{}"}, {type, needle}, {"integer", "1"}});
        native(type + " empty/no start", {{type + "[]", "{}"}, {type, needle}});
    }

    // A public replacement is opaque: do not force builtin empty/NULL/type
    // semantics on it. Both overloads, including explicit immutable, retain
    // every input cell and arbitrary scalar result type without upfront calls.
    const std::vector<std::vector<ExprValue>> overrideInputs = {
        {{"integer[]", "{}"}, {"integer", "", true}, {"integer", "", true}},
        {{"integer[]", "", true}, {"integer", "1"}, {"integer", "", true}},
        {{"integer[]", "{}"}, {"integer", "1"}, {"text", "bad"}},
        {{"integer[]", "{1,NULL}"}, {"integer", "", true}, {"integer", "", true}},
        {},
    };
    for (const auto& context : std::vector<std::string>{"", database})
        for (const bool explicitVolatility : {false, true})
            for (const auto& cells : overrideInputs) {
                session.currentDB = context;
                ExprEvaluator evaluator; evaluator.setCurrentDB(context);
                size_t calls = 0; bool sameInputs = true;
                const auto callback = [&](const std::vector<ExprValue>& actual) {
                    ++calls; sameInputs = sameInputs && actual.size() == cells.size();
                    for (size_t i = 0; i < actual.size() && i < cells.size(); ++i)
                        sameInputs = sameInputs && actual[i].typeName == cells[i].typeName &&
                            actual[i].value == cells[i].value && actual[i].isNull == cells[i].isNull;
                    return ExprValue("text", "owned");
                };
                if (explicitVolatility) evaluator.registerFunction("array_position", callback, 'i');
                else evaluator.registerFunction("array_position", callback);
                auto call = cellCall(cells); RowContext row; row.setParameters(cells);
                try {
                    const bool pure = evaluator.hasScalarFunction(&call, &g_engine) &&
                        evaluator.scalarFunctionResultType(&call, &g_engine).empty() &&
                        evaluator.scalarFunctionVolatility(&call, &g_engine) == (explicitVolatility ? 'i' : 'v') &&
                        calls == 0;
                    evaluator.bindScalarFunctions(&call, &g_engine);
                    const bool pureBind = calls == 0 && evaluator.scalarFunctionResultType(&call, &g_engine).empty();
                    const auto actual = evaluator.eval(&call, row);
                    check(pure && pureBind && calls == 1 && sameInputs && actual.typeName == "text" &&
                        actual.value == "owned" && !actual.isNull, "opaque native ownership");
                } catch (const std::exception& error) {check(false, "opaque native ownership " + std::string(error.what()));}
            }
    session.currentDB = database;
    check(controls.size() == 80 && checks == 207, "complete 80 SQL x2 +27 native +20 opaque matrix");
    std::cout << "ARRAY_POSITION_EMPTY_NULL_START_COMPLETE " << checks << " FAILURES " << failures << '\n';
    if (g_engine.dropDatabase(database) != DBStatus::OK) return 2;
    cleanupTestDb(testName);
    return failures ? 1 : 0;
}
