#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "expression/ExprEvaluator.h"
#include "parser/query_binding.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap(); StorageEngine engine;
    struct Boundary {const char* type; const char* minimum; const char* safe; const char* positive;};
    const Boundary boundaries[] = {
        {"smallint","-32768","-32767","32767"},
        {"integer","-2147483648","-2147483647","2147483647"},
        {"bigint","-9223372036854775808","-9223372036854775807","9223372036854775807"}
    };
    size_t failures = 0;
    const auto require = [&](bool condition, const std::string& label) {
        if (!condition) {++failures; std::cerr << "UNARY_STATE_FAILURE " << label << '\n';}
    };
    for (const auto& boundary : boundaries) {
        ColumnRefExpr input; input.column = "v";
        UnaryOpExpr minus; minus.op = "-";
        minus.operand = std::make_unique<ColumnRefExpr>(input);
        RowContext row; row.set("v",ExprValue(boundary.type,boundary.minimum));
        ExprEvaluator evaluator;
        bool structured = false;
        try {(void)evaluator.eval(&minus,row);}
        catch (const DbError& error) {structured = error.sqlState() == "22003";}
        catch (const std::exception& error) {std::cerr << boundary.type << " raw evaluator exception: " << error.what() << '\n';}
        require(structured,std::string(boundary.type)+" evaluator creation-site SQLSTATE");

        QueryBindingDatum datum; datum.identity = "unary:v"; datum.name = "v";
        datum.type = boundary.type; datum.value = boundary.minimum;
        auto query = prepareQuery("SELECT -v AS result",{datum},{});
        structured = false;
        try {
            auto result = QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedSelectPlan(
                &engine,"","",std::move(query)));
            require(!result.ok && result.rows.empty() && result.structuredRows.empty() &&
                result.structuredNulls.empty(),std::string(boundary.type)+" checked plan has no partial results");
            try {result.throwIfFailed();}
            catch (const DbError& error) {structured = error.sqlState() == "22003";}
        } catch (const std::exception& error) {
            std::cerr << boundary.type << " raw checked-plan exception: " << error.what() << '\n';
        }
        require(structured,std::string(boundary.type)+" checked-plan typed SQL error");

        row.set("v",ExprValue(boundary.type,"",true));
        auto value = evaluator.eval(&minus,row);
        require(value.isNull && value.typeName == boundary.type,std::string(boundary.type)+" typed NULL");
        if (boundary.safe) {
            row.set("v",ExprValue(boundary.type,boundary.safe));
            value = evaluator.eval(&minus,row);
            require(!value.isNull && value.typeName == boundary.type && value.value == boundary.positive,
                std::string(boundary.type)+" safe adjacent boundary");
        }
    }
    // The generic evaluator has an existing MONEY API branch, but PG18
    // deliberately has no SQL unary-minus MONEY operator. Exercise only
    // that lower-level branch's error class, not nonexistent SQL support.
    UnaryOpExpr moneyMinus; moneyMinus.op = "-";
    auto moneyInput = std::make_unique<ColumnRefExpr>(); moneyInput->column = "money_input";
    moneyMinus.operand = std::move(moneyInput);
    RowContext moneyRow; moneyRow.set("money_input",ExprValue("money","-92233720368547758.08"));
    bool structuredMoney = false;
    try { ExprEvaluator evaluator; (void)evaluator.eval(&moneyMinus,moneyRow); }
    catch (const DbError& error) { structuredMoney = error.sqlState() == "22003"; }
    catch (const std::exception&) {}
    require(structuredMoney,"generic MONEY evaluator API creation-site SQLSTATE");
    assert(failures == 0);
    std::cout << "[UNARY OVERFLOW SQLSTATE] creation-site metadata, checked errors and NULL/boundaries passed\n";
}
