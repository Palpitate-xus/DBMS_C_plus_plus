#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "executor/ExecutionPlan.h"
#include "parser/parser.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    SQLParser parser;
    ExprEvaluator evaluator;
    for (const auto& control : std::vector<std::pair<std::string,std::string>>{
        {"B'001' < B'01'", "t"}, {"B'1' = B'01'", "f"},
        {"B'1' <> B'01'", "t"}, {"B'00' > B'0'", "t"},
        {"B'' < B'0'", "t"}, {"B'001' <= B'01'", "t"},
        {"B'001' >= B'01'", "f"}, {"B'1' IS DISTINCT FROM B'01'", "t"},
        {"B'001'::varbit < B'01'", "t"},
        {"CASE B'1' WHEN B'01' THEN 1 ELSE 2 END", "2"},
        {"B'11111111111111111111111111111111111111111111111111111111111111111' "
         "< B'111111111111111111111111111111111111111111111111111111111111111111'", "t"}}) {
        auto parsed = parser.parse("SELECT " + control.first);
        assert(parsed.isValid());
        const auto* select = static_cast<SelectStmt*>(parsed.stmt.get());
        const auto value = evaluator.eval(select->selectList.front().expr.get(), {});
        assert(!value.isNull && value.value == control.second);
    }
    for (const auto& types : std::vector<std::pair<std::string,std::string>>{
            {"bit","bit"}, {"bit varying","bit varying"}, {"bit","bit varying"}}) {
        const auto equality = ExprEvaluator::resolveComparison("=", types.first, types.second);
        assert(equality.strict && equality.hashable && !equality.identity.empty());
        const auto first = evaluator.coerceComparison(equality, ExprValue(types.first,"1"), true);
        const auto second = evaluator.coerceComparison(equality, ExprValue(types.second,"01"), false);
        assert(!evaluator.comparePrepared(equality, first, second).asBool());
        assert(ExprEvaluator::comparisonHashKey(equality, first, true) !=
               ExprEvaluator::comparisonHashKey(equality, second, false));
        assert(evaluator.comparePrepared(equality, first,
            ExprValue(equality.rightType,"",true)).isNull);
        const auto ordered = ExprEvaluator::resolveComparison("<", types.first, types.second);
        assert(!ordered.hashable);
        assert(evaluator.comparePrepared(ordered, ExprValue(types.first,"001"),
               ExprValue(types.second,"01")).asBool());
    }
    StorageEngine owner;
    Column bitColumn;
    bitColumn.dataType="bit varying";
    bitColumn.isVariableLength=true;
    bitColumn.dsize=8388608;
    assert(StorageEngine::compareValues(bitColumn,"001",false,"01",false,"<")==StorageEngine::PredicateTruth::True);
    assert(StorageEngine::compareValues(bitColumn,"1",false,"01",false,"=")==StorageEngine::PredicateTruth::False);
    assert(StorageEngine::compareValues(bitColumn,"",false,"",false,"=")==StorageEngine::PredicateTruth::True);
    assert(StorageEngine::compareValues(bitColumn,"",true,"",false,"=")==StorageEngine::PredicateTruth::Unknown);
    for (const auto& control : std::vector<std::pair<std::string,std::string>>{
        {"SELECT B'1'=ANY(ARRAY[B'01'])", "f"},
        {"SELECT B'001'<ANY(ARRAY[B'01'])", "t"},
        {"SELECT B'1'=ANY(SELECT B'01')", "f"},
        {"SELECT B'001'<ALL(SELECT B'01')", "t"}}) {
        auto prepared = std::make_shared<PreparedQuery>(prepareQuery(control.first, {}, {}));
        auto plan = QueryPlanner::buildPreparedQueryPlan(&owner,"unused",prepared,prepared->ast.get());
        auto cursor = QueryPlanner::makePreparedCursor(std::move(plan),prepared->output);
        std::vector<ExprValue> row;
        const bool obtained = cursor->next(row);
        std::cerr << "[BIT prepared] " << control.first << " obtained=" << obtained
                  << " cells=" << row.size() << " actual=" << (row.empty()?"<none>":row.front().value)
                  << " expected=" << control.second << '\n';
        assert(obtained && row.size()==1 && row.front().typeName=="boolean" &&
               !row.front().isNull && row.front().value==control.second);
        assert(!cursor->next(row));
        cursor->close();
    }
    std::cout << "[BIT COMPARISON VALUE] ordered bits, lengths, NULLs and prepared hash operators passed\n";
}
