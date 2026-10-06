#include "executor/ExecutionPlan.h"
#include "expression/prepared_query_execution.h"
#include "parser/query_binding.h"
#include "common/DbError.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    QueryBindingMetadata metadata;
    metadata.relation = [](const std::string& name) {
        if (name == "view_rows")
            return QueryRelationMetadata{"public",name,{},"SELECT id,\"V\",v FROM base_rows"};
        if (name == "bad_view")
            return QueryRelationMetadata{"public",name,{},"SELECT local_value"};
        if (name == "unknown_view")
            return QueryRelationMetadata{"public",name,{},"SELECT NULL AS n,'' AS e"};
        return QueryRelationMetadata{"public",name,{{"id","integer"},{"V","bigint"},{"v","integer"}}, {}};
    };
    // View preparation must not accidentally turn the defining query's
    // column into a same-named datum from the caller's procedural namespace.
    std::vector<QueryBindingDatum> datums = {
        {"local:id","id","integer",{},true,std::string("99"),0},
        {"local:value","local_value","integer",{},true,std::string("17"),0}};
    auto view = prepareQuery("SELECT view_rows.id FROM view_rows",datums,metadata);
    assert(view.sourceRanges.size() == 1 && view.sourceRanges.front().viewQuery);
    const auto& child = *view.sourceRanges.front().viewQuery;
    assert(child.parameters.empty());
    const auto* viewSelect = dynamic_cast<const SelectStmt*>(child.ast.get());
    const auto* column = dynamic_cast<const ColumnRefExpr*>(viewSelect->selectList.front().expr.get());
    assert(column && column->binding && column->binding->declaredType == "integer");
    bool rejected = false;
    try { (void)prepareQuery("SELECT * FROM bad_view",datums,metadata); }
    catch (const DbError& error) { rejected = error.sqlState() == "42703"; }
    assert(rejected);
    view = prepareQuery("SELECT n,e FROM unknown_view",{},metadata);
    assert(view.output[0].type == "text" && view.output[1].type == "text");
    assert(view.sourceRanges.front().viewQuery->output[0].type == "text");
    viewSelect = dynamic_cast<const SelectStmt*>(view.sourceRanges.front().viewQuery->ast.get());
    assert(dynamic_cast<const CastExpr*>(viewSelect->selectList[0].expr.get()));
    assert(dynamic_cast<const CastExpr*>(viewSelect->selectList[1].expr.get()));

    auto query = std::make_shared<PreparedQuery>(prepareQuery(
        "SELECT a.\"V\",b.v,CASE a.\"V\" WHEN CAST(9007199254740992 AS DOUBLE PRECISION) "
        "THEN 1 ELSE 2 END FROM base_rows a JOIN base_rows b ON a.id=b.id ORDER BY b.v DESC",{},metadata));
    auto* select = dynamic_cast<SelectStmt*>(query->ast.get());
    assert(select && QueryPlanner::supportsPreparedSourceSelectPlan(*select));
    size_t a = 0, b = 0;
    for (const auto& range : query->sourceRanges) {
        if (range.name == "a") a = range.ordinal;
        if (range.name == "b") b = range.ordinal;
    }
    assert(a != b);
    StorageEngine engine;
    PreparedQueryExecution binding(query,&engine,"");
    size_t calls = 0;
    auto source = std::make_unique<PreparedSourceContextsOp>(
        [&](size_t index,RowContext& row) {
            ++calls;
            if (index == 2) return false;
            row = RowContext{};
            binding.setSourceRow(row,a,{ExprValue("integer",std::to_string(index+1)),
                ExprValue("bigint",index ? "" : "9007199254740993",index!=0),ExprValue("integer","5")});
            binding.setSourceRow(row,b,{ExprValue("integer",std::to_string(index+1)),
                ExprValue("bigint","2147483648"),ExprValue("integer",index ? "2" : "1")});
            return true;
        });
    const auto result = QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedSelectPlan(
        &engine,"",query,select,TableSchema{},std::move(source)));
    result.throwIfFailed();
    assert(result.structuredRowsAvailable && calls == 3);
    assert((result.structuredRows == std::vector<std::vector<std::string>>{
        {"","2","2"},{"9007199254740993","1","1"}}));
    assert((result.structuredNulls == std::vector<std::vector<bool>>{
        {true,false,false},{false,false,false}}));
    std::cout << "prepared source contexts preserve source identity, NULL, width and view isolation\n";
}
