#include "executor/ExecutionPlan.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine owner;
    QueryBindingMetadata metadata;
    metadata.relation = [](const std::string&) {
        return QueryRelationMetadata{"pg_temp","t",{{"id","integer"},{"ID","text"}}, {}};
    };
    auto query = std::make_shared<PreparedQuery>(prepareQuery(
        "SELECT pg_temp.t.* FROM pg_temp.t WHERE TRUE ORDER BY id", {}, metadata));
    auto* select = static_cast<SelectStmt*>(query->ast.get());
    assert(query->sourceRanges.size() == 1);
    const auto sourceId = query->sourceRanges.front().ordinal;
    // The pure metadata owner has canonicalized the namespace alias. Runtime
    // star expansion must consume the resolved output bindings, not repeat a
    // raw qualifier lookup against this canonical range spelling.
    query->sourceRanges.front().schema = "pg_temp_42";
    query->sourceRanges.front().relationSchema = "pg_temp_42";
    auto original = select->selectList.front().expr.get();
    size_t reads = 0, closes = 0;
    auto source = std::make_unique<PreparedSourceContextsOp>(
        [&](size_t at, RowContext& row) {
            ++reads;
            if (at == 3) return false;
            row.setBoundColumn(sourceId,0,ExprValue("integer",std::to_string(at+1),false));
            row.setBoundColumn(sourceId,1,ExprValue("text",at==1?"NULL":"",at==0));
            return true;
        }, [&]() { ++closes; });
    auto result = QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedSelectPlan(
        &owner,"unused",query,select,TableSchema{},std::move(source)));
    result.throwIfFailed();
    assert(result.ok && result.structuredRows.size() == 3);
    for (size_t i=0;i<result.structuredRows.size();++i) {
        std::cerr << "STAR_ROW_WIDTH " << result.structuredRows[i].size() << '\n';
        assert(result.structuredRows[i].size() == 2 && result.structuredNulls[i].size() == 2);
    }
    assert(result.structuredNulls[0][1] && !result.structuredNulls[1][1] && !result.structuredNulls[2][1]);
    assert(result.structuredRows[1][1] == "NULL" && result.structuredRows[2][1].empty());
    assert(reads == 4 && closes == 1 && !owner.inTransaction());
    assert(select->selectList.size() == 1 && select->selectList.front().expr.get() == original);
    std::cout << "[PREPARED STAR BINDING IDENTITY] passed\n";
}
