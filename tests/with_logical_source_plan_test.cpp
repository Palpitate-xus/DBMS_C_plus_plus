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
    metadata.relation=[](const std::string&) {
        return QueryRelationMetadata{"public","target",{{"a","integer"},{"b","integer"}}, {}};
    };
    auto query=std::make_shared<PreparedQuery>(prepareQuery(
        "WITH \"Same\" AS(SELECT 1 AS id,2 AS id) INSERT INTO target SELECT \"Same\".* FROM \"Same\"",{},metadata));
    auto* envelope=dynamic_cast<WithStmt*>(query->ast.get());
    auto* insert=dynamic_cast<InsertStmt*>(envelope->statement.get());
    auto* select=dynamic_cast<SelectStmt*>(insert->selectSource.get());
    assert(query->statementOutputs.at(select).size()==2);
    const auto source=std::find_if(query->sourceRanges.begin(),query->sourceRanges.end(),
        [&](const auto& range){return range.owner==select;});
    assert(source!=query->sourceRanges.end() && source->cteStatement==envelope->ctes.front().query.get());
    assert(source->columns[0].name=="id" && source->columns[1].name=="id");
    TableSchema schema;
    for(const auto& column:source->columns) {
        Column value;value.dataName=column.name;
        assert(TypeRegistry::instance().resolveColumnType(value,column.type,{},false).empty());
        schema.append(value);
    }
    const auto originalTarget=select->selectList.front().expr.get();
    size_t reads=0;
    auto logical=std::make_unique<PreparedSourceRowsOp>(source->columns,[&](size_t at,std::vector<ExprValue>& cells){
        ++reads;if(at) return false;
        cells={ExprValue("integer","1",false),ExprValue("integer","2",false)};return true;
    });
    auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedSelectPlan(
        &owner,"unused",query,select,schema,std::move(logical)));
    assert(result.ok && (result.structuredRows==std::vector<std::vector<std::string>>{{"1","2"}}));
    assert(select->selectList.size()==1 && select->selectList.front().expr.get()==originalTarget);
    assert(reads==2 && !owner.inTransaction());
    // Text boundaries and SQL NULL survive a typed filter/sort. The source
    // provider is first touched by next(), not metadata or open(). LIMIT 0
    // never starts a read CTE producer.
    for(const bool zero:{false,true}) {
        auto value=std::make_shared<PreparedQuery>(prepareQuery(std::string(
            "WITH c AS(SELECT NULL::TEXT AS v) INSERT INTO target(a) SELECT c.v FROM c WHERE true ORDER BY c.v NULLS FIRST")+
            (zero?" LIMIT 0":""),{},metadata));
        auto* with=static_cast<WithStmt*>(value->ast.get());
        auto* projection=static_cast<SelectStmt*>(static_cast<InsertStmt*>(with->statement.get())->selectSource.get());
        const auto& range=*std::find_if(value->sourceRanges.begin(),value->sourceRanges.end(),
            [&](const auto& item){return item.owner==projection;});
        TableSchema textSchema;Column text;text.dataName="v";
        assert(TypeRegistry::instance().resolveColumnType(text,"text",{},false).empty());textSchema.append(text);
        std::vector<ExprValue> values={ExprValue("text","",true),ExprValue("text","",false),ExprValue("text","NULL",false)};
        reads=0;
        auto rows=std::make_unique<PreparedSourceRowsOp>(range.columns,[&](size_t at,std::vector<ExprValue>& cells){
            ++reads;if(at>=values.size())return false;cells={values[at]};return true;
        });
        auto output=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedSelectPlan(
            &owner,"unused",value,projection,textSchema,std::move(rows)));
        assert(output.ok);
        if(zero)assert(reads==0 && output.structuredRows.empty());
        else {
            assert(output.structuredRows.size()==3 && output.structuredNulls[0][0]);
            assert(output.structuredRows[1][0].empty() && !output.structuredNulls[1][0]);
            assert(output.structuredRows[2][0]=="NULL" && !output.structuredNulls[2][0]);
        }
    }
    // A whole-statement child executor receives the genuine child AST and
    // nullable ancestor cells. It never reparses fake PL variables or merges
    // separate SELECT sites by SQL spelling/value.
    for(const bool correlated:{false,true}) {
        auto prepared=std::make_shared<PreparedQuery>(prepareQuery(std::string(
            "WITH c AS(SELECT 1 AS id) INSERT INTO target SELECT ")+
            (correlated?"(SELECT c.id),(SELECT c.id)":"(SELECT 1),(SELECT 1)")+" FROM c",{},metadata));
        auto* with=static_cast<WithStmt*>(prepared->ast.get());
        auto* child=static_cast<SelectStmt*>(static_cast<InsertStmt*>(with->statement.get())->selectSource.get());
        const auto& range=*std::find_if(prepared->sourceRanges.begin(),prepared->sourceRanges.end(),
            [&](const auto& item){return item.owner==child;});
        TableSchema input;Column id;id.dataName="id";
        assert(TypeRegistry::instance().resolveColumnType(id,"integer",{},false).empty());input.append(id);
        auto source=std::make_unique<PreparedSourceRowsOp>(range.columns,[](size_t at,std::vector<ExprValue>& cells){
            if(at>=3)return false;cells={ExprValue("integer",std::to_string(at+1),false)};return true;
        });
        size_t calls=0;std::set<const Stmt*> sites;
        PreparedChildExecutor executor=[&](const Stmt* original,const RowContext& row,size_t demand){
            assert(demand==2 && prepared->statementOutputs.count(original));++calls;sites.insert(original);
            return PreparedQueryRows{{correlated?row.boundColumn(range.ordinal,0):ExprValue("integer","1",false)}};
        };
        auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedSelectPlan(
            &owner,"unused",prepared,child,input,std::move(source),{},executor));
        assert(result.ok && result.structuredRows.size()==3 && sites.size()==2);
        assert(calls==(correlated?6:2));
        if(correlated)assert((result.structuredRows.back()==std::vector<std::string>{"3","3"}));
    }
    std::cout<<"[WITH LOGICAL SOURCE PLAN] passed\n";
}
