#include "commands/TableManage.h"
#include "expression/expr_helper.h"
#include "expression/prepared_query_execution.h"
#include "parser/parser.h"
#include "catalog/type_registry.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    SQLParser parser;
    for(const auto& expression:std::vector<std::string>{
        "2 BETWEEN 1 AND 3", "2 NOT BETWEEN 1 AND 3",
        "'b' BETWEEN 'a' AND 'c'", "NULL::INT BETWEEN 1 AND 3",
        "2 BETWEEN NULL AND 1", "2 NOT BETWEEN NULL AND 1",
        "9007199254740993::BIGINT BETWEEN 9007199254740993 AND 9007199254740993"}) {
        auto parsed=parser.parse("SELECT "+expression);
        const auto* select=parsed.isValid()?dynamic_cast<const SelectStmt*>(parsed.stmt.get()):nullptr;
        assert(select && select->selectList.size()==1);
        const auto* range=dynamic_cast<const FunctionCallExpr*>(select->selectList.front().expr.get());
        assert(range && range->schema.empty() && range->args.size()==3);
        const auto actual=ExprHelper::inferParsedResultType(range);
        std::cout<<"BETWEEN_GRAMMAR_TYPE "<<expression<<" actual="<<actual<<std::endl;
        assert(actual=="boolean");
        const auto value=ExprEvaluator{}.eval(range,{});
        assert(value.typeName=="boolean");
    }
    const auto db=testDbPath("between_predicate_type");
    assert(g_engine.createDatabase(db)==DBStatus::OK);
    TableSchema table;table.tablename="ranges";
    table.append(makeIntColumn("id",false,2));
    table.append(makeTextColumn("name",false));
    assert(g_engine.createTable(db,table)==DBStatus::OK);
    assert(g_engine.createSequence(db,"range_effects")==DBStatus::OK);
    for(const auto& predicate:std::vector<std::string>{
        "id BETWEEN 1 AND 3", "id NOT BETWEEN 1 AND 3",
        "name BETWEEN 'a' AND 'c'", "name NOT BETWEEN 'a' AND 'c'",
        "id BETWEEN NULL AND 3", "id NOT BETWEEN 1 AND NULL"}) {
        auto prepared=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,
            "SELECT "+predicate+" AS predicate FROM ranges"));
        assert(prepared->output.size()==1 && prepared->output.front().type=="boolean");
        auto* select=dynamic_cast<SelectStmt*>(prepared->ast.get());assert(select);
        assert(ExprHelper::canonicalResultTypeName(prepared->sourceRanges.front().columns.front().type)=="integer");
        const auto* range=dynamic_cast<const FunctionCallExpr*>(select->selectList.front().expr.get());
        assert(range && range->resolvedResultType=="boolean");
        PreparedQueryExecution execution(prepared,&g_engine,db);
        execution.prepareExpression(select->selectList.front().expr.get());
        auto row=execution.context();
        execution.setSourceRow(row,prepared->sourceRanges.front().ordinal,
            {ExprValue("integer","2",false),ExprValue("text","b",false)});
        assert(execution.evaluate(select->selectList.front().expr.get(),row).typeName=="boolean");
        for(const auto& sql:std::vector<std::string>{
            "UPDATE ranges SET id=nextval('range_effects') WHERE "+predicate,
            "DELETE FROM ranges WHERE "+predicate}) {
            const auto bound=g_engine.prepareBoundQuery(db,sql);
            const Expr* where=nullptr;
            if(const auto* update=dynamic_cast<const UpdateStmt*>(bound.ast.get()))where=update->whereClause.get();
            if(const auto* remove=dynamic_cast<const DeleteStmt*>(bound.ast.get()))where=remove->whereClause.get();
            assert(where && ExprHelper::inferParsedResultType(where,{},db,&g_engine)=="boolean");
        }
    }
    assert(g_engine.nextval(db,"range_effects")==1);
    std::cout<<"[BETWEEN PREDICATE TYPE] actual parser nodes, scalar/bound descriptors, empty UPDATE/DELETE binding and no preparation effects passed\n";
}
