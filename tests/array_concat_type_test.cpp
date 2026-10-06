#include "parser/parser.h"
#include "expression/expr_helper.h"
#include "expression/ExprEvaluator.h"
#include "expression/prepared_query_execution.h"
#include "executor/ExecutionPlan.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    SQLParser parser;
    auto parsed=parser.parseForBinding("SELECT ARRAY[1,2] || ARRAY[3]");
    assert(parsed.success);
    auto* select=dynamic_cast<SelectStmt*>(parsed.stmt.get());
    auto* binary=dynamic_cast<BinaryOpExpr*>(select->selectList.front().expr.get());
    assert(binary && dynamic_cast<ArrayExpr*>(binary->left.get()) && dynamic_cast<ArrayExpr*>(binary->right.get()));
    ExprHelper::prepareArrayTypes(binary);
    assert(binary->arrayConcat && binary->arrayConcat->elementType=="integer");
    ExprEvaluator evaluator;
    const auto value=evaluator.eval(binary,RowContext{});
    assert(!value.isNull && value.typeName=="integer[]" && value.value=="{1,2,3}");
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
        {"ARRAY[1] || ARRAY[2::BIGINT]","bigint[]"},
        {"ARRAY[1] || ARRAY[2.5]","numeric[]"},
        {"ARRAY['NULL',NULL]","text[]"},
        {"NULL::INT[] || ARRAY[1]","integer[]"},
        {"ARRAY[]::INT[] || ARRAY[1]","integer[]"},
        {"ARRAY[x] || ARRAY[y]","bigint[]"}}) {
        const std::map<std::string,std::string> types={{"x","integer"},{"y","bigint"}};
        assert(ExprHelper::inferResultType(item.first,types)==item.second);
        const auto evaluated=ExprHelper::evalString(item.first,{{"x","1"},{"y","2147483648"}},types);
        assert(evaluated.ok && evaluated.typeName==item.second);
    }
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
        {"ARRAY[] || ARRAY[1]","42P18"},{"ARRAY[1] || ARRAY[TRUE]","42883"},
        {"ARRAY[1,'bad']","22P02"},{"ARRAY[1] || '2'","22P02"}}) {
        auto query=parser.parseForBinding("SELECT "+item.first);assert(query.success);
        auto* statement=dynamic_cast<SelectStmt*>(query.stmt.get());
        bool failed=false;
        try{ExprHelper::prepareArrayTypes(statement->selectList.front().expr.get());}
        catch(const DbError& error){failed=error.sqlState()==item.second;}
        assert(failed);
    }
    auto text=ExprHelper::evalString("'{1,2}'::TEXT || '{3,4}'::TEXT",{});
    assert(text.ok && text.typeName=="text" && text.value=="{1,2}{3,4}");
    StorageEngine owner;
    const std::string db=testDbPath("array_concat_type");
    assert(!owner.databaseExists(db) && owner.createDatabase(db,"utf8")==DBStatus::OK);
    TableSchema schema;schema.len=1;schema.cols[0].dataName="id";
    schema.cols[0].dataType="int";schema.cols[0].dsize=4;
    assert(owner.createTable(db,"sink",schema)==DBStatus::OK);
    assert(owner.createUDF(db,"array_writer",{"arg"},{"int"},
        "BEGIN INSERT INTO sink VALUES(arg); RETURN arg; END;",'v',"plpgsql","int")==DBStatus::OK);
    assert(owner.createUDF(db,"array_returner",{}, {},
        "BEGIN RETURN ARRAY[1,2]; END;",'v',"plpgsql","int[]")==DBStatus::OK);
    const auto sinkRows=[&]{size_t rows=0;assert(owner.forEachRow(db,"sink",[&](uint32_t,uint16_t,const char*,size_t){++rows;}));return rows;};
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
        {"SELECT ARRAY[array_writer(1)] || ARRAY[TRUE]","42883"},
        {"SELECT ARRAY[array_writer(1),'bad']","22P02"},
        {"SELECT ARRAY[array_writer(1)] || 'bad'","22P02"}}) {
        bool rejected=false;
        try{(void)owner.prepareBoundQuery(db,item.first);}
        catch(const DbError& error){rejected=error.sqlState()==item.second;}
        assert(rejected && !owner.inTransaction() && sinkRows()==0);
    }
    auto good=QueryPlanner::buildPreparedSelectPlan(&owner,db,"",
        owner.prepareBoundQuery(db,"SELECT ARRAY[array_writer(1)] || ARRAY[array_writer(2)]"));
    assert(!owner.inTransaction() && sinkRows()==0);
    assert(owner.beginTransaction(db)==DBStatus::OK && owner.beginSqlCommand());
    assert(good->open());std::string displayed;assert(good->next(displayed));
    std::vector<ExprValue> structured;
    assert(good->lastStructuredValues(structured) && structured.size()==1 &&
        structured[0].typeName=="integer[]" && structured[0].value=="{1,2}" && !structured[0].isNull);
    assert(!good->next(displayed));good->close();
    assert(owner.finishSqlCommand() && owner.beginSqlCommand());
    assert(sinkRows()==2);
    assert(owner.rollbackTransaction()==DBStatus::OK && sinkRows()==0);
    auto nested=QueryPlanner::buildPreparedSelectPlan(&owner,db,"",
        owner.prepareBoundQuery(db,"SELECT ARRAY[array_returner()] || ARRAY[ARRAY[3,4]]"));
    assert(nested->open() && nested->next(displayed));
    assert(nested->lastStructuredValues(structured) && structured[0].typeName=="integer[]" && structured[0].value=="{{1,2},{3,4}}");
    assert(!nested->next(displayed));nested->close();
    auto poly=owner.prepareBoundQuery(db,"SELECT array_cat(ARRAY[1],ARRAY[2])");
    const auto descriptor=poly.output;
    auto cursor=QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedSelectPlan(&owner,db,"",std::move(poly)),descriptor);
    assert(cursor->next(structured) && structured[0].typeName=="integer[]" && structured[0].value=="{1,2}");
    assert(!cursor->next(structured));cursor->close();
    assert(owner.dropDatabase(db)==DBStatus::OK);
    std::cout<<"[ARRAY CONCAT TYPE] passed\n";
}
