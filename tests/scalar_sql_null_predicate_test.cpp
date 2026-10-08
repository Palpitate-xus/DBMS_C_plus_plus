#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database=testDbPath("scalar_sql_null_predicate");
    assert(g_engine.createDatabase(database)==DBStatus::OK);
    TableSchema table;table.tablename="rows";
    for(const auto& specification:std::vector<std::pair<std::string,std::string>>{
            {"id","integer"},{"i","integer"},{"n","bigint"},{"v","varbit"},{"t","text"},{"a","text"}}) {
        Column column;column.dataName=specification.first;column.isNull=column.dataName!="id";column.isPrimaryKey=column.dataName=="id";
        assert(TypeRegistry::instance().resolveColumnType(column,specification.second,{},false).empty());table.append(column);
    }
    assert(table.cols[0].dataType=="integer" && table.cols[0].dsize==4);
    assert(g_engine.createTable(database,table)==DBStatus::OK);
    table.tablename="empty_rows";assert(g_engine.createTable(database,table)==DBStatus::OK);
    assert(g_engine.insertRow(database,"rows",{{"id","1"},{"i","1"},{"n","9007199254740993"},{"v","01"},{"t","NULL"},{"a","NULL"}})==DBStatus::OK);
    assert(g_engine.insertRow(database,"rows",{{"id","2"},{"i","2"},{"n","2"},{"v",""},{"t",""},{"a",""}})==DBStatus::OK);
    assert(g_engine.insertRow(database,"rows",{{"id","3"},{"i",std::nullopt},{"n",std::nullopt},{"v",std::nullopt},{"t",std::nullopt},{"a",std::nullopt}})==DBStatus::OK);
    assert(g_engine.createIndex(database,"rows","v")==DBStatus::OK);
    assert(g_engine.createIndex(database,"rows","a")==DBStatus::OK);
    size_t controls=0,failures=0;
    const auto require=[&](bool okay,const std::string& label) {
        ++controls;if(!okay){++failures;std::cerr<<"SCALAR_SQL_NULL_FAILURE "<<label<<'\n';}
    };
    const auto sqlQuery=[&](const std::string& name,const std::vector<std::string>& conditions) {
        return g_engine.query(database,name,conditions,{"id"},{},false,false,false,0,{},nullptr,nullptr,nullptr);
    };
    for(const auto& name:{"rows","empty_rows"})
        for(const auto& column:{"id","i","n","v","t","a"})
            for(const auto& operation:{"=","<>","!=","<",">","<=",">="}) {
                const auto condition=std::string(operation)+column+" NULL";
                const auto parsed=StorageEngine::parseConditions({condition});
                require(parsed.size()==1 && parsed[0].patternIsNull && parsed[0].decodedLiteralRhs,
                        condition+" genuine nullable SQL carrier");
                require(sqlQuery(name,{condition}).empty(),condition+" actual SQL compact consumer");
                PlanContext context;context.dbname=database;context.tablename=name;context.selectCols={"id"};context.conds=parsed;
                auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildSelectPlan(&g_engine,context));
                require(result.ok && result.rows.empty(),condition+" public primary/secondary/heap consumer");
                require(context.conds[0].patternIsNull && context.conds[0].patternType=="unknown",
                        condition+" caller NULL/type ownership unchanged");
            }
    for(const auto& condition:{"=a 'NULL'","apicond =a NULL"}) {
        const auto parsed=StorageEngine::parseConditions({condition});
        require(parsed.size()==1 && !parsed[0].patternIsNull,"quoted/API text NULL is data");
        require(sqlQuery("rows",{condition})==std::vector<std::string>{"1 "},"real text NULL data matches indexed row");
    }
    require(sqlQuery("rows",{"=a ''"})==std::vector<std::string>{"2 "},"real empty text is data");
    require(g_engine.query(database,"rows",{"=a NULL"},{"id"})==std::vector<std::string>{"1 "},
            "native data overload deliberately keeps unquoted NULL as text data");
    for(const auto& predicate:{"NOT(v=NULL)","CASE WHEN v=NULL THEN TRUE ELSE FALSE END",
                              "COALESCE(v=NULL,FALSE)","(v=NULL) AND (id=1 OR id=2)"}) {
        require(sqlQuery("rows",{std::string("typedexpr ")+predicate}).empty(),
                std::string(predicate)+" keeps NULL value/NOT/CASE/function roles");
    }
    for(const auto& name:{"rows","empty_rows"}) {
        std::string state;
        try{(void)sqlQuery(name,{"=v NULL","=v 'xg'"});}
        catch(const DbError& error){state=error.sqlState();}
        require(state=="22P02","all inputs admitted before known NULL empty demand");
        PlanContext context;context.dbname=database;context.tablename=name;context.selectCols={"id"};context.orderByCol="id";
        const auto branches=std::vector<std::vector<StorageEngine::Condition>>{
            StorageEngine::parseConditions({"=v NULL"}),StorageEngine::parseConditions({"=id 1"})};
        auto plan=QueryPlanner::buildDisjunctiveSelectPlan(&g_engine,context,branches);
        require(bool(plan),"real OR retains indexed non-NULL branch");
        bool matched=false;
        if(plan){const auto result=QueryPlanner::executePlanChecked(std::move(plan));
            matched=result.ok && result.rows==(std::string(name)=="rows"?std::vector<std::string>{"1 "}:std::vector<std::string>{});}
        require(matched,"actual OR rows with NULL branch excluded");
        for(const auto& condition:{"=missing NULL","typedexpr missing=NULL",
                                  "typedexpr NULL=missing","typedexpr (missing=NULL) OR id=1"}) {
            std::string error;
            try{(void)sqlQuery(name,{condition});}catch(const DbError& failure){error=failure.sqlState();}
            require(error=="42703","SQL compact nullable condition admits real column owner before empty scan");
            error.clear();StorageEngine::SelectExpr projection;projection.colName="id";
            StorageEngine::QueryExprExecutionOptions options;options.maxProjectionRows=0;
            try{(void)g_engine.queryExpr(database,name,{condition},{projection},{},options);}
            catch(const DbError& failure){error=failure.sqlState();}
            require(error=="42703","queryExpr cap0 retains missing-column admission");
            error.clear();PlanContext invalid;invalid.dbname=database;invalid.tablename=name;
            invalid.selectCols={"id"};invalid.limit=0;invalid.conds=StorageEngine::parseConditions({condition});
            try{(void)QueryPlanner::buildSelectPlan(&g_engine,invalid);}
            catch(const DbError& failure){error=failure.sqlState();}
            require(error=="42703","public planner cap0 retains missing-column admission");
        }
    }
    std::cout<<"[SCALAR SQL NULL] complete controls="<<controls<<" failures="<<failures<<'\n';
    return failures?1:0;
}
