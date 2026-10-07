#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database=testDbPath("bit_compact_list_pre_scan");
    if(g_engine.createDatabase(database,"utf8")!=DBStatus::OK)return 2;
    TableSchema table;table.append(makeIntColumn("id",false,4,true));
    Column bit;bit.dataName="b";bit.isNull=true;
    if(!TypeRegistry::instance().resolveColumnType(bit,"bit",{"2"},false).empty())return 2;
    table.append(bit);
    for(const auto& name:{"populated","empty_bits","null_bits"}) {
        table.tablename=name;if(g_engine.createTable(database,table)!=DBStatus::OK)return 2;
    }
    if(g_engine.insertRow(database,"populated",{{"id","1"},{"b","01"}})!=DBStatus::OK)return 2;
    if(g_engine.insertRow(database,"null_bits",{{"id","1"},{"b",std::nullopt}})!=DBStatus::OK)return 2;
    size_t controls=0,failures=0;
    StorageEngine::SelectExpr projection;projection.displayName="id";projection.colName="id";
    const std::vector<std::pair<std::string,std::string>> conditions={
        {"inb B'01' 1","42883"},{"notinb B'01' 1","42883"},
        {"inb 1 B'01'","42883"},{"notinb 1 B'01'","42883"},
        {"betweenb B'01' 1","42883"},{"notbetweenb B'01' 1","42883"},
        {"betweenb 1 B'01'","42883"},{"notbetweenb 1 B'01'","42883"},
        {"inb B'01' '102'","22P02"},{"notinb B'01' '102'","22P02"},
        {"betweenb B'01' '102'","22P02"},{"notbetweenb B'01' '102'","22P02"},
        {"inb B'01' NULL 1","42883"},{"notinb B'01' NULL 1","42883"}};
    for(const auto& name:{"populated","empty_bits","null_bits"})
        for(const auto& condition:conditions)
            for(const bool planner:{false,true}) {
                ++controls;std::string actual;size_t rows=0;
                try {
                    if(planner) {
                        PlanContext context;context.dbname=database;context.tablename=name;
                        context.selectCols={"id"};context.conds=StorageEngine::parseConditions({condition.first});
                        const auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildSelectPlan(&g_engine,context));
                        actual=result.ok?"":result.errorSqlState;rows=result.rows.size();
                    } else rows=g_engine.queryExpr(database,name,{condition.first},{projection}).size();
                } catch(const DbError& error){actual=error.sqlState();}
                if(actual!=condition.second) {
                    ++failures;std::cerr<<"[BIT COMPACT LIST FAIL] table="<<name<<" planner="<<planner
                        <<" condition="<<condition.first<<" expected="<<condition.second<<" actual="<<actual<<" rows="<<rows<<'\n';
                }
            }
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;
    std::cout<<"[BIT COMPACT LIST PRE-SCAN] controls="<<controls<<" failures="<<failures<<'\n';
    return failures?1:0;
}
