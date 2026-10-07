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
    const auto database=testDbPath("bit_compact_list_receiver_admission");
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
    const auto require=[&](bool condition,const std::string& label) {
        ++controls;if(!condition){++failures;std::cerr<<"[BIT COMPACT RECEIVER FAIL] "<<label<<'\n';}
    };
    StorageEngine::SelectExpr projection;projection.displayName="id";projection.colName="id";
    const std::vector<std::pair<std::string,std::string>> conditions={
        {"inb B'01' 1","42883"},{"notinb B'01' 1","42883"},
        {"inb 1 B'01'","42883"},{"notinb 1 B'01'","42883"},
        {"betweenb B'01' 1","42883"},{"notbetweenb B'01' 1","42883"},
        {"betweenb 1 B'01'","42883"},{"notbetweenb 1 B'01'","42883"},
        {"inb B'01' '102'","22P02"},{"notinb B'01' '102'","22P02"},
        {"betweenb B'01' '102'","22P02"},{"notbetweenb B'01' '102'","22P02"},
        {"inb B'01' NULL 1","42883"},{"notinb B'01' NULL 1","42883"}};
    for(const auto& name:{"populated","empty_bits","null_bits"}) {
        for(const auto& condition:conditions)
            for(int path=0;path!=6;++path) {
                std::string state;
                try {
                    if(path<3) {
                        StorageEngine::QueryExprExecutionOptions options;
                        options.maxProjectionRows=path==0?0:1;
                        if(path==2)options.conditionAlternatives={{"=id 1"},{condition.first}};
                        (void)g_engine.queryExpr(database,name,{path==2?"=id 1":condition.first},{projection},{},options);
                    } else if(path<5) {
                        PlanContext context;context.dbname=database;context.tablename=name;context.selectCols={"id"};
                        if(path==3)context.conds=StorageEngine::parseConditions({"=id 1",condition.first});
                        else context.disjunctiveConds={StorageEngine::parseConditions({"=id 1"}),StorageEngine::parseConditions({condition.first})};
                        (void)QueryPlanner::buildSelectPlan(&g_engine,context);
                    } else {
                        auto filter=std::make_unique<FilterOp>(std::make_unique<TableScanOp>(&g_engine,database,name),
                            g_engine.getTableSchema(database,name),StorageEngine::parseConditions({condition.first}));
                        (void)filter->open();filter->close();
                    }
                } catch(const DbError& error){state=error.sqlState();}
                require(state==condition.second,std::string(name)+" path="+std::to_string(path)+" "+condition.first+
                    " expected="+condition.second+" actual="+state);
            }
        for(const auto& condition:std::vector<std::pair<std::string,std::string>>{
            {"inb B'01' id","42883"},{"inb B'01' missing_column","42703"}})
            for(const bool planner:{false,true}) {
                std::string state;
                try {
                    if(planner){PlanContext context;context.dbname=database;context.tablename=name;context.selectCols={"id"};
                        context.conds=StorageEngine::parseConditions({condition.first});(void)QueryPlanner::buildSelectPlan(&g_engine,context);}
                    else (void)g_engine.queryExpr(database,name,{condition.first},{projection});
                } catch(const DbError& error){state=error.sqlState();}
                require(state==condition.second,"schema-owned member "+condition.first+" actual="+state);
            }
        // Real ParameterExpr may resolve UNKNOWN context, but must never be
        // executed during metadata admission without a runtime parameter row.
        bool admitted=true;
        try {PlanContext context;context.dbname=database;context.tablename=name;context.selectCols={"id"};
            context.conds=StorageEngine::parseConditions({"inb B'01' $1"});(void)QueryPlanner::buildSelectPlan(&g_engine,context);}
        catch(const DbError&){admitted=false;}
        require(admitted,"unexecuted parameter metadata admits");
        for(const auto& condition:{"inb B'01' NULL","notinb B'01' NULL","betweenb B'01' B'01'","notbetweenb B'01' B'01'"}) {
            auto rows=g_engine.queryExpr(database,name,{condition},{projection});
            const bool expected=std::string(name)=="populated" && std::string(condition).rfind("not",0)!=0;
            require(rows.size()==(expected?1U:0U),std::string(name)+" legal empty/NULL "+condition);
        }
    }
    TableSchema sink;sink.tablename="calls";sink.append(makeIntColumn("id",false,4));
    if(g_engine.createTable(database,sink)!=DBStatus::OK)return 2;
    if(g_engine.createUDF(database,"compact_effect",{"p"},{"integer"},
        "BEGIN INSERT INTO calls VALUES(p); RETURN p; END;",'v',"plpgsql","integer")!=DBStatus::OK)return 2;
    StorageEngine::SelectExpr writer;writer.displayName="effect";writer.isScalar=true;
    writer.funcName="compact_effect";writer.funcArgs={"id"};
    const auto calls=[&]{return g_engine.queryExpr(database,"calls",{},{projection}).size();};
    for(const auto& condition:std::vector<std::pair<std::string,std::string>>{
        {"inb B'01' 1","42883"},{"inb B'01' '102'","22P02"}})
        for(const size_t cap:{size_t(0),size_t(1)}) {
            std::string state;StorageEngine::QueryExprExecutionOptions options;options.maxProjectionRows=cap;
            try {(void)g_engine.queryExpr(database,"populated",{condition.first},{writer},{},options);}
            catch(const DbError& error){state=error.sqlState();}
            require(state==condition.second && calls()==0,"invalid receiver never invokes real writer");
        }
    StorageEngine::QueryExprExecutionOptions options;options.maxProjectionRows=1;
    auto valid=g_engine.queryExpr(database,"populated",{"inb B'01' NULL"},{writer},{},options);
    require(valid.size()==1 && calls()==1,"admitted real writer runs once");
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;
    std::cout<<"[BIT COMPACT RECEIVER ADMISSION] controls="<<controls<<" failures="<<failures<<'\n';
    return failures?1:0;
}
