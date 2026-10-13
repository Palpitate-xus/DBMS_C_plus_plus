#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "Session.h"
#include "test_utils.h"
#include <algorithm>
#include <iostream>
#include <optional>

extern dbms::StorageEngine g_engine;
using namespace dbms;
struct Input { int id; std::optional<std::string> number,text; int group; };
int main() {
    TypeRegistry::instance().bootstrap();
    const auto database=testDbPath("window_numeric_partition");
    if(g_engine.createDatabase(database,"utf8")!=DBStatus::OK)return 2;
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    DdlExecutor ddl;
    if(ddl.executeSql("CREATE TABLE t(id INT,n NUMERIC,t TEXT)",session))return 2;
    const std::vector<Input> data={
        {1,"1.0","01",1},{2,"1.00","01",1},{3,"1","01",1},{4,"2","01",2},
        {5,std::nullopt,"",3},{6,std::nullopt,"",3},{7,"1","1",1},
        {8,"9007199254740993.0","01",4},{9,"9007199254740992.00","01",5},
        {10,"9007199254740993.000","01",4},{11,"9007199254740992.000","01",5},
        {12,"-0.0","01",6},{13,"0.00","01",6},
        {14,"12345678901234567890.12345678901",std::nullopt,7},
        {15,"12345678901234567890.123456789010",std::nullopt,7},
        {16,"12345678901234567890.12345678902",std::nullopt,8}};
    for(const auto& row:data)
        if(g_engine.insertRow(database,"t",{{"id",std::to_string(row.id)},
            {"n",row.number},{"t",row.text}})!=DBStatus::OK)return 2;
    size_t checks=0,failures=0;
    for(int partition:{0,1,2})for(bool ascending:{true,false})for(bool outputAscending:{true,false}) {
        PlanContext context;context.dbname=database;context.tablename="t";
        context.orderByCol="id";context.orderByAsc=outputAscending;
        context.windowTargets={{false,"id",0}};
        for(const std::string name:{"count","row_number","sum"}) {
            WindowFunctionSpec spec;spec.name=name;spec.partitionBy=partition==0?
                std::vector<std::string>{"n"}:partition==1?std::vector<std::string>{"n","t"}:std::vector<std::string>{"t"};
            spec.argument=name=="count"?"*":name=="sum"?"id":"";
            if(name=="row_number"){spec.orderBy="id";spec.orderAscending=ascending;}
            context.windowTargets.push_back({true,"",context.windowFunctions.size()});
            context.windowFunctions.push_back(spec);
        }
        const auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildSelectPlan(&g_engine,context));
        bool good=result.ok && result.structuredRowsAvailable && result.structuredRows.size()==data.size();
        for(size_t index=0;good && index<data.size();++index) {
            const auto& current=data[outputAscending?index:data.size()-1-index];
            std::vector<int> members;
            for(const auto& row:data)if(partition==2?row.text==current.text:
                row.group==current.group && (partition==0 || row.text==current.text))members.push_back(row.id);
            if(!ascending)std::reverse(members.begin(),members.end());
            const auto rank=std::find(members.begin(),members.end(),current.id)-members.begin()+1;
            int total=0;for(int id:members)total+=id;
            const std::vector<std::string> expected={std::to_string(current.id),
                std::to_string(members.size()),std::to_string(rank),std::to_string(total)};
            good=result.structuredRows[index]==expected && result.structuredNulls[index]==std::vector<bool>(4,false);
        }
        ++checks;if(!good){++failures;std::cerr<<"WINDOW_PARTITION_NATIVE_FAILURE "<<partition<<' '<<ascending<<' '<<outputAscending<<'\n';}
    }
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;
    cleanupTestDb("window_numeric_partition");
    std::cout<<"WINDOW_PARTITION_NATIVE_COMPLETE "<<checks<<" FAILURES "<<failures<<std::endl;
    return failures?1:0;
}
