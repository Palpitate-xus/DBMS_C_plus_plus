#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "Session.h"
#include "test_utils.h"
#include <algorithm>
#include <iostream>
#include <optional>
#include <set>

extern dbms::StorageEngine g_engine;
using namespace dbms;
struct Input { std::optional<std::string> value; int group; };
struct Case { std::string name,type; std::vector<Input> rows; };
int main() {
    TypeRegistry::instance().bootstrap();
    const auto database=testDbPath("window_declared_comparison");
    if(g_engine.createDatabase(database,"utf8")!=DBStatus::OK)return 2;
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    DdlExecutor ddl;
    const std::vector<Case> cases={
        {"text_rows","TEXT COLLATE \"C\"",{{"2",5},{"10",4},{"1",3},{"01",2},{"",0},
            {std::nullopt,99},{"NULL",6},{"hello world",7},{"2",5}}},
        {"bigint_rows","BIGINT",{{"9007199254740993",3},{"9007199254740992",2},
            {"9223372036854775807",4},{"-9223372036854775808",0},
            {"-9007199254740993",1},{std::nullopt,99},{"9007199254740993",3}}},
        {"numeric_rows","NUMERIC",{{"9007199254740993.00002",3},
            {"9007199254740993.00001",2},{"9007199254740992.99999",1},
            {"-12345678901234567890.123456789",0},{std::nullopt,99},
            {"9007199254740993.00002",3}}},
        {"special_rows","NUMERIC",{{"NaN",4},{"Infinity",3},{"-Infinity",0},
            {"-1",1},{"0",2},{"NaN",4},{std::nullopt,99}}},
        {"null_rows","TEXT",{{std::nullopt,99},{std::nullopt,99}}},
        {"literal_rows","TEXT",{{"",0},{"NULL",1},{std::nullopt,99}}},
        {"collated_rows","TEXT COLLATE \"C\"",{{"10",0},{"2",1},{"A",2},{"a",3},{std::nullopt,99}}}};
    size_t checks=0,failures=0;
    for(const auto& fixture:cases) {
        if(ddl.executeSql("CREATE TABLE "+fixture.name+"(id INT,k "+fixture.type+")",session))return 2;
        for(size_t index=0;index<fixture.rows.size();++index)
            if(g_engine.insertRow(database,fixture.name,{{"id",std::to_string(index+1)},
                {"k",fixture.rows[index].value}})!=DBStatus::OK)return 2;
        const auto key=[&](const Input& row,bool ascending) {
            if(!row.value)return ascending?100:-100;
            return ascending?row.group:-row.group;
        };
        std::optional<std::string> minimum,maximum;
        int minGroup=100,maxGroup=-1;
        for(const auto& row:fixture.rows)if(row.value) {
            if(row.group<minGroup){minGroup=row.group;minimum=row.value;}
            if(row.group>maxGroup){maxGroup=row.group;maximum=row.value;}
        }
        for(bool ascending:{true,false})for(bool outputAscending:{true,false})
        for(bool outputKey:{false,true})for(int nullOrder:{-1,0,1}) {
            PlanContext context;context.dbname=database;context.tablename=fixture.name;
            context.orderByCol=outputKey?"k":"id";context.orderByAsc=outputAscending;
            context.hasExplicitOrderNulls=nullOrder>=0;context.orderByNullsFirst=nullOrder==1;
            context.windowTargets={{false,"id",0}};
            for(const std::string name:{"rank","dense_rank","count","nonnull_count","min","max"}) {
                WindowFunctionSpec spec;spec.name=name=="nonnull_count"?"count":name;
                if(name=="min" || name=="max")spec.argument="k";
                else if(name=="nonnull_count")spec.argument="k";
                else {spec.orderBy="k";spec.orderAscending=ascending;if(name=="count")spec.argument="*";}
                context.windowTargets.push_back({true,"",context.windowFunctions.size()});
                context.windowFunctions.push_back(spec);
            }
            const auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildSelectPlan(&g_engine,context));
            bool good=result.ok && result.structuredRowsAvailable && result.structuredRows.size()==fixture.rows.size();
            std::vector<size_t> outputOrder;
            for(size_t index=0;index<fixture.rows.size();++index)outputOrder.push_back(index);
            std::stable_sort(outputOrder.begin(),outputOrder.end(),[&](size_t left,size_t right) {
                if(!outputKey)return outputAscending?left<right:left>right;
                const auto& a=fixture.rows[left];const auto& b=fixture.rows[right];
                if(bool(a.value)!=bool(b.value)) {
                    const bool first=nullOrder<0?!outputAscending:nullOrder==1;
                    return (!a.value)==first;
                }
                if(!a.value)return false;
                return outputAscending?a.group<b.group:a.group>b.group;
            });
            for(size_t output=0;good && output<fixture.rows.size();++output) {
                const size_t index=outputOrder[output];
                const auto& current=fixture.rows[index];
                size_t before=0,through=0;std::set<int> earlierGroups;
                for(const auto& row:fixture.rows) {
                    if(key(row,ascending)<key(current,ascending)){++before;earlierGroups.insert(key(row,ascending));}
                    if(key(row,ascending)<=key(current,ascending))++through;
                }
                const auto nonnull=std::count_if(fixture.rows.begin(),fixture.rows.end(),[](const auto& row){return bool(row.value);});
                const std::vector<std::string> expected={std::to_string(index+1),std::to_string(before+1),
                    std::to_string(earlierGroups.size()+1),std::to_string(through),std::to_string(nonnull),
                    minimum.value_or(""),maximum.value_or("")};
                const std::vector<bool> nulls={false,false,false,false,false,!minimum,!maximum};
                good=result.structuredRows[output]==expected && result.structuredNulls[output]==nulls;
                if(!good)std::cerr<<"WINDOW_DECLARED_ROW_FAILURE "<<fixture.name<<' '<<index+1<<'\n';
            }
            ++checks;if(!good){++failures;std::cerr<<"WINDOW_DECLARED_CASE_FAILURE "<<fixture.name<<' '<<ascending<<' '<<outputAscending<<'\n';}
        }
    }
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;
    cleanupTestDb("window_declared_comparison");
    std::cout<<"WINDOW_DECLARED_NATIVE_COMPLETE "<<checks<<" FAILURES "<<failures<<std::endl;
    return failures?1:0;
}
