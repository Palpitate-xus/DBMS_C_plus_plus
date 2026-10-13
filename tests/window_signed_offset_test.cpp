#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "Session.h"
#include "test_utils.h"
#include <iostream>
#include <optional>

extern dbms::StorageEngine g_engine;
using namespace dbms;
int main() {
    TypeRegistry::instance().bootstrap();
    const auto database=testDbPath("window_signed_offset");
    if(g_engine.createDatabase(database,"utf8")!=DBStatus::OK)return 2;
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    DdlExecutor ddl;if(ddl.executeSql("CREATE TABLE t(id INT,v BIGINT,payload TEXT)",session))return 2;
    const std::vector<std::optional<std::string>> numbers={"9223372036854775807","9223372036854775806",
        std::nullopt,"-9223372036854775808","-9223372036854775807",std::nullopt};
    const std::vector<std::optional<std::string>> texts={"","NULL",std::nullopt,"a b"," spaced ",std::nullopt};
    for(size_t i=0;i<numbers.size();++i)
        if(g_engine.insertRow(database,"t",{{"id",std::to_string(i+1)},{"v",numbers[i]},{"payload",texts[i]}})!=DBStatus::OK)return 2;
    size_t checks=0,failures=0;
    for(const int32_t offset:{0,1,-1,INT32_MIN,INT32_MAX})for(bool ascending:{true,false})
        for(const std::string function:{"lag","lead"})for(bool text:{false,true})for(int fallback:{0,1,2}) {
        WindowFunctionSpec spec;spec.name=function;spec.argument=text?"payload":"v";
        spec.orderBy="id";spec.orderAscending=ascending;spec.offset=offset;
        spec.hasDefault=fallback!=0;spec.defaultIsNull=fallback==1;
        spec.defaultValue=text?"NULL":"-2147483649";
        PlanContext context;context.dbname=database;context.tablename="t";context.orderByCol="id";
        context.windowFunctions={spec};context.windowTargets={{false,"id",0},{true,"",0}};
        const auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildSelectPlan(&g_engine,context));
        bool good=result.ok && result.structuredRowsAvailable && result.structuredRows.size()==6;
        for(size_t i=0;good && i<6;++i) {
            const int64_t position=ascending?int64_t(i):5-int64_t(i);
            const int64_t target=position+(function=="lag"?-int64_t(offset):int64_t(offset));
            std::optional<std::string> wanted;
            if(target>=0 && target<6)wanted=(text?texts:numbers)[ascending?size_t(target):size_t(5-target)];
            else if(fallback==2)wanted=spec.defaultValue;
            good=result.structuredRows[i][0]==std::to_string(i+1) &&
                result.structuredNulls[i]==std::vector<bool>({false,!wanted});
            if(wanted)good=good && result.structuredRows[i][1]==*wanted;
        }
        ++checks;if(!good){++failures;std::cerr<<"SIGNED_WINDOW_OFFSET_FAILURE "<<function<<' '
            <<offset<<' '<<ascending<<' '<<text<<' '<<fallback<<'\n';}
    }
    for(const std::string function:{"lag","lead"}) {
        bool good=false;
        try {(void)prepareQuery("SELECT "+function+"() OVER()",{},QueryBindingMetadata{});}
        catch(const DbError& error){good=error.sqlState()=="42883";}
        ++checks;if(!good)++failures;
    }
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;
    cleanupTestDb("window_signed_offset");
    std::cout<<"SIGNED_WINDOW_OFFSET_NATIVE_COMPLETE "<<checks<<" FAILURES "<<failures<<std::endl;
    return failures?1:0;
}
