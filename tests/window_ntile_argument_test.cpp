#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "Session.h"
#include "test_utils.h"
#include <iostream>

extern dbms::StorageEngine g_engine;
using namespace dbms;
int main() {
    TypeRegistry::instance().bootstrap();
    const auto database=testDbPath("window_ntile_argument");
    if(g_engine.createDatabase(database,"utf8")!=DBStatus::OK)return 2;
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    DdlExecutor ddl;
    if(ddl.executeSql("CREATE TABLE t(id INT,p TEXT)",session) ||
       ddl.executeSql("CREATE TABLE empty_t(id INT,p TEXT)",session))return 2;
    for(int id=1;id<=9;++id)
        if(g_engine.insertRow(database,"t",{{"id",std::to_string(id)},
            {"p",id<=6?"a":"b"}})!=DBStatus::OK)return 2;
    size_t checks=0,failures=0;
    for(const std::string argument:{"0","-1","-2147483648","NULL","2"})
        for(bool ascending:{true,false})for(bool empty:{false,true}) {
        WindowFunctionSpec spec;spec.name="ntile";spec.argument=argument;
        spec.partitionBy={"p"};spec.orderBy="id";spec.orderAscending=ascending;
        PlanContext context;context.dbname=database;context.tablename=empty?"empty_t":"t";
        context.orderByCol="id";context.windowFunctions={spec};
        context.windowTargets={{false,"id",0},{true,"",0}};
        const auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildSelectPlan(&g_engine,context));
        const bool invalid=!empty && argument!="NULL" && argument!="2";
        bool good=invalid?!result.ok && result.errorSqlState=="22014":result.ok;
        if(good && !invalid) {
            good=result.structuredRowsAvailable && result.structuredRows.size()==(empty?0:9);
            for(size_t row=0;good && row<result.structuredRows.size();++row) {
                const int id=static_cast<int>(row+1);
                const int position=id<=6?(ascending?id-1:6-id):(ascending?id-7:9-id);
                const int bucket=id<=6?(position<3?1:2):(position<2?1:2);
                good=result.structuredRows[row][0]==std::to_string(id) &&
                    result.structuredNulls[row]==std::vector<bool>({false,argument=="NULL"});
                if(argument!="NULL")good=good && result.structuredRows[row][1]==std::to_string(bucket);
            }
        }
        ++checks;if(!good){++failures;std::cerr<<"NTILE_ARGUMENT_FAILURE "<<argument<<' '
            <<ascending<<' '<<empty<<" actual "<<result.ok<<' '<<result.errorSqlState<<'\n';}
    }
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;
    cleanupTestDb("window_ntile_argument");
    std::cout<<"NTILE_ARGUMENT_NATIVE_COMPLETE "<<checks<<" FAILURES "<<failures<<std::endl;
    return failures?1:0;
}
