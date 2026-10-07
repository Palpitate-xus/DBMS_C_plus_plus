#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "Session.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <iostream>
#include <optional>

extern dbms::StorageEngine g_engine;
using namespace dbms;

int main() {
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("window_null_order");
    assert(g_engine.createDatabase(database,"utf8") == DBStatus::OK);
    Session session;
    session.username="testuser";session.permission=1;session.currentDB=database;
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t(id INT,k INT,v INT)",session));
    assert(!ddl.executeSql("CREATE TABLE empty_t(id INT,k INT,v INT)",session));
    for (int id=1;id<=4;++id) {
        const std::optional<std::string> key = id==2 ? std::nullopt
            : std::optional<std::string>(std::to_string(id==1?1:id-1));
        assert(g_engine.insertRow(database,"t",{{"id",std::to_string(id)},
            {"k",key},{"v",std::to_string(id*10)}}) == DBStatus::OK);
    }
    size_t controls=0;
    for (bool ascending:{true,false}) for (bool bounded:{false,true})
    for (int inputNullOrder:{-1,0,1}) for (int outputNullOrder:{-1,0,1})
    for (int outputOrder:{0,1,2,3}) {
        PlanContext context;
        context.dbname=database;context.tablename="t";
        if (outputOrder!=0) {
            context.orderByCol=outputOrder==1?"id":"k";
            context.orderByAsc=outputOrder!=3;
            context.hasExplicitOrderNulls=outputNullOrder>=0;
            context.orderByNullsFirst=outputNullOrder==1;
        }
        context.windowTargets={{false,"id",0},{false,"k",0}};
        for (const std::string function:{"row_number","rank","sum","count"}) {
            WindowFunctionSpec spec;
            spec.name=function;spec.orderBy="k";spec.orderAscending=ascending;
            spec.hasExplicitOrderNulls=inputNullOrder>=0;
            spec.orderByNullsFirst=inputNullOrder==1;
            spec.argument=function=="count"?"*":function=="sum"?"v":"";
            spec.hasFrame=bounded;spec.frameStartOffset=1;spec.frameEndOffset=0;
            context.windowTargets.push_back({true,"",context.windowFunctions.size()});
            context.windowFunctions.push_back(spec);
        }
        auto plan=QueryPlanner::buildSelectPlan(&g_engine,context);
        assert(dynamic_cast<WindowOp*>(plan.get()));
        assert(plan->open());
        const auto ids=[](bool asc,int nullOrder) {
            std::vector<int> result=asc?std::vector<int>{1,3,4}:std::vector<int>{4,3,1};
            const bool first=nullOrder<0?!asc:nullOrder==1;
            if (first) result.insert(result.begin(),2);else result.push_back(2);
            return result;
        };
        const auto inner=ids(ascending,inputNullOrder);
        const auto wantedOrder=outputOrder==1?std::vector<int>{1,2,3,4}
            :outputOrder==2?ids(true,outputNullOrder):outputOrder==3?ids(false,outputNullOrder):inner;
        std::string rendered;size_t row=0;
        while (plan->next(rendered)) {
            std::vector<std::string> cells;std::vector<bool> nulls;
            assert(plan->lastStructuredRow(cells,nulls));
            assert(row<wantedOrder.size() && cells.size()==6 && nulls.size()==6);
            const int id=wantedOrder[row];
            const auto found=std::find(inner.begin(),inner.end(),id);
            const size_t rank=static_cast<size_t>(found-inner.begin())+1;
            int total=0;
            const size_t begin=bounded && rank>1?rank-2:0;
            for (size_t position=begin;position<rank;++position) total+=inner[position]*10;
            const std::vector<std::string> wanted={std::to_string(id),id==2?"":
                std::to_string(id==1?1:id-1),std::to_string(rank),std::to_string(rank),
                std::to_string(total),std::to_string(bounded?std::min<size_t>(rank,2):rank)};
            for (size_t cell=0;cell<6;++cell) {
                const bool expectedNull=cell==1 && id==2;
                if (nulls[cell]!=expectedNull || (!expectedNull && cells[cell]!=wanted[cell])) {
                    std::cerr<<"WINDOW_NULL_ORDER asc="<<ascending<<" bounded="<<bounded
                        <<" output="<<outputOrder<<" row="<<row<<" cell="<<cell
                        <<" actual="<<cells[cell]<<" expected="<<wanted[cell]<<'\n';
                    assert(false);
                }
            }
            ++row;
        }
        assert(row==4 && !plan->hasError());plan->close();
        context.tablename="empty_t";
        const auto empty=QueryPlanner::executePlanChecked(QueryPlanner::buildSelectPlan(&g_engine,context));
        assert(empty.ok && empty.rows.empty());
        ++controls;
    }
    assert(controls==144);
    assert(g_engine.dropDatabase(database)==DBStatus::OK);
    cleanupTestDb("window_null_order");
    std::cout<<"[WINDOW NULL ORDER] complete144 actual planner/default/explicit/bounded/input/output/NULL controls passed\n";
}
