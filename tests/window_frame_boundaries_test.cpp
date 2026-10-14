#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "executor/ExecutionPlan.h"
#include "Session.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <iostream>
#include <limits>
#include <numeric>
#include <optional>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    using Bound=WindowDef::BoundKind;
    TypeRegistry::instance().bootstrap();
    const auto database=testDbPath("window_frame_boundaries");
    assert(g_engine.createDatabase(database,"utf8")==DBStatus::OK);
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    DdlExecutor ddl;assert(!ddl.executeSql("CREATE TABLE t(id INT,k INT,v INT)",session));
    const std::vector<int> keys={1,1,3,3,5,8};
    const std::vector<std::optional<int>> values={10,std::nullopt,30,30,50,80};
    for(size_t i=0;i<keys.size();++i)
        assert(g_engine.insertRow(database,"t",{{"id",std::to_string(i+1)},{"k",std::to_string(keys[i])},
            {"v",values[i]?std::optional<std::string>(std::to_string(*values[i])):std::nullopt}})==DBStatus::OK);
    struct Bounds {Bound start;int64_t before;Bound end;int64_t after;};
    const std::vector<Bounds> definitions={
        {Bound::UnboundedPreceding,0,Bound::CurrentRow,0},
        {Bound::CurrentRow,0,Bound::UnboundedFollowing,0},
        {Bound::UnboundedPreceding,0,Bound::UnboundedFollowing,0},
        {Bound::Preceding,1,Bound::Preceding,1},{Bound::Preceding,2,Bound::Preceding,1},
        {Bound::CurrentRow,0,Bound::CurrentRow,0},{Bound::CurrentRow,0,Bound::Following,2},
        {Bound::Following,1,Bound::Following,2},{Bound::Following,2,Bound::Following,1},
        {Bound::Preceding,3,Bound::Preceding,5},{Bound::UnboundedPreceding,0,Bound::Preceding,1},
        {Bound::Following,2,Bound::UnboundedFollowing,0},
        {Bound::Preceding,INT64_MAX,Bound::Preceding,1},
        {Bound::Following,INT64_MAX,Bound::UnboundedFollowing,0}
    };
    size_t checks=0,failures=0;
    const auto check=[&](bool passed,const std::string& label) {
        ++checks;if(!passed){++failures;std::cerr<<"WINDOW_FRAME_NATIVE_FAILURE "<<label<<'\n';}
    };
    const auto coordinateBound=[](Bound kind,int64_t distance,long double current) {
        if(kind==Bound::UnboundedPreceding)return -std::numeric_limits<long double>::infinity();
        if(kind==Bound::UnboundedFollowing)return std::numeric_limits<long double>::infinity();
        if(kind==Bound::CurrentRow)return current;
        return current+(kind==Bound::Following?static_cast<long double>(distance):-static_cast<long double>(distance));
    };
    for(const std::string mode:{"rows","groups","range"})for(const bool ascending:{true,false}) {
        std::vector<size_t> order(keys.size());std::iota(order.begin(),order.end(),0);
        std::stable_sort(order.begin(),order.end(),[&](size_t a,size_t b){return ascending?keys[a]<keys[b]:keys[a]>keys[b];});
        std::vector<int> group(order.size());
        for(size_t i=1;i<order.size();++i)group[i]=group[i-1]+(keys[order[i]]!=keys[order[i-1]]);
        for(const auto& bounds:definitions)for(const std::string exclusion:{"no others","current row","group","ties"}) {
            WindowFrame frame;frame.mode=mode;frame.start=bounds.start;frame.end=bounds.end;
            frame.startOffset=ExprValue("bigint",std::to_string(bounds.before));
            frame.endOffset=ExprValue("bigint",std::to_string(bounds.after));frame.validateOffsets();
            PlanContext context;context.dbname=database;context.tablename="t";context.orderByCol="id";
            context.windowTargets={{false,"id",0}};
            for(const std::string function:{"sum","count","first_value","last_value"}) {
                WindowFunctionSpec spec;spec.name=function;spec.argument=function=="count"?"*":"v";
                spec.orderBy="k";spec.orderAscending=ascending;spec.hasFrame=true;
                spec.preparedFrame=frame;spec.frameExclusion=exclusion;
                context.windowTargets.push_back({true,"",context.windowFunctions.size()});
                context.windowFunctions.push_back(spec);
            }
            const auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildSelectPlan(&g_engine,context));
            bool good=result.ok && result.structuredRowsAvailable && result.structuredRows.size()==order.size();
            for(size_t position=0;position<order.size() && good;++position) {
                const auto coordinate=[&](size_t i)->long double {
                    if(mode=="rows")return i;
                    if(mode=="groups")return group[i];
                    return ascending?keys[order[i]]:-keys[order[i]];
                };
                const auto low=coordinateBound(bounds.start,bounds.before,coordinate(position));
                const auto high=coordinateBound(bounds.end,bounds.after,coordinate(position));
                std::vector<size_t> members;
                for(size_t j=0;j<order.size();++j)if(low<=coordinate(j) && coordinate(j)<=high) {
                    if(exclusion=="current row" && j==position)continue;
                    if(exclusion=="group" && group[j]==group[position])continue;
                    if(exclusion=="ties" && j!=position && group[j]==group[position])continue;
                    members.push_back(order[j]);
                }
                int total=0,nonnull=0;
                for(size_t member:members)if(values[member]){total+=*values[member];++nonnull;}
                const std::optional<int> first=members.empty()?std::nullopt:values[members.front()];
                const std::optional<int> last=members.empty()?std::nullopt:values[members.back()];
                const size_t original=order[position];
                const auto& row=result.structuredRows.at(original);
                good=row.size()==5 && row[0]==std::to_string(original+1) && row[2]==std::to_string(members.size()) &&
                    result.structuredNulls.at(original)==std::vector<bool>({false,!nonnull,false,!first,!last});
                if(good && nonnull)good=row[1]==std::to_string(total);
                if(good && first)good=row[3]==std::to_string(*first);
                if(good && last)good=row[4]==std::to_string(*last);
            }
            check(good,mode+" "+std::to_string(ascending)+" "+std::to_string(static_cast<int>(bounds.start))+" "+
                std::to_string(bounds.before)+" "+std::to_string(static_cast<int>(bounds.end))+" "+std::to_string(bounds.after)+" "+exclusion);
        }
    }
    QueryBindingMetadata metadata;
    metadata.relation=[](const std::string&){return QueryRelationMetadata{"public","t",{{"id","integer"},{"k","integer"},{"v","integer"}}, {}};};
    metadata.functionType=[](const FunctionCallExpr*){return std::string("bigint");};
    for(const auto& [frame,state]:std::vector<std::pair<std::string,std::string>>{
        {"ROWS BETWEEN CURRENT ROW AND 1 PRECEDING","42P20"},
        {"ROWS BETWEEN 1 FOLLOWING AND CURRENT ROW","42P20"},
        {"ROWS BETWEEN id PRECEDING AND CURRENT ROW","42P10"},
        {"ROWS (WITH q AS(SELECT t.id AS x) SELECT x FROM q) PRECEDING","42P10"},
        {"ROWS (SELECT q.x FROM(SELECT t.id AS x) q) PRECEDING","42P10"},
        {"ROWS BETWEEN '1'::text PRECEDING AND CURRENT ROW","42804"},
        {"RANGE BETWEEN 1.5 PRECEDING AND CURRENT ROW","0A000"}}) {
        std::string actual;
        try{(void)prepareQuery("SELECT sum(v) OVER(ORDER BY k "+frame+") FROM t",{},metadata);}
        catch(const DbError& error){actual=error.sqlState();}
        check(actual==state,"pure binding "+frame+" got "+actual);
    }
    for(const auto* offset:{"(WITH q AS(SELECT t.id AS x) SELECT x FROM q)",
                           "(SELECT q.x FROM(SELECT t.id AS x) q)"})
        for(const auto* suffix:{""," WHERE FALSE"," LIMIT 0"}) {
            std::string actual;
            try{(void)g_engine.prepareBoundQuery(database,std::string("SELECT sum(v) OVER(ORDER BY id ROWS ")+
                offset+" PRECEDING) AS total FROM t"+suffix);}
            catch(const DbError& error){actual=error.sqlState();}
            check(actual=="42P10",std::string("query-host binding ")+offset+suffix+" got "+actual);
        }
    assert(g_engine.dropDatabase(database)==DBStatus::OK);cleanupTestDb("window_frame_boundaries");
    std::cout<<"WINDOW_FRAME_NATIVE_COMPLETE "<<checks<<" FAILURES "<<failures<<std::endl;
    return failures?1:0;
}
