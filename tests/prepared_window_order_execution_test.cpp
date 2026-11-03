#include "executor/ExecutionPlan.h"
#include "expression/prepared_query_execution.h"
#include "commands/DdlExecutor.h"
#include "Session.h"
#include "test_utils.h"
#include <algorithm>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;
namespace {
using namespace dbms;
using Rows=std::vector<std::vector<ExprValue>>;
struct Fixture {std::shared_ptr<PreparedQuery> query;std::unique_ptr<PreparedQueryCursor> cursor;};
Fixture prepare(const std::string& db,const std::string& sql,const std::vector<QueryBindingDatum>& inputs={}) {
    auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql,inputs));
    auto plan=QueryPlanner::buildPreparedQueryPlan(&g_engine,db,query,query->ast.get());
    return {query,QueryPlanner::makePreparedCursor(std::move(plan),query->output)};
}
// This retained-child factory deliberately leaves root constant planning
// to its public caller. Exercise these SQL demand controls with the same
// explicit planned owner supplied by a genuine whole-query host.
Fixture preparePlanned(const std::string& db,const std::string& sql) {
    auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql));
    auto execution=std::make_shared<PreparedQueryExecution>(query,&g_engine,db);
    execution->planStatementConstants(query->ast.get());
    auto plan=QueryPlanner::buildPreparedQueryPlan(&g_engine,db,query,query->ast.get(),{},
        [execution]{return execution;});
    return {query,QueryPlanner::makePreparedCursor(std::move(plan),query->output)};
}
std::vector<Operator*> nodes(Operator* root) {
    std::vector<Operator*> result,pending{root};std::set<Operator*> visited;
    while(!pending.empty()) {auto* node=pending.back();pending.pop_back();
        if(!node || !visited.insert(node).second)continue;
        result.push_back(node);for(auto* child:node->preparedPlanChildren())pending.push_back(child);
        // These existing physical wrappers expose their actual public child()
        // rather than the private prepared-node visitor used above.
        if(auto* limit=dynamic_cast<LimitOp*>(node))pending.push_back(limit->child());
        if(auto* offset=dynamic_cast<OffsetOp*>(node))pending.push_back(offset->child());
    }
    return result;
}
std::vector<Operator*> named(Operator* root,const std::string& name) {
    auto result=nodes(root);result.erase(std::remove_if(result.begin(),result.end(),[&](auto* node){return node->preparedPlanNodeName()!=name;}),result.end());return result;
}
Operator* one(Operator* root,const std::string& name) {
    const auto result=named(root,name);if(result.size()!=1)throw DbError("XX000","expected one actual "+name+" node");return result.front();
}
Operator* heap(Operator* root) {
    Operator* found=nullptr;for(auto* node:nodes(root))if(dynamic_cast<TableScanOp*>(node)) {
        if(found)throw DbError("XX000","expected one genuine heap scan");found=node;
    }
    if(!found)throw DbError("XX000","actual query did not retain its heap scan");return found;
}
bool pure(Operator* root) {for(auto* node:nodes(root))if(node->runtimeLoops() || node->runtimeRows())return false;return true;}
Rows consume(PreparedQueryCursor& cursor) {Rows rows;std::vector<ExprValue> row;while(cursor.next(row))rows.push_back(row);cursor.close();cursor.close();return rows;}
bool values(const Rows& rows,const std::vector<std::vector<std::optional<std::string>>>& expected) {
    if(rows.size()!=expected.size())return false;
    for(size_t i=0;i<rows.size();++i){if(rows[i].size()!=expected[i].size())return false;
        for(size_t j=0;j<rows[i].size();++j)if(expected[i][j] ? rows[i][j].isNull || rows[i][j].value!=*expected[i][j] : !rows[i][j].isNull)return false;}
    return true;
}
size_t slot(const PreparedQuery& query) {
    std::optional<size_t> found;for(const auto& use:query.uses)
        if(query.source.substr(use.begin,use.end-use.begin)=="$1"){
            if(found && *found!=use.slot)throw DbError("XX000","actual parameter sites disagree");found=use.slot;}
    if(!found || *found>=query.parameters.size())throw DbError("XX000","missing actual parameter slot");return *found;
}
bool sameCell(const ExprValue& a,const ExprValue& b) {
    return a.typeName==b.typeName && a.value==b.value && a.isNull==b.isNull && a.collation==b.collation &&
        a.objectOid==b.objectOid && a.elementOids==b.elementOids && !a.recordFields && !b.recordFields;
}
}
int main() {
    using namespace dbms;TypeRegistry::instance().bootstrap();
    const auto db=testDbPath("prepared_window_order");cleanupTestDb("prepared_window_order");
    if(g_engine.createDatabase(db,"utf8")!=DBStatus::OK)return 2;
    Session session;session.username="admin";session.permission=1;session.currentDB=db;
    auto* previous=currentSession();setCurrentSession(&session);DdlExecutor ddl;
    bool setup=!ddl.executeSql("CREATE TABLE window_order_heap(id INTEGER,p INTEGER,v BIGINT)",session);
    for(int id:{4,2,1,3})setup=(g_engine.insertRow(db,"window_order_heap",{{"id",std::to_string(id)},
        {"p",id<=2?"0":"1"},{"v",id==3?std::optional<std::string>{}:std::optional<std::string>{id==1?"9007199254740993":id==2?"9007199254740994":"-9007199254740993"}}})==DBStatus::OK) && setup;
    if(!setup){setCurrentSession(previous);return 2;}
    size_t controls=0,failures=0,sequenceIndex=0;
    const auto sequence=[&]{const auto name="window_order_sequence_"+std::to_string(sequenceIndex++);
        if(ddl.executeSql("CREATE SEQUENCE "+name,session) || g_engine.nextval(db,name)!=1)throw DbError("XX000","real sequence setup failed");return name;};
    const auto check=[&](const std::string& label,const std::function<bool()>& action){++controls;bool good=false;
        try{good=action();}catch(const DbError& error){std::cerr<<"SQLSTATE="<<error.sqlState()<<" message="<<error.message()<<'\n';}
        catch(const std::exception& error){std::cerr<<"exception="<<error.what()<<'\n';}
        good=good && StorageEngine::captureNullRowBinding().engine==nullptr;failures+=!good;
        std::cout<<"PREPARED_WINDOW_ORDER "<<controls<<' '<<label<<" good="<<good<<'\n';};
    check("actual compatible order removes TypedSort and LIMIT1 runs one window argument",[&]{
        const auto s=sequence();auto f=prepare(db,"SELECT id,lag(nextval('"+s+"'),0) OVER(ORDER BY id) AS x FROM window_order_heap ORDER BY id LIMIT 1");
        auto* root=f.cursor->plan();auto* scan=heap(root);auto* window=one(root,"WindowAgg");
        bool good=pure(root) && named(root,"TypedSort").empty() && g_engine.currval(db,s)==1;
        const auto result=consume(*f.cursor);
        return good && values(result,{{"1","2"}}) && g_engine.currval(db,s)==2 && scan->runtimeRows()==4 && scan->runtimeLoops()==5 && window->runtimeRows()==1 && window->runtimeLoops()==1;
    });
    check("opposite order retains actual TypedSort and consumes all window arguments",[&]{
        const auto s=sequence();auto f=prepare(db,"SELECT id,lag(nextval('"+s+"'),0) OVER(ORDER BY id) AS x FROM window_order_heap ORDER BY id DESC LIMIT 1");
        auto* root=f.cursor->plan();auto* window=one(root,"WindowAgg");bool good=pure(root) && named(root,"TypedSort").size()==1;
        return values(consume(*f.cursor),{{"4","5"}}) && good && g_engine.currval(db,s)==5 && window->runtimeRows()==4 && window->runtimeLoops()==5;
    });
    check("conditionally demanded producer cannot guarantee final input order",[&]{
        auto f=prepare(db,"SELECT id,CASE WHEN false THEN row_number() OVER(ORDER BY id) ELSE 0 END AS n FROM window_order_heap ORDER BY id LIMIT 1");
        bool good=pure(f.cursor->plan()) && named(f.cursor->plan(),"TypedSort").size()==1;
        return values(consume(*f.cursor),{{"1","0"}}) && good;
    });
    check("potential later incompatible stage retains Sort across actual caller rebind",[&]{
        QueryBindingDatum input{"window-order-active","active","boolean",{},false,"false",1};input.origin=ParameterOrigin::RuntimeCell;
        auto f=prepare(db,"SELECT id,row_number() OVER(ORDER BY id DESC) AS n,CASE WHEN $1 THEN row_number() OVER(ORDER BY id) ELSE 0 END AS m FROM window_order_heap ORDER BY id DESC LIMIT 1",{input});
        auto* graph=f.cursor->plan();const auto index=slot(*f.query);
        bool good=pure(graph) && named(graph,"WindowAgg").size()==2 && named(graph,"TypedSort").size()==1 && f.cursor->supportsRestart();
        good=values(consume(*f.cursor),{{"4","1","0"}}) && good;
        auto cells=f.query->parameters;cells[index]=ExprValue("boolean","true");RowContext caller;caller.setParameters(std::move(cells));f.cursor->restart(caller);
        return values(consume(*f.cursor),{{"4","1","4"}}) && good && f.cursor->plan()==graph && f.query->parameters[index].value=="false" && caller.parameter(index).value=="true";
    });
    check("unique WITH TIES first next and early close evaluate no boundary argument",[&]{
        const auto s=sequence();auto f=prepare(db,"SELECT id,lag(nextval('"+s+"'),0) OVER(ORDER BY id) AS x FROM window_order_heap ORDER BY id FETCH FIRST 1 ROW WITH TIES");
        auto* root=f.cursor->plan();auto* window=one(root,"WindowAgg");const auto children=root->preparedPlanChildren();
        bool good=pure(root) && root->preparedPlanNodeName()=="LimitWithTies" && named(root,"TypedSort").empty() && children.size()==1 && root->supportsStructuredRows()==children[0]->supportsStructuredRows() && root->supportsPreparedContexts()==children[0]->supportsPreparedContexts() && f.cursor->supportsRestart()==children[0]->supportsPreparedOuterRow();
        std::vector<ExprValue> row,snapshot;good=f.cursor->next(row) && values({row},{{"1","2"}}) && root->lastStructuredValues(snapshot) && snapshot.size()==row.size() && sameCell(snapshot[1],row[1]) && good;
        good=good && g_engine.currval(db,s)==2 && window->runtimeRows()==1 && window->runtimeLoops()==1;
        f.cursor->close();f.cursor->close();return good && g_engine.currval(db,s)==2 && window->runtimeLoops()==1;
    });
    check("unique WITH TIES complete consume evaluates exactly one real boundary row",[&]{
        const auto s=sequence();auto f=prepare(db,"SELECT id,lag(nextval('"+s+"'),0) OVER(ORDER BY id) AS x FROM window_order_heap ORDER BY id FETCH FIRST 1 ROW WITH TIES");
        auto* window=one(f.cursor->plan(),"WindowAgg");bool good=pure(f.cursor->plan()) && named(f.cursor->plan(),"TypedSort").empty();
        return values(consume(*f.cursor),{{"1","2"}}) && good && g_engine.currval(db,s)==3 && window->runtimeRows()==2 && window->runtimeLoops()==2;
    });
    check("peer WITH TIES preserves stable physical input ordinals and boundary demand",[&]{
        const auto s=sequence();auto f=prepare(db,"SELECT id,lag(nextval('"+s+"'),0) OVER(ORDER BY p) AS x FROM window_order_heap ORDER BY p FETCH FIRST 1 ROW WITH TIES");
        auto* window=one(f.cursor->plan(),"WindowAgg");bool good=named(f.cursor->plan(),"TypedSort").empty();
        return values(consume(*f.cursor),{{"2","2"},{"1","3"}}) && good && g_engine.currval(db,s)==4 && window->runtimeRows()==3 && window->runtimeLoops()==3;
    });
    check("OFFSET and peers consume actual skipped and boundary window rows",[&]{
        const auto s=sequence();auto f=prepare(db,"SELECT id,lag(nextval('"+s+"'),0) OVER(ORDER BY p) AS x FROM window_order_heap ORDER BY p OFFSET 1 ROW FETCH FIRST 1 ROW WITH TIES");
        auto* window=one(f.cursor->plan(),"WindowAgg");bool good=named(f.cursor->plan(),"TypedSort").empty();
        return values(consume(*f.cursor),{{"1","3"}}) && good && g_engine.currval(db,s)==4 && window->runtimeRows()==3 && window->runtimeLoops()==3;
    });
    check("zero WITH TIES never opens the heap or evaluates invalid ntile",[&]{
        auto f=prepare(db,"SELECT id,ntile(0) OVER(ORDER BY id) AS x FROM window_order_heap ORDER BY id FETCH FIRST 0 ROWS WITH TIES");
        auto* scan=heap(f.cursor->plan());auto* window=one(f.cursor->plan(),"WindowAgg");bool good=pure(f.cursor->plan()) && named(f.cursor->plan(),"TypedSort").empty();
        return consume(*f.cursor).empty() && good && scan->runtimeRows()==0 && scan->runtimeLoops()==0 && window->runtimeRows()==0 && window->runtimeLoops()==0;
    });
    check("partition key is a valid shorter final ORDER prefix",[&]{
        auto f=prepare(db,"SELECT id,row_number() OVER(PARTITION BY p ORDER BY id) AS n FROM window_order_heap ORDER BY p LIMIT 1");
        bool good=pure(f.cursor->plan()) && named(f.cursor->plan(),"TypedSort").empty();
        return values(consume(*f.cursor),{{"1","1"}}) && good;
    });
    check("typed bigint NULL and real regtype OIDs survive ordered ties snapshots",[&]{
        auto f=prepare(db,"SELECT id,lag(v,0) OVER(ORDER BY id) AS v,lag(CASE WHEN id=2 THEN NULL::regtype ELSE 'integer'::regtype END,0) OVER(ORDER BY id) AS o FROM window_order_heap ORDER BY id FETCH FIRST 4 ROWS WITH TIES");
        const auto descriptor=f.cursor->descriptor();bool good=descriptor.size()==3 && descriptor[0].typeOid==23 && descriptor[1].typeOid==20 && descriptor[2].typeOid==2206 && named(f.cursor->plan(),"TypedSort").empty();
        Rows rows;std::vector<ExprValue> row,snapshot;while(f.cursor->next(row)) {
            good=f.cursor->plan()->lastStructuredValues(snapshot) && snapshot.size()==row.size() && good;
            for(size_t i=0;i<row.size() && i<snapshot.size();++i)good=sameCell(row[i],snapshot[i]) && good;
            rows.push_back(row);
        }
        f.cursor->close();f.cursor->close();good=values(rows,{{"1","9007199254740993","integer"},{"2","9007199254740994",{}},{"3",{},"integer"},{"4","-9007199254740993","integer"}}) && good;
        if(rows.size()!=4)return false;
        for(size_t i=0;i<rows.size();++i)good=rows[i][1].typeName=="bigint" && rows[i][2].typeName=="regtype" && (i==1?!rows[i][2].objectOid:rows[i][2].objectOid==23) && good;
        return good;
    });
    for(const auto origin:{ParameterOrigin::StatementInput,ParameterOrigin::RuntimeCell,ParameterOrigin::MetadataPlaceholder})check("same graph ordered ties restart preserves parameter owner "+std::to_string(static_cast<unsigned>(origin)),[&]{
        QueryBindingDatum input{"window-order-offset","offset","integer",{},false,"1",1};input.origin=origin;
        auto f=prepare(db,"SELECT id,lag(v,$1) OVER(ORDER BY id) AS x FROM window_order_heap ORDER BY id OFFSET 1 ROW FETCH FIRST 1 ROW WITH TIES",{input});
        auto* graph=f.cursor->plan();const auto index=slot(*f.query);const auto original=f.query->parameters[index];
        bool good=pure(graph) && named(graph,"TypedSort").empty() && graph->preparedPlanNodeName()=="LimitWithTies" && f.cursor->supportsRestart();
        good=values(consume(*f.cursor),{{"2","9007199254740993"}}) && good;
        auto cells=f.query->parameters;cells[index]=ExprValue("integer","0");RowContext caller;caller.setParameters(std::move(cells));f.cursor->restart(caller);
        good=values(consume(*f.cursor),{{"2",origin==ParameterOrigin::StatementInput?"9007199254740993":"9007199254740994"}}) && sameCell(caller.parameter(index),ExprValue("integer","0")) && good;
        auto nulls=f.query->parameters;nulls[index]=ExprValue("integer","",true);RowContext nullCaller;nullCaller.setParameters(std::move(nulls));f.cursor->restart(nullCaller);
        const auto result=consume(*f.cursor);good=(origin==ParameterOrigin::StatementInput?values(result,{{"2","9007199254740993"}}):values(result,{{"2",{}}})) && good;
        return good && f.cursor->plan()==graph && sameCell(f.query->parameters[index],original) && sameCell(nullCaller.parameter(index),ExprValue("integer","",true));
    });
    check("first COALESCE argument guarantees window order and exact OFFSET prefix demand",[&]{
        const auto s=sequence();auto f=preparePlanned(db,"SELECT COALESCE(lag(nextval('"+s+"')) OVER(ORDER BY id),0::bigint) AS result FROM window_order_heap ORDER BY id LIMIT 2 OFFSET 1");
        auto* root=f.cursor->plan();auto* window=one(root,"WindowAgg");
        const bool good=pure(root) && named(root,"TypedSort").empty() && g_engine.currval(db,s)==1;
        return values(consume(*f.cursor),{{"2"},{"3"}}) && good && g_engine.currval(db,s)==3 &&
            window->runtimeRows()==3 && window->runtimeLoops()==3;
    });
    check("later COALESCE window cannot guarantee order or run an unused argument",[&]{
        const auto s=sequence();auto f=preparePlanned(db,"SELECT id,COALESCE(7::bigint,lag(nextval('"+s+"'),0) OVER(ORDER BY id)) AS result FROM window_order_heap ORDER BY id LIMIT 1");
        const bool good=pure(f.cursor->plan()) && named(f.cursor->plan(),"TypedSort").size()==1;
        return values(consume(*f.cursor),{{"1","7"}}) && good && g_engine.currval(db,s)==1;
    });
    const bool dropped=g_engine.dropDatabase(db)==DBStatus::OK;setCurrentSession(previous);
    if(controls!=16 || !dropped)++failures;
    std::cout<<"PREPARED_WINDOW_ORDER_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';return failures?1:0;
}
