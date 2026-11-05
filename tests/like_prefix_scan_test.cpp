#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "expression/SqlPattern.h"
#include "catalog/type_registry.h"
#include "utils/Session.h"
#include "test_utils.h"
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    Session session;session.username="testuser";session.permission=1;
    session.currentDB=testDbPath("like_prefix_scan");
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* prior;~Restore(){setCurrentSession(prior);}} restore{previous};
    size_t checked=0,failed=0;
    const auto require=[&](bool pass,const std::string& role) {
        ++checked;if(!pass){++failed;std::cerr<<"LIKE_PREFIX_SCAN_FAILED "<<role<<'\n';}
    };
    require(g_engine.createDatabase(session.currentDB,"utf8")==DBStatus::OK,"create real database");
    DdlExecutor ddl;
    require(!ddl.executeSql("CREATE TABLE like_native(id integer PRIMARY KEY,tag text,pad text)",session),"create real heap");
    const std::vector<std::optional<std::string>> tags{
        "a","ab","a界","a界tail","aZtail","a_x","a%x","a\\x","b",{},"","NULL",
        "界","界尾","界\ntail","界x","hiu","hitTail","hit","~界","~","a\nnewline","a'quote","other",
        "ÿx","ÿ","߿尾","􏿿"};
    for(size_t i=0;i<tags.size();++i) {
        StorageEngine::SqlRow row{{"id",std::to_string(i+1)},{"tag",tags[i]},
            {"pad",i%3?std::optional<std::string>("payload 界\n"+std::to_string(i)):std::nullopt}};
        require(g_engine.insertRow(session.currentDB,"like_native",row)==DBStatus::OK,"insert complete row");
    }
    const auto build=[&](const std::string& predicate) {
        auto prepared=g_engine.prepareBoundQuery(session.currentDB,
            "SELECT id,tag,pad FROM like_native WHERE "+predicate+" ORDER BY id");
        return QueryPlanner::buildPreparedSelectPlan(&g_engine,session.currentDB,"like_native",std::move(prepared));
    };
    const auto read=[&](OpPtr plan) {
        auto result=QueryPlanner::executePlanChecked(std::move(plan));
        result.throwIfFailed();require(result.ok&&result.structuredRowsAvailable,"full structured publication");
        return result;
    };
    const std::vector<std::string> predicates{
        "tag LIKE 'a%'","tag LIKE '界%'","tag LIKE 'a_%'","tag LIKE 'a%tail'",
        "tag LIKE 'a\\_%'","tag LIKE 'a!_%' ESCAPE '!'","tag LIKE 'a界_%' ESCAPE '界'",
        "tag LIKE 'a\\_%' ESCAPE ''","tag LIKE 'a'","tag LIKE 'hit%'","tag LIKE '~%'",
        "tag LIKE 'a%' AND id>3","tag LIKE 'a%' OR id=9","tag ILIKE 'a%'",
        "tag NOT LIKE 'a%'","tag LIKE '%a'","tag LIKE '_%'","tag LIKE '%'","tag LIKE NULL",
        "tag LIKE 'ÿ%'","tag LIKE '߿%'","tag LIKE '􏿿%'"};
    std::vector<PlanExecutionResult> heap;
    for(const auto& predicate:predicates)heap.push_back(read(build(predicate)));
    require(!ddl.executeSql("CREATE INDEX like_native_gist ON like_native USING gist(tag)",session),"create actual GiST");
    for(size_t i=0;i<predicates.size();++i) {
        auto plan=build(predicates[i]);
        if(i<12) {
            require(QueryPlanner::explain(plan,&g_engine,session.currentDB).find("GiSTScan")!=std::string::npos,
                "real prefix source "+predicates[i]);
            require(QueryPlanner::explainJson(plan,&g_engine,session.currentDB).find("\"nodeType\":\"GiSTScan\"")!=std::string::npos,
                "JSON real GiST source "+predicates[i]);
        }
        bool validJsonText=true;
        try { (void)sql_pattern::characters(QueryPlanner::explainJson(plan,&g_engine,session.currentDB)); }
        catch(const DbError&){validJsonText=false;}
        require(validJsonText,"JSON index bounds remain valid UTF8 "+predicates[i]);
        auto result=read(std::move(plan));
        require(result.structuredRows==heap[i].structuredRows &&
            result.structuredNulls==heap[i].structuredNulls,"indexed / heap all values and NULL "+predicates[i]);
        for(size_t r=0;r<result.structuredRows.size();++r)
            require(result.structuredRows[r].size()==3&&result.structuredNulls[r].size()==3,"each full cell/NULL vector");
    }
    // The native API and ordinary EXPLAIN also use GiST, with the same
    // wildcard/escape interpretation rather than treating '_' as literal.
    const std::vector<std::string> patterns{
        "a%","界%","a_%","a%tail","a\\_%","a!_%","a界_%","a\\_%","a","hit%","~%"};
    for(size_t i=0;i<patterns.size();++i) {
        PlanContext context;context.dbname=session.currentDB;context.tablename="like_native";
        context.orderByCol="id";
        StorageEngine::Condition condition{"like","tag",patterns[i]};
        condition.decodedLiteralRhs=true;condition.patternType="text";
        if(i==5)condition.patternEscape="!";
        if(i==6)condition.patternEscape="界";
        if(i==7)condition.patternEscape="";
        context.conds.push_back(std::move(condition));
        auto result=read(QueryPlanner::buildSelectPlan(&g_engine,context));
        require(result.structuredRows==heap[i].structuredRows&&
            result.structuredNulls==heap[i].structuredNulls,"native LIKE all data "+patterns[i]);
    }
    // A plan is built while the index exists, then opened after genuine DDL.
    // It must read the visible heap rather than mistake "not probed" for an
    // empty successful index result.
    require(g_engine.analyzeTable(session.currentDB,"like_native"),"actual prefix density statistics");
    const auto savedCost=QueryPlanner::costModel();
    auto normalCost=savedCost;normalCost.randomPageCost=4;normalCost.seqPageCost=1;
    QueryPlanner::setCostModel(normalCost);
    auto dense=build("tag LIKE 'a%'");
    require(QueryPlanner::explain(dense,&g_engine,session.currentDB).find("GiSTScan")==std::string::npos,
        "dense analyzed prefix retains heap");
    auto denseRows=read(std::move(dense));
    require(denseRows.structuredRows==heap[0].structuredRows&&
        denseRows.structuredNulls==heap[0].structuredNulls,"dense path keeps every value");
    normalCost.randomPageCost=0.000001;QueryPlanner::setCostModel(normalCost);
    auto stale=build("tag LIKE 'a%'");
    require(QueryPlanner::explain(stale,&g_engine,session.currentDB).find("GiSTScan")!=std::string::npos,
        "actual live cost override permits prefix index");
    QueryPlanner::setCostModel(savedCost);
    require(!ddl.executeSql("DROP INDEX like_native_gist",session),"drop before source open");
    auto dropped=read(std::move(stale));
    require(dropped.structuredRows==heap[0].structuredRows&&
        dropped.structuredNulls==heap[0].structuredNulls,"index disappeared before open preserves complete result");
    std::cout<<"LIKE_PREFIX_SCAN_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    return failed?1:0;
}
