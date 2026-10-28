#include "parser/query_binding.h"
#include "common/DbError.h"
#include <iostream>
#include <string>
#include <vector>
#ifndef DBMS_R318_MODEL_MERGED_METADATA
#include "Session.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"
extern dbms::StorageEngine g_engine;
static bool engineViewScope(const std::function<void(bool,const std::string&)>& check) {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string fixture="cte_window_view_scope",database=testDbPath(fixture);
    cleanupTestDb(fixture);if(g_engine.createDatabase(database)!=DBStatus::OK)return false;
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore{Session* value;~Restore(){setCurrentSession(value);}}restore{previous};
    DdlExecutor ddl;
    if(ddl.executeSql("CREATE TABLE qsrc(v integer)",session) ||
       ddl.executeSql("CREATE TABLE c(v text)",session) ||
       ddl.executeSql("CREATE VIEW saved_view AS SELECT v FROM c",session))return false;
    const auto actual=g_engine.prepareBoundQuery(database,"SELECT * FROM qsrc",{},true).output;
    size_t hits=0;
    const auto lookup=[&](const std::string& name)->std::optional<QueryRelationMetadata>{
        if(name!="c")return std::nullopt;
        ++hits;return QueryRelationMetadata{"","c",actual,{}};
    };
    const auto query=g_engine.prepareBoundQuery(database,"SELECT first_value(v) OVER() FROM saved_view",{},true,lookup);
    check(query.output.size()==1 && query.output[0].type=="text" && query.output[0].typeOid==25,"StorageEngine view window declaration TEXT");
    const auto source=std::find_if(query.sourceRanges.begin(),query.sourceRanges.end(),[](const auto& r){return bool(r.viewQuery);});
    check(source!=query.sourceRanges.end() && source->viewQuery->output.size()==1 && source->viewQuery->output[0].type=="text" &&
        source->viewQuery->output[0].typeOid==25,"StorageEngine retained view child remains TEXT");
    check(hits==0,"StorageEngine query-local callback cannot enter stored view namespace");
    const auto owned=g_engine.prepareBoundQuery(database,"SELECT first_value(v) OVER() FROM c",{},true,lookup);
    check(owned.output.size()==1 && owned.output[0].type=="integer" && owned.output[0].typeOid==23,"StorageEngine actual lexical source remains INTEGER");
    const auto later=g_engine.prepareBoundQuery(database,"SELECT first_value(v) OVER() FROM c",{},true);
    check(later.output.size()==1 && later.output[0].type=="text" && later.output[0].typeOid==25,"StorageEngine later call remains catalog TEXT");
    cleanupTestDb(fixture);finalCleanupTestData();return true;
}
#endif

int main() {
    using namespace dbms;
    size_t controls=0,failures=0,ownedHits=0;
    const auto check=[&](bool good,const std::string& label){++controls;if(!good){++failures;std::cerr<<"CTE_WINDOW_VIEW_SCOPE_FAILURE "<<label<<'\n';}};
    const QueryRowDescriptor text={{"v","text",false,0,25,-1}};
    const QueryRowDescriptor integer={{"v","integer",false,0,23,-1}};
    const auto catalog=[&](const std::string& name)->QueryRelationMetadata {
        if(name=="c" || name=="public.c")return {"public","c",text,{}};
        if(name=="saved_view")return {"public","saved_view",text,"SELECT v FROM c"};
        if(name=="outer_view")return {"public","outer_view",text,"SELECT v FROM saved_view"};
        throw DbError("42P01","missing model catalog relation: "+name);
    };
    const auto local=[&](const std::string& name)->std::optional<QueryRelationMetadata>{
        if(name!="c")return std::nullopt;
        ++ownedHits;return QueryRelationMetadata{"","c",integer,{}};
    };
    QueryBindingMetadata metadata;
#ifdef DBMS_R318_MODEL_MERGED_METADATA
    // Exact V1 provider composition, reproduced against unchanged accepted
    // binder objects. This is a component model, not a server acceptance run.
    metadata.relation=[&](const std::string& name){auto owned=local(name);return owned?*owned:catalog(name);};
#else
    metadata.relation=catalog;
    metadata.queryRelation=local;
#endif
    const auto typed=[&](const PreparedQuery& q,const std::string& type,uint32_t oid){return q.output.size()==1 && q.output[0].type==type && q.output[0].typeOid==oid;};
    for(const std::string sql:{"SELECT v FROM saved_view","SELECT v FROM outer_view",
            "WITH c AS(SELECT v FROM public.c) SELECT v FROM saved_view"}) {
        ownedHits=0;
        const auto query=prepareQuery(sql,{},metadata);
        check(typed(query,"text",25),"outer view declaration "+sql);
        std::function<void(const PreparedQuery&)> children;
        children=[&](const PreparedQuery& parent){
            for(const auto& range:parent.sourceRanges)if(range.viewQuery){
                check(typed(*range.viewQuery,"text",25),"independent stored view child "+sql);
                children(*range.viewQuery);
            }
        };
        children(query);
        check(ownedHits==0,"view metadata must not consult caller's query-local c "+sql);
    }
    check(typed(prepareQuery("SELECT v FROM c",{},metadata),"integer",23),"real lexical query source still inherited");
    check(typed(prepareQuery("SELECT v FROM public.c",{},metadata),"text",25),"qualified catalog source bypasses lexical c");
    QueryBindingMetadata plain;plain.relation=catalog;
    check(typed(prepareQuery("SELECT v FROM c",{},plain),"text",25),"query-local provider never leaks to later preparation");
    check(typed(prepareQuery("WITH c AS(SELECT v FROM public.c) SELECT v FROM c",{},metadata),"text",25),"local WITH retains its real declaration owner");
#ifndef DBMS_R318_MODEL_MERGED_METADATA
    if(!engineViewScope(check))return 2;
#endif
    std::cout<<"CTE_WINDOW_VIEW_SCOPE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
