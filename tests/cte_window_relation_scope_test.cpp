#include "Session.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "catalog/catalog.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <algorithm>
#include <iostream>
#include <string>
#include <vector>
extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string fixture="cte_window_relation_scope",database=testDbPath(fixture);
    cleanupTestDb(fixture);
    if(g_engine.createDatabase(database)!=DBStatus::OK)return 2;
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore{Session* value;~Restore(){setCurrentSession(value);}}restore{previous};
    DdlExecutor ddl;
    if(ddl.executeSql("CREATE TABLE r318_scope(id integer,p text,v integer,s text,n integer,d integer)",session) ||
       ddl.executeSql("CREATE TABLE c(id integer,p text,v text,s text,n integer,d integer)",session))return 2;
    const auto actual=g_engine.prepareBoundQuery(database,"SELECT * FROM r318_scope",{},true).output;
    size_t controls=0,failures=0,ownedLookups=0;
    const auto check=[&](bool good,const std::string& label){++controls;if(!good){++failures;std::cerr<<"CTE_WINDOW_RELATION_SCOPE_FAILURE "<<label<<'\n';}};
    const auto lookup=[&](const std::string& spelling)->std::optional<QueryRelationMetadata>{
        CatalogManager::QualifiedName name;
        if(!CatalogManager::parseQualifiedName(spelling,name,true) || !name.schema.empty() ||
           (name.name!="c" && name.name!="Mixed cte"))return std::nullopt;
        ++ownedLookups;return QueryRelationMetadata{"",name.name,actual,{}};
    };
    struct Case{std::string invocation,type;uint32_t oid;};
    for(const Case item:std::vector<Case>{{"lag(v,1,2147483648)","bigint",20},
            {"lead(v,1,2147483648)","bigint",20},{"lag(s,1,s)","text",25},
            {"first_value(v)","integer",23},{"nth_value(v,2)","integer",23},
            {"sum(v)","bigint",20},{"count(v)","bigint",20},
            {"row_number()","bigint",20},{"ntile(3)","integer",23}}) {
        const std::string sql="SELECT "+item.invocation+" OVER(PARTITION BY p ORDER BY id) AS result FROM c LIMIT 0";
        try {
            const auto prepared=g_engine.prepareBoundQuery(database,sql,{},true,lookup);
            check(prepared.output.size()==1 && prepared.output[0].type==item.type && prepared.output[0].typeOid==item.oid,"owned declaration "+sql);
            const auto* select=dynamic_cast<const SelectStmt*>(prepared.ast.get());
            const auto range=std::find_if(prepared.sourceRanges.begin(),prepared.sourceRanges.end(),[&](const auto& r){return r.owner==select;});
            check(range!=prepared.sourceRanges.end() && range->columns.size()==6 && range->columns[2].type=="integer","real scoped source contract "+sql);
        }catch(const DbError& e){check(false,sql+" state="+e.sqlState());check(false,"missing owned source "+sql);}
    }
    const auto expect=[&](const std::string& sql,const std::string& type,uint32_t oid,const std::string& state,bool scoped=true){
        try{
            const auto prepared=scoped?g_engine.prepareBoundQuery(database,sql,{},true,lookup):g_engine.prepareBoundQuery(database,sql,{},true);
            check(state.empty() && prepared.output.size()==1 && prepared.output[0].type==type && prepared.output[0].typeOid==oid,sql);
        }catch(const DbError& error){check(!state.empty() && error.sqlState()==state,sql+" state="+error.sqlState());}
    };
    expect("SELECT first_value(v) OVER(ORDER BY id) FROM public.c","text",25,"");
    ownedLookups=0;
    expect("WITH c AS(SELECT id,p,s AS v FROM r318_scope) SELECT first_value(v) OVER(ORDER BY id) FROM c","text",25,"");
    check(ownedLookups==0,"local WITH owns c before inherited relation provider");
    expect("SELECT first_value(v) OVER(ORDER BY id) FROM \"Mixed cte\"","integer",23,"");
    expect("SELECT first_value(v) OVER(ORDER BY id) FROM mixed_cte","",0,"42P01");
    expect("SELECT lag(v,1,2147483648) OVER(ORDER BY id) FROM r318_scope","bigint",20,"");
    expect("SELECT lag(v,1,2147483648) OVER(ORDER BY id) FROM c","",0,"42883",false);
    expect("SELECT first_value(v) OVER(ORDER BY id) FROM c","text",25,"",false);
    expect("SELECT lag(v,1,2,3) OVER(ORDER BY id) FROM c","",0,"42883");
    cleanupTestDb(fixture);finalCleanupTestData();
    std::cout<<"CTE_WINDOW_RELATION_SCOPE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
