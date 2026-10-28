#include "Session.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "parser/ast.h"
#include "test_utils.h"
#include <iostream>
#include <string>
#include <vector>
extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string fixture="cte_window_owner_binding",database=testDbPath(fixture);
    cleanupTestDb(fixture);
    if(g_engine.createDatabase(database)!=DBStatus::OK)return 2;
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* previous;~Restore(){setCurrentSession(previous);}} restore{previous};
    DdlExecutor ddl;
    if(ddl.executeSql("CREATE TABLE r318_n(id integer,p text,v integer,s text,n integer,d integer)",session) ||
       ddl.executeSql("CREATE TABLE w(id integer,p text,v text,s text,n integer,d integer)",session))return 2;
    size_t controls=0,failures=0;
    const auto check=[&](bool good,const std::string& label) {
        ++controls;if(!good){++failures;std::cerr<<"CTE_WINDOW_OWNER_BINDING_FAILURE "<<label<<'\n';}
    };
    struct Case {std::string invocation,type;uint32_t oid;};
    for(const Case item:std::vector<Case>{{"lag(v,1,2147483648)","bigint",20},
            {"lead(v,1,2147483648)","bigint",20},{"lag(s,1,s)","text",25},
            {"first_value(v)","integer",23},{"nth_value(v,2)","integer",23},
            {"sum(v)","bigint",20},{"count(v)","bigint",20},
            {"row_number()","bigint",20},{"ntile(3)","integer",23}}) {
        for(const std::string name:{"c","w"}) {
            const std::string body="SELECT "+item.invocation+" OVER(PARTITION BY p ORDER BY id) AS result FROM "+name+" LIMIT 0";
            const std::string sql="WITH "+name+" AS(SELECT id,p,v,s,n,d FROM r318_n) "+body;
            try {
                const auto query=g_engine.prepareBoundQuery(database,sql,{},true);
                check(query.output.size()==1 && query.output.front().type==item.type &&
                    query.output.front().typeOid==item.oid,"whole declaration "+sql);
                const Stmt* root=query.ast.get();
                if(const auto* with=dynamic_cast<const WithStmt*>(root))root=with->statement.get();
                const auto* select=dynamic_cast<const SelectStmt*>(root);
                const auto range=std::find_if(query.sourceRanges.begin(),query.sourceRanges.end(),
                    [&](const auto& value){return value.owner==select;});
                check(select && range!=query.sourceRanges.end() && range->cteStatement &&
                    range->columns.size()==6 && range->columns[2].type=="integer",
                    "actual CTE source contract "+sql);
            }catch(const DbError& error){check(false,"whole declaration "+sql+" state="+error.sqlState());check(false,"source contract missing "+sql);}
            if(name=="c") {
                std::string state;
                try{(void)g_engine.prepareBoundQuery(database,body,{},true);}
                catch(const DbError& error){state=error.sqlState();}
                check(state=="42P01","isolated SQL must lose CTE owner "+body+" state="+state);
            }
        }
    }
    for(const std::string fn:{"lag","lead"}) {
        std::string state;
        try{(void)g_engine.prepareBoundQuery(database,"SELECT "+fn+"(v,1,2147483648) OVER(PARTITION BY p ORDER BY id) FROM w",{},true);}
        catch(const DbError& error){state=error.sqlState();}
        check(state=="42883","isolated SQL chooses unrelated physical w TEXT input "+fn+" state="+state);
    }
    cleanupTestDb(fixture);finalCleanupTestData();
    std::cout<<"CTE_WINDOW_OWNER_BINDING_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
