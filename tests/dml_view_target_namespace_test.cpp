#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "parser/parser.h"
#include "process/OutputCapture.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="dml_view_target_namespace",db=testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session;session.username="admin";session.permission=1;
    session.currentDB=db;session.pid=4242;
    DdlExecutor ddl;
    const auto setup=[&](const std::string& sql){assert(!ddl.executeSql(sql,session));};
    setup("CREATE TABLE base(id INT)");
    assert(g_engine.insert(db,"base",{{"id","1"}})==DBStatus::OK);
    setup("CREATE SCHEMA reporting");
    setup("CREATE VIEW public.session_view AS SELECT id FROM public.base");
    setup("CREATE VIEW reporting.session_view AS SELECT id FROM public.base");
    setup("CREATE VIEW reporting.\"CaseView\" AS SELECT id FROM public.base");
    setup("CREATE VIEW public.table_shadow AS SELECT id FROM public.base");
    setup("CREATE TABLE reporting.table_shadow(id INT)");
    setup("CREATE VIEW public.temp_shadow AS SELECT id FROM public.base");
    setup("CREATE TEMP TABLE temp_shadow(id INT)");
    const auto initialBase=g_engine.query(db,"base",{},{"id"});
    assert(initialBase.size()==1);
    session.searchPath="reporting, public";
    Session otherCaller;otherCaller.searchPath="public";
    Session* previous=currentSession();setCurrentSession(&otherCaller);
    size_t failures=0;
    const auto check=[&](const std::string& sql,bool expectedHandled,bool expectedUnsupported=false){
        bool handled=true,error=true;
        std::string state;
        std::ostringstream output;
        try {ScopedOutputCapture capture(output);error=tryDmlBridge(sql,SQLParser::classify(sql),session,handled,sql);}
        catch(const DbError& e){state=e.sqlState();}
        const auto result=takeLastDmlResult();
        const bool correct=state.empty()&&error==expectedUnsupported&&handled==expectedHandled&&
            (!expectedUnsupported||output.str().find("SQLSTATE 0A000")!=std::string::npos)&&
            currentSession()==&otherCaller&&
            (expectedHandled||(!result.available&&result.rows.empty()&&result.commandTag.empty()));
        if(!correct){++failures;std::cerr<<"[VIEW TARGET FAIL] "<<sql<<" handled="<<handled<<" error="<<error<<" state="<<state<<'\n';}
        else std::cout<<"[VIEW TARGET PASS] "<<sql<<'\n';
    };
    for(const std::string target:{"public.session_view","reporting.session_view","session_view","reporting.\"CaseView\""}) {
        check("INSERT INTO "+target+" VALUES(9)",false);
        check("UPDATE "+target+" SET id=9",false);
        check("DELETE FROM "+target,false);
        check("UPDATE "+target+" SET id=DEFAULT",false);
        // These source shapes have an explicit existing unsupported boundary.
        // Namespace spelling must preserve that exact 0A000/no-effects route,
        // not turn it into a missing-table error or unsafe legacy mutation.
        check("UPDATE "+target+" SET id=s.id FROM base s WHERE true",true,true);
        check("UPDATE "+target+" SET id=s.id FROM base s JOIN base b ON s.id=b.id WHERE true",true,true);
        check("DELETE FROM "+target+" USING base s WHERE true",true,true);
        check("DELETE FROM "+target+" USING base s JOIN base b ON s.id=b.id WHERE true",true,true);
    }
    // A same-named public view must not divert the resolved real table.
    check("INSERT INTO table_shadow VALUES(7)",true);
    check("UPDATE table_shadow SET id=8",true);
    check("DELETE FROM table_shadow",true);
    check("INSERT INTO temp_shadow VALUES(7)",true);
    check("UPDATE temp_shadow SET id=8",true);
    check("DELETE FROM temp_shadow",true);
    assert(g_engine.query(db,"base",{},{"id"})==initialBase);
    assert(g_engine.query(db,"reporting__table_shadow",{},{"id"}).empty());
    assert(g_engine.query(db,tempTablePrefix(session,"temp_shadow"),{},{"id"}).empty());
    assert(currentSession()==&otherCaller);setCurrentSession(previous);
    assert(!g_engine.inTransaction());
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    if(failures){std::cerr<<"[VIEW TARGET NAMESPACE] failures="<<failures<<'\n';return 1;}
    std::cout<<"[VIEW TARGET NAMESPACE] 38 dispatch/caller/no-effects controls passed\n";
}
