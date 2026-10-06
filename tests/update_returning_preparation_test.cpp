#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="update_returning_preparation",db=testDbPath(name);
    cleanupTestDb(name); assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session; session.username="admin"; session.permission=1; session.currentDB=db;
    DdlExecutor ddl; assert(!ddl.executeSql("CREATE TABLE returning_input(id INT)",session));
    assert(g_engine.insertRow(db,"returning_input",{{"id","1"}})==DBStatus::OK);
    assert(g_engine.createSequence(db,"returning_input_effects",1,1)==DBStatus::OK);
    assert(g_engine.createUDF(db,"returning_input_writer",{"p"},{"integer"},
        "BEGIN PERFORM nextval('returning_input_effects'); RETURN p; END;",'v',"plpgsql","integer")==DBStatus::OK);
    const std::vector<std::pair<std::string,std::string>> controls={
        {"UPDATE returning_input SET id='bad' RETURNING missing_returning_input(1)","42883"},
        {"UPDATE returning_input SET id=returning_input_writer(2) RETURNING missing_returning_input(1)","42883"},
        {"UPDATE returning_input SET id=2 WHERE false RETURNING missing_returning_input(1)","42883"},
        {"UPDATE returning_input SET id=2 RETURNING missing_returning_column","42703"},
        {"UPDATE returning_input SET id='bad' RETURNING id","22P02"},
    };
    size_t failures=0;
    for(const auto& control:controls) {
        clearLastDmlResult(); bool handled=false; std::string state;
        try {
            const bool failed=tryDmlBridge(control.first,SQLParser::classify(control.first),session,handled,control.first);
            if(failed) state="unstructured error";
            else if(!handled) state="legacy fallback";
        } catch(const DbError& error) { state=error.sqlState(); }
        std::cout<<"UPDATE RETURNING PREPARE "<<control.first<<" actual="<<state<<" expected="<<control.second<<std::endl;
        if(state!=control.second) ++failures;
        assert(!takeLastDmlResult().available);
        assert(g_engine.query(db,"returning_input",{}, {"id"})==std::vector<std::string>{"1 "});
    }
    assert(g_engine.nextval(db,"returning_input_effects")==1);
    assert(failures==0);
    const std::string valid="UPDATE returning_input SET id='2' RETURNING id";
    bool handled=false; assert(!tryDmlBridge(valid,SQLParser::classify(valid),session,handled,valid));
    assert(handled);
    const auto output=takeLastDmlResult();
    assert(output.available && output.commandTag=="UPDATE 1");
    assert(output.columnTypes.size()==1);
    assert(ExprHelper::canonicalResultTypeName(output.columnTypes.front())=="integer");
    assert(output.rows==std::vector<std::vector<std::string>>{{"2"}});
    cleanupTestDb(name);
    std::cout<<"[UPDATE RETURNING PREPARATION] passed\n";
}
