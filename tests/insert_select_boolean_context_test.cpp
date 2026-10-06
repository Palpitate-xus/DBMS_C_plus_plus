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
    const std::string name="insert_select_boolean_context",db=testDbPath(name);
    cleanupTestDb(name); assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session; session.username="admin"; session.permission=1; session.currentDB=db;
    DdlExecutor ddl; assert(!ddl.executeSql("CREATE TABLE bool_input(id INT)",session));
    assert(!ddl.executeSql("CREATE TABLE bool_source(id INT)",session));
    assert(g_engine.insertRow(db,"bool_source",{{"id","10"}})==DBStatus::OK);
    assert(g_engine.insertRow(db,"bool_source",{{"id","20"}})==DBStatus::OK);
    assert(g_engine.createSequence(db,"bool_input_effects",1,1)==DBStatus::OK);
    assert(g_engine.createUDF(db,"bool_input_writer",{"p"},{"integer"},
        "BEGIN PERFORM nextval('bool_input_effects'); RETURN p; END;",'v',"plpgsql","integer")==DBStatus::OK);
    size_t failures=0;
    for(const auto& control:std::vector<std::pair<std::string,std::string>>{
        {"SELECT bool_input_writer(2) WHERE 'bad'","22P02"},
        {"SELECT bool_input_writer(2) WHERE 1","42804"},
        {"SELECT bool_input_writer(2) WHERE NULL::TEXT","42804"},
        {"SELECT bool_input_writer(id) FROM bool_source WHERE 'bad'","22P02"}}) {
        const auto sql="INSERT INTO bool_input "+control.first;
        clearLastDmlResult(); bool handled=false; std::string state;
        try { const bool failed=tryDmlBridge(sql,SQLParser::classify(sql),session,handled,sql);
            if(failed) state="unstructured error"; else if(!handled) state="legacy fallback";
        } catch(const DbError& error){state=error.sqlState();}
        std::cout<<"INSERT SELECT BOOL "<<sql<<" actual="<<state<<" expected="<<control.second<<std::endl;
        if(state!=control.second)++failures;
        assert(!takeLastDmlResult().available);
        assert(g_engine.query(db,"bool_input",{}, {"id"}).empty());
    }
    assert(g_engine.nextval(db,"bool_input_effects")==1);
    for(const auto& control:std::vector<std::pair<std::string,size_t>>{
        {"SELECT '3' WHERE 'true'",1}, {"SELECT '4' WHERE 'false'",0},
        {"SELECT '4' WHERE NULL",0}, {"SELECT '5' WHERE '1'",1},
        {"SELECT id FROM bool_source WHERE 'true'",2},
        {"SELECT id FROM bool_source WHERE 'false'",0}}) {
        const auto sql="INSERT INTO bool_input "+control.first+" RETURNING id";
        clearLastDmlResult(); bool handled=false;
        const bool failed=tryDmlBridge(sql,SQLParser::classify(sql),session,handled,sql);
        const auto output=takeLastDmlResult();
        const bool valid=!failed && handled && output.available && output.rows.size()==control.second &&
            output.columnTypes.size()==1 &&
            ExprHelper::canonicalResultTypeName(output.columnTypes.front())=="integer" &&
            output.commandTag=="INSERT 0 "+std::to_string(control.second);
        std::cout<<"INSERT SELECT BOOL "<<sql<<" handled="<<handled<<" expected rows="<<control.second<<std::endl;
        if(!valid)++failures;
    }
    assert(failures==0);
    cleanupTestDb(name);
    std::cout<<"[INSERT SELECT BOOLEAN CONTEXT] passed\n";
}
