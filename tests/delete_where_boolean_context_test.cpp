#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/expr_helper.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    const std::string name="delete_where_boolean_context",db=testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db)==DBStatus::OK);
    TableSchema table{};table.tablename="target";table.len=2;
    table.cols[0].dataName="id";table.cols[0].dataType="int";
    table.cols[1].dataName="flag";table.cols[1].dataType="text";
    assert(g_engine.createTable(db,table)==DBStatus::OK);
    assert(g_engine.createSequence(db,"effects")==DBStatus::OK);
    const std::vector<std::pair<std::string,std::string>> controls{
        {"DELETE FROM target WHERE 1","42804"},
        {"DELETE FROM target WHERE NULL::TEXT","42804"},
        {"DELETE FROM target WHERE flag","42804"},
        {"DELETE FROM target WHERE 'bad'","22P02"},
        {"WITH p AS(SELECT 1) DELETE FROM target WHERE 1","42804"},
        {"WITH p AS(SELECT 1) DELETE FROM target WHERE 'bad'","22P02"},
        {"DELETE FROM target WHERE missing_delete_function(1)=1","42883"},
        {"DELETE FROM target WHERE missing_delete_column=1","42703"},
        {"DELETE FROM target WHERE 1 RETURNING missing_delete_function(1)","42804"},
        {"DELETE FROM target WHERE 'bad' RETURNING missing_delete_function(1)","22P02"},
        {"DELETE FROM target WHERE 'true'",""},
        {"DELETE FROM target WHERE 'false'",""},
        {"DELETE FROM target WHERE NULL",""},
        {"DELETE FROM target WHERE true",""},
        {"DELETE FROM target WHERE id=nextval('effects')",""},
    };
    size_t failures=0;
    for(const auto& control:controls) {
        std::string state;
        try {
            const auto prepared=g_engine.prepareBoundQuery(db,control.first);
            const auto* statement=prepared.ast.get();
            if(const auto* with=dynamic_cast<const WithStmt*>(statement))statement=with->statement.get();
            const auto* remove=dynamic_cast<const DeleteStmt*>(statement);
            assert(remove && remove->whereClause);
            if(control.second.empty() &&
               ExprHelper::inferParsedResultType(remove->whereClause.get(),{},db,&g_engine)!="boolean")
                ++failures;
        } catch(const DbError& error) {state=error.sqlState();}
        std::cout<<control.first<<" actual "<<state<<" expected "<<control.second<<std::endl;
        if(state!=control.second)++failures;
    }
    assert(g_engine.nextval(db,"effects")==1);
    std::cout<<"DELETE BOOLEAN failures "<<failures<<std::endl;
    assert(failures==0);
    assert(g_engine.dropDatabase(db)==DBStatus::OK);
    cleanupTestDb(name);
}
