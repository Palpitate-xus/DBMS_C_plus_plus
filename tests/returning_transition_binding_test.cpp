#include "commands/TableManage.h"
#include "common/DbError.h"
#include "parser/query_binding.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    const std::string name="returning_transition_binding",db=testDbPath(name);
    cleanupTestDb(name); assert(g_engine.createDatabase(db)==DBStatus::OK);
    TableSchema table; table.append(makeIntColumn("id",false,2)); table.append(makeIntColumn("v",true,2));
    assert(g_engine.createTable(db,"returning_rows",table)==DBStatus::OK);
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
        {"INSERT INTO returning_rows VALUES(1,10) RETURNING old.id,new.id,id",""},
        {"INSERT INTO returning_rows VALUES(1,10) RETURNING old.*,new.*,id",""},
        {"INSERT INTO returning_rows VALUES(1,10) RETURNING WITH(OLD AS before_row,NEW AS after_row) before_row.id,after_row.id,id",""},
        {"UPDATE returning_rows SET v=v+1 RETURNING old.v,new.v,v",""},
        {"DELETE FROM returning_rows RETURNING old.v,new.v,v",""},
        {"UPDATE returning_rows SET v=v+1 RETURNING WITH(OLD AS new) new.v",""},
        {"UPDATE returning_rows SET v=v+1 RETURNING WITH(NEW AS old) old.v",""},
        {"UPDATE returning_rows SET v=v+1 RETURNING WITH(OLD AS o,NEW AS \"O\") o.v,\"O\".v",""},
        {"UPDATE returning_rows AS old SET v=v+1 RETURNING old.v,new.v",""},
        {"UPDATE returning_rows AS old SET v=v+1 RETURNING WITH(OLD AS old) old.v","42712"},
        {"INSERT INTO returning_rows VALUES(1,10) RETURNING WITH(OLD AS same_row,NEW AS same_row) same_row.id","42712"},
        {"INSERT INTO returning_rows VALUES(1,10) RETURNING WITH(OLD AS before_row) old.id","42P01"},
        {"INSERT INTO returning_rows VALUES(old.id,10)","42P01"},
        {"UPDATE returning_rows SET v=old.v","42P01"},
        {"DELETE FROM returning_rows WHERE new.v=1","42P01"},
        {"INSERT INTO returning_rows VALUES(1,10) RETURNING old.missing","42703"},
        {"UPDATE returning_rows SET v=v+1 RETURNING new.missing","42703"}}) {
        std::string state;
        try { auto prepared=g_engine.prepareBoundQuery(db,item.first); assert(prepared.ast);
              for(const auto& output:prepared.output) assert(output.type=="integer");
              for(const auto& range:prepared.sourceRanges)
                  if(range.name=="old" || range.name=="new" || range.name=="before_row" || range.name=="after_row")
                      if(range.relationName.empty()) assert(range.relationSchema.empty() && range.columns.size()==2); }
        catch(const DbError& error){state=error.sqlState();}
        if(state!=item.second) std::cerr<<"RETURNING_BIND "<<item.first<<" actual "<<state<<" expected "<<item.second<<'\n';
        assert(state==item.second);
        assert(g_engine.query(db,"returning_rows",{}, {"id"}).empty());
    }
    assert(g_engine.dropDatabase(db)==DBStatus::OK); cleanupTestDb(name);
    std::cout<<"[RETURNING TRANSITION BINDING] descriptor-only OLD/NEW aliases, star visibility and isolation passed\n";
}
