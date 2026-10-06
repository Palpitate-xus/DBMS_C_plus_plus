#include "commands/TableManage.h"
#include "common/DbError.h"
#include "parser/query_binding.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    const std::string name="insert_conflict_binding",db=testDbPath(name);
    cleanupTestDb(name); assert(g_engine.createDatabase(db)==DBStatus::OK);
    TableSchema table; table.append(makeIntColumn("id",false,2)); table.append(makeIntColumn("v",true,2));
    assert(g_engine.createTable(db,"conflict_rows",table)==DBStatus::OK);
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
        {"ON CONFLICT(id) DO UPDATE SET v=excluded.v+1 WHERE conflict_rows.id=excluded.id RETURNING id,v",""},
        {"ON CONFLICT(id) DO UPDATE SET v=conflict_rows.v+excluded.v RETURNING conflict_rows.v",""},
        {"ON CONFLICT(id) DO UPDATE SET v=excluded.missing","42703"},
        {"ON CONFLICT(id) DO UPDATE SET v=v+1","42702"},
        {"ON CONFLICT(id) DO UPDATE SET v=excluded.v WHERE id=excluded.id","42702"},
        {"ON CONFLICT(id) DO UPDATE SET v=excluded.v RETURNING excluded.v","42P01"},
        {"ON CONFLICT(id) DO NOTHING RETURNING excluded.v","42P01"},
        {"RETURNING excluded.v","42P01"}}) {
        const std::string sql="INSERT INTO conflict_rows VALUES(1,10) "+item.first;
        std::string state;
        try { auto prepared=g_engine.prepareBoundQuery(db,sql); assert(prepared.ast);
              if(!prepared.output.empty()) assert(prepared.output.front().type=="integer");
              for (const auto& range : prepared.sourceRanges)
                  if(range.name=="excluded") {
                      assert(range.relationSchema.empty() && range.relationName.empty());
                      assert(range.columns.size()==2 && range.columns[1].type=="integer");
                  } }
        catch(const DbError& error){state=error.sqlState();}
        if(state!=item.second) std::cerr<<"CONFLICT_BIND "<<sql<<" actual "<<state<<" expected "<<item.second<<'\n';
        assert(state==item.second);
        assert(g_engine.query(db,"conflict_rows",{}, {"id"}).empty());
    }
    assert(g_engine.dropDatabase(db)==DBStatus::OK); cleanupTestDb(name);
    std::cout<<"[INSERT CONFLICT BINDING] qualified transition namespace, target visibility and RETURNING isolation passed\n";
}
