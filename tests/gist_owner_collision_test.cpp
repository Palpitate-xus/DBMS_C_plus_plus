#include "commands/TableManage.h"
#include "commands/DdlExecutor.h"
#include "catalog/type_registry.h"
#include "executor/ExecutionPlan.h"
#include "expression/prepared_query_execution.h"
#include "common/Config.h"
#include "utils/Session.h"
#include "test_utils.h"
#include <iostream>
#include <filesystem>
#include <vector>
extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;
int main() try {
 using namespace dbms;TypeRegistry::instance().bootstrap();g_config.autoAnalyzeEnabled=false;g_config.autoVacuumEnabled=false;g_engine.setBackgroundIntervals(60000,60000);
 const auto db=testDbPath("gist_owner_collision");Session s;s.username="testuser";s.permission=1;s.currentDB=db;auto*old=currentSession();setCurrentSession(&s);struct S{Session*p;~S(){setCurrentSession(p);}} guard{old};DdlExecutor ddl;unsigned fail=0;
 auto need=[](bool b){if(!b)throw std::runtime_error("real collision setup failed");};need(g_engine.createDatabase(db,"utf8")==DBStatus::OK);
 for(const auto&sql:{"CREATE TABLE owner(id integer PRIMARY KEY,a_k text COLLATE \"C\")","CREATE TABLE owner_a(id integer PRIMARY KEY,k text COLLATE \"C\")"})need(!ddl.executeSql(sql,s));
 need(g_engine.insertRow(db,"owner_a",{{"id",std::string("1")},{"k",std::string()}})==DBStatus::OK);need(g_engine.insertRow(db,"owner_a",{{"id",std::string("2")},{"k",std::string("x")}})==DBStatus::OK);
 need(g_engine.insertRow(db,"owner",{{"id",std::string("1")},{"a_k",std::string("z")}})==DBStatus::OK);need(g_engine.insertRow(db,"owner",{{"id",std::string("2")},{"a_k",std::nullopt}})==DBStatus::OK);
 for(const auto&sql:{"CREATE INDEX owner_a_idx ON owner_a USING gist(k)","CREATE INDEX owner_idx ON owner USING gist(a_k)"})need(!ddl.executeSql(sql,s));
 const auto path=g_engine.dbPath(db)/"owner_a_k.gist";need(std::filesystem::is_regular_file(path));auto a=g_engine.getTableSchema(db,"owner_a"),b=g_engine.getTableSchema(db,"owner");std::cout<<"GIST_OWNER_SHARED_PATH "<<path<<" physical_ids="<<a.physicalRelationId<<','<<b.physicalRelationId<<'\n';need(a.physicalRelationId!=b.physicalRelationId);
 for(const auto&value:{std::string(),std::string("x")}){
  const auto expected=value.empty()?"1":"2";
  auto q=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,"SELECT id,k FROM owner_a WHERE k='"+value+"'"));auto plan=QueryPlanner::buildPreparedQueryPlan(&g_engine,db,q,q->ast.get());auto cursor=QueryPlanner::makePreparedCursor(std::move(plan),q->output);std::vector<ExprValue>cells;std::vector<std::string>ids;while(cursor->next(cells)){need(cells.size()==2&&!cells[0].isNull&&!cells[1].isNull);ids.push_back(cells[0].value);need(cells[1].value==value);}cursor->close();const bool good=ids==std::vector<std::string>{expected};fail+=!good;std::cout<<"GIST_OWNER_PREPARED valuebytes="<<value.size()<<" expected="<<expected<<" actual=";for(auto&id:ids)std::cout<<id<<',';std::cout<<" pass="<<good<<'\n';
  std::vector<std::vector<std::string>>rows;std::vector<std::vector<bool>>nulls;std::vector<int64_t>rids;(void)g_engine.query(db,"owner_a",{"=k '"+value+"'"},{"id","k"},{},false,false,false,0,{},&rows,&nulls,&rids);ids.clear();for(auto&row:rows){need(row.size()==2);ids.push_back(row[0]);}const bool ok=ids==std::vector<std::string>{expected};fail+=!ok;std::cout<<"GIST_OWNER_NATIVE valuebytes="<<value.size()<<" expected="<<expected<<" actual=";for(auto&id:ids)std::cout<<id<<',';std::cout<<" pass="<<ok<<'\n';
 }
 std::cout<<"GIST_OWNER_COLLISION failures="<<fail<<'\n';return fail?1:0;
}catch(const std::exception&e){std::cerr<<"GIST_OWNER_COLLISION_FATAL "<<e.what()<<'\n';return 2;}
