#include "catalog/type_registry.h"
#include "catalog/CatalogService.h"
#include "commands/DdlExecutor.h"
#include "common/Config.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <fstream>
#include <iostream>
#include <map>
#include <stdexcept>
#include <sys/wait.h>
#include <unistd.h>
extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;
namespace {
using namespace dbms;
size_t checks=0;
void need(bool good,const char* message){++checks;if(!good)throw std::runtime_error(message);}
std::string bytes(const std::filesystem::path& path){std::ifstream in(path,std::ios::binary);need(bool(in),"actual statistics file");return {std::istreambuf_iterator<char>(in),{}};}
StorageEngine::SqlRow input(int id){return {{"id",std::to_string(id)},{"k",id<128?std::nullopt:std::optional<std::string>(id<928?"7":std::to_string(id+1000))},{"pad",id%17?std::optional<std::string>(id%7?"payload 界":""):std::nullopt}};}
PlanContext context(const std::string& db,const std::string& value){PlanContext c;c.dbname=db;c.tablename="cost_entries";c.conds={{"=","k",value}};return c;}
void result(OpPtr plan,const std::string& value){auto run=QueryPlanner::executePlanChecked(std::move(plan));run.throwIfFailed();need(run.structuredRowsAvailable,"actual complete structured result");std::map<int,StorageEngine::SqlRow> rows;
 for(size_t i=0;i<run.structuredRows.size();++i){const auto& row=run.structuredRows[i];const auto& nulls=run.structuredNulls[i];need(row.size()==3&&nulls.size()==3,"all actual result columns and NULL metadata");const int id=std::stoi(row[0]);need(rows.emplace(id,StorageEngine::SqlRow{}).second,"each matching real row once");const auto expected=input(id);for(size_t c=0;c<3;++c){const auto name=std::vector<std::string>{"id","k","pad"}[c];const auto& wanted=expected.at(name);need(nulls[c]==!wanted&&(!wanted||row[c]==*wanted),"all selected values and NULL flags exact");}}
 std::map<int,StorageEngine::SqlRow> expected;for(int id=0;id<1024;++id){const auto k=input(id).at("k");if(k&&std::stoi(*k)==std::stoi(value))expected.emplace(id,StorageEngine::SqlRow{});}need(rows==expected,"complete actual qualifying population");
}
double forceSequential(const std::string& strategy,double,double,bool){return strategy=="seq_scan"?0:strategy=="index_scan"?1e9:-1;}
std::map<std::string,std::string> files(const std::filesystem::path& directory){std::map<std::string,std::string> out;for(const auto& file:std::filesystem::recursive_directory_iterator(directory))if(file.is_regular_file())out.emplace(file.path().lexically_relative(directory).string(),bytes(file.path()));return out;}
}
int main(int argc,char** argv){try{TypeRegistry::instance().bootstrap();const auto db=argc==3?std::string(argv[2]):testDbPath("equality_scan_cost");Session session;session.username="admin";session.permission=1;session.currentDB=db;struct Scope{Session* previous;bool automatic;QueryPlanner::CostModel cost;~Scope(){setCurrentSession(previous);g_config.autoAnalyzeEnabled=automatic;QueryPlanner::setCostModel(cost);}}scope{currentSession(),g_config.autoAnalyzeEnabled,QueryPlanner::costModel()};setCurrentSession(&session);g_config.autoAnalyzeEnabled=false;QueryPlanner::setCostModel({});
 if(argc==3){need(std::string(argv[1])=="--cold","actual cold invocation");g_engine.catalogService().evict(db);const auto before=files(g_engine.dbPath(db));auto hot=QueryPlanner::buildSelectPlan(&g_engine,context(db,"7"));need(QueryPlanner::explain(hot,&g_engine,db).find("TableScan(")!=std::string::npos,"cold persisted MCV chooses sequential access");auto cold=QueryPlanner::buildSelectPlan(&g_engine,context(db,"2023"));need(QueryPlanner::explain(cold,&g_engine,db).find("IndexScan(")!=std::string::npos,"cold persisted sparse value chooses index");need(!g_engine.catalogService().has(db),"cold physical planning does not bootstrap a mutable catalog");need(files(g_engine.dbPath(db))==before,"cold plan/EXPLAIN reads do not write any database file");std::cout<<"EQUALITY_SCAN_COST_COLD checks="<<checks<<" failures=0\n";return 0;}
 need(argc==1,"actual ordinary invocation");need(g_engine.createDatabase(db,"utf8")==DBStatus::OK,"real database creation");DdlExecutor ddl;need(!ddl.executeSql("CREATE TABLE cost_entries(id integer PRIMARY KEY,k integer,pad text)",session),"actual source DDL");need(g_engine.beginTransaction(db)==DBStatus::OK,"real seed transaction");for(int i=0;i<1024;++i)need(g_engine.insertRow(db,"cost_entries",input((i*37)%1024))==DBStatus::OK,"actual stored source population");need(g_engine.commitTransaction()==DBStatus::OK,"real seed commit");need(!ddl.executeSql("CREATE INDEX cost_entries_k ON cost_entries(k)",session),"actual secondary B-tree");need(g_engine.analyzeTable(db,"cost_entries"),"real ANALYZE");const auto legacy=bytes(g_engine.dbPath(db)/".stats"),typed=bytes(g_engine.dbPath(db)/".analyzed_statistics");
 auto hot=QueryPlanner::buildSelectPlan(&g_engine,context(db,"7"));const auto hotExplain=QueryPlanner::explain(hot,&g_engine,db);std::cout<<"EQUALITY_SCAN_HOT_PLAN\n"<<hotExplain;
 need(hotExplain.find("TableScan(")!=std::string::npos&&hotExplain.find("IndexScan(")==std::string::npos,"hot MCV chooses cheaper actual sequential path");result(std::move(hot),"7");
 for(const auto& probe:{"2023","42"}){auto cold=QueryPlanner::buildSelectPlan(&g_engine,context(db,probe));const auto text=QueryPlanner::explain(cold,&g_engine,db);std::cout<<"EQUALITY_SCAN_COLD_PLAN "<<probe<<'\n'<<text;need(text.find("IndexScan(")!=std::string::npos,"rare and unseen values retain selective B-tree access");result(std::move(cold),probe);}
 auto aliases=QueryPlanner::buildSelectPlan(&g_engine,context(db,"0007"));need(QueryPlanner::explain(aliases,&g_engine,db).find("TableScan(")!=std::string::npos,"typed integer equality matches alternate MCV spelling");result(std::move(aliases),"7");
 auto costs=QueryPlanner::costModel();costs.randomPageCost=0.000001;QueryPlanner::setCostModel(costs);hot=QueryPlanner::buildSelectPlan(&g_engine,context(db,"7"));const auto indexedHot=QueryPlanner::explain(hot,&g_engine,db);std::cout<<"EQUALITY_SCAN_LOW_RANDOM_COST_PLAN\n"<<indexedHot;need(indexedHot.find("IndexScan(")!=std::string::npos&&indexedHot.find("rows=800")!=std::string::npos,"real scan cost GUC changes access choice and MCV row estimate");need(QueryPlanner::explainJson(hot,&g_engine,db).find("\"rows\":800")!=std::string::npos,"JSON uses same actual hot equality estimate");result(std::move(hot),"7");
 costs={};costs.customCost=&forceSequential;QueryPlanner::setCostModel(costs);auto forced=QueryPlanner::buildSelectPlan(&g_engine,context(db,"2023"));need(QueryPlanner::explain(forced,&g_engine,db).find("TableScan(")!=std::string::npos,"actual scan custom hook controls ordinary access path");result(std::move(forced),"2023");QueryPlanner::setCostModel({});
 need(bytes(g_engine.dbPath(db)/".stats")==legacy&&bytes(g_engine.dbPath(db)/".analyzed_statistics")==typed,"planning and read execution preserve both actual statistics files");need(g_engine.catalogService().persistAll(),"persist actual catalog for cold exec");const auto child=::fork();need(child>=0,"independent cold fork");if(!child){::execl(argv[0],argv[0],"--cold",db.c_str(),nullptr);_exit(127);}int status=0;need(::waitpid(child,&status,0)==child&&WIFEXITED(status)&&WEXITSTATUS(status)==0,"actual cold exec normal0");std::cout<<"EQUALITY_SCAN_COST checks="<<checks<<" failures=0\n";return 0;
}catch(const std::exception& e){std::cerr<<"EQUALITY_SCAN_COST_FAIL checks="<<checks<<' '<<e.what()<<'\n';return 1;}}
