// Real DDL/heap/ANALYZE integration. No product helper clone or fabricated RID.
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "common/Config.h"
#include "executor/ExecutionPlan.h"
#include "expression/prepared_query_execution.h"
#include "parser/parser.h"
#include "Session.h"
#include "test_utils.h"
#include <algorithm>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;
namespace {
using namespace dbms;
using Cell=std::optional<std::string>;
using Rows=std::vector<std::vector<Cell>>;
void require(bool good,const char* message){if(!good)throw std::runtime_error(message);}
bool mutate(const std::string& sql,Session& session){
    bool handled=false;clearLastDmlResult();std::ostringstream captured;
    struct Output {std::streambuf* before;~Output(){std::cout.rdbuf(before);}} output{std::cout.rdbuf(captured.rdbuf())};
    const bool good=!tryDmlBridge(sql,SQLParser::classify(sql),session,handled,sql)&&handled;
    const auto text=captured.str();return good&&text.find("SQLSTATE ")==std::string::npos;
}
Rows physical(const std::string& db,const std::string& name){
    const auto table=g_engine.getTableSchema(db,name);TableScanOp scan(&g_engine,db,name);
    require(scan.open(),"actual heap scan opens");Rows result;std::string raw;
    while(scan.next(raw)){
        std::vector<Cell> row;
        for(size_t i=0;i<table.len;++i){
            if(table.cols[i].generatedKind=='v')continue;
            row.push_back(scan.lastColumnIsNull(i)?Cell{}:Cell{g_engine.extractColumnValue(raw,table,i,db,false)});
        }
        result.push_back(std::move(row));
    }
    require(!scan.hasError(),"actual heap scan completes");scan.close();scan.close();
    std::sort(result.begin(),result.end());return result;
}
std::set<std::string> attributes(StorageEngine& engine,const std::string& db,const std::string& name){
    std::set<std::string> result;
    for(const auto& row:engine.getPgStatsRows(db,name)){std::istringstream in(row);std::string schema,table,column;in>>schema>>table>>column;require(schema==db&&table==name,"actual stats owner");result.insert(column);}
    return result;
}
std::set<std::string> persistedAttributes(const std::string& db,const std::string& name){
    std::ifstream in(std::filesystem::path(db)/".stats");std::string line;std::set<std::string> result;
    while(std::getline(in,line)){std::istringstream fields(line);std::string table,column;fields>>table>>column;if(table==name&&column!="__rows__"&&column!="__multi__")result.insert(column);}
    return result;
}
std::string section(const std::string& db,const std::string& name,const std::string& key={}){
    std::ifstream in(std::filesystem::path(db)/".stats");std::string line,out;
    while(std::getline(in,line)){std::istringstream fields(line);std::string table,column;fields>>table>>column;if(table==name&&(key.empty()||column==key))out+=line+'\n';}
    return out;
}
bool equal(const StorageEngine::ColumnStats& a,const StorageEngine::ColumnStats& b){return a.nullCount==b.nullCount&&a.cardinality==b.cardinality&&a.minVal==b.minVal&&a.maxVal==b.maxVal&&a.histogram==b.histogram&&a.mcv==b.mcv;}
bool scalarStats(const std::string& db,const std::string& table,const std::string& column,size_t nulls,size_t cardinality,const std::string& minimum,const std::string& maximum){
    const auto actual=g_engine.getColumnStats(db,table,column);
    return actual.nullCount==nulls&&actual.cardinality==cardinality&&actual.minVal==minimum&&actual.maxVal==maximum;
}
bool fraction(const std::string& db,const std::string& table,const std::string& column,double expected){
    const auto rows=g_engine.getPgStatsRows(db,table,column);if(rows.size()!=1)return false;
    std::istringstream input(rows.front());std::string schema,relation,attribute;double value=-1;input>>schema>>relation>>attribute>>value;
    return schema==db&&relation==table&&attribute==column&&std::abs(value-expected)<0.000001;
}
std::string selectedError(const std::string& db,const std::string& sql){
    try{
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql));
        auto owner=std::make_shared<PreparedQueryExecution>(query,&g_engine,db);owner->planStatementConstants(query->ast.get());
        auto plan=QueryPlanner::buildPreparedQueryPlan(&g_engine,db,query,query->ast.get(),{},[owner]{return owner;});
        auto cursor=QueryPlanner::makePreparedCursor(std::move(plan),query->output);std::vector<ExprValue> row;
        while(cursor->next(row)){}cursor->close();
    }catch(const DbError& error){return error.sqlState();}
    return {};
}
}
int main(){
    using namespace dbms;TypeRegistry::instance().bootstrap();cleanupTestDb("analyze_virtual_statistics");const auto db=testDbPath("analyze_virtual_statistics");
    Session session;session.username="admin";session.permission=1;session.currentDB=db;
    struct Scope {Session* before;bool autoAnalyze;~Scope(){setCurrentSession(before);g_config.autoAnalyzeEnabled=autoAnalyze;}} scope{currentSession(),g_config.autoAnalyzeEnabled};
    setCurrentSession(&session);g_config.autoAnalyzeEnabled=false;
    size_t count=0,failed=0;
    const auto check=[&](const char* name,const std::function<bool()>& action){++count;bool good=false;try{good=action();}catch(const DbError& e){std::cerr<<"SQLSTATE="<<e.sqlState()<<" "<<e.message()<<'\n';}catch(const std::exception& e){std::cerr<<e.what()<<'\n';}failed+=!good;std::cout<<"ANALYZE_VIRTUAL "<<count<<' '<<name<<" good="<<good<<'\n';};
    try{
        require(g_engine.createDatabase(db,"utf8")==DBStatus::OK,"actual database create");DdlExecutor ddl;
        require(!ddl.executeSql("CREATE TABLE av_stats(id integer,p integer,s integer GENERATED ALWAYS AS(p*2) STORED,v smallint GENERATED ALWAYS AS(id) VIRTUAL,bad integer GENERATED ALWAYS AS(1/(id-id)) VIRTUAL)",session),"actual generated DDL");
        require(!ddl.executeSql("CREATE TABLE av_allnull(id integer,p integer,s integer GENERATED ALWAYS AS(p*2) STORED,v smallint GENERATED ALWAYS AS(id) VIRTUAL)",session),"actual allNULL DDL");
        // Virtual positions are interleaved with nullable physical and STORED
        // positions. No schema compression: original bitmap ordinals matter.
        require(!ddl.executeSql("CREATE TABLE av_toast(id integer,v smallint GENERATED ALWAYS AS(id) VIRTUAL,p text,s integer GENERATED ALWAYS AS(id+1) STORED)",session),"actual interleaved TOAST DDL");
        require(!ddl.executeSql("CREATE TABLE av_control(id integer,p integer)",session),"actual ordinary DDL");
        require(mutate("INSERT INTO av_stats(id,p) VALUES(40000,2),(40001,NULL),(40002,2),(40003,4)",session),"invalid virtual remains unselected on actual insert");
        require(mutate("INSERT INTO av_allnull(id,p) VALUES(40000,NULL),(40001,NULL)",session),"actual allNULL seed");
        require(mutate("INSERT INTO av_control(id,p) VALUES(1,3),(2,NULL),(3,3)",session),"actual control seed");
        const std::set<std::string> ordinary{"id","p","s"};
        check("real-schema-virtual-and-stored-roles",[&]{const auto t=g_engine.getTableSchema(db,"av_stats");return t.len==5&&t.cols[2].generatedKind=='s'&&t.cols[3].generatedKind=='v'&&t.cols[4].generatedKind=='v';});
        check("before-analyze-no-persisted-statistics",[&]{return attributes(g_engine,db,"av_stats").empty()&&persistedAttributes(db,"av_stats").empty();});
        check("actual-physical-and-stored-before",[&]{return physical(db,"av_stats")==Rows{{"40000","2","4"},{"40001",std::nullopt,std::nullopt},{"40002","2","4"},{"40003","4","8"}};});
        check("analyze-does-not-read-bad-virtual",[&]{return g_engine.analyzeTable(db,"av_stats")&&g_engine.getTableRowCount(db,"av_stats")==4;});
        check("virtual-absent-from-catalog-and-durable-stats",[&]{return attributes(g_engine,db,"av_stats")==ordinary&&persistedAttributes(db,"av_stats")==ordinary;});
        check("ordinary-null-minmax-cardinality-and-MCV",[&]{const auto p=g_engine.getColumnStats(db,"av_stats","p");return scalarStats(db,"av_stats","id",0,4,"40000","40003")&&scalarStats(db,"av_stats","p",1,2,"2","4")&&p.mcv==std::vector<std::pair<std::string,size_t>>{{"2",2},{"4",1}}&&fraction(db,"av_stats","p",0.25);});
        check("STORED-real-values-and-null-fraction",[&]{return scalarStats(db,"av_stats","s",1,2,"4","8")&&fraction(db,"av_stats","s",0.25);});
        check("selected-virtual-assignment-still22003",[&]{return selectedError(db,"SELECT v FROM av_stats ORDER BY id")=="22003";});
        check("selected-virtual-expression-still22012",[&]{return selectedError(db,"SELECT bad FROM av_stats ORDER BY id")=="22012";});
        check("repeated-analyze-preserves-all-persisted-bytes",[&]{const auto before=section(db,"av_stats");return g_engine.analyzeTable(db,"av_stats")&&section(db,"av_stats")==before;});
        check("unrelated-and-multicol-statistics-preserved",[&]{require(g_engine.analyzeTable(db,"av_control"),"actual ordinary ANALYZE");require(g_engine.analyzeMultiColumn(db,"av_stats",{"id","p"}),"actual physical-only multi-column ANALYZE");const auto other=section(db,"av_control"),multi=section(db,"av_stats","__multi__");return !other.empty()&&!multi.empty()&&g_engine.analyzeTable(db,"av_stats")&&section(db,"av_control")==other&&section(db,"av_stats","__multi__")==multi;});
        check("actual-update-unselected-virtual-and-STORED-null",[&]{return mutate("UPDATE av_stats SET p=NULL WHERE id=40002",session)&&physical(db,"av_stats")==Rows{{"40000","2","4"},{"40001",std::nullopt,std::nullopt},{"40002",std::nullopt,std::nullopt},{"40003","4","8"}};});
        check("after-update-analysis-null-fractions",[&]{return g_engine.analyzeTable(db,"av_stats")&&scalarStats(db,"av_stats","p",2,2,"2","4")&&scalarStats(db,"av_stats","s",2,2,"4","8")&&fraction(db,"av_stats","p",0.5)&&attributes(g_engine,db,"av_stats")==ordinary;});
        check("after-delete-analysis-real-rowcount-and-NULL",[&]{return mutate("DELETE FROM av_stats WHERE id=40003",session)&&g_engine.analyzeTable(db,"av_stats")&&g_engine.getTableRowCount(db,"av_stats")==3&&scalarStats(db,"av_stats","p",2,1,"2","2")&&fraction(db,"av_stats","s",2.0/3.0);});
        check("actual-cold-persisted-reader-excludes-virtual",[&]{require(g_engine.checkpoint(db),"actual checkpoint");StorageEngine cold;return attributes(cold,db,"av_stats")==ordinary&&cold.getTableRowCount(db,"av_stats")==3&&equal(cold.getColumnStats(db,"av_stats","s"),g_engine.getColumnStats(db,"av_stats","s"));});
        check("allNULL-physical-and-STORED-statistics",[&]{return g_engine.analyzeTable(db,"av_allnull")&&attributes(g_engine,db,"av_allnull")==ordinary&&scalarStats(db,"av_allnull","p",2,0,"","")&&scalarStats(db,"av_allnull","s",2,0,"","")&&fraction(db,"av_allnull","p",1.0)&&fraction(db,"av_allnull","s",1.0);});
        check("allNULL-update-and-delete-use-real-new-image",[&]{return mutate("UPDATE av_allnull SET p=3 WHERE id=40000",session)&&g_engine.analyzeTable(db,"av_allnull")&&fraction(db,"av_allnull","s",0.5)&&mutate("DELETE FROM av_allnull WHERE id=40001",session)&&g_engine.analyzeTable(db,"av_allnull")&&scalarStats(db,"av_allnull","s",0,1,"6","6");});
        const std::string large(2048,'Q');
        for(int i=0;i<32;++i){const std::string value=i%4==0?"NULL":i%4==1?"''":i%4==2?"'NULL'":"'"+large+"'";require(mutate("INSERT INTO av_toast(id,p) VALUES("+std::to_string(40000+i)+","+value+")",session),"actual nullable TOAST seed");}
        const auto toastBefore=physical(db,"av_toast");
        check("interleaved-bitmap-TOAST-empty-and-literal-NULL",[&]{const auto binding=StorageEngine::captureNullRowBinding();require(g_engine.analyzeTable(db,"av_toast"),"actual interleaved ANALYZE");const auto after=StorageEngine::captureNullRowBinding();const auto p=g_engine.getColumnStats(db,"av_toast","p");const std::map<std::string,size_t> mcv(p.mcv.begin(),p.mcv.end());return p.nullCount==8&&p.cardinality==3&&p.minVal.empty()&&p.maxVal==large&&mcv==std::map<std::string,size_t>{{"",8},{"NULL",8},{large,8}}&&p.histogram.size()==10&&binding.engine==after.engine&&binding.database==after.database&&binding.table==after.table&&binding.rid==after.rid&&binding.natts==after.natts;});
        check("ANALYZE-preserves-actual-TOAST-values-and-STORED",[&]{return physical(db,"av_toast")==toastBefore&&g_engine.getTableRowCount(db,"av_toast")==32&&scalarStats(db,"av_toast","s",0,32,"40001","40032")&&attributes(g_engine,db,"av_toast")==ordinary&&persistedAttributes(db,"av_toast")==ordinary;});
        check("repeated-cold-TOAST-statistics-and-heap-owner",[&]{const auto before=section(db,"av_toast");require(g_engine.analyzeTable(db,"av_toast"),"actual repeated TOAST ANALYZE");require(g_engine.checkpoint(db),"actual TOAST checkpoint");StorageEngine cold;return section(db,"av_toast")==before&&equal(cold.getColumnStats(db,"av_toast","p"),g_engine.getColumnStats(db,"av_toast","p"))&&attributes(cold,db,"av_toast")==ordinary&&physical(db,"av_toast")==toastBefore;});
    }catch(const DbError& error){std::cerr<<"SETUP SQLSTATE="<<error.sqlState()<<" "<<error.message()<<'\n';++failed;}catch(const std::exception& error){std::cerr<<"SETUP "<<error.what()<<'\n';++failed;}
    std::cout<<"ANALYZE_VIRTUAL_TOTAL controls="<<count<<" failed="<<failed<<'\n';
    const auto dropped=g_engine.dropDatabase(db);if(dropped!=DBStatus::OK){std::cerr<<"actual fixture cleanup failed\n";return 2;}
    return count==20&&failed==0?0:1;
}
