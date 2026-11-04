#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "commands/DdlExecutor.h"
#include "common/Config.h"
#include "executor/ExecutionPlan.h"
#include "storage/PageAllocator.h"
#include "test_utils.h"
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>
extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;
using Engine=dbms::StorageEngine;
using Cell=std::optional<std::string>;
using Rows=std::vector<std::vector<Cell>>;
static void require(bool ok,const char* message){if(!ok)throw std::runtime_error(message);}
static Rows snapshot(Engine& engine,const std::string& db,const dbms::TableSchema& table){
    dbms::TableScanOp scan(&engine,db,table.tablename);require(scan.open(),"real heap scan open");
    Rows rows;std::string raw;
    while(scan.next(raw)){std::vector<Cell> row;for(size_t i=0;i<table.len;++i)row.push_back(scan.lastColumnIsNull(i)?Cell{}:Cell{Engine::extractColumnValueStatic(raw,table,i)});rows.push_back(std::move(row));}
    require(!scan.hasError(),"real heap scan complete");scan.close();return rows;
}
static bool equal(const Engine::ColumnStats&a,const Engine::ColumnStats&b){return a.cardinality==b.cardinality&&a.nullCount==b.nullCount&&a.minVal==b.minVal&&a.maxVal==b.maxVal&&a.histogram==b.histogram&&a.mcv==b.mcv;}
static std::string bytes(const std::filesystem::path&p){std::ifstream in(p,std::ios::binary);return {std::istreambuf_iterator<char>(in),{}};}
static Engine::ColumnStats reference(const dbms::Column& column,
                                     const Rows& rows, size_t ordinal) {
    Engine::ColumnStats expected;
    std::vector<std::string> values;
    for (const auto& row : rows) {
        if (!row[ordinal]) ++expected.nullCount;
        else values.push_back(*row[ordinal]);
    }
    std::sort(values.begin(), values.end(), [&column](const auto& left, const auto& right) {
        const bool leftComparable = Engine::compareValues(column, left, false,
            left, false, "=") != Engine::PredicateTruth::Unknown;
        const bool rightComparable = Engine::compareValues(column, right, false,
            right, false, "=") != Engine::PredicateTruth::Unknown;
        if (leftComparable != rightComparable) return leftComparable;
        if (!leftComparable) return left < right;
        return Engine::compareValues(column, left, false, right, false, "<")
            == Engine::PredicateTruth::True;
    });
    if (!values.empty()) {
        expected.minVal = values.front();
        expected.maxVal = values.back();
    }
    if (values.size() >= 20) {
        const size_t width = values.size() / 10;
        for (size_t i = 0; i < 10; ++i)
            expected.histogram.push_back({values[i * width],
                values[i == 9 ? values.size() - 1 : (i + 1) * width]});
    }
    for (const auto& value : values) {
        if (!expected.mcv.empty() && (expected.mcv.back().first == value ||
            Engine::compareValues(column, expected.mcv.back().first, false,
                value, false, "=") == Engine::PredicateTruth::True))
            ++expected.mcv.back().second;
        else expected.mcv.push_back({value, 1});
    }
    expected.cardinality = expected.mcv.size();
    std::stable_sort(expected.mcv.begin(), expected.mcv.end(),
        [](const auto& left, const auto& right) { return left.second > right.second; });
    if (expected.mcv.size() > 10) expected.mcv.resize(10);
    return expected;
}

static dbms::TableSchema plain(const std::string& name,bool nullable=true){
    dbms::TableSchema t;t.tablename=name;t.append(dbms::makeIntColumn("id",false,4,true));t.append(dbms::makeIntColumn("v",nullable,4));t.append(dbms::makeVarCharColumn("payload",nullable,64));t.append(dbms::makeIntColumn("n",nullable,4));return t;
}
static void insert(const std::string& db,const dbms::TableSchema&t,size_t n,bool mixed=false,bool allNull=false,Engine* owner=nullptr){
    Engine& engine=owner?*owner:g_engine;
    require(engine.createTable(db,t)==dbms::DBStatus::OK,"real table create");require(engine.beginTransaction(db)==dbms::DBStatus::OK,"real insert BEGIN");
    for(size_t i=1;i<=n;++i){Engine::SqlRow row{{"id",std::to_string(i)},{"v",std::to_string(i%5)},{"payload",i%2?"":"NULL"},{"n",i%7?std::to_string(i):std::to_string(std::numeric_limits<int32_t>::max())}};
        if(allNull || (mixed&&i%3==0))row["v"]=std::nullopt;
        if(allNull || (mixed&&i%4==0))row["payload"]=std::nullopt;
        if(allNull || (mixed&&i%6==0))row["n"]=std::nullopt;
        require(engine.insertRow(db,t.tablename,row)==dbms::DBStatus::OK,"real typed row insert");}
    require(engine.commitTransaction()==dbms::DBStatus::OK,"real insert COMMIT");
}
static void verify(const std::string& db,const std::string& name,const std::vector<size_t>& only={},Engine* owner=nullptr){
    Engine& engine=owner?*owner:g_engine;
    const auto table=engine.getTableSchema(db,name);const auto before=snapshot(engine,db,table);
    require(engine.analyzeTable(db,name),"actual ANALYZE success");require(engine.getTableRowCount(db,name)==before.size(),"stored row count");
    for(size_t i=0;i<table.len;++i){if(!only.empty()&&std::find(only.begin(),only.end(),i)==only.end())continue;require(equal(engine.getColumnStats(db,name,table.cols[i].dataName),reference(table.cols[i],before,i)),"complete typed stats/null/min/max/hist/MCV oracle");}
    require(snapshot(engine,db,table)==before,"ANALYZE preserves real values and NULLs");
}
int main(){
    using namespace dbms;TypeRegistry::instance().bootstrap();const std::string name="analyze_bitmap_caller",db=testDbPath(name);cleanupTestDb(name);
    const bool oldAuto=g_config.autoAnalyzeEnabled;g_config.autoAnalyzeEnabled=false;
    struct Restore{bool value;Session* old;~Restore(){StorageEngine::unbindNullRow();g_config.autoAnalyzeEnabled=value;setCurrentSession(old);}}restore{oldAuto,currentSession()};
    Session session;session.username="admin";session.permission=1;session.currentDB=db;setCurrentSession(&session);
    require(g_engine.createDatabase(db,"utf8")==DBStatus::OK,"actual private database");
    size_t checked=0,failed=0;
    const auto run=[&](const char* label,const std::function<void()>& fn){++checked;try{fn();std::cout<<"ANALYZE_BITMAP_CASE "<<checked<<' '<<label<<" PASS\n";}catch(const std::exception&e){++failed;std::cerr<<"ANALYZE_BITMAP_CASE "<<checked<<' '<<label<<" FAIL "<<e.what()<<'\n';}};
    run("empty",[&]{auto t=plain("empty");insert(db,t,0);verify(db,t.tablename);});
    run("one-all-null",[&]{auto t=plain("one");insert(db,t,1,false,true);verify(db,t.tablename);require(g_engine.getColumnStats(db,t.tablename,"payload").nullCount==1,"one actual NULL");});
    run("mixed-null-empty-and-literal-NULL",[&]{auto t=plain("mixed");insert(db,t,32,true);verify(db,t.tablename);require(g_engine.getColumnStats(db,t.tablename,"payload").nullCount==8,"NULL differs fromempty/literalNULL");});
    run("all-null-columns",[&]{auto t=plain("all_null");insert(db,t,32,false,true);verify(db,t.tablename);require(g_engine.getColumnStats(db,t.tablename,"n").cardinality==0,"allNULL has no values");});
    run("all-NOT-NULL-columns",[&]{auto t=plain("not_null",false);insert(db,t,32);verify(db,t.tablename);});
    run("nonnull-sentinel-bit-values",[&]{auto t=plain("maximum_values");insert(db,t,28);verify(db,t.tablename);const auto s=g_engine.getColumnStats(db,t.tablename,"n");require(s.maxVal=="2147483647"&&s.nullCount==0,"valid maxint neverNULL sentinel");});
    run("numeric-real-precision",[&]{TableSchema t;t.tablename="precise";t.append(makeIntColumn("id",false,4,true));t.append(makeDecimalColumn("v",true,30,6));require(g_engine.createTable(db,t)==DBStatus::OK,"numeric table");for(size_t i=1;i<=24;++i)require(g_engine.insertRow(db,t.tablename,{{"id",std::to_string(i)},{"v",i%5?Cell{"12345678901234567890.123450"}:Cell{}}})==DBStatus::OK,"numeric input");verify(db,t.tablename);});
    DdlExecutor ddl;
    run("stored-generated-physical-bitmap",[&]{require(!ddl.executeSql("CREATE TABLE stored_rows(id integer PRIMARY KEY,base integer,g integer GENERATED ALWAYS AS(base+3) STORED)",session),"stored generated DDL");for(size_t i=1;i<=24;++i)require(g_engine.insertRow(db,"stored_rows",{{"id",std::to_string(i)},{"base",i%3?Cell{std::to_string(i)}:Cell{}}})==DBStatus::OK,"stored generated insert");verify(db,"stored_rows");});
    run("caller-real-heap-binding-survives-nested-ANALYZE",[&]{const auto table=g_engine.getTableSchema(db,"mixed");bool observed=false;
        require(g_engine.forEachRow(db,"mixed",[&](uint32_t page,uint16_t slot,const char* data,size_t n){if(observed)return;observed=true;const std::string raw(data,n);const int64_t rid=Engine::encodeRid(page,slot);auto saved=Engine::captureNullRowBinding();Engine::bindNullRow(&g_engine,db,"mixed",rid,table.len);const auto before=Engine::captureNullRowBinding();std::vector<std::string> values;for(size_t i=0;i<table.len;++i)values.push_back(g_engine.extractColumnValue(raw,table,i,db));require(g_engine.analyzeTable(db,"maximum_values"),"nested real ANALYZE");const auto after=Engine::captureNullRowBinding();require(after.engine==before.engine&&after.database==before.database&&after.table==before.table&&after.rid==before.rid&&after.natts==before.natts,"actual caller binding fields unchanged");for(size_t i=0;i<table.len;++i)require(g_engine.extractColumnValue(raw,table,i,db)==values[i],"actual outer heap extraction unchanged");Engine::restoreNullRowBinding(std::move(saved));}),"actual caller heap traversal");require(observed,"actual RID present");});
    run("virtual-schema-original-demand-fallback",[&]{require(!ddl.executeSql("CREATE TABLE virtual_rows(id integer PRIMARY KEY,v integer,payload varchar(64),g integer GENERATED ALWAYS AS(id+10) VIRTUAL)",session),"virtual DDL");for(size_t i=1;i<=24;++i)require(g_engine.insertRow(db,"virtual_rows",{{"id",std::to_string(i)},{"v",i%3?Cell{std::to_string(i)}:Cell{}},{"payload",i%4?Cell{"NULL"}:Cell{}}})==DBStatus::OK,"virtual fixture input");
        std::vector<std::vector<std::string>> before,after;std::vector<std::vector<bool>> bn,an;
        (void)g_engine.query(db,"virtual_rows",{}, {"id","g"},{},false,false,false,0,{},&before,&bn);verify(db,"virtual_rows",{0,1,2});(void)g_engine.query(db,"virtual_rows",{}, {"id","g"},{},false,false,false,0,{},&after,&an);require(before==after&&bn==an&&before.size()==24,"actual virtual values/demand preserved");for(const auto&r:after)require(r.size()==2&&std::stoll(r[1])==std::stoll(r[0])+10,"genuine virtual expression");});
    run("other-multicol-stats-and-cold-reader",[&]{require(g_engine.analyzeMultiColumn(db,"mixed",{"id","v"}),"real multicol ANALYZE");const auto multi=g_engine.getMultiColumnStats(db,"mixed","id,v");const auto untouched=g_engine.getColumnStats(db,"precise","v");verify(db,"mixed");require(equal(g_engine.getMultiColumnStats(db,"mixed","id,v"),multi)&&equal(g_engine.getColumnStats(db,"precise","v"),untouched),"other/multicol entries retained");require(g_engine.checkpoint(db),"real public checkpoint before fresh cold reader");Engine peer;require(equal(peer.getColumnStats(db,"precise","v"),untouched)&&equal(peer.getMultiColumnStats(db,"mixed","id,v"),multi),"actual same-directory cold stats reader");const auto table=peer.getTableSchema(db,"mixed");require(snapshot(peer,db,table).size()==32,"cold actual heap source");});
    run("missing-current-heap-no-stat-publication",[&]{
        const auto t=plain("missing_heap");const std::string missingDb=db+"_closed_owner";
        { Engine owner;require(owner.createDatabase(missingDb,"utf8")==DBStatus::OK,"real owned failure fixture database");insert(missingDb,t,8,false,false,&owner);verify(missingDb,t.tablename,{},&owner);require(owner.checkpoint(missingDb),"real owned heap flush"); }
        const auto before=bytes(std::filesystem::path(missingDb)/".stats");
        const auto heap=std::filesystem::path(missingDb)/(t.tablename+".dt"),saved=std::filesystem::path(missingDb)/(t.tablename+".dt.saved");
        { std::filesystem::rename(heap,saved);struct RestoreHeap{std::filesystem::path a,b;~RestoreHeap(){std::error_code e;if(std::filesystem::exists(b,e))std::filesystem::rename(b,a,e);}}guard{heap,saved};
          Engine cold;require(!cold.analyzeTable(missingDb,t.tablename),"missing actual current file rejected");require(bytes(std::filesystem::path(missingDb)/".stats")==before,"failure leaves atomic stats unchanged"); }
        Engine cleanup;require(cleanup.dropDatabase(missingDb)==DBStatus::OK,"normal closed-owner fixture cleanup");
    });
    require(checked==12,"fixed complete control count");require(g_engine.dropDatabase(db)==DBStatus::OK,"normal owned cleanup");cleanupTestDb(name);
    std::cout<<"ANALYZE_BITMAP_CALLER_CONTROLS="<<checked<<" EXPECTED=12 FAILED="<<failed<<'\n';return failed?1:0;
}
