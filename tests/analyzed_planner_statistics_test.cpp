#include "catalog/AnalyzedStatisticsStore.h"
#include "catalog/CatalogService.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "catalog/type_registry.h"
#include "utils/Session.h"
#include "test_utils.h"
#include <iostream>
#include <future>
#include <sys/wait.h>
#include <unistd.h>

extern dbms::StorageEngine g_engine;
namespace {
using namespace dbms;
namespace stats=analyzed_statistics;
size_t checks=0;
void need(bool pass,const std::string& label){++checks;if(!pass)throw std::runtime_error(label);}
std::string bytes(const std::filesystem::path& path){std::ifstream in(path,std::ios::binary);need(bool(in),"complete file exists");return{std::istreambuf_iterator<char>(in),{}};}
OpPtr plan(const std::string& db,const std::string& predicate) {
    return QueryPlanner::buildPreparedSelectPlan(&g_engine,db,"typed_cost",
        g_engine.prepareBoundQuery(db,"SELECT id,k,pad FROM typed_cost WHERE "+predicate+" ORDER BY id"));
}
void checkRows(const std::string& db,const std::string& predicate,const std::vector<size_t>& ids) {
    auto result=QueryPlanner::executePlanChecked(plan(db,predicate));result.throwIfFailed();
    need(result.structuredRowsAvailable&&result.structuredRows.size()==ids.size(),"complete typed query row count");
    for(size_t i=0;i<ids.size();++i) {
        const auto id=ids[i];need(result.structuredRows[i][0]==std::to_string(id),"ordered complete id");
        need(result.structuredRows[i][2]=="payload 界\n"+std::to_string(id),"complete delimiter-safe payload");
        need(!result.structuredNulls[i][0]&&!result.structuredNulls[i][2],"separate structured NULL bits");
    }
}
void full(const std::string& db) {
    const auto first=g_engine.getAnalyzedColumnStatistics(db,"typed_cost","k");
    need(first&&first->totalRows==320&&first->sampleRows==320&&first->nullRows==20,"real complete typed statistics");
    need(first->attributeType->actualOid==25&&first->attributeType->codecOid==25,"genuine text declaration");
    const auto again=g_engine.getAnalyzedColumnStatistics(db,"typed_cost","k");
    need(first.get()==again.get(),"same decoded owning generation reused");
    const auto legacy=g_engine.getColumnStats(db,"typed_cost","k");
    need(legacy.mcv.empty(),"unchanged legacy parser actually loses newline MCVs");
    auto hotPlan=plan(db,"k LIKE 'hot%界'");
    const auto heap=QueryPlanner::explain(hotPlan,&g_engine,db);
    std::cout<<"ANALYZED_TYPED_HOT_PLAN\n"<<heap;
    need(heap.find("TableScan(")!=std::string::npos&&heap.find("GiSTScan(")==std::string::npos,
        "typed MCV density avoids hot random heap fetches");
    std::vector<size_t> hot;for(size_t i=1;i<=240;++i)hot.push_back(i);
    checkRows(db,"k LIKE 'hot%界'",hot);
    checkRows(db,"k='hot\n|;,界'",hot);
    auto costs=QueryPlanner::costModel();costs.randomPageCost=.000001;QueryPlanner::setCostModel(costs);
    PlanContext context;context.dbname=db;context.tablename="typed_cost";
    context.conds={{"=","k","hot\n|;,界"}};context.conds[0].decodedLiteralRhs=true;
    auto indexed=QueryPlanner::buildSelectPlan(&g_engine,context);
    const auto estimate=QueryPlanner::explain(indexed,&g_engine,db);
    std::cout<<"ANALYZED_TYPED_EQUALITY_PLAN\n"<<estimate;
    need(estimate.find("IndexScan(")!=std::string::npos&&estimate.find("rows=240")!=std::string::npos,
        "actual equality path and typed MCV rows");
    QueryPlanner::setCostModel({});
    checkRows(db,"k='needle'",{261});
    checkRows(db,"k='absent'",{});
    const auto intStats=g_engine.getAnalyzedColumnStatistics(db,"typed_cost","n");
    need(intStats&&intStats->attributeType->actualOid==23,"typed integer evidence available");
}
} // namespace

int main(int argc,char** argv) try {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();Session session;session.username="testuser";session.permission=1;
    session.currentDB=argc>2?argv[2]:testDbPath("analyzed_planner_statistics");
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* previous;~Restore(){setCurrentSession(previous);QueryPlanner::setCostModel({});}} restore{previous};
    const auto db=session.currentDB;
    if(argc>1&&std::string(argv[1])=="--cold") {full(db);std::cout<<"ANALYZED_PLANNER_COLD checks="<<checks<<" failures=0\n";return 0;}
    need(g_engine.createDatabase(db,"utf8")==DBStatus::OK,"create real database");
    DdlExecutor ddl;
    need(!ddl.executeSql("CREATE TABLE typed_cost(id integer PRIMARY KEY,k text,n integer,pad text)",session),"actual relation declaration");
    for(size_t id=1;id<=320;++id) {
        std::optional<std::string> key;
        if(id<=240)key="hot\n|;,界";
        else if(id>260)key=id==261?"needle":id==262?"":id==263?"NULL":"cold\n|;,"+std::to_string(id);
        need(g_engine.insertRow(db,"typed_cost",StorageEngine::SqlRow{{"id",std::to_string(id)},{"k",key},
            {"n",std::to_string(id%11)},{"pad","payload 界\n"+std::to_string(id)}})==DBStatus::OK,"actual complete row insert");
    }
    need(!ddl.executeSql("CREATE INDEX typed_cost_btree ON typed_cost(k)",session),"real B-tree");
    need(!ddl.executeSql("CREATE INDEX typed_cost_gist ON typed_cost USING gist(k)",session),"real GiST");
    need(g_engine.analyzeTable(db,"typed_cost"),"real typed/native ANALYZE");
    g_engine.catalogService().evict(db);
    need(!g_engine.catalogService().has(db),"actual cold catalog control");
    need(bool(g_engine.getAnalyzedColumnStatistics(db,"typed_cost","k")),"owning evidence from cold catalog");
    need(!g_engine.catalogService().has(db),"cold planning preserves the pure catalog service boundary");
    full(db);
    StorageEngine peer;
    need(g_engine.beginTransaction(db,true)==DBStatus::OK,"actual exclusive database owner");
    const auto foreign=std::async(std::launch::async,[&] {
        return peer.getAnalyzedColumnStatistics(db,"typed_cost","k");
    }).get();
    need(!foreign,"foreign active writer fence declines optional planner evidence");
    need(g_engine.rollbackTransaction()==DBStatus::OK,"release actual exclusive owner");
    const auto path=g_engine.dbPath(db)/".analyzed_statistics";
    const auto original=stats::read(path);need(bool(original),"durable owned snapshot exists");
    const auto prior=g_engine.getAnalyzedColumnStatistics(db,"typed_cost","k");
    for(size_t mutation=0;mutation<7;++mutation) {
        auto changed=*original;
        auto found=std::find_if(changed.columns.begin(),changed.columns.end(),[&](const auto& value){return value.second.attributeType->actualOid==25;});
        need(found!=changed.columns.end(),"actual text column located by identity");auto& column=found->second;
        switch(mutation) {
            case 0: ++column.physicalRelation;break;
            case 1: ++column.schemaGeneration;break;
            case 2: ++column.collation;break;
            case 3: ++column.target;break;
            case 4: for(auto& slot:column.slots)if(slot.kind==stats::Kind::MostCommon)slot.comparisonOperator=999999;break;
            case 5: {auto type=std::make_shared<DatumType>(*column.attributeType);type->actualOid=1043;column.attributeType=type;break;}
            case 6: {const auto value=column;const auto key=found->first;changed.columns.erase(found);changed.columns.emplace(stats::Key{key.relation,int16_t(key.attribute+20),false},value);break;}
        }
        need(stats::replace(path,changed)!=StatsSnapshotPublication::NotPublished,"publish actual invalid-identity control image");
        need(!g_engine.getAnalyzedColumnStatistics(db,"typed_cost","k"),"mismatched relation/column/declaration/operator declines");
        need(prior->physicalRelation!=0&&prior->totalRows==320,"prior owning column survives cache replacement");
        need(stats::replace(path,*original)!=StatsSnapshotPublication::NotPublished,"restore exact typed image");
    }
    const auto legacyBytes=bytes(g_engine.dbPath(db)/".stats"),typedBytes=bytes(path);
    full(db);need(bytes(path)==typedBytes&&bytes(g_engine.dbPath(db)/".stats")==legacyBytes,"read-only planning preserves both statistics images");
    need(g_engine.catalogService().persistAll(),"persist catalogs for independent cold reader");
    const auto child=::fork();need(child>=0,"actual cold fork");
    if(!child){::execl(argv[0],argv[0],"--cold",db.c_str(),nullptr);_exit(127);}
    int status=0;need(::waitpid(child,&status,0)==child&&WIFEXITED(status)&&WEXITSTATUS(status)==0,"independent cold exec complete success");
    need(!ddl.executeSql("ALTER TABLE typed_cost RENAME COLUMN k TO renamed",session),"rename genuine column");
    need(!g_engine.getAnalyzedColumnStatistics(db,"typed_cost","k")&&!g_engine.getAnalyzedColumnStatistics(db,"typed_cost","renamed"),"DDL schema image invalidates old evidence");
    need(g_engine.analyzeTable(db,"typed_cost"),"reanalyze renamed declaration");
    need(bool(g_engine.getAnalyzedColumnStatistics(db,"typed_cost","renamed")),"reanalyze publishes current identity");
    need(prior->totalRows==320,"lease survives DDL and reanalysis");
    need(!ddl.executeSql("DROP TABLE typed_cost",session),"drop actual analyzed relation");
    need(!ddl.executeSql("CREATE TABLE typed_cost(id integer PRIMARY KEY,k text,n integer,pad text)",session),"recreate same physical name");
    need(!g_engine.getAnalyzedColumnStatistics(db,"typed_cost","k"),"recreated name cannot borrow prior relation identity");
    need(prior->totalRows==320&&prior->nullRows==20,"old immutable column survives DROP and recreation");
    std::cout<<"ANALYZED_PLANNER checks="<<checks<<" failures=0\n";return 0;
} catch(const std::exception& e) {
    std::cerr<<"ANALYZED_PLANNER_FAIL checks="<<checks<<' '<<e.what()<<'\n';return 1;
}
