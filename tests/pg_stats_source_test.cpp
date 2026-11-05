#include "catalog/AnalyzedStatistics.h"
#include "catalog/AnalyzedStatisticsStore.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "executor/ExecutionPlan.h"
#include "common/Config.h"
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
using namespace dbms;namespace stats=analyzed_statistics;
size_t checks=0;
void need(bool b,const std::string& text){++checks;if(!b)throw std::runtime_error(text);}
struct Result {std::shared_ptr<PreparedQuery> query;PlanExecutionResult rows;};
Result read(const std::string& db,const std::string& sql) {
    auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql));
    auto rows=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedQueryPlan(&g_engine,db,query,query->ast.get()),0,true);
    rows.throwIfFailed();need(rows.structuredRowsAvailable&&rows.typedRowsAvailable,"actual owning SQL rows: "+sql);
    std::cout<<"PG_STATS_READ_SQL "<<sql<<";\n";return {std::move(query),std::move(rows)};
}
Oid identity(const std::string& db) {
    const auto metadata=g_engine.catalogService().metadataSnapshot(db);
    const auto view=std::find_if(metadata.relations.begin(),metadata.relations.end(),[](const auto& r){return r.relnamespace==11&&r.relname=="pg_stats";});
    need(view!=metadata.relations.end()&&view->relkind=='v'&&view->relnatts==17&&view->reltype,"genuine view identity, seventeen fields and actual row type");
    const auto type=std::find_if(metadata.types.begin(),metadata.types.end(),[&](const auto& t){return t.oid==view->reltype;});
    need(type!=metadata.types.end()&&type->typrelid==view->oid&&type->typtype=='c',"actual view row type points to its relation");
    for(size_t i=0;i<stats::fields.size();++i) {
        const auto a=std::find_if(metadata.attributes.begin(),metadata.attributes.end(),[&](const auto& a){return a.attrelid==view->oid&&a.attnum==int(i+1);});
        need(a!=metadata.attributes.end()&&!a->attisdropped&&a->attname==stats::fields[i].name&&a->atttypid==stats::fields[i].typeOid&&a->attlen==stats::fields[i].size,"actual view attribute identity "+std::to_string(i+1));
    }
    return view->oid;
}
void full(const std::string& db) {
    auto result=read(db,"SELECT * FROM pg_catalog.pg_stats WHERE tablename='stats_source' ORDER BY attname");
    need(result.query->output.size()==17&&result.rows.typedRows.size()==5,"five real analyzed columns, all seventeen outputs");
    for(size_t i=0;i<stats::fields.size();++i) {
        const auto& field=result.query->output[i];need(field.name==stats::fields[i].name&&field.typeOid==stats::fields[i].typeOid,"all seventeen actual output type identities");
        need(field.typeMetadata&&field.typeMetadata->actualOid==field.typeOid,"metadata belongs to actual field type");
    }
    for(const auto& row:result.rows.typedRows) {
        need(row.size()==17&&row[0].value=="public"&&row[1].value=="stats_source"&&row[3].value=="f","actual catalog labels and inherited flag");
        if(row[2].value=="nothing") {
            need(row[4].value=="1"&&row[5].value=="4"&&row[6].value=="0","allNULL real base fields");
            for(size_t i=7;i<17;++i)need(row[i].isNull,"allNULL does not invent a slot field");
        } else {
            need(row[4].value=="0.25","actual NULL fraction for each remaining full-sample attribute");
            if(row[2].value=="r") {
                need(!row[14].isNull&&!row[15].isNull&&!row[16].isNull,"actual range length, empty fraction and bounds slots");
                need(row[14].compound&&row[14].compound->type->actualOid==2277&&row[14].compound->type->arrayElement->actualOid==701,"anyarray length keeps actual float8 element");
                need(row[16].compound&&row[16].compound->type->arrayElement->actualOid==3904,"anyarray bounds keeps actual range element");
                need(row[7].isNull&&row[8].isNull&&row[9].isNull&&row[10].isNull,"ranges do not manufacture scalar slots");
            } else if(row[2].value=="a") {
                need(!row[11].isNull&&!row[12].isNull&&!row[13].isNull,"array dedicated slots are usable through SQL");
                need(row[11].compound&&row[11].compound->type->actualOid==2277&&row[11].compound->type->arrayElement->actualOid==23,"member values retain actual integer element");
            }
        }
        for(size_t i:{size_t(7),size_t(9),size_t(11),size_t(14),size_t(16)})if(!row[i].isNull)
            need(row[i].compound&&row[i].compound->arrayElements&&row[i].compound->type->kind==DatumKind::Array&&row[i].compound->type->arrayElement,"every nonNULL anyarray owns its values, dimensions and actual element metadata");
    }
}
std::map<std::string,std::string> files(const std::filesystem::path& dir) {
    std::map<std::string,std::string> result;
    if(std::filesystem::exists(dir))for(const auto& f:std::filesystem::recursive_directory_iterator(dir))if(f.is_regular_file()) {
        std::ifstream in(f.path(),std::ios::binary);result[f.path().lexically_relative(dir).string()]={std::istreambuf_iterator<char>(in),{}};
    }
    return result;
}
}
int main(int argc,char** argv) {
    try {
        TypeRegistry::instance().bootstrap();const auto db=argc==4?std::string(argv[2]):testDbPath("pg_stats_source");
        Session session;session.username="admin";session.permission=1;session.currentDB=db;
        struct Scope{Session* old;bool automatic;~Scope(){setCurrentSession(old);g_config.autoAnalyzeEnabled=automatic;}}scope{currentSession(),g_config.autoAnalyzeEnabled};
        setCurrentSession(&session);g_config.autoAnalyzeEnabled=false;
        if(argc==4&&std::string(argv[1])=="--cold") {
            g_engine.catalogService().evict(db);const auto before=files(g_engine.dbPath(db)/"pg_catalog");
            need(!g_engine.catalogService().has(db),"cold catalog starts outside cache");
            full(db);need(identity(db)==std::stoul(argv[3]),"cold view OID stable");
            need(!g_engine.catalogService().has(db)&&files(g_engine.dbPath(db)/"pg_catalog")==before,"cold reads neither allocate nor persist nor bootstrap catalog state");
            std::cout<<"PG_STATS_SOURCE_COLD checks="<<checks<<" failures=0\n";return 0;
        }
        need(argc==1,"fixed native source protocol");need(g_engine.createDatabase(db,"utf8")==DBStatus::OK,"actual database creation");
        const auto initialFiles=files(g_engine.dbPath(db)/"pg_catalog");
        need(!initialFiles.empty()&&!g_engine.catalogService().has(db),"CREATE publishes actual system metadata without a mutable query cache");
        auto empty=read(db,"SELECT * FROM pg_catalog.pg_stats LIMIT 0");
        need(empty.query->output.size()==17&&empty.rows.typedRows.empty(),"new empty database immediately has the full statistics descriptor");
        need(files(g_engine.dbPath(db)/"pg_catalog")==initialFiles&&!g_engine.catalogService().has(db),"empty source read never bootstraps, persists or allocates metadata");
        DdlExecutor ddl;
        const auto create=std::string("CREATE TABLE stats_source(i integer,t text,a integer[],r int4range,nothing integer)");
        need(!ddl.executeSql(create,session),"actual physical source relation");std::cout<<"PG_STATS_REFERENCE_SQL "<<create<<";\n";
        auto zero=read(db,"SELECT * FROM pg_catalog.pg_stats WHERE FALSE");need(zero.query->output.size()==17&&zero.rows.typedRows.empty(),"full17 zero-row SQL descriptor");
        const auto oid=identity(db);
        const std::vector<StorageEngine::SqlRow> input={
            {{"i","1"},{"t","x"},{"a","{1,NULL}"},{"r","[1,4)"},{"nothing",{}}},
            {{"i","1"},{"t",""},{"a","{}"},{"r","empty"},{"nothing",{}}},
            {{"i","2"},{"t",{}},{"a",{}},{"r",{}},{"nothing",{}}},
            {{"i",{}},{"t","NULL"},{"a","{2,3}"},{"r","(,)"},{"nothing",{}}}};
        for(const auto& row:input)need(g_engine.insertRow(db,"stats_source",row)==DBStatus::OK,"actual source heap insert");
        std::cout<<"PG_STATS_REFERENCE_SQL INSERT INTO stats_source VALUES(1,'x','{1,NULL}','[1,4)',NULL),(1,'','{}','empty',NULL),(2,NULL,NULL,NULL,NULL),(NULL,'NULL','{2,3}','(,)',NULL);\n";
        need(g_engine.analyzeTable(db,"stats_source"),"actual full ANALYZE");std::cout<<"PG_STATS_REFERENCE_SQL ANALYZE stats_source;\n";full(db);
        auto alias=read(db,"SELECT s.attname,s.null_frac FROM pg_catalog.pg_stats AS s WHERE s.tablename='stats_source' ORDER BY s.attname LIMIT 2");need(alias.rows.structuredRows.size()==2,"normal alias/projection/predicate/order/limit");
        auto cte=read(db,"WITH q AS(SELECT attname,null_frac FROM pg_stats WHERE tablename='stats_source') SELECT attname FROM q WHERE null_frac=1");need(cte.rows.structuredRows==std::vector<std::vector<std::string>>{{"nothing"}},"actual CTE scalar source consumer");
        auto snapshot=stats::read(g_engine.dbPath(db)/".analyzed_statistics");need(snapshot&&snapshot->columns.size()==5,"real typed snapshot still contains exactly analyzed attributes");
        need(g_engine.catalogService().persistAll(),"persist actual view/type/attribute identities");
        const auto child=::fork();need(child>=0,"cold reader fork");
        if(!child){const auto id=std::to_string(oid);::execl(argv[0],argv[0],"--cold",db.c_str(),id.c_str(),nullptr);_exit(127);}
        int status=0;need(::waitpid(child,&status,0)==child&&WIFEXITED(status)&&WEXITSTATUS(status)==0,"independent cold exec normal0");
        std::cout<<"PG_STATS_SOURCE checks="<<checks<<" failures=0 view="<<oid<<"\n";return 0;
    } catch(const DbError& e){std::cerr<<"PG_STATS_SOURCE_FAIL checks="<<checks<<" SQLSTATE="<<e.sqlState()<<" "<<e.what()<<'\n';return 1;}
      catch(const std::exception& e){std::cerr<<"PG_STATS_SOURCE_FAIL checks="<<checks<<" "<<e.what()<<'\n';return 1;}
}
