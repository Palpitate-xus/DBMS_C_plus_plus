#include "catalog/CatalogService.h"
#include "catalog/PgDefaultCatalog.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <fstream>
#include <iostream>
#include <map>
#include <stdexcept>

extern dbms::StorageEngine g_engine;
using namespace dbms;
namespace {
unsigned checks=0;
void need(bool value,const std::string& reason){++checks;if(!value)throw std::runtime_error(reason);}
std::map<std::string,std::string> bytes(const std::filesystem::path& root) {
    std::map<std::string,std::string> result;
    for(const auto& f:std::filesystem::recursive_directory_iterator(root))if(f.is_regular_file()) {
        std::ifstream in(f.path(),std::ios::binary);result.emplace(f.path().lexically_relative(root).string(),
            std::string(std::istreambuf_iterator<char>(in),{}));
    }
    return result;
}
}
int main() {
    try {
        TypeRegistry::instance().bootstrap();g_engine.setBackgroundIntervals(60000,60000);
        const auto db=testDbPath("catalog_default_snapshot_demand");cleanupTestDb("catalog_default_snapshot_demand");
        need(g_engine.createDatabase(db,"utf8")==DBStatus::OK,"actual database");
        Session session;session.username="admin";session.permission=1;session.currentDB=db;
        struct Scope{Session* old=currentSession();Scope(Session& s){setCurrentSession(&s);}~Scope(){setCurrentSession(old);}}scope(session);
        const auto ddl=[&](const std::string& sql){need(!DdlExecutor{}.executeSql(sql,session),"actual DDL: "+sql);};
        ddl("CREATE TABLE snapshot_target(k integer)");
        ddl("CREATE TABLE snapshot_owner(r regclass DEFAULT 'snapshot_target',v integer DEFAULT 7)");
        const auto owner=g_engine.catalogService().acquire(db);
        const auto relation=owner->getClassByName("snapshot_owner",2200);need(bool(relation),"actual owner relation");
        const auto definition=owner->getAttrdefForColumn(relation->oid,1);need(bool(definition),"actual default object");
        const auto basic=g_engine.catalogService().metadataSnapshot(db,false);
        const auto full=g_engine.catalogService().metadataSnapshot(db);
        need(!basic.defaultObjectsIncluded && basic.defaults.empty() && basic.dependencies.empty(),"ordinary declaration acquisition omits default programs and graph copies");
        need(full.defaultObjectsIncluded && full.defaults.size()==2 && !full.dependencies.empty(),"public default acquisition retains the genuine programs and graph");
        need(basic.relations.size()==full.relations.size() && basic.attributes.size()==full.attributes.size() &&
             basic.types.size()==full.types.size(),"ordinary declarations retain the same complete shape");
        bool incomplete=false;try{(void)default_catalog::rows(basic,false);}catch(const DbError& e){incomplete=e.sqlState()=="XX001";}
        need(incomplete,"catalog producer rejects an omitted data set instead of returning a fabricated empty catalog");
        const auto rows=[&](const std::string& sql,const std::vector<std::vector<std::string>>& wanted){
            auto q=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql));
            auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedQueryPlan(&g_engine,db,q,q->ast.get()));
            result.throwIfFailed();need(result.structuredRows==wanted,"whole actual rows: "+sql);
        };
        rows("SELECT adnum FROM pg_attrdef WHERE adrelid='snapshot_owner'::regclass ORDER BY adnum",{{"1"},{"2"}});
        rows("SELECT deptype FROM pg_depend WHERE classid=2604 AND objid="+std::to_string(definition->oid)+" ORDER BY deptype",{{"a"},{"n"}});
        rows("SELECT attname FROM pg_attribute WHERE attrelid='snapshot_owner'::regclass AND attnum>0 ORDER BY attnum",{{"r"},{"v"}});
        need(g_engine.checkpoint(db),"actual checkpoint before cold catalog readers");
        const auto directory=g_engine.dbPath(db)/"pg_catalog";const auto before=bytes(directory);
        const auto coldBasic=CatalogManager::readMetadataSnapshot(directory.string(),false);
        const auto coldFull=CatalogManager::readMetadataSnapshot(directory.string());
        need(!coldBasic.defaultObjectsIncluded && coldBasic.defaults.empty() && coldBasic.dependencies.empty(),"cold declaration reader also avoids result graph copies");
        need(coldFull.defaultObjectsIncluded && coldFull.defaults.size()==2,"existing complete cold-read API retains its objects");
        need(bytes(directory)==before,"both cold acquisition demands leave all catalog file bytes unchanged");
        need(g_engine.beginTransaction(db)==DBStatus::OK,"actual native transaction");
        ddl("ALTER TABLE snapshot_owner ALTER COLUMN v SET DEFAULT 9");
        rows("SELECT adnum FROM pg_attrdef WHERE adrelid='snapshot_owner'::regclass ORDER BY adnum",{{"1"},{"2"}});
        need(g_engine.rollbackTransaction()==DBStatus::OK,"actual catalog/physical rollback");
        rows("SELECT adnum FROM pg_attrdef WHERE adrelid='snapshot_owner'::regclass ORDER BY adnum",{{"1"},{"2"}});
        need(g_engine.catalogService().metadataSnapshot(db,true).defaults.size()==2,"explicit data demand survives rollback model replacement");
        std::cout<<"CATALOG_DEFAULT_SNAPSHOT_DEMAND_CHECKED="<<checks<<" FAILED=0\n";return 0;
    } catch(const std::exception& error){std::cerr<<"CATALOG_DEFAULT_SNAPSHOT_DEMAND_CHECKED="<<checks<<" FAIL="<<error.what()<<'\n';return 1;}
}
