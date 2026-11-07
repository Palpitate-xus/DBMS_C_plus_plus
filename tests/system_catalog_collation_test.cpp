#include "catalog/CatalogService.h"
#include "catalog/SystemCatalogCollation.h"
#include "catalog/pg_owned_catalog_query.h"
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
void need(bool ok,const std::string& why){++checks;if(!ok)throw std::runtime_error(why);}
using Files=std::map<std::string,std::string>;
Files bytes(const std::filesystem::path& path) {
    Files result;
    for(const auto& file:std::filesystem::recursive_directory_iterator(path))if(file.is_regular_file()) {
        std::ifstream input(file.path(),std::ios::binary);
        result.emplace(file.path().lexically_relative(path).string(),std::string(std::istreambuf_iterator<char>(input),{}));
    }
    return result;
}
using Collations=std::map<std::pair<Oid,int16_t>,Oid>;
Collations values(const CatalogManager::MetadataSnapshot& snapshot) {
    Collations result;for(const auto& a:snapshot.attributes)result[{a.attrelid,a.attnum}]=a.attcollation;return result;
}
// Independent PG18.6 full-wire reference expectations, including physical
// field numbers. They are not generated from the product declaration table.
struct Expected {Oid relation;int16_t number;const char* name;Oid type;};
const std::vector<Expected> expected={{1247,30,"typdefaultbin",194},{1247,31,"typdefault",25},
    {1249,23,"attoptions",1009},{1249,24,"attfdwoptions",1009},
    {1259,33,"reloptions",1009},{1259,34,"relpartbound",194}};
void check(CatalogManager& catalog,Oid collation) {
    for(const auto& e:expected) {
        const auto a=catalog.getAttribute(e.relation,e.name);
        need(a && a->attnum==e.number && a->atttypid==e.type && a->attcollation==collation,
            "actual stored catalog field "+std::string(e.name));
    }
}
void set(CatalogManager& catalog,Oid relation,const std::string& name,Oid collation) {
    auto attributes=catalog.findUserAttributes(relation);bool found=false;
    for(auto& a:attributes)if(a.attname==name){a.attcollation=collation;found=true;}
    need(found && catalog.replaceAttributes(relation,attributes),"isolated actual field fixture");
}
PlanExecutionResult query(const std::string& database,const std::string& sql) {
    auto prepared=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,sql));
    auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedQueryPlan(&g_engine,database,prepared,prepared->ast.get()));
    result.throwIfFailed();return result;
}
}
int main() {
    try {
        TypeRegistry::instance().bootstrap();g_engine.setBackgroundIntervals(60000,60000);
        const auto database=testDbPath("system_catalog_collation");cleanupTestDb("system_catalog_collation");
        need(g_engine.createDatabase(database)==DBStatus::OK,"actual database creation");
        Session session;session.username="admin";session.permission=1;session.currentDB=database;
        struct Scope {Session* previous=currentSession();Scope(Session& s){setCurrentSession(&s);}~Scope(){setCurrentSession(previous);}} scope(session);
        const auto path=g_engine.dbPath(database)/"pg_catalog";
        need(system_catalog_collation::published(path),"checked persisted collation generation");
        {auto catalog=g_engine.catalogService().acquire(database);check(*catalog,950);
         for(Oid type:{25U,1009U,194U})need(catalog->getType(type)->typcollation==100,"type keeps default collation");}
        DdlExecutor ddl;
        need(!ddl.executeSql("CREATE TABLE collation_values(t text,v varchar(9),a text[])",session),"ordinary collatable user columns");
        need(query(database,"SELECT attname,attcollation FROM pg_attribute WHERE attrelid='collation_values'::regclass AND attnum>0 ORDER BY attnum").structuredRows==
            std::vector<std::vector<std::string>>{{"t","100"},{"v","100"},{"a","100"}},"user fields retain their actual default collation");
        const auto prepared=g_engine.prepareBoundQuery(database,"SELECT t.typname,t.typcollation FROM pg_catalog.pg_type AS t WHERE t.oid=25");
        need(ownsPgOwnedCatalogSource(prepared),"actual bound pg_type is an owned source");
        const auto local=g_engine.prepareBoundQuery(database,"WITH pg_type AS (SELECT 'local'::text AS typname) SELECT typname FROM pg_type");
        need(!ownsPgOwnedCatalogSource(local),"same-spelled CTE never becomes an owned catalog");
        const auto current=values(g_engine.catalogService().metadataSnapshot(database));const auto disk=bytes(path);
        g_engine.catalogService().evict(database);
        need(values(CatalogManager::readMetadataSnapshot(path.string()))==current && bytes(path)==disk &&
            !g_engine.catalogService().has(database),"cold values stay actual, complete and read-only");

        const std::filesystem::path legacyPath="__t_catalog_collation_legacy";
        std::filesystem::copy(path,legacyPath,std::filesystem::copy_options::recursive);
        need(std::filesystem::remove(legacyPath/".system_catalog_collation_version"),"isolated pre-upgrade marker state");
        CatalogManager legacy(legacyPath.string(),false);
        for(const auto& e:expected)set(legacy,e.relation,e.name,100);
        check(legacy,100);need(legacy.persistAll(),"publish actual known old catalog image");
        const auto oldValues=values(legacy.metadataSnapshot());const auto oldDisk=bytes(legacyPath);
        need(values(CatalogManager::readMetadataSnapshot(legacyPath.string()))==oldValues && bytes(legacyPath)==oldDisk,
            "old cold reader reports stored default values without repair");
        CatalogManager owner(legacyPath.string(),false);
        need(owner.bootstrapSystemCatalogCollations(false),"mutable startup upgrades known old fields");check(owner,950);
        need(bytes(legacyPath)==oldDisk,"full preflight does not publish early");
        need(!owner.bootstrapSystemCatalogCollations(true),"current fields validate idempotently");
        need(owner.persistAll(),"publish complete upgraded actual attribute image");
        {std::ofstream marker(legacyPath/".system_catalog_collation_version");marker<<system_catalog_collation::version;}
        const auto upgraded=values(owner.metadataSnapshot());const auto upgradedDisk=bytes(legacyPath);
        CatalogManager loaded(legacyPath.string(),false);check(loaded,950);
        need(!loaded.bootstrapSystemCatalogCollations(true) && values(loaded.metadataSnapshot())==upgraded && bytes(legacyPath)==upgradedDisk,
            "published upgrade survives reload without rewrites");
        set(loaded,1247,"typdefault",100);const auto dirty=values(loaded.metadataSnapshot());bool rejected=false;
        try{(void)loaded.bootstrapSystemCatalogCollations(true);}catch(const DbError& e){rejected=e.sqlState()=="XX001";}
        need(rejected && values(loaded.metadataSnapshot())==dirty && bytes(legacyPath)==upgradedDisk,
            "published caller change is rejected without overwriting it");
        for(const auto& e:expected)set(loaded,e.relation,e.name,100);
        set(loaded,1259,"relpartbound",0);const auto partial=values(loaded.metadataSnapshot());rejected=false;
        try{(void)loaded.bootstrapSystemCatalogCollations(false);}catch(const DbError& e){rejected=e.sqlState()=="XX001";}
        need(rejected && values(loaded.metadataSnapshot())==partial && bytes(legacyPath)==upgradedDisk,
            "unknown old field rejects the whole edit before changing earlier fields");
        need(loaded.persistAll(),"persist isolated inconsistent published fixture");const auto malformed=bytes(legacyPath);rejected=false;
        try{(void)CatalogManager::readMetadataSnapshot(legacyPath.string());}catch(const DbError& e){rejected=e.sqlState()=="XX001";}
        need(rejected && bytes(legacyPath)==malformed,"cold published corruption fails without repair");
        std::cout<<"SYSTEM_CATALOG_COLLATION_CHECKED="<<checks<<" FAILED=0\n";return 0;
    }catch(const std::exception& e){std::cerr<<"SYSTEM_CATALOG_COLLATION_CHECKED="<<checks<<" FAIL="<<e.what()<<'\n';return 1;}
}
