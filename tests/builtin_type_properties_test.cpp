#include "catalog/BuiltinTypeProperties.h"
#include "catalog/CatalogService.h"
#include "catalog/SystemAttributeDeclaration.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>

extern dbms::StorageEngine g_engine;
using namespace dbms;
namespace {
unsigned checks=0;
void need(bool value,const std::string& reason){++checks;if(!value)throw std::runtime_error(reason);}
std::map<std::string,std::string> bytes(const std::filesystem::path& root) {
    std::map<std::string,std::string> result;
    for(const auto& entry:std::filesystem::recursive_directory_iterator(root))if(entry.is_regular_file()) {
        std::ifstream input(entry.path(),std::ios::binary);
        result.emplace(entry.path().lexically_relative(root).string(),std::string(std::istreambuf_iterator<char>(input),{}));
    }
    return result;
}
std::string physical(const CatalogManager::MetadataSnapshot& snapshot) {
    std::ostringstream output;
    for(const auto& t:snapshot.types)output<<t.oid<<':'<<t.typname<<':'<<t.typnamespace<<':'<<t.typlen<<':'<<t.typbyval<<':'<<t.typalign<<':'<<t.typstorage<<':'<<t.typcollation<<'\n';
    for(const auto& a:snapshot.attributes)output<<a.attrelid<<':'<<a.attname<<':'<<a.attnum<<':'<<a.atttypid<<':'<<a.attlen<<':'<<a.attbyval<<':'<<a.attalign<<':'<<a.attstorage<<':'<<a.attcollation<<'\n';
    return output.str();
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
        const auto database=testDbPath("builtin_type_properties");cleanupTestDb("builtin_type_properties");
        need(g_engine.createDatabase(database)==DBStatus::OK,"actual database creation");
        Session session;session.username="admin";session.permission=1;session.currentDB=database;
        struct Scope {Session* previous=currentSession();Scope(Session& s){setCurrentSession(&s);}~Scope(){setCurrentSession(previous);}} scope(session);
        auto catalog=g_engine.catalogService().acquire(database);
        const auto path=g_engine.dbPath(database)/"pg_catalog";
        need(builtin_type_properties::published(path),"checked persisted property generation");
        // Independent representative expectations, transcribed from PG18.6
        // pg_type.dat and checked separately against its actual wire output.
        struct Expected {Oid oid;int16_t size;bool byval;char align,storage;Oid collation;};
        for(const auto& e:std::vector<Expected>{{16,1,true,'c','p',0},{19,64,false,'c','p',950},
            {20,8,sizeof(uintptr_t)>=8,'d','p',0},{21,2,true,'s','p',0},{23,4,true,'i','p',0},
            {25,-1,false,'i','x',100},{26,4,true,'i','p',0},{27,6,false,'s','p',0},
            {28,4,true,'i','p',0},{29,4,true,'i','p',0},{650,-1,false,'i','m',0},
            {1007,-1,false,'i','x',0},{1016,-1,false,'d','x',0},{1043,-1,false,'i','x',100},
            {1186,16,false,'d','p',0},{1700,-1,false,'i','m',0},{2950,16,false,'c','p',0}}) {
            const auto t=catalog->getType(e.oid);
            need(t && t->typlen==e.size && t->typbyval==e.byval && t->typalign==e.align &&
                t->typstorage==e.storage && t->typcollation==e.collation,"actual type properties "+std::to_string(e.oid));
        }
        // DDL publishes a new catalog generation. Release the setup lease
        // before changing the schema, then inspect the published generation.
        catalog.reset();DdlExecutor ddl;
        need(!ddl.executeSql("CREATE TABLE physical_values(id smallint,k bigint,label varchar(17),a integer[],t timestamp,u uuid,n numeric)",session),"actual user schema declaration");
        need(query(database,"SELECT attname,atttypid,attlen,attndims,attbyval,attalign,attstorage,attcollation FROM pg_attribute WHERE attrelid='physical_values'::regclass AND attnum>0 ORDER BY attnum").structuredRows==
            std::vector<std::vector<std::string>>{{"id","21","2","0","t","s","p","0"},
                {"k","20","8","0",sizeof(uintptr_t)>=8?"t":"f","d","p","0"},
                {"label","1043","-1","0","f","i","x","100"},{"a","1007","-1","1","f","i","x","0"},
                {"t","1114","8","0",sizeof(uintptr_t)>=8?"t":"f","d","p","0"},
                {"u","2950","16","0","f","c","p","0"},{"n","1700","-1","0","f","i","m","0"}},
            "ordinary DDL copies genuine declared type properties and array identity");
        catalog=g_engine.catalogService().acquire(database);
        const auto current=physical(catalog->metadataSnapshot());const auto disk=bytes(path);
        need(!catalog->bootstrapBuiltinTypeProperties(true) && physical(catalog->metadataSnapshot())==current && bytes(path)==disk,"published generation validates without writes");
        g_engine.catalogService().evict(database);
        need(physical(CatalogManager::readMetadataSnapshot(path.string()))==current && bytes(path)==disk &&
            !g_engine.catalogService().has(database),"independent cold reads preserve every persisted physical property");

        const std::filesystem::path legacyPath="__t_type_properties_legacy";
        CatalogManager legacy(legacyPath.string(),false);legacy.bootstrapSystemNamespaces();legacy.bootstrapSystemTypes();
        for(const Oid oid:{16U,19U,20U,21U,23U,25U,26U,27U,28U,29U,1007U,1043U}) {
            auto t=*legacy.getType(oid);t.typbyval=false;t.typalign='i';t.typstorage='p';t.typcollation=0;
            need(legacy.updateType(oid,t),"known legacy type signature");
        }
        PgClassRow relation;relation.oid=50000;relation.relname="legacy_rows";relation.relnamespace=2200;relation.relnatts=2;
        need(legacy.createClass(relation)==50000,"actual legacy relation identity");
        PgAttributeRow small;small.attrelid=50000;small.attnum=1;small.attname="id";small.atttypid=21;small.attlen=2;
        PgAttributeRow text=small;text.attnum=2;text.attname="label";text.atttypid=1043;text.attlen=-1;text.attstorage='x';
        legacy.addAttribute(small);legacy.addAttribute(text);
        need(legacy.bootstrapSystemAttributes(false),"actual old system fields use old stored type properties");
        need(legacy.persistAll(),"old format fixture published");
        {std::ofstream marker(legacyPath/".system_attribute_version");marker<<system_attributes::version;}
        const auto oldDisk=bytes(legacyPath);const auto oldPhysical=physical(legacy.metadataSnapshot());
        need(physical(CatalogManager::readMetadataSnapshot(legacyPath.string()))==oldPhysical && bytes(legacyPath)==oldDisk,
            "cold old-format reader does not normalize values or write files");
        CatalogManager owner(legacyPath.string(),false);
        need(owner.bootstrapBuiltinTypeProperties(false),"mutable owner upgrades recognized old signatures");
        need(bytes(legacyPath)==oldDisk,"staged metadata change does not publish early");
        need(owner.getType(21)->typbyval && owner.getType(21)->typalign=='s' && owner.getType(1043)->typcollation==100,
            "old actual type rows upgraded");
        need(owner.getAttribute(50000,"id")->attbyval && owner.getAttribute(50000,"id")->attalign=='s' &&
            owner.getAttribute(50000,"label")->attcollation==100,"old actual user fields upgraded");
        need(owner.getAttribute(50000,"tableoid")->attbyval && owner.getAttribute(50000,"ctid")->attalign=='s',
            "old actual negative fields upgraded coherently");
        need(owner.persistAll(),"complete upgraded image published");
        {std::ofstream marker(legacyPath/".builtin_type_properties_version");marker<<builtin_type_properties::version;}
        const auto upgraded=physical(owner.metadataSnapshot());const auto upgradedDisk=bytes(legacyPath);
        CatalogManager reloaded(legacyPath.string(),false);
        need(builtin_type_properties::published(legacyPath) && !reloaded.bootstrapBuiltinTypeProperties(true) &&
            physical(reloaded.metadataSnapshot())==upgraded && bytes(legacyPath)==upgradedDisk,"upgrade survives actual reload and is idempotent");

        auto wrong=*reloaded.getType(16);wrong.typbyval=false;wrong.typalign='i';need(reloaded.updateType(16,wrong),"published conflict fixture");
        const auto conflicted=physical(reloaded.metadataSnapshot());bool rejected=false;
        try{(void)reloaded.bootstrapBuiltinTypeProperties(true);}catch(const DbError& e){rejected=e.sqlState()=="XX001";}
        need(rejected && physical(reloaded.metadataSnapshot())==conflicted && bytes(legacyPath)==upgradedDisk,
            "published changed metadata is rejected without repair or publication");
        auto unknown=*reloaded.getType(650);unknown.typstorage='e';need(reloaded.updateType(650,unknown),"unrecognized legacy fixture");
        const auto partial=physical(reloaded.metadataSnapshot());rejected=false;
        try{(void)reloaded.bootstrapBuiltinTypeProperties(false);}catch(const DbError& e){rejected=e.sqlState()=="XX001";}
        need(rejected && physical(reloaded.metadataSnapshot())==partial && bytes(legacyPath)==upgradedDisk,
            "failed complete preflight exposes no partial type or field edits");
        std::cout<<"BUILTIN_TYPE_PROPERTIES_CHECKED="<<checks<<" FAILED=0\n";return 0;
    } catch(const std::exception& e){std::cerr<<"BUILTIN_TYPE_PROPERTIES_CHECKED="<<checks<<" FAIL="<<e.what()<<'\n';return 1;}
}
