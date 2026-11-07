#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "catalog/SystemCatalogRows.h"
#include "catalog/type_registry.h"
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
std::map<std::string,std::string> catalogBytes(const std::filesystem::path& root) {
    std::map<std::string,std::string> result;
    for(const auto& entry:std::filesystem::recursive_directory_iterator(root))if(entry.is_regular_file()) {
        std::ifstream input(entry.path(),std::ios::binary);
        result.emplace(entry.path().lexically_relative(root).string(),std::string(std::istreambuf_iterator<char>(input),{}));
    }
    return result;
}
PlanExecutionResult query(StorageEngine& engine,const std::string& database,const std::string& sql) {
    auto prepared=std::make_shared<PreparedQuery>(engine.prepareBoundQuery(database,sql));
    auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedQueryPlan(&engine,database,prepared,prepared->ast.get()));
    result.throwIfFailed();return result;
}
}
int main() {
    try {
        TypeRegistry::instance().bootstrap();g_engine.setBackgroundIntervals(60000,60000);
        const auto database=testDbPath("system_catalog_source");cleanupTestDb("system_catalog_source");
        need(g_engine.createDatabase(database)==DBStatus::OK,"actual database creation");
        Session session;session.username="admin";session.permission=1;session.currentDB=database;
        struct Scope {Session* previous=currentSession();Scope(Session& value){setCurrentSession(&value);}~Scope(){setCurrentSession(previous);}} scope(session);
        auto catalog=g_engine.catalogService().acquire(database);auto metadata=catalog->metadataSnapshot();
        for(const auto& expected:std::vector<std::tuple<Oid,const char*,Oid,Oid,int16_t>>{
            {1247,"pg_type",71,210,32},{1249,"pg_attribute",75,270,25}}) {
            const auto oid=std::get<0>(expected);const auto relation=catalog->getClass(oid);
            need(relation && relation->relname==std::get<1>(expected) && relation->relnamespace==11 &&
                relation->reltype==std::get<2>(expected) && relation->relnatts==std::get<4>(expected),"complete real system relation");
            const auto row=catalog->getType(relation->reltype),array=catalog->getType(std::get<3>(expected));
            need(row && array && row->typrelid==oid && row->typarray==array->oid && array->typelem==row->oid,
                "real fixed composite and array identities");
            const auto fields=catalog->findAttributes(oid);need(fields.size()==size_t(std::get<4>(expected)),"every physical field declared");
            const auto all=catalog->findAttributesIncludingSystem(oid);
            need(all.size()==fields.size()+6,"six real stored system attributes in the complete metadata image");
            for(const auto& name:std::vector<std::string>{"ctid","xmin","cmin","xmax","cmax","tableoid"}) {
                const auto found=std::find_if(all.begin(),all.end(),[&](const auto& a){return a.attname==name && a.attnum<0;});
                need(found!=all.end() && found->attnotnull,"actual system attribute "+name);
            }
            for(const auto& field:fields) {
                const auto type=catalog->getType(field.atttypid);need(type && type->typlen==field.attlen,"every declared field has actual type and width");
                const auto edges=catalog->findDepends(PgClassOid_Class,oid,field.attnum);
                need(std::count_if(edges.begin(),edges.end(),[&](const auto& edge){return edge.refclassid==PgClassOid_Type &&
                    edge.refobjid==field.atttypid && edge.deptype=='n';})==1,"one actual field-type dependency");
            }
        }
        need(!system_catalog::bootstrapRelations(*catalog,true),"published declarations validate idempotently");
        const auto identities=query(g_engine,database,"SELECT 'pg_type'::regclass::oid,'pg_class'::regclass::oid,'pg_attribute'::regclass::oid");
        need(identities.structuredRows==std::vector<std::vector<std::string>>{{"1247","1259","1249"}},"REGCLASS resolves real stored system identities");
        for(const auto& item:std::vector<std::pair<std::string,std::string>>{{"pg_type","38"},{"pg_attribute","31"},{"pg_class","40"}}) {
            const auto result=query(g_engine,database,"SELECT count(*) FROM pg_attribute WHERE attrelid='"+item.first+"'::regclass");
            need(result.structuredRows==std::vector<std::vector<std::string>>{{item.second}},"full actual attribute source: "+item.first);
        }
        const auto classQuery=g_engine.prepareBoundQuery(database,"SELECT reltype,relkind,relowner FROM pg_class");
        const auto& classRange=classQuery.sourceRanges.front();
        need(classRange.columns.size()==30,"all owned class fields have typed producers");
        for(const auto& entry:std::vector<std::pair<std::string,int>>{{"reltype",4},{"relkind",18},{"relowner",6},{"relminmxid",31}}) {
            const auto found=std::find_if(classRange.columns.begin(),classRange.columns.end(),[&](const auto& c){return c.name==entry.first;});
            need(found!=classRange.columns.end() && found->physicalAttributeNumber==entry.second,"physical attribute number independent of compact values");
        }
        DdlExecutor ddl;
        need(!ddl.executeSql("CREATE TABLE catalog_values(k integer PRIMARY KEY,id smallint GENERATED ALWAYS AS IDENTITY,v varchar(11),g integer GENERATED ALWAYS AS(k+1) STORED)",session),"actual user table");
        const auto flags=query(g_engine,database,"SELECT attname,atthasdef,attidentity,attgenerated FROM pg_attribute WHERE attrelid='catalog_values'::regclass ORDER BY attnum");
        need(flags.structuredRows==std::vector<std::vector<std::string>>{
            {"tableoid","f","",""},{"cmax","f","",""},{"xmax","f","",""},
            {"cmin","f","",""},{"xmin","f","",""},{"ctid","f","",""},
            {"k","f","",""},{"id","f","a",""},{"v","f","",""},{"g","t","","s"}},
            "actual system/user generation flags and non-NULL zero-char output");
        need(!ddl.executeSql("ALTER TABLE catalog_values ALTER COLUMN v SET STATISTICS 23",session),"actual statistics target declaration");
        const auto stats=query(g_engine,database,"SELECT attstattarget,pg_typeof(attstattarget),pg_typeof(attndims),pg_typeof(attinhcount) FROM pg_attribute WHERE attrelid='catalog_values'::regclass AND attname='v'");
        need(stats.structuredRows==std::vector<std::vector<std::string>>{{"23","smallint","smallint","smallint"}},"PG18 int2 metadata fields use actual widths");
        const auto unspecified=query(g_engine,database,"SELECT attstattarget FROM pg_attribute WHERE attrelid='catalog_values'::regclass AND attname='k'");
        need(unspecified.structuredRows.size()==1 && unspecified.structuredNulls==std::vector<std::vector<bool>>{{true}},"unspecified PG18 statistics target is SQL NULL");
        for(const auto& sql:std::vector<std::string>{"SELECT * FROM pg_class WHERE false","SELECT * FROM pg_attribute WHERE false","SELECT reloptions FROM pg_class WHERE false","SELECT attcompression FROM pg_attribute WHERE false"}) {
            bool rejected=false;try{(void)g_engine.prepareBoundQuery(database,sql);}catch(const DbError& error){rejected=error.sqlState()=="0A000";}
            need(rejected,"unowned catalog values are rejected without fabricated NULLs");
        }
        const auto bytes=catalogBytes(g_engine.dbPath(database)/"pg_catalog");
        {
            StorageEngine reader;reader.setBackgroundIntervals(60000,60000);
            reader.catalogService().evict(database);
            need(!reader.catalogService().has(database),"independent reader starts without a mutable catalog cache");
            need(query(reader,database,"SELECT relname,reltype,relnatts FROM pg_class WHERE oid=1249").structuredRows==
                std::vector<std::vector<std::string>>{{"pg_attribute","75","25"}},"independent cold catalog query");
            need(!reader.catalogService().has(database),"catalog value query never creates a mutable owner");
        }
        need(bytes==catalogBytes(g_engine.dbPath(database)/"pg_catalog"),"complete catalog bytes unchanged by cold reads");
        CatalogManager reload((g_engine.dbPath(database)/"pg_catalog").string(),false);
        need(!system_catalog::bootstrapRelations(reload,true),"declarations/dependencies reload and remain idempotent");
        auto broken=catalog->metadataSnapshot();broken.attributes.erase(std::remove_if(broken.attributes.begin(),broken.attributes.end(),
            [](const auto& a){return a.attrelid==1249 && a.attnum==25;}),broken.attributes.end());
        bool rejected=false;try{(void)system_catalog::attributeRows(broken);}catch(const DbError& error){rejected=error.sqlState()=="XX001";}
        need(rejected,"copied reader rejects an incomplete physical declaration");
        const auto copy=g_engine.dbPath(database)/"isolated-declaration-control";
        std::filesystem::copy(g_engine.dbPath(database)/"pg_catalog",copy,std::filesystem::copy_options::recursive);
        CatalogManager control(copy.string(),false);need(control.dropClass(1247),"isolated missing class fixture");rejected=false;
        try{(void)system_catalog::bootstrapRelations(control,true);}catch(const DbError& error){rejected=error.sqlState()=="XX001";}
        need(rejected,"published missing system declaration is not recreated");
        std::cout<<"SYSTEM_CATALOG_SOURCE_CHECKED="<<checks<<" FAILED=0\n";return 0;
    } catch(const std::exception& error){std::cerr<<"SYSTEM_CATALOG_SOURCE_CHECKED="<<checks<<" FAIL="<<error.what()<<'\n';return 1;}
}
