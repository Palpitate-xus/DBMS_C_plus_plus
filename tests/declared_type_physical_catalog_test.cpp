#include "catalog/CatalogService.h"
#include "catalog/declared_type.h"
#include "commands/TableManage.h"
#include "expression/expr_helper.h"
#include "common/DbError.h"
#include "utils/Session.h"
#include "test_utils.h"
#include <filesystem>
#include <fstream>
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    struct Case {std::string spelling,state;Oid oid;};
    const Case cases[]={
        {"\"integer\"","42704",0},
        {"\"integer\"[]","42704",0},
        {"pg_catalog.\"integer\"","42704",0},
        {"pg_catalog.\"integer\"[]","42704",0},
        {"\"smallint\"","42704",0},
        {"\"smallint\"[]","42704",0},
        {"pg_catalog.\"smallint\"","42704",0},
        {"pg_catalog.\"smallint\"[]","42704",0},
        {"\"bigint\"","42704",0},
        {"\"bigint\"[]","42704",0},
        {"pg_catalog.\"bigint\"","42704",0},
        {"pg_catalog.\"bigint\"[]","42704",0},
        {"\"boolean\"","42704",0},
        {"\"boolean\"[]","42704",0},
        {"pg_catalog.\"boolean\"","42704",0},
        {"pg_catalog.\"boolean\"[]","42704",0},
        {"\"real\"","42704",0},
        {"\"real\"[]","42704",0},
        {"pg_catalog.\"real\"","42704",0},
        {"pg_catalog.\"real\"[]","42704",0},
        {"\"double precision\"","42704",0},
        {"\"double precision\"[]","42704",0},
        {"pg_catalog.\"double precision\"","42704",0},
        {"pg_catalog.\"double precision\"[]","42704",0},
        {"\"character\"","42704",0},
        {"\"character\"[]","42704",0},
        {"pg_catalog.\"character\"","42704",0},
        {"pg_catalog.\"character\"[]","42704",0},
        {"\"character varying\"","42704",0},
        {"\"character varying\"[]","42704",0},
        {"pg_catalog.\"character varying\"","42704",0},
        {"pg_catalog.\"character varying\"[]","42704",0},
        {"\"bit varying\"","42704",0},
        {"\"bit varying\"[]","42704",0},
        {"pg_catalog.\"bit varying\"","42704",0},
        {"pg_catalog.\"bit varying\"[]","42704",0},
        {"\"decimal\"","42704",0},
        {"\"decimal\"[]","42704",0},
        {"pg_catalog.\"decimal\"","42704",0},
        {"pg_catalog.\"decimal\"[]","42704",0},
        {"\"int4\"","",23},
        {"\"int4\"[]","",1007},
        {"pg_catalog.\"int4\"","",23},
        {"pg_catalog.\"int4\"[]","",1007},
        {"\"int2\"","",21},
        {"\"int2\"[]","",1005},
        {"pg_catalog.\"int2\"","",21},
        {"pg_catalog.\"int2\"[]","",1005},
        {"\"int8\"","",20},
        {"\"int8\"[]","",1016},
        {"pg_catalog.\"int8\"","",20},
        {"pg_catalog.\"int8\"[]","",1016},
        {"\"bool\"","",16},
        {"\"bool\"[]","",1000},
        {"pg_catalog.\"bool\"","",16},
        {"pg_catalog.\"bool\"[]","",1000},
        {"\"float4\"","",700},
        {"\"float4\"[]","",1021},
        {"pg_catalog.\"float4\"","",700},
        {"pg_catalog.\"float4\"[]","",1021},
        {"\"float8\"","",701},
        {"\"float8\"[]","",1022},
        {"pg_catalog.\"float8\"","",701},
        {"pg_catalog.\"float8\"[]","",1022},
        {"\"bpchar\"","",1042},
        {"\"bpchar\"[]","",1014},
        {"pg_catalog.\"bpchar\"","",1042},
        {"pg_catalog.\"bpchar\"[]","",1014},
        {"\"varchar\"","",1043},
        {"\"varchar\"[]","",1015},
        {"pg_catalog.\"varchar\"","",1043},
        {"pg_catalog.\"varchar\"[]","",1015},
        {"\"varbit\"","",1562},
        {"\"varbit\"[]","",1563},
        {"pg_catalog.\"varbit\"","",1562},
        {"pg_catalog.\"varbit\"[]","",1563},
        {"\"numeric\"","",1700},
        {"\"numeric\"[]","",1231},
        {"pg_catalog.\"numeric\"","",1700},
        {"pg_catalog.\"numeric\"[]","",1231},
        {"\"char\"","",18},
        {"\"char\"[]","",1002},
        {"pg_catalog.\"char\"","",18},
        {"pg_catalog.\"char\"[]","",1002},
        {"\"text\"","",25},
        {"\"text\"[]","",1009},
        {"pg_catalog.\"text\"","",25},
        {"pg_catalog.\"text\"[]","",1009},
        {"\"name\"","",19},
        {"\"name\"[]","",1003},
        {"pg_catalog.\"name\"","",19},
        {"pg_catalog.\"name\"[]","",1003},
        {"\"bytea\"","",17},
        {"\"bytea\"[]","",1001},
        {"pg_catalog.\"bytea\"","",17},
        {"pg_catalog.\"bytea\"[]","",1001},
        {"\"_int8\"","",1016},
        {"\"_int8\"[]","42704",0},
        {"pg_catalog.\"_int8\"","",1016},
        {"pg_catalog.\"_int8\"[]","42704",0},
        {"\"_char\"","",1002},
        {"\"_char\"[]","42704",0},
        {"pg_catalog.\"_char\"","",1002},
        {"pg_catalog.\"_char\"[]","42704",0},
    };
    size_t checked=0,failed=0;
    const auto require=[&](bool pass,const std::string& role) {
        ++checked;failed+=!pass;
        std::cout<<"PHYSICAL_CATALOG "<<role<<" pass="<<pass<<'\n';
    };
    const auto database=testDbPath("declared_type_physical_catalog");
    require(g_engine.createDatabase(database)==DBStatus::OK,"create isolated database");
    const auto catalogDirectory=g_engine.dbPath(database)/"pg_catalog";
    const auto diskSnapshot=[&] {
        std::map<std::string,std::string> files;
        if(!std::filesystem::exists(catalogDirectory))return files;
        files.emplace("directory:","");
        for(const auto& entry:std::filesystem::recursive_directory_iterator(catalogDirectory)) {
            const auto relative=entry.path().lexically_relative(catalogDirectory).string();
            if(entry.is_directory())files.emplace("directory:"+relative,"");
            else if(entry.is_regular_file()) {
                std::ifstream input(entry.path(),std::ios::binary);
                if(!input)throw std::runtime_error("cannot read actual catalog fixture file");
                const std::string bytes((std::istreambuf_iterator<char>(input)),std::istreambuf_iterator<char>());
                if(input.bad())throw std::runtime_error("cannot finish actual catalog fixture file read");
                files.emplace("file:"+relative,bytes);
            } else throw std::runtime_error("unexpected catalog fixture file kind");
        }
        return files;
    };
    const auto diskBefore=diskSnapshot();
    const auto before=g_engine.catalogService().metadataSnapshot(database);
    require(before.types.empty() && before.namespaces.empty() && !g_engine.catalogService().has(database),
        "actual cold catalog before type lookup");
    for(const auto& expected:cases) {
        const auto check=[&](const std::string& role,const auto& lookup) {
            std::string state;Oid oid=0;
            try{oid=lookup();}catch(const DbError& error){state=error.sqlState();}
            require(state==expected.state && (!state.empty() || oid==expected.oid),role+" "+expected.spelling);
        };
        check("standalone physical identity",[&]{return resolveDeclaredTypeName(expected.spelling,nullptr,nullptr).typeOid;});
        check("cold scalar input identity",[&]{return mapBuiltinTypeNameToOid(
            ExprHelper::declaredTypeInput(expected.spelling,database,&g_engine));});
        check("cold prepare without value demand",[&] {
            const auto query=g_engine.prepareBoundQuery(database,"SELECT NULL::"+expected.spelling+" AS value WHERE false");
            return query.output.size()==1?query.output[0].typeOid:Oid(0);
        });
    }
    const auto after=g_engine.catalogService().metadataSnapshot(database);
    require(after.types.empty() && after.namespaces.empty() && !g_engine.catalogService().has(database),
        "type lookup/preparation did not initialize or write catalog");
    require(diskSnapshot()==diskBefore,"cold lookup left catalog directory/file bytes unchanged");
    (void)g_engine.catalogService().get(database);
    const auto metadata=g_engine.catalogService().metadataSnapshot(database);
    require(!metadata.types.empty(),"actual initialized catalog");
    for(const auto& expected:cases) {
        const auto check=[&](const std::string& role,const auto& lookup) {
            std::string state;Oid oid=0;
            try{oid=lookup();}catch(const DbError& error){state=error.sqlState();}
            require(state==expected.state && (!state.empty() || oid==expected.oid),role+" "+expected.spelling);
        };
        check("copied initialized physical identity",[&]{return resolveDeclaredTypeName(expected.spelling,&metadata,nullptr).typeOid;});
        check("initialized prepare without value demand",[&] {
            const auto query=g_engine.prepareBoundQuery(database,"SELECT NULL::"+expected.spelling+" AS value WHERE false");
            return query.output.size()==1?query.output[0].typeOid:Oid(0);
        });
    }
    bool shared=true;
    for(const auto& expected:CatalogManager::builtinTypeRows()) {
        const auto found=std::find_if(metadata.types.begin(),metadata.types.end(),
            [&](const PgTypeRow& type){return type.oid==expected.oid;});
        shared=shared && found!=metadata.types.end() && found->typnamespace==expected.typnamespace &&
            found->typname==expected.typname && found->typlen==expected.typlen &&
            found->typtype==expected.typtype && found->typcategory==expected.typcategory &&
            found->typelem==expected.typelem && found->typarray==expected.typarray;
    }
    require(shared,"pure physical definitions equal actual initialized rows");
    std::cout<<"PHYSICAL_CATALOG_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    return checked==526 && !failed?0:1;
}
