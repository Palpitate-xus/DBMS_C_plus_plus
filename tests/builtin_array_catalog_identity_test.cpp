#include "catalog/CatalogService.h"
#include "catalog/declared_type.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    struct Array {Oid oid;std::string name;Oid element;};
    const std::vector<Array> arrays={
        {1000,"_bool",16},
        {1001,"_bytea",17},
        {1002,"_char",18},
        {1003,"_name",19},
        {1005,"_int2",21},
        {1007,"_int4",23},
        {1009,"_text",25},
        {1014,"_bpchar",1042},
        {1015,"_varchar",1043},
        {1016,"_int8",20},
        {1017,"_point",600},
        {1018,"_lseg",601},
        {1019,"_path",602},
        {1020,"_box",603},
        {1021,"_float4",700},
        {1022,"_float8",701},
        {1027,"_polygon",604},
        {629,"_line",628},
        {719,"_circle",718},
        {1040,"_macaddr",829},
        {1041,"_inet",869},
        {651,"_cidr",650},
        {775,"_macaddr8",774},
        {791,"_money",790},
        {1182,"_date",1082},
        {1183,"_time",1083},
        {1115,"_timestamp",1114},
        {1185,"_timestamptz",1184},
        {1187,"_interval",1186},
        {1270,"_timetz",1266},
        {1561,"_bit",1560},
        {1563,"_varbit",1562},
        {1231,"_numeric",1700},
        {2951,"_uuid",2950},
        {199,"_json",114},
        {3807,"_jsonb",3802},
        {143,"_xml",142},
    };
    size_t checked=0,failed=0;
    const auto require=[&](bool pass,const std::string& role){++checked;failed+=!pass;std::cout<<"BUILTIN_ARRAY_CATALOG "<<role<<" pass="<<pass<<'\n';};
    const auto database=testDbPath("builtin_array_catalog");
    require(g_engine.createDatabase(database)==DBStatus::OK,"create isolated database");
    (void)g_engine.catalogService().get(database);
    const auto metadata=g_engine.catalogService().metadataSnapshot(database);
    for(const auto& expected:arrays) {
        const PgTypeRow* array=nullptr;const PgTypeRow* base=nullptr;
        for(const auto& type:metadata.types){if(type.oid==expected.oid)array=&type;if(type.oid==expected.element)base=&type;}
        require(array && array->typnamespace==11 && array->typname==expected.name && array->typcategory=='A' &&
            array->typlen==-1 && array->typelem==expected.element,"actual array row "+expected.name);
        require(base && base->typnamespace==11 && base->typname==expected.name.substr(1) && base->typarray==expected.oid,
            "actual element backlink "+expected.name);
        bool pass=false;
        try{const auto binding=resolveDeclaredTypeName("pg_catalog."+expected.name.substr(1)+"[]",&metadata,nullptr);pass=binding.typeOid==expected.oid && binding.inputType.size()>=2 && binding.inputType.substr(binding.inputType.size()-2)=="[]";}
        catch(const DbError&){ }
        require(pass,"copied catalog declared array identity "+expected.name);
    }
    require(g_engine.catalogService().persistAll(),"persist actual catalog records");
    // Read the persisted snapshot directly: no cache or bootstrap may fill
    // missing array fields and hide an on-disk regression.
    const auto persisted=CatalogManager::readMetadataSnapshot(
        (g_engine.dbPath(database)/"pg_catalog").string());
    for(const auto& expected:arrays) {
        const PgTypeRow* array=nullptr;const PgTypeRow* base=nullptr;
        for(const auto& type:persisted.types){if(type.oid==expected.oid)array=&type;if(type.oid==expected.element)base=&type;}
        require(array && array->typnamespace==11 && array->typname==expected.name &&
            array->typelem==expected.element,"persisted array element "+expected.name);
        require(base && base->typnamespace==11 && base->typarray==expected.oid,
            "persisted scalar backlink "+expected.name);
    }
    std::cout<<"BUILTIN_ARRAY_CATALOG_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    return checked==187 && !failed?0:1;
}
