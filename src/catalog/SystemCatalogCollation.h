#pragma once
#include "catalog/systables.h"
#include "common/DbError.h"
#include <array>
#include <filesystem>
#include <fstream>

namespace dbms::system_catalog_collation {
// PostgreSQL 18 genbki.pl uses C collation for collatable bootstrap-catalog
// fields. The declared type itself retains its normal/default collation.
inline constexpr char version[]="DBMS_SYSTEM_CATALOG_COLLATION1\n";
struct Field {Oid relation;const char* relationName;int16_t number;const char* name;Oid type;};
inline constexpr std::array<Field,6> fields{{
    {1247,"pg_type",30,"typdefaultbin",194},{1247,"pg_type",31,"typdefault",25},
    {1249,"pg_attribute",23,"attoptions",1009},{1249,"pg_attribute",24,"attfdwoptions",1009},
    {1259,"pg_class",33,"reloptions",1009},{1259,"pg_class",34,"relpartbound",194}}};
inline bool published(const std::filesystem::path& directory) {
    const auto path=directory/".system_catalog_collation_version";std::error_code error;
    const auto status=std::filesystem::symlink_status(path,error);
    if(error==std::errc::no_such_file_or_directory || (!error && !std::filesystem::exists(status)))return false;
    if(error)throw DbError("58030","cannot inspect system catalog collation version");
    if(!std::filesystem::is_regular_file(status))throw DbError("XX001","invalid system catalog collation version");
    std::ifstream input(path,std::ios::binary);if(!input)throw DbError("58030","cannot read system catalog collation version");
    const std::string bytes((std::istreambuf_iterator<char>(input)),{});
    if(input.bad() || bytes!=version)throw DbError("XX001","invalid system catalog collation version");
    return true;
}
inline std::vector<size_t> inspect(const std::vector<PgClassRow>& relations,
    const std::vector<PgAttributeRow>& attributes,const std::vector<PgTypeRow>& types,
    bool requireExisting,bool acceptLegacy) {
    std::vector<size_t> indices;
    for(const auto& field:fields) {
        const PgClassRow* relation=nullptr;
        for(const auto& r:relations)if(r.oid==field.relation || (r.relnamespace==11 && r.relname==field.relationName)) {
            if(relation)throw DbError("XX001","duplicate system catalog collation identity");
            relation=&r;
        }
        if(!relation) {
            if(requireExisting)throw DbError("XX001","missing system catalog collation relation");
            continue;
        }
        if(relation->oid!=field.relation || relation->relname!=field.relationName ||
           relation->relnamespace!=11 || relation->relkind!='r')
            throw DbError("XX001","conflicting system catalog collation relation");
        const PgTypeRow* type=nullptr;
        for(const auto& t:types)if(t.oid==field.type) {
            if(type)throw DbError("XX001","duplicate system catalog collation type");
            type=&t;
        }
        if(!type || type->typnamespace!=11 || type->typlen!=-1 || type->typcollation!=100 || !type->typisdefined)
            throw DbError("XX001","conflicting system catalog collation type");
        size_t index=attributes.size();
        for(size_t i=0;i<attributes.size();++i) {
            const auto& a=attributes[i];
            if(a.attrelid==field.relation && (a.attnum==field.number || a.attname==field.name)) {
                if(index!=attributes.size())throw DbError("XX001","duplicate system catalog collation field");
                index=i;
            }
        }
        if(index==attributes.size())throw DbError("XX001","missing system catalog collation field");
        const auto& a=attributes[index];
        if(a.attnum!=field.number || a.attname!=field.name || a.atttypid!=field.type || a.attlen!=-1 ||
           a.attisdropped || (a.attcollation!=950 && !(acceptLegacy && a.attcollation==100)))
            throw DbError("XX001","conflicting system catalog collation field");
        indices.push_back(index);
    }
    return indices;
}
inline void validate(const std::vector<PgClassRow>& relations,const std::vector<PgAttributeRow>& attributes,
    const std::vector<PgTypeRow>& types) {(void)inspect(relations,attributes,types,true,false);}
} // namespace dbms::system_catalog_collation
