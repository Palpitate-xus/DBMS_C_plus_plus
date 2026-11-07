#pragma once
#include "catalog/systables.h"
#include "common/DbError.h"
#include <algorithm>
#include <array>
#include <filesystem>
#include <fstream>

namespace dbms::system_attributes {
inline constexpr char version[]="DBMS_SYSTEM_ATTRIBUTES1\n";
struct Field {const char* name;Oid type;int16_t size;};
inline constexpr std::array<Field,6> fields{{
    {"ctid",27,6},{"xmin",28,4},{"cmin",29,4},{"xmax",28,4},{"cmax",29,4},{"tableoid",26,4}}};
inline bool eligible(char kind) {
    return kind=='r' || kind=='S' || kind=='t' || kind=='m' || kind=='p' || kind=='f';
}
inline bool reserved(const std::string& name) {
    return std::any_of(fields.begin(),fields.end(),[&](const auto& f){return name==f.name;});
}
inline bool published(const std::filesystem::path& directory) {
    const auto path=directory/".system_attribute_version";std::error_code error;
    const auto status=std::filesystem::symlink_status(path,error);
    if(error==std::errc::no_such_file_or_directory || (!error && !std::filesystem::exists(status)))return false;
    if(error)throw DbError("58030","cannot inspect system attribute version");
    if(!std::filesystem::is_regular_file(status))throw DbError("XX001","invalid system attribute version file");
    std::ifstream input(path,std::ios::binary);if(!input)throw DbError("58030","cannot read system attribute version");
    const std::string bytes((std::istreambuf_iterator<char>(input)),{});
    if(input.bad() || bytes!=version)throw DbError("XX001","invalid system attribute version");
    return true;
}
inline std::vector<PgAttributeRow> declarations(Oid relation,const std::vector<PgTypeRow>& types) {
    std::vector<PgAttributeRow> rows;
    for(size_t i=0;i<fields.size();++i) {
        const auto& field=fields[i];const PgTypeRow* type=nullptr;
        for(const auto& t:types)if(t.oid==field.type){if(type)throw DbError("XX001","duplicate system attribute type");type=&t;}
        if(!type || type->typnamespace!=11 || type->typlen!=field.size || !type->typisdefined)
            throw DbError("XX001","missing actual system attribute type");
        PgAttributeRow a;a.attrelid=relation;a.attname=field.name;a.atttypid=field.type;
        a.attlen=field.size;a.attnum=-int16_t(i+1);a.attbyval=type->typbyval;
        a.attalign=type->typalign;a.attstorage=type->typstorage;a.attnotnull=true;rows.push_back(a);
    }
    return rows;
}
inline void validate(const PgClassRow& relation,const std::vector<PgAttributeRow>& attributes,
    const std::vector<PgTypeRow>& types,bool requireExisting) {
    std::vector<PgAttributeRow> loaded;
    for(const auto& a:attributes)if(a.attrelid==relation.oid && a.attnum<0)loaded.push_back(a);
    if(!eligible(relation.relkind)) {
        if(!loaded.empty())throw DbError("XX001","unexpected system attributes for relation kind");
        return;
    }
    if(loaded.empty() && !requireExisting)return;
    if(loaded.size()!=fields.size())throw DbError("XX001","incomplete system attribute declaration");
    std::sort(loaded.begin(),loaded.end(),[](const auto& a,const auto& b){return a.attnum>b.attnum;});
    const auto expected=declarations(relation.oid,types);
    for(size_t i=0;i<expected.size();++i) {const auto& a=loaded[i];const auto& e=expected[i];
        if(a.attname!=e.attname || a.atttypid!=e.atttypid || a.attnum!=e.attnum || a.attlen!=e.attlen ||
           a.atttypmod!=-1 || !a.attnotnull || !a.attislocal || a.attinhcount || a.attisdropped ||
           a.atthasdef || a.attidentity || a.attgenerated || a.attndims || a.attcollation ||
           a.attbyval!=e.attbyval || a.attalign!=e.attalign || a.attstorage!=e.attstorage)
            throw DbError("XX001","conflicting system attribute declaration");
    }
}
inline void validateAll(const std::vector<PgClassRow>& relations,const std::vector<PgAttributeRow>& attributes,
    const std::vector<PgTypeRow>& types) {
    for(const auto& relation:relations)validate(relation,attributes,types,true);
}
} // namespace dbms::system_attributes
