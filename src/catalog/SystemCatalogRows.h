#include "catalog/SystemAttributeDeclaration.h"
#pragma once
#include "catalog/PgClassCatalog.h"
#include "catalog/SystemCatalogDeclaration.h"
#include "catalog/type_registry.h"
#include "parser/query_binding.h"
#include <charconv>
#include <cmath>
#include <limits>

namespace dbms::system_catalog {
template<class Fields>
inline void validateRead(const CatalogManager::MetadataSnapshot& snapshot,Oid oid,const char* name,
    Oid rowOid,Oid arrayOid,const Fields& fields,size_t firstNullable) {
    const PgClassRow* relation=nullptr;
    for(const auto& r:snapshot.relations)if(r.oid==oid || (r.relnamespace==11 && r.relname==name)) {
        if(relation)throw DbError("XX001","duplicate system catalog read identity");
        relation=&r;
    }
    if(!relation || relation->oid!=oid || relation->relname!=name || relation->relnamespace!=11 ||
       relation->relkind!='r' || relation->reltype!=rowOid || relation->relnatts!=int16_t(fields.size()))
        throw DbError("XX001","missing or conflicting system catalog read declaration");
    const PgTypeRow* row=nullptr;const PgTypeRow* array=nullptr;
    for(const auto& type:snapshot.types){if(type.oid==rowOid)row=&type;if(type.oid==arrayOid)array=&type;}
    if(!row || !array || row->typrelid!=oid || row->typtype!='c' || row->typarray!=arrayOid ||
       array->typelem!=rowOid || row->typnamespace!=11 || array->typnamespace!=11)
        throw DbError("XX001","missing system catalog read row type");
    std::vector<const PgAttributeRow*> attributes;
    for(const auto& a:snapshot.attributes)if(a.attrelid==oid && a.attnum>0)attributes.push_back(&a);
    std::sort(attributes.begin(),attributes.end(),[](auto* a,auto* b){return a->attnum<b->attnum;});
    if(attributes.size()!=fields.size())throw DbError("XX001","incomplete system catalog read fields");
    for(size_t i=0;i<fields.size();++i){const auto& a=*attributes[i];const auto& field=fields[i];
        if(a.attname!=field.name || a.attnum!=int16_t(i+1) || a.atttypid!=field.type || a.attlen!=field.size ||
           a.attnotnull!=(i<firstNullable) || a.attisdropped || a.atttypmod!=-1)
            throw DbError("XX001","conflicting system catalog read field");}
}
template<class Fields>
inline QueryRowDescriptor ownedDescriptor(const Fields& fields,const std::vector<std::string>& names) {
    QueryRowDescriptor result;
    for(const auto& name:names) {
        const auto found=std::find_if(fields.begin(),fields.end(),[&](const auto& f){return name==f.name;});
        if(found==fields.end())throw DbError("XX000","unknown owned catalog declaration field");
        QueryOutputColumn column;column.name=name;column.typeOid=found->type;
        for(const auto& type:CatalogManager::builtinTypeRows())if(type.oid==found->type) {
            column.type=type.oid==18?"\"char\"":TypeRegistry::instance().normalizeTypeName(type.typname);
            if(column.type.empty())column.type=type.typname;
        }
        if(column.type.empty())throw DbError("XX001","missing owned catalog field type");
        column.physicalAttributeNumber=int16_t(std::distance(fields.begin(),found)+1);
        result.push_back(std::move(column));
    }
    return result;
}
inline const QueryRowDescriptor& classDescriptor() {
    static const auto fields=ownedDescriptor(class_catalog::fields,{
        "oid","relname","relnamespace","reltype","reloftype","relowner","relam","relfilenode",
        "reltablespace","relpages","reltuples","relallvisible","reltoastrelid","relhasindex",
        "relisshared","relpersistence","relkind","relnatts","relchecks","relhasrules","relhastriggers",
        "relhassubclass","relrowsecurity","relforcerowsecurity","relispopulated","relreplident",
        "relispartition","relrewrite","relfrozenxid","relminmxid"});
    return fields;
}
inline const QueryRowDescriptor& attributeDescriptor() {
    static const auto fields=ownedDescriptor(attributeFields,{
        "attrelid","attname","atttypid","attlen","attnum","atttypmod","attndims","attbyval",
        "attalign","attstorage","attnotnull","atthasdef","attidentity","attgenerated","attisdropped",
        "attislocal","attinhcount","attcollation","attstattarget"});
    return fields;
}
inline bool unownedColumn(const std::string& relation,const QueryRowDescriptor& owned,const std::string& name) {
    if(std::any_of(owned.begin(),owned.end(),[&](const auto& f){return f.name==name;}))return false;
    const auto contains=[&](const auto& fields){return std::any_of(fields.begin(),fields.end(),
        [&](const auto& field){return name==field.name;});};
    return relation=="pg_class"?contains(class_catalog::fields):
        relation=="pg_attribute"?contains(attributeFields):relation=="pg_type"?contains(typeFields):false;
}
inline std::string character(char value){return value?std::string(1,value):std::string{};}
inline std::string floating(float value) {
    if(std::isnan(value))return "NaN";
    if(std::isinf(value))return value<0?"-Infinity":"Infinity";
    char buffer[64];const auto result=std::to_chars(buffer,buffer+sizeof(buffer),value);
    if(result.ec!=std::errc{})throw DbError("XX000","catalog float4 output failed");
    return {buffer,result.ptr};
}
inline std::vector<ExprValue> classCells(const PgClassRow& r) {
    const std::vector<std::string> values={std::to_string(r.oid),r.relname,std::to_string(r.relnamespace),
        std::to_string(r.reltype),std::to_string(r.reloftype),std::to_string(r.relowner),std::to_string(r.relam),
        std::to_string(r.relfilenode),std::to_string(r.reltablespace),std::to_string(r.relpages),floating(r.reltuples),
        std::to_string(r.relallvisible),std::to_string(r.reltoastrelid),r.relhasindex?"t":"f",r.relisshared?"t":"f",
        character(r.relpersistence),character(r.relkind),std::to_string(r.relnatts),std::to_string(r.relchecks),
        r.relhasrules?"t":"f",r.relhastriggers?"t":"f",r.relhassubclass?"t":"f",r.relrowsecurity?"t":"f",
        r.relforcerowsecurity?"t":"f",r.relispopulated?"t":"f",character(r.relreplident),r.relispartition?"t":"f",
        std::to_string(r.relrewrite),std::to_string(r.relfrozenxid),std::to_string(r.relminmxid)};
    const auto& descriptor=classDescriptor();std::vector<ExprValue> cells;cells.reserve(values.size());
    for(size_t i=0;i<values.size();++i)cells.emplace_back(descriptor[i].type,values[i],false);
    return cells;
}
inline std::vector<ExprValue> attributeCells(const PgAttributeRow& r) {
    if(r.attstattarget < -1)throw DbError("XX001","invalid catalog statistics target");
    for(int32_t value:{r.attndims,r.attinhcount,r.attstattarget})if(value<std::numeric_limits<int16_t>::min() ||
       value>std::numeric_limits<int16_t>::max())throw DbError("XX001","catalog int2 field out of range");
    const std::vector<std::string> values={std::to_string(r.attrelid),r.attname,std::to_string(r.atttypid),
        std::to_string(r.attlen),std::to_string(r.attnum),std::to_string(r.atttypmod),std::to_string(r.attndims),
        r.attbyval?"t":"f",character(r.attalign),character(r.attstorage),r.attnotnull?"t":"f",r.atthasdef?"t":"f",
        character(r.attidentity),character(r.attgenerated),r.attisdropped?"t":"f",r.attislocal?"t":"f",
        std::to_string(r.attinhcount),std::to_string(r.attcollation),r.attstattarget<0?"":std::to_string(r.attstattarget)};
    const auto& descriptor=attributeDescriptor();std::vector<ExprValue> cells;cells.reserve(values.size());
    for(size_t i=0;i<values.size();++i)cells.emplace_back(descriptor[i].type,values[i],i+1==values.size() && r.attstattarget<0);
    return cells;
}
inline std::vector<std::vector<ExprValue>> classRows(const CatalogManager::MetadataSnapshot& snapshot) {
    validateRead(snapshot,1259,"pg_class",83,273,class_catalog::fields,31);
    std::vector<std::vector<ExprValue>> rows;rows.reserve(snapshot.relations.size());
    for(const auto& row:snapshot.relations)rows.push_back(classCells(row));
    return rows;
}
inline std::vector<std::vector<ExprValue>> attributeRows(const CatalogManager::MetadataSnapshot& snapshot) {
    validateRead(snapshot,1249,"pg_attribute",75,270,attributeFields,20);
    if(snapshot.systemAttributesMaterialized)system_attributes::validateAll(snapshot.relations,snapshot.attributes,snapshot.types);
    std::vector<std::vector<ExprValue>> rows;rows.reserve(snapshot.attributes.size());
    for(const auto& row:snapshot.attributes)rows.push_back(attributeCells(row));
    return rows;
}
} // namespace dbms::system_catalog
