#pragma once
#include "catalog/catalog.h"
#include "common/DbError.h"
#include <algorithm>
#include <array>

namespace dbms::sequence_catalog {
inline constexpr Oid relationOid=2224;
struct Field {const char* name;Oid type;int16_t size;};
inline constexpr std::array<Field,8> fields{{
    {"seqrelid",26,4},{"seqtypid",26,4},{"seqstart",20,8},{"seqincrement",20,8},
    {"seqmax",20,8},{"seqmin",20,8},{"seqcache",20,8},{"seqcycle",16,1}}};

inline void validateReadDeclaration(const CatalogManager::MetadataSnapshot& metadata) {
    const PgClassRow* relation=nullptr;
    for(const auto& row:metadata.relations)
        if(row.oid==relationOid || (row.relnamespace==11 && row.relname=="pg_sequence")) {
            if(relation)throw DbError("XX001","duplicate sequence catalog read declaration");relation=&row;
        }
    if(!relation || relation->oid!=relationOid || relation->relname!="pg_sequence" ||
       relation->relnamespace!=11 || relation->relkind!='r' || relation->relnatts!=8)
        throw DbError("XX001","missing actual sequence catalog read declaration");
    std::vector<const PgAttributeRow*> attributes;
    for(const auto& row:metadata.attributes)if(row.attrelid==relationOid && row.attnum>0)attributes.push_back(&row);
    std::sort(attributes.begin(),attributes.end(),[](const auto* a,const auto* b){return a->attnum<b->attnum;});
    if(attributes.size()!=fields.size())throw DbError("XX001","incomplete sequence catalog read attributes");
    for(size_t i=0;i<fields.size();++i) {
        const auto& attribute=*attributes[i];const auto& field=fields[i];
        if(attribute.attnum!=int16_t(i+1) || attribute.attname!=field.name ||
           attribute.atttypid!=field.type || attribute.attlen!=field.size || !attribute.attnotnull ||
           attribute.attisdropped || attribute.attndims || attribute.atttypmod!=-1)
            throw DbError("XX001","conflicting sequence catalog read attribute");
    }
}

// Only CatalogService's locked mutable startup owner initializes this
// declaration. A copied reader cannot create identities or repair corruption.
inline bool bootstrapRelation(CatalogManager& catalog,bool requireExisting=false) {
    const auto metadata=catalog.metadataSnapshot();
    const auto space=catalog.getNamespaceByName("pg_catalog");
    if(!space || space->oid!=11)throw DbError("XX001","missing sequence catalog namespace");
    const PgClassRow* existing=nullptr;
    for(const auto& row:metadata.relations)
        if(row.oid==relationOid || (row.relnamespace==space->oid && row.relname=="pg_sequence")) {
            if(existing)throw DbError("XX001","duplicate sequence catalog declaration");
            existing=&row;
        }
    if(requireExisting && !existing)throw DbError("XX001","missing published sequence catalog declaration");
    if(existing && (existing->oid!=relationOid || existing->relname!="pg_sequence" ||
       existing->relnamespace!=space->oid || existing->relowner!=space->nspowner ||
       existing->relkind!='r' || existing->relpersistence!='p' || existing->relnatts!=8 || !existing->reltype))
        throw DbError("XX001","conflicting sequence catalog declaration");
    std::vector<PgAttributeRow> attributes;
    for(size_t i=0;i<fields.size();++i) {
        const auto& field=fields[i];const PgTypeRow* type=nullptr;
        for(const auto& row:metadata.types)if(row.oid==field.type) {
            if(type)throw DbError("XX001","duplicate sequence catalog field type");type=&row;
        }
        if(!type || type->typnamespace!=space->oid || type->typlen!=field.size || !type->typisdefined)
            throw DbError("XX001","missing actual sequence catalog field type");
        PgAttributeRow attribute;attribute.attrelid=relationOid;attribute.attname=field.name;
        attribute.atttypid=field.type;attribute.attlen=field.size;attribute.attnum=i+1;
        attribute.attnotnull=true;attribute.attbyval=type->typbyval;
        attribute.attalign=type->typalign;attribute.attstorage=type->typstorage;
        attributes.push_back(std::move(attribute));
    }
    std::vector<PgAttributeRow> loaded;
    for(const auto& row:metadata.attributes)if(row.attrelid==relationOid && row.attnum>0)loaded.push_back(row);
    std::sort(loaded.begin(),loaded.end(),[](const auto& a,const auto& b){return a.attnum<b.attnum;});
    if(!existing && !loaded.empty())throw DbError("XX001","orphan sequence catalog attributes");
    if(existing) {
        if(loaded.size()!=attributes.size())throw DbError("XX001","incomplete sequence catalog attributes");
        for(size_t i=0;i<loaded.size();++i) {
            const auto& a=loaded[i];const auto& b=attributes[i];
            if(a.attnum!=b.attnum || a.attname!=b.attname || a.atttypid!=b.atttypid ||
               a.attlen!=b.attlen || !a.attnotnull || a.attbyval!=b.attbyval ||
               a.attalign!=b.attalign || a.attstorage!=b.attstorage || a.attisdropped ||
               a.attndims || a.atthasdef || a.attidentity || a.attgenerated || a.atttypmod!=-1)
                throw DbError("XX001","conflicting sequence catalog attribute");
        }
        const auto row=catalog.getType(existing->reltype);
        const auto array=row?catalog.getType(row->typarray):std::nullopt;
        if(!row || row->typnamespace!=space->oid || row->typname!="pg_sequence" ||
           row->typtype!='c' || row->typcategory!='C' || row->typrelid!=relationOid ||
           row->typlen!=-1 || row->typbyval || row->typalign!='d' || row->typstorage!='x' ||
           !array || array->typnamespace!=space->oid || array->typname!="_pg_sequence" ||
           array->typtype!='b' || array->typcategory!='A' || array->typelem!=row->oid ||
           array->typlen!=-1 || array->typbyval || array->typalign!='d' || array->typstorage!='x')
            throw DbError("XX001","invalid actual sequence catalog row type");
        for(const auto& edge:std::array<std::pair<Oid,Oid>,2>{{{row->oid,relationOid},{array->oid,row->oid}}}) {
            const auto classId=edge.first==row->oid?PgClassOid_Class:PgClassOid_Type;
            const auto dependencies=catalog.findDepends(PgClassOid_Type,edge.first,0);
            if(std::count_if(dependencies.begin(),dependencies.end(),[&](const auto& dependency) {
                return dependency.refclassid==classId && dependency.refobjid==edge.second &&
                    !dependency.refobjsubid && dependency.deptype=='i';
            })!=1)throw DbError("XX001","incomplete sequence catalog type dependency");
        }
        for(const auto& attribute:attributes) {
            const auto dependencies=catalog.findDepends(PgClassOid_Class,relationOid,attribute.attnum);
            if(std::count_if(dependencies.begin(),dependencies.end(),[&](const auto& dependency) {
                return dependency.refclassid==PgClassOid_Type && dependency.refobjid==attribute.atttypid &&
                    !dependency.refobjsubid && dependency.deptype=='n';
            })!=1)throw DbError("XX001","incomplete sequence catalog attribute dependency");
        }
        return false;
    }
    if(catalog.getTypeByName("pg_sequence",space->oid) || catalog.getTypeByName("_pg_sequence",space->oid))
        throw DbError("XX001","orphan sequence catalog row type");
    PgClassRow relation;relation.oid=relationOid;relation.relname="pg_sequence";relation.relnamespace=space->oid;
    relation.relowner=space->nspowner;relation.relkind='r';relation.relnatts=8;relation.reltype=catalog.allocateOid();
    PgTypeRow row;row.oid=relation.reltype;row.typname=relation.relname;row.typnamespace=space->oid;
    row.typowner=space->nspowner;row.typtype='c';row.typcategory='C';row.typlen=-1;
    row.typalign='d';row.typstorage='x';row.typrelid=relationOid;row.typarray=catalog.allocateOid();
    PgTypeRow array;array.oid=row.typarray;array.typname="_pg_sequence";array.typnamespace=space->oid;
    array.typowner=space->nspowner;array.typtype='b';array.typcategory='A';array.typlen=-1;
    array.typalign='d';array.typstorage='x';array.typelem=row.oid;
    if(catalog.createClass(relation)!=relationOid || catalog.createType(row)!=row.oid ||
       catalog.createType(array)!=array.oid || !catalog.replaceAttributes(relationOid,attributes))
        throw DbError("XX001","could not initialize sequence catalog declaration");
    PgDependRow dependency;dependency.classid=PgClassOid_Type;dependency.objid=row.oid;
    dependency.refclassid=PgClassOid_Class;dependency.refobjid=relationOid;dependency.deptype='i';catalog.addDepend(dependency);
    dependency.objid=array.oid;dependency.refclassid=PgClassOid_Type;dependency.refobjid=row.oid;catalog.addDepend(dependency);
    for(const auto& attribute:attributes) {
        PgDependRow field;field.classid=PgClassOid_Class;field.objid=relationOid;field.objsubid=attribute.attnum;
        field.refclassid=PgClassOid_Type;field.refobjid=attribute.atttypid;field.deptype='n';catalog.addDepend(field);
    }
    return true;
}
}
