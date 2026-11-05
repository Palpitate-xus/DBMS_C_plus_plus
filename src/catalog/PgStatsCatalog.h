#pragma once
#include "catalog/AnalyzedStatistics.h"
#include "catalog/catalog.h"
#include "common/DbError.h"
#include <algorithm>

namespace dbms::analyzed_statistics {
// Called only while a mutable catalog generation is being initialized and
// before CatalogService publishes that generation. Cold metadata reads never
// call this function or invent an identity for a missing view.
inline bool bootstrapView(CatalogManager& catalog) {
    const auto space=catalog.getNamespaceByName("pg_catalog");
    if(!space || space->oid!=11)throw DbError("XX001","missing actual statistics catalog namespace");
    if(const auto existing=catalog.getClassByName("pg_stats",space->oid)) {
        if(existing->relkind!='v')throw DbError("XX001","statistics view identity conflicts with a relation");
        const auto metadata=catalog.metadataSnapshot();
        std::vector<const PgAttributeRow*> attributes;
        for(const auto& a:metadata.attributes)if(a.attrelid==existing->oid)attributes.push_back(&a);
        std::sort(attributes.begin(),attributes.end(),[](const auto* a,const auto* b){return a->attnum<b->attnum;});
        if(attributes.size()!=fields.size() || existing->relnatts!=int16_t(fields.size()))
            throw DbError("XX001","incomplete statistics view declaration");
        for(size_t i=0;i<fields.size();++i)if(attributes[i]->attnum!=int16_t(i+1) ||
            attributes[i]->attname!=fields[i].name || attributes[i]->atttypid!=fields[i].typeOid ||
            attributes[i]->attlen!=fields[i].size || attributes[i]->attisdropped)
            throw DbError("XX001","conflicting statistics view attribute declaration");
        const auto rowtype=catalog.getType(existing->reltype);
        if(!rowtype || rowtype->typtype!='c' || rowtype->typrelid!=existing->oid ||
           rowtype->typnamespace!=space->oid || rowtype->typname!=existing->relname)
            throw DbError("XX001","invalid actual statistics view row type");
        return false;
    }
    if(catalog.getTypeByName("pg_stats",space->oid))
        throw DbError("XX001","statistics view row type conflicts with an existing declaration");
    PgClassRow view;view.oid=catalog.allocateOid();view.relname="pg_stats";view.relnamespace=space->oid;
    view.relowner=space->nspowner;view.relkind='v';view.relnatts=fields.size();view.reltype=catalog.allocateOid();
    std::vector<PgAttributeRow> attributes;attributes.reserve(fields.size());
    const auto metadata=catalog.metadataSnapshot();
    for(size_t i=0;i<fields.size();++i) {
        const auto& spec=fields[i];
        const auto type=std::find_if(metadata.types.begin(),metadata.types.end(),[&](const auto& t){return t.oid==spec.typeOid;});
        if(type==metadata.types.end() || type->typnamespace!=11 || type->typlen!=spec.size)
            throw DbError("XX001","missing actual statistics field type");
        PgAttributeRow a;a.attrelid=view.oid;a.attname=spec.name;a.atttypid=spec.typeOid;
        a.attlen=spec.size;a.attnum=i+1;a.attbyval=type->typbyval;a.attalign=type->typalign;
        a.attstorage=type->typstorage;attributes.push_back(std::move(a));
    }
    PgTypeRow rowtype;rowtype.oid=view.reltype;rowtype.typname=view.relname;rowtype.typnamespace=space->oid;
    rowtype.typtype='c';rowtype.typcategory='C';rowtype.typlen=-1;rowtype.typalign='d';rowtype.typstorage='x';rowtype.typrelid=view.oid;
    if(catalog.createClass(view)!=view.oid || catalog.createType(rowtype)!=rowtype.oid ||
       !catalog.replaceAttributes(view.oid,attributes))
        throw DbError("XX001","could not initialize statistics view declaration");
    PgDependRow dependency;dependency.classid=PgClassOid_Type;dependency.objid=rowtype.oid;
    dependency.refclassid=PgClassOid_Class;dependency.refobjid=view.oid;dependency.deptype='i';catalog.addDepend(dependency);
    return true;
}
} // namespace dbms::analyzed_statistics
