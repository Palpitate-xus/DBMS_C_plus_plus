#pragma once
#include "catalog/catalog.h"
#include "common/DbError.h"
#include <algorithm>
#include <array>

namespace dbms::class_catalog {
// Declaration of the virtual catalog, in PostgreSQL 18 attribute order. This
// does not supply heap storage, the remaining value renderers, or ACL codecs.
struct Field { const char* name; Oid type; int16_t size; };
inline constexpr std::array<Field,34> fields{{
    {"oid",26,4},{"relname",19,64},{"relnamespace",26,4},{"reltype",26,4},
    {"reloftype",26,4},{"relowner",26,4},{"relam",26,4},{"relfilenode",26,4},
    {"reltablespace",26,4},{"relpages",23,4},{"reltuples",700,4},
    {"relallvisible",23,4},{"relallfrozen",23,4},{"reltoastrelid",26,4},
    {"relhasindex",16,1},{"relisshared",16,1},{"relpersistence",18,1},
    {"relkind",18,1},{"relnatts",21,2},{"relchecks",21,2},
    {"relhasrules",16,1},{"relhastriggers",16,1},{"relhassubclass",16,1},
    {"relrowsecurity",16,1},{"relforcerowsecurity",16,1},{"relispopulated",16,1},
    {"relreplident",18,1},{"relispartition",16,1},{"relrewrite",26,4},
    {"relfrozenxid",28,4},{"relminmxid",28,4},{"relacl",1034,-1},
    {"reloptions",1009,-1},{"relpartbound",194,-1}}};

// Only the mutable, locked CatalogService startup owner calls this function.
// Validate the entire declaration before editing it. A missing/corrupt loaded
// declaration is not repaired piecemeal, nor synthesized by cold readers.
inline bool bootstrapRelation(CatalogManager& catalog) {
    const auto snapshot=catalog.metadataSnapshot();
    const auto space=catalog.getNamespaceByName("pg_catalog");
    if(!space || space->oid!=11)
        throw DbError("XX001","missing actual pg_class catalog namespace");
    std::array<PgTypeRow,5> declarations;
    const auto type=[&](size_t i,Oid oid,const char* name,int16_t size,
                        char kind,char category,char align,char storage) -> PgTypeRow& {
        auto& row=declarations[i];row.oid=oid;row.typname=name;row.typnamespace=space->oid;
        row.typowner=space->nspowner;row.typlen=size;row.typtype=kind;
        row.typcategory=category;row.typalign=align;row.typstorage=storage;
        // No input/output function identity is invented for a declaration.
        return row;
    };
    type(0,83,"pg_class",-1,'c','C','d','x').typrelid=PgClassOid_Class;
    declarations[0].typarray=273;
    type(1,273,"_pg_class",-1,'b','A','d','x').typelem=83;
    type(2,194,"pg_node_tree",-1,'b','Z','i','x').typcollation=100;
    type(3,1033,"aclitem",16,'b','U','d','p').typarray=1034;
    type(4,1034,"_aclitem",-1,'b','A','d','x').typelem=1033;
    size_t present=0;
    for(const auto& expected:declarations) {
        const PgTypeRow* found=nullptr;
        for(const auto& actual:snapshot.types)
            if(actual.oid==expected.oid ||
               (actual.typnamespace==space->oid && actual.typname==expected.typname)) {
                if(found)throw DbError("XX001","duplicate pg_class type declaration");
                found=&actual;
            }
        if(!found)continue;
        ++present;
        if(found->oid!=expected.oid || found->typname!=expected.typname ||
           found->typnamespace!=expected.typnamespace || found->typowner!=expected.typowner ||
           found->typlen!=expected.typlen || found->typbyval ||
           found->typtype!=expected.typtype || found->typcategory!=expected.typcategory ||
           found->typrelid!=expected.typrelid || found->typelem!=expected.typelem ||
           found->typarray!=expected.typarray || found->typalign!=expected.typalign ||
           found->typstorage!=expected.typstorage || found->typcollation!=expected.typcollation ||
           !found->typisdefined || found->typbasetype || found->typinput || found->typoutput ||
           found->typreceive || found->typsend || found->typmodin || found->typmodout)
            throw DbError("XX001","conflicting pg_class type declaration");
    }
    const PgClassRow* existing=nullptr;
    for(const auto& row:snapshot.relations)
        if(row.oid==PgClassOid_Class ||
           (row.relnamespace==space->oid && row.relname=="pg_class")) {
            if(existing)throw DbError("XX001","duplicate pg_class relation declaration");
            existing=&row;
        }
    if(existing && (existing->oid!=PgClassOid_Class || existing->relname!="pg_class" ||
       existing->relnamespace!=space->oid || existing->relowner!=space->nspowner ||
       existing->reltype!=83 || existing->relkind!='r' || existing->relpersistence!='p' ||
       existing->relnatts!=int16_t(fields.size())))
        throw DbError("XX001","conflicting pg_class relation declaration");
    if(present!=(existing?declarations.size():0))
        throw DbError("XX001","incomplete pg_class type declaration");

    std::vector<PgAttributeRow> attributes;
    attributes.reserve(fields.size());
    for(size_t i=0;i<fields.size();++i) {
        const auto& field=fields[i];
        const PgTypeRow* fieldType=nullptr;
        // For a new generation, additional declarations are not published
        // until all existing builtin field dependencies have been checked.
        for(const auto& row:snapshot.types)if(row.oid==field.type) {
            if(fieldType)throw DbError("XX001","duplicate pg_class field type");
            fieldType=&row;
        }
        if(!fieldType && !existing)
            for(const auto& row:declarations)if(row.oid==field.type)fieldType=&row;
        if(!fieldType || fieldType->typnamespace!=space->oid || fieldType->typlen!=field.size)
            throw DbError("XX001","missing actual pg_class field type");
        const auto& builtins=CatalogManager::builtinTypeRows();
        const auto builtin=std::find_if(builtins.begin(),builtins.end(),[&](const auto& t){return t.oid==field.type;});
        if(builtin!=builtins.end() &&
           (fieldType->typname!=builtin->typname || fieldType->typtype!=builtin->typtype))
            throw DbError("XX001","conflicting actual pg_class field type");
        PgAttributeRow a;a.attrelid=PgClassOid_Class;a.attname=field.name;
        a.atttypid=field.type;a.attlen=field.size;a.attnum=int16_t(i+1);
        a.attbyval=fieldType->typbyval;a.attalign=fieldType->typalign;
        a.attstorage=fieldType->typstorage;a.attcollation=fieldType->typcollation;
        a.attndims=fieldType->typcategory=='A'?1:0;a.attnotnull=i<31;
        attributes.push_back(std::move(a));
    }
    std::vector<const PgAttributeRow*> loaded;
    for(const auto& a:snapshot.attributes)if(a.attrelid==PgClassOid_Class)loaded.push_back(&a);
    std::sort(loaded.begin(),loaded.end(),[](const auto* a,const auto* b){return a->attnum<b->attnum;});
    if(!existing && !loaded.empty())throw DbError("XX001","orphan pg_class attributes");
    if(existing) {
        if(loaded.size()!=attributes.size())throw DbError("XX001","incomplete pg_class attributes");
        for(size_t i=0;i<attributes.size();++i) {
            const auto& a=*loaded[i];const auto& e=attributes[i];
            if(a.attnum!=e.attnum || a.attname!=e.attname || a.atttypid!=e.atttypid ||
               a.attlen!=e.attlen || a.attndims!=e.attndims || a.atttypmod!=-1 ||
               a.attbyval!=e.attbyval || a.attalign!=e.attalign || a.attstorage!=e.attstorage ||
               a.attnotnull!=e.attnotnull || a.attcollation!=e.attcollation ||
               a.attisdropped || a.atthasdef)
                throw DbError("XX001","conflicting pg_class attribute declaration");
        }
        const auto dependency=[&](Oid cls,Oid obj,int32_t sub,Oid refcls,Oid ref,char kind) {
            const auto rows=catalog.findDepends(cls,obj,sub);
            if(std::count_if(rows.begin(),rows.end(),[&](const auto& d) {
                return d.refclassid==refcls && d.refobjid==ref && d.refobjsubid==0 && d.deptype==kind;
            })!=1)throw DbError("XX001","incomplete pg_class dependency declaration");
        };
        dependency(PgClassOid_Class,PgClassOid_Class,0,PgClassOid_Namespace,space->oid,'n');
        for(const auto& t:declarations)dependency(PgClassOid_Type,t.oid,0,PgClassOid_Namespace,space->oid,'n');
        dependency(PgClassOid_Type,83,0,PgClassOid_Class,PgClassOid_Class,'i');
        dependency(PgClassOid_Type,273,0,PgClassOid_Type,83,'i');
        dependency(PgClassOid_Type,1034,0,PgClassOid_Type,1033,'i');
        for(const auto& a:attributes)dependency(PgClassOid_Class,PgClassOid_Class,a.attnum,PgClassOid_Type,a.atttypid,'n');
        return false;
    }
    PgClassRow relation;relation.oid=PgClassOid_Class;relation.relname="pg_class";
    relation.relnamespace=space->oid;relation.relowner=space->nspowner;
    relation.reltype=83;relation.relnatts=int16_t(fields.size());
    for(const auto& row:declarations)
        if(catalog.createType(row)!=row.oid)throw DbError("XX001","could not initialize pg_class types");
    if(catalog.createClass(relation)!=relation.oid || !catalog.replaceAttributes(relation.oid,attributes))
        throw DbError("XX001","could not initialize pg_class declaration");
    const auto depend=[&](Oid objectClass,Oid object,int32_t sub,Oid referenceClass,Oid reference,char kind) {
        PgDependRow d;d.classid=objectClass;d.objid=object;d.objsubid=sub;
        d.refclassid=referenceClass;d.refobjid=reference;d.deptype=kind;catalog.addDepend(d);
    };
    depend(PgClassOid_Type,83,0,PgClassOid_Class,relation.oid,'i');
    depend(PgClassOid_Type,273,0,PgClassOid_Type,83,'i');
    depend(PgClassOid_Type,1034,0,PgClassOid_Type,1033,'i');
    for(const auto& a:attributes)depend(PgClassOid_Class,relation.oid,a.attnum,PgClassOid_Type,a.atttypid,'n');
    return true;
}
} // namespace dbms::class_catalog
