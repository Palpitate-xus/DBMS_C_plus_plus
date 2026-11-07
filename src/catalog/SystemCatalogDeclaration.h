#pragma once
#include "catalog/catalog.h"
#include "common/DbError.h"
#include <algorithm>
#include <array>

namespace dbms::system_catalog {
struct Field {const char* name;Oid type;int16_t size;bool nullable=false;};
// Physical declarations from PG REL_18_6. Value producers independently expose
// only fields the engine actually owns; a declaration never invents a value.
inline constexpr std::array<Field,32> typeFields{{
    {"oid",26,4},{"typname",19,64},{"typnamespace",26,4},{"typowner",26,4},
    {"typlen",21,2},{"typbyval",16,1},{"typtype",18,1},{"typcategory",18,1},
    {"typispreferred",16,1},{"typisdefined",16,1},{"typdelim",18,1},{"typrelid",26,4},
    {"typsubscript",24,4},{"typelem",26,4},{"typarray",26,4},{"typinput",24,4},
    {"typoutput",24,4},{"typreceive",24,4},{"typsend",24,4},{"typmodin",24,4},
    {"typmodout",24,4},{"typanalyze",24,4},{"typalign",18,1},{"typstorage",18,1},
    {"typnotnull",16,1},{"typbasetype",26,4},{"typtypmod",23,4},{"typndims",23,4},
    {"typcollation",26,4},{"typdefaultbin",194,-1,true},{"typdefault",25,-1,true},{"typacl",1034,-1,true}}};
inline constexpr std::array<Field,25> attributeFields{{
    {"attrelid",26,4},{"attname",19,64},{"atttypid",26,4},{"attlen",21,2},
    {"attnum",21,2},{"atttypmod",23,4},{"attndims",21,2},{"attbyval",16,1},
    {"attalign",18,1},{"attstorage",18,1},{"attcompression",18,1},{"attnotnull",16,1},
    {"atthasdef",16,1},{"atthasmissing",16,1},{"attidentity",18,1},{"attgenerated",18,1},
    {"attisdropped",16,1},{"attislocal",16,1},{"attinhcount",21,2},{"attcollation",26,4},
    {"attstattarget",21,2,true},{"attacl",1034,-1,true},{"attoptions",1009,-1,true},
    {"attfdwoptions",1009,-1,true},{"attmissingval",2277,-1,true}}};

template<size_t N>
inline bool bootstrap(CatalogManager& catalog,Oid relationOid,const char* name,
    Oid rowOid,Oid arrayOid,const std::array<Field,N>& fields,bool requireExisting) {
    const auto snapshot=catalog.metadataSnapshot();const auto space=catalog.getNamespace(11);
    if(!space || space->nspname!="pg_catalog")throw DbError("XX001","missing system catalog namespace");
    const auto fail=[&]{throw DbError("XX001",std::string("conflicting or incomplete ")+name+" declaration");};
    const PgClassRow* existing=nullptr;
    for(const auto& row:snapshot.relations)if(row.oid==relationOid || (row.relnamespace==11 && row.relname==name)) {
        if(existing)fail();
        existing=&row;
    }
    if(requireExisting && !existing)fail();
    if(existing && (existing->oid!=relationOid || existing->relname!=name || existing->relnamespace!=11 ||
       existing->reltype!=rowOid || existing->relnatts!=int16_t(N) || existing->relkind!='r' ||
       existing->relpersistence!='p' || existing->relowner!=space->nspowner))fail();
    PgTypeRow row;row.oid=rowOid;row.typname=name;row.typnamespace=11;row.typowner=space->nspowner;
    row.typlen=-1;row.typtype='c';row.typcategory='C';row.typalign='d';row.typstorage='x';
    row.typrelid=relationOid;row.typarray=arrayOid;
    PgTypeRow array;array.oid=arrayOid;array.typname="_"+std::string(name);array.typnamespace=11;
    array.typowner=space->nspowner;array.typlen=-1;array.typcategory='A';array.typalign='d';array.typstorage='x';array.typelem=rowOid;
    for(const auto& expected:std::array<PgTypeRow,2>{row,array}) {
        const PgTypeRow* actual=nullptr;
        for(const auto& item:snapshot.types)if(item.oid==expected.oid ||
            (item.typnamespace==11 && item.typname==expected.typname)){if(actual)fail();actual=&item;}
        if(!actual){if(existing)fail();continue;}
        if(!existing || actual->oid!=expected.oid || actual->typname!=expected.typname ||
           actual->typnamespace!=11 || actual->typowner!=expected.typowner || actual->typlen!=-1 ||
           actual->typbyval || actual->typtype!=expected.typtype || actual->typcategory!=expected.typcategory ||
           actual->typalign!='d' || actual->typstorage!='x' || actual->typrelid!=expected.typrelid ||
           actual->typelem!=expected.typelem || actual->typarray!=expected.typarray || !actual->typisdefined ||
           actual->typbasetype || actual->typinput || actual->typoutput || actual->typreceive || actual->typsend ||
           actual->typispreferred || actual->typnotnull || actual->typmodin || actual->typmodout ||
           actual->typanalyze || actual->typcollation || actual->typtypmod!=-1 || actual->typndims ||
           actual->typdelim!=',')fail();
    }
    std::vector<PgAttributeRow> attributes;
    for(size_t i=0;i<N;++i) {
        const auto& field=fields[i];const auto type=catalog.getType(field.type);
        if(!type || type->typnamespace!=11 || type->typlen!=field.size || !type->typisdefined)fail();
        PgAttributeRow a;a.attrelid=relationOid;a.attname=field.name;a.atttypid=field.type;
        a.attlen=field.size;a.attnum=int16_t(i+1);a.attnotnull=!field.nullable;
        a.attbyval=type->typbyval;a.attalign=type->typalign;a.attstorage=type->typstorage;
        a.attcollation=type->typcollation?950:0;a.attndims=type->typcategory=='A'?1:0;attributes.push_back(a);
    }
    auto loaded=catalog.findUserAttributes(relationOid);
    std::sort(loaded.begin(),loaded.end(),[](const auto& a,const auto& b){return a.attnum<b.attnum;});
    if(!existing && !loaded.empty())fail();
    const auto depend=[&](Oid cls,Oid object,int32_t sub,Oid refClass,Oid reference,char kind) {
        if(existing) {
            const auto edges=catalog.findDepends(cls,object,sub);
            if(std::count_if(edges.begin(),edges.end(),[&](const auto& d){return d.refclassid==refClass &&
                d.refobjid==reference && !d.refobjsubid && d.deptype==kind;})!=1)fail();
        } else {PgDependRow d;d.classid=cls;d.objid=object;d.objsubid=sub;d.refclassid=refClass;
            d.refobjid=reference;d.deptype=kind;catalog.addDepend(d);}
    };
    if(existing) {
        if(loaded.size()!=N)fail();
        for(size_t i=0;i<N;++i) {const auto& a=loaded[i];const auto& e=attributes[i];
            if(a.attnum!=e.attnum || a.attname!=e.attname || a.atttypid!=e.atttypid || a.attlen!=e.attlen ||
               a.attnotnull!=e.attnotnull || a.attbyval!=e.attbyval || a.attalign!=e.attalign ||
               a.attstorage!=e.attstorage || a.attcollation!=e.attcollation || a.attndims!=e.attndims ||
               a.atttypmod!=-1 || a.attisdropped || a.atthasdef || a.attidentity || a.attgenerated)fail();}
    } else {
        PgClassRow c;c.oid=relationOid;c.relname=name;c.relnamespace=11;c.relowner=space->nspowner;
        c.reltype=rowOid;c.relnatts=int16_t(N);
        if(catalog.createType(row)!=rowOid || catalog.createType(array)!=arrayOid ||
           catalog.createClass(c)!=relationOid || !catalog.replaceAttributes(relationOid,attributes))fail();
    }
    if(existing) {
        depend(PgClassOid_Class,relationOid,0,PgClassOid_Namespace,11,'n');
        depend(PgClassOid_Type,rowOid,0,PgClassOid_Namespace,11,'n');
        depend(PgClassOid_Type,arrayOid,0,PgClassOid_Namespace,11,'n');
    }
    depend(PgClassOid_Type,rowOid,0,PgClassOid_Class,relationOid,'i');
    depend(PgClassOid_Type,arrayOid,0,PgClassOid_Type,rowOid,'i');
    for(const auto& a:attributes)depend(PgClassOid_Class,relationOid,a.attnum,PgClassOid_Type,a.atttypid,'n');
    return !existing;
}
inline bool bootstrapRelations(CatalogManager& catalog,bool requireExisting=false) {
    const bool types=bootstrap(catalog,1247,"pg_type",71,210,typeFields,requireExisting);
    const bool attributes=bootstrap(catalog,1249,"pg_attribute",75,270,attributeFields,requireExisting);
    return types || attributes;
}
} // namespace dbms::system_catalog
