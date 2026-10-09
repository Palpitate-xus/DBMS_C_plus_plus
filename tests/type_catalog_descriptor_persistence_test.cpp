#include "catalog/catalog.h"
#include "common/DbError.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <tuple>

int main() {
    using namespace dbms;
    const std::filesystem::path path("__t_type_catalog_descriptor");
    std::filesystem::create_directory(path);
    PgTypeRow expected;
    expected.oid=17000; expected.typname="owned_descriptor"; expected.typnamespace=2200;
    expected.typowner=10; expected.typlen=8; expected.typbyval=true;
    expected.typtype='d'; expected.typcategory='N'; expected.typispreferred=true;
    expected.typisdefined=false; expected.typdelim=';'; expected.typrelid=17001;
    expected.typelem=23; expected.typarray=17002; expected.typinput=17003;
    expected.typoutput=17004; expected.typreceive=17005; expected.typsend=17006;
    expected.typmodin=17007; expected.typmodout=17008; expected.typanalyze=17009;
    expected.typalign='d'; expected.typstorage='x'; expected.typnotnull=true;
    expected.typbasetype=23; expected.typtypmod=7; expected.typndims=2;
    expected.typcollation=100;
    const auto fields=[](const PgTypeRow& row) {
        return std::tie(row.oid,row.typname,row.typnamespace,row.typowner,row.typlen,
            row.typbyval,row.typtype,row.typcategory,row.typispreferred,row.typisdefined,
            row.typdelim,row.typrelid,row.typelem,row.typarray,row.typinput,row.typoutput,
            row.typreceive,row.typsend,row.typmodin,row.typmodout,row.typanalyze,row.typalign,
            row.typstorage,row.typnotnull,row.typbasetype,row.typtypmod,row.typndims,row.typcollation);
    };
    {
        CatalogManager catalog(path.string());
        assert(catalog.createType(expected)==expected.oid);
        assert(catalog.persistAll());
    }
    const auto snapshot=CatalogManager::readMetadataSnapshot(path.string());
    assert(snapshot.types.size()==1 && fields(snapshot.types[0])==fields(expected));
    {
        CatalogManager catalog(path.string());
        const auto* row=catalog.findType(expected.oid);
        assert(row && fields(*row)==fields(expected));
    }
    // Historical formats retain their documented defaults and domain facts.
    const std::filesystem::path legacy("__t_type_catalog_descriptor_legacy");
    std::filesystem::create_directory(legacy);
    {
        std::ofstream out(legacy/"pg_type.cat");
        out<<"23,\"int4\",11,4,b,N,0,0\n"
           <<"17001,\"old_domain\",2200,4,d,N,0,0,D2,23,1,7,2\n";
    }
    const auto old=CatalogManager::readMetadataSnapshot(legacy.string());
    assert(old.types.size()==2 && old.types[0].typalign=='i' && old.types[0].typstorage=='p');
    assert(old.types[1].typbasetype==23 && old.types[1].typnotnull &&
           old.types[1].typtypmod==7 && old.types[1].typndims==2);
    {
        std::ofstream out(legacy/"pg_type.cat",std::ios::app);
        out<<"17002,\"torn_descriptor\",2200,4,b,N,0,0,D3,0,0,-1,0,10,1\n";
    }
    bool rejected=false;
    try { (void)CatalogManager::readMetadataSnapshot(legacy.string()); }
    catch(const DbError& error) { rejected=error.sqlState()=="XX001"; }
    assert(rejected);
    std::cout<<"[TYPE CATALOG DESCRIPTOR] all28fields/live/disk/legacy8/D2/tornD3 passed\n";
}
