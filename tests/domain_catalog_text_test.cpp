#include "catalog/catalog.h"
#include "common/DbError.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
int main() {
    using namespace dbms;
    const std::filesystem::path path("domain_catalog_text");
    std::filesystem::create_directory(path);
    { std::ofstream old(path/"pg_type.cat"); old << "23,\"int4\",11,4,b,N,0,0\n"; }
    {
        CatalogManager catalog(path.string());
        const auto* old = catalog.findType(23); assert(old && old->typbasetype == INVALID_OID && !old->typnotnull);
        PgTypeRow type; type.oid=17000; type.typname="d_text"; type.typnamespace=2200;
        type.typtype='d'; type.typcategory='N'; type.typbasetype=23; type.typnotnull=true;
        type.typtypmod=123; type.typndims=0;
        assert(catalog.createType(type)==17000 && catalog.persistAll());
    }
    {
        CatalogManager catalog(path.string());
        const auto* type=catalog.findType(17000);
        assert(type && type->typbasetype==23 && type->typnotnull && type->typtypmod==123);
        assert(catalog.findType(23));
    }
    { std::ofstream corrupt(path/"pg_type.cat",std::ios::app);
      corrupt << "17001,\"broken\",2200,-1,d,S,0,0,D2,23,2,-1,0\n"; }
    bool rejected=false;
    try { (void)CatalogManager::readMetadataSnapshot(path.string()); }
    catch(const DbError& error) { rejected=error.sqlState()=="XX001"; }
    assert(rejected);
    std::cout<<"[DOMAIN CATALOG TEXT] old8/newD2/real flags/strict corrupt guard passed\n";
}
