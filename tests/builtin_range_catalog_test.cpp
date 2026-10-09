#include "catalog/CatalogService.h"
#include "catalog/declared_type.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <algorithm>
#include <filesystem>
#include <iostream>
extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    struct Range { const char* name; Oid oid,array; char alignment; };
    const Range ranges[] = {
        {"int4range",3904,3905,'i'}, {"numrange",3906,3907,'i'},
        {"tsrange",3908,3909,'d'}, {"tstzrange",3910,3911,'d'},
        {"daterange",3912,3913,'i'}, {"int8range",3926,3927,'d'}
    };
    size_t checked=0,failures=0;
    const auto require=[&](bool good,const std::string& role) {
        ++checked;failures+=!good;
        std::cout<<"BUILTIN_RANGE "<<role<<" pass="<<good<<'\n';
    };
    const auto metadataCheck=[&](const auto& types,const std::string& role) {
        for(const auto& range:ranges) for(bool array:{false,true}) {
            const Oid oid=array?range.array:range.oid;
            const auto row=std::find_if(types.begin(),types.end(),[&](const auto& type){return type.oid==oid;});
            require(row!=types.end() && row->typname==(array?"_":"")+std::string(range.name) &&
                row->typnamespace==11 && row->typlen==-1 && !row->typbyval &&
                row->typtype==(array?'b':'r') && row->typcategory==(array?'A':'R') &&
                row->typelem==(array?range.oid:Oid(0)) && row->typarray==(array?Oid(0):range.array) &&
                row->typalign==range.alignment && row->typstorage=='x',role+" "+std::to_string(oid));
        }
    };
    metadataCheck(CatalogManager::builtinTypeRows(),"pure physical definition");
    const auto database=testDbPath("builtin_range_catalog");
    require(g_engine.createDatabase(database)==DBStatus::OK,"isolated database");
    const auto catalogDirectory=g_engine.dbPath(database)/"pg_catalog";
    require(!std::filesystem::exists(catalogDirectory) && !g_engine.catalogService().has(database),"actual cold catalog");
    const auto bindings=[&](const std::string& role,bool standalone) {
        for(const auto& range:ranges) {
            const std::string name=range.name;
            for(const auto& spelling:{name,"\""+name+"\"","pg_catalog."+name,"pg_catalog.\""+name+"\""}) {
                for(bool array:{false,true}) {
                    const auto declaration=spelling+(array?"[]":"");
                    bool good=false;
                    try {
                        if(standalone) {
                            const auto bound=resolveDeclaredTypeName(declaration,nullptr,nullptr);
                            good=bound.typeOid==(array?range.array:range.oid) &&
                                bound.typeName==name+(array?"[]":"");
                        } else {
                            const auto query=g_engine.prepareBoundQuery(database,"SELECT CAST(NULL AS "+declaration+") AS value WHERE false");
                            good=query.output.size()==1 && query.output[0].typeOid==(array?range.array:range.oid);
                        }
                    } catch(const DbError&) {}
                    require(good,role+" "+declaration);
                }
            }
        }
    };
    bindings("standalone",true);
    bindings("cold preparation",false);
    require(!std::filesystem::exists(catalogDirectory) && !g_engine.catalogService().has(database),"cold lookup has no directory/cache side effects");
    (void)g_engine.catalogService().get(database);
    metadataCheck(g_engine.catalogService().metadataSnapshot(database).types,"actual warm physical definition");
    bindings("warm preparation",false);
    require(g_engine.catalogService().persistAll(),"persist actual catalog");
    g_engine.catalogService().evict(database);
    const auto disk=CatalogManager::readMetadataSnapshot(g_engine.dbPath(database).string());
    metadataCheck(disk.types,"actual persisted definition");
    bindings("uncached persisted preparation",false);
    std::cout<<"BUILTIN_RANGE_CHECKED="<<checked<<" FAILED="<<failures<<'\n';
    return checked==232 && !failures?0:1;
}
