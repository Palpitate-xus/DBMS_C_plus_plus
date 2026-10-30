#include "catalog/catalog.h"
#include "test_utils.h"
#include <filesystem>
#include <iostream>
#include <map>
#include <optional>
#include <string>
#include <vector>

using namespace dbms;
namespace {
size_t controls=0,failures=0;
void check(bool good,const std::string& label) {
    ++controls;
    if(!good){++failures;std::cerr<<"ENUM_ARRAY_CATALOG_FAIL "<<controls<<' '<<label<<'\n';}
}
Oid add(CatalogManager& catalog,Oid ns,const std::string& name,char kind='e') {
    PgTypeRow type;type.typname=name;type.typnamespace=ns;type.typowner=42;
    type.typlen=4;type.typbyval=true;type.typtype=kind;type.typcategory=kind=='e'?'E':'U';
    return catalog.createType(type);
}
Oid pair(CatalogManager& catalog,Oid base,const std::string& expected) {
    const auto scalar=catalog.getType(base);
    const Oid oid=scalar?scalar->typarray:INVALID_OID;
    const auto array=oid?catalog.getType(oid):std::optional<PgTypeRow>{};
    check(scalar && scalar->typtype=='e' && scalar->typcategory=='E',"enum scalar "+expected);
    check(oid && oid!=base,"distinct array OID "+expected);
    check(array.has_value(),"array row "+expected);
    check(array && array->typname==expected,"generated name "+expected);
    check(array && scalar && array->typnamespace==scalar->typnamespace,"namespace "+expected);
    check(array && scalar && array->typowner==scalar->typowner,"initial owner "+expected);
    check(array && array->typtype=='b' && array->typcategory=='A',"array kind "+expected);
    check(array && array->typlen==-1 && !array->typbyval && array->typalign=='i' &&
          array->typstorage=='x',"array layout "+expected);
    check(array && array->typelem==base && !array->typarray,"element and backlink "+expected);
    check(array && array->typinput==750 && array->typoutput==751 &&
          array->typreceive==2400 && array->typsend==2401,"array codecs "+expected);
    check(array && array->typisdefined && !array->typispreferred && array->typdelim==',',
          "array flags "+expected);
    bool internal=false,space=false;
    if(array)for(const auto& d:catalog.findDepends(PgClassOid_Type,oid)) {
        internal=internal || (d.refclassid==PgClassOid_Type && d.refobjid==base && d.deptype=='i');
        space=space || (d.refclassid==PgClassOid_Namespace && d.refobjid==array->typnamespace && d.deptype=='n');
    }
    check(internal && space,"automatic/internal and namespace dependencies "+expected);
    check(catalog.findEnumLabels(oid).empty(),"labels belong only to scalar "+expected);
    return oid;
}
}

int main() {
    const std::string test="enum_array_catalog";
    cleanupTestDb(test);
    const auto db=testDbPath(test);
    Oid ns=0,rank=0,rankArray=0,other=0,legacy=0;
    std::vector<std::pair<Oid,std::string>> types;
    {
        CatalogManager catalog(db);
        ns=catalog.createNamespace("array_owner",42);
        const Oid ns2=catalog.createNamespace("array_other",42);
        for(const std::string name:{"plain","_lead","Rank[]","Quote\"Rank","name with space"}) {
            const Oid oid=add(catalog,ns,name);types.push_back({oid,"_"+name});
            pair(catalog,oid,"_"+name);
        }
        const std::string ascii(63,'a');
        const Oid longOid=add(catalog,ns,ascii);
        types.push_back({longOid,"_"+std::string(62,'a')});pair(catalog,longOid,types.back().second);
        std::string unicode;
        for(int i=0;i<21;++i)unicode+="汉";
        const Oid unicodeOid=add(catalog,ns,unicode);
        types.push_back({unicodeOid,"_"+unicode.substr(0,60)});pair(catalog,unicodeOid,types.back().second);
        add(catalog,ns,"_collision",'b');
        add(catalog,ns,"_collision_1",'b');
        const Oid collision=add(catalog,ns,"collision");
        types.push_back({collision,"_collision_2"});pair(catalog,collision,types.back().second);
        rank=add(catalog,ns,"rank");
        rankArray=pair(catalog,rank,"_rank");
        const Oid escaped=add(catalog,ns,"_rank");
        pair(catalog,rank,"__rank");
        pair(catalog,escaped,"__rank_1");
        const auto kept=catalog.getType(rank);
        check(kept && kept->typarray==rankArray,"renaming automatic array preserves OID");
        types.push_back({rank,"__rank"});types.push_back({escaped,"__rank_1"});
        other=add(catalog,ns2,"rank");pair(catalog,other,"_rank");
        types.push_back({other,"_rank"});
        check(catalog.replaceEnumLabels(rank,{"10","2","1"}),"scalar enum labels");
        check(catalog.findEnumLabels(rank).size()==3,"only scalar owns three labels");
        const auto protectedPlan=catalog.planDrop(PgClassOid_Type,rankArray,CatalogManager::DropBehavior::Cascade);
        check(!protectedPlan.ok(),"automatic array cannot be an independent SQL drop root");
        legacy=add(catalog,ns,"legacy");
        const Oid legacyArray=pair(catalog,legacy,"_legacy");
        // Low-level dropType is a repair primitive; dropObject/SQL protect
        // internal roots. This creates a real old/missing-backlink catalog.
        check(catalog.dropType(legacyArray),"seed missing historical array row");
        check(catalog.persistAll(),"persist enum and array rows");
    }
    {
        CatalogManager catalog(db);
        for(const auto& [oid,name]:types)pair(catalog,oid,name);
        pair(catalog,legacy,"_legacy");
        const auto kept=catalog.getType(rank);
        check(kept && kept->typarray==rankArray,"cold load preserves existing array OIDs");
        check(catalog.findEnumLabels(rank).size()==3,"cold enum labels");
        check(catalog.persistAll(),"persist repaired legacy array");
        const auto readonly=CatalogManager::readMetadataSnapshot(db);
        size_t enumCount=0,arrayCount=0;
        for(const auto& type:readonly.types) {
            enumCount+=type.typtype=='e';
            arrayCount+=type.typcategory=='A' && type.typnamespace!=11;
        }
        check(enumCount==arrayCount && enumCount==types.size()+1,"read-only snapshot has complete pairs");
        const auto plan=catalog.planDrop(PgClassOid_Type,rank,CatalogManager::DropBehavior::Restrict);
        check(plan.ok(),"scalar restrict includes its internal array");
        check(plan.objectsToDrop==std::vector<std::pair<Oid,Oid>>{
            {PgClassOid_Type,rankArray},{PgClassOid_Type,rank}},"array precedes scalar in drop plan");
        check(catalog.applyDropPlan(plan),"apply paired drop plan");
        check(!catalog.getType(rank) && !catalog.getType(rankArray),"paired rows removed");
        check(catalog.findEnumLabels(rank).empty(),"paired drop removes scalar labels");
        const auto otherRow=catalog.getType(other);
        check(otherRow && otherRow->typarray && catalog.getType(otherRow->typarray),
              "same name in another namespace survives");
    }
    cleanupTestDb(test);finalCleanupTestData();
    constexpr size_t expected=341;
    std::cout<<"ENUM_ARRAY_CATALOG_COMPLETE "<<controls<<" EXPECTED "<<expected<<" FAILURES "<<failures<<'\n';
    return controls==expected && !failures?0:1;
}
