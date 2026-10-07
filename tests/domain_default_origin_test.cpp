#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "test_utils.h"
#include <cassert>
#include <cstring>
#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>

extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto db=testDbPath("domain_default_origin");
    assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session;session.currentDB=db;session.username="testuser";session.permission=1;
    setCurrentSession(&session);
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE DOMAIN origin_domain AS INT DEFAULT 1",session));
    assert(!ddl.executeSql("CREATE TABLE origin_rows(id INT,v origin_domain,c origin_domain DEFAULT 1,n origin_domain DEFAULT NULL)",session));
    const auto original=g_engine.getTableSchema(db,"origin_rows");
    assert(original.cols[1].defaultOrigin==Column::DefaultOrigin::Domain);
    assert(original.cols[2].defaultOrigin==Column::DefaultOrigin::Column);
    assert(original.cols[3].defaultOrigin==Column::DefaultOrigin::Column);
    const auto relationId=original.physicalRelationId;assert(relationId!=0);
    auto info=g_engine.getDomain(db,"origin_domain");info.defaultValue="2";
    assert(g_engine.alterDomain(db,"origin_domain",info)==DBStatus::OK);
    const auto current=g_engine.getTableSchema(db,"origin_rows");
    assert(current.cols[1].defaultValue=="2" && current.cols[2].defaultValue=="1" && current.cols[3].defaultValue=="NULL");
    assert(current.physicalRelationId==relationId);
    assert(g_engine.insert(db,"origin_rows",{{"id","1"}})==DBStatus::OK);
    assert(g_engine.query(db,"origin_rows",{"=v 2","=c 1","isnull n"},{"id"}).size()==1);
    std::cerr << "DOMAIN_ORIGIN_COLD_REOPEN\n";
    StorageEngine reader;
    const auto cold=reader.getTableSchema(db,"origin_rows");
    assert(cold.cols[1].defaultValue=="2" && cold.cols[1].defaultOrigin==Column::DefaultOrigin::Domain);
    assert(cold.physicalRelationId==relationId);
    assert(!g_engine.physicalRestore(db,db+"_missing_backup"));
    assert(g_engine.getTableSchema(db,"origin_rows").cols[1].defaultValue=="2");
    // DFT1 and DSO1 coexist in B; neither the full SQL default nor RID1 may
    // be mistaken for a trailing extension of the other.
    const std::string longText(180,'a');
    assert(!ddl.executeSql("CREATE DOMAIN long_origin_domain AS TEXT DEFAULT '"+longText+"'",session));
    assert(!ddl.executeSql("CREATE TABLE long_origin_rows(v long_origin_domain)",session));
    const auto longSchema=g_engine.getTableSchema(db,"long_origin_rows");
    assert(longSchema.cols[0].defaultOrigin==Column::DefaultOrigin::Domain);
    assert(longSchema.cols[0].defaultValue=="'"+longText+"'");
    assert(longSchema.physicalRelationId!=0);
    assert(g_engine.insertDefaultValues(db,"long_origin_rows",longSchema)==DBStatus::OK);
    assert(g_engine.query(db,"long_origin_rows",{"=v "+longText},{"v"}).size()==1);
    auto& catalog=g_engine.catalogService().get(db);
    const auto* relation=catalog.resolveRelation("origin_rows",{"public"});assert(relation);
    for (const auto& attr:catalog.findAttributes(relation->oid)) {
        if(attr.attname=="v")assert(!attr.atthasdef);
        if(attr.attname=="c" || attr.attname=="n")assert(attr.atthasdef);
    }
    const auto path=g_engine.dbPath(db)/"origin_rows.stc";
    std::ifstream input(path,std::ios::binary);
    const std::string image{std::istreambuf_iterator<char>(input),{}};input.close();
    int revision=0;
    const auto decode=[&](const std::string& image) {
        {std::ofstream output(path,std::ios::binary|std::ios::trunc);output.write(image.data(),image.size());assert(output);}
        std::filesystem::last_write_time(path,std::filesystem::file_time_type::clock::now()+std::chrono::seconds(++revision));
        return g_engine.getTableSchema(db,"origin_rows");
    };
    uint32_t version=0;std::memcpy(&version,image.data(),4);assert(version==0x4442000B);
    std::cerr << "DOMAIN_ORIGIN_FORMAT_GUARDS\n";
    assert(decode(image).physicalRelationId==relationId);
    assert(decode(image).cols[1].defaultOrigin==Column::DefaultOrigin::Domain);
    assert(decode(image.substr(0,image.size()-1)).len==0);
    assert(decode(image+"junk").len==0);
    auto corrupt=image;const auto extension=corrupt.rfind("DSO1");assert(extension!=std::string::npos);
    corrupt[extension+6]=3;assert(decode(corrupt).len==0);
    corrupt=image;corrupt[extension+4]=0;assert(decode(corrupt).len==0);
    // Old formats retain their default bytes without comparing them to the
    // domain's value. Both same-value explicit and copied legacy sources are
    // frozen. Their RID1 remains intact and new readers don't auto-migrate.
    const auto originExtension=image.rfind("DSO1");assert(originExtension!=std::string::npos);
    auto old10=image.substr(0,originExtension);
    version=0x4442000A;std::memcpy(old10.data(),&version,4);
    assert(decode(old10).cols[1].defaultValue=="1");
    assert(decode(old10).cols[1].defaultOrigin==Column::DefaultOrigin::LegacyFrozen);
    assert(decode(old10).physicalRelationId==relationId);
    const auto ridExtension=old10.rfind("RID1");assert(ridExtension!=std::string::npos);
    auto old9=old10.substr(0,ridExtension);
    version=0x44420009;std::memcpy(old9.data(),&version,4);
    assert(decode(old9).cols[1].defaultValue=="1" && decode(old9).physicalRelationId==0);
    assert(decode(image).physicalRelationId==relationId);
    std::cerr << "DOMAIN_ORIGIN_COLUMN_DEFAULT_ALTER\n";
    assert(g_engine.alterTableSetDefault(db,"origin_rows","v","7")==DBStatus::OK);
    assert(g_engine.getTableSchema(db,"origin_rows").cols[1].defaultOrigin==Column::DefaultOrigin::Column);
    assert(g_engine.alterTableDropDefault(db,"origin_rows","v")==DBStatus::OK);
    assert(g_engine.getTableSchema(db,"origin_rows").cols[1].defaultValue=="2");
    setCurrentSession(nullptr);
    assert(g_engine.dropDatabase(db)==DBStatus::OK);
    std::cout << "[DOMAIN DEFAULT ORIGIN] passed\n";
}
