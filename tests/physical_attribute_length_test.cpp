#include "catalog/catalog.h"
#include "catalog/CatalogService.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "test_utils.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    size_t checks=0, failures=0;
    const auto check=[&](bool pass,const std::string& label) {
        ++checks; failures+=!pass;
        std::cout<<"PHYSICAL_ATTRIBUTE_LENGTH_NATIVE "<<label<<" pass="<<pass<<'\n';
    };
    const auto bytes=[](const std::filesystem::path& path) {
        std::ifstream input(path,std::ios::binary);
        return std::string(std::istreambuf_iterator<char>(input),std::istreambuf_iterator<char>());
    };
    const std::string db=testDbPath("physical_attribute_length");
    check(g_engine.createDatabase(db,"utf8")==DBStatus::OK,"create isolated database");
    Session session;
    session.username="testuser";session.permission=1;session.currentDB=db;
    setCurrentSession(&session);
    DdlExecutor ddl;
    check(!ddl.executeSql("CREATE TABLE widths(d DATE,t TIME,z TIMETZ,i INTERVAL,p INTERVAL DAY TO SECOND(3),"
        "c CHAR(3),n NUMERIC(8,2),a INTEGER[],ia INTERVAL[],g POINT,ln LINE,u UUID,da DATE[],ta TIME[])",session),"create physical declarations");
    struct Field {std::string name;int16_t length;};
    std::vector<Field> fields={{"d",4},{"t",8},{"z",12},{"i",16},{"p",16},{"c",-1},{"n",-1},
        {"a",-1},{"ia",-1},{"g",16},{"ln",24},{"u",16},{"da",-1},{"ta",-1}};
    const auto verify=[&](const std::string& phase) {
        auto& cat=g_engine.catalogService().get(db);
        const auto ns=cat.getNamespaceByName("public");
        const auto table=ns?cat.getClassByName("widths",ns->oid):std::nullopt;
        for(const auto& field:fields) {
            const auto attribute=table?cat.getAttribute(table->oid,field.name):std::nullopt;
            check(attribute && attribute->attlen==field.length,phase+" "+field.name+" logical width");
        }
    };
    verify("new");
    const auto beforeSchema=g_engine.getTableSchema(db,"widths");
    check(beforeSchema.cols[0].dsize==12,"DATE retains physical 12 byte codec");
    check(beforeSchema.cols[1].dsize==8 &&
          beforeSchema.cols[1].storageCodec==Column::StorageCodec::TemporalMicros,
          "new SQL TIME uses explicit eight-byte microsecond codec");
    check(beforeSchema.cols[2].dsize==4,"TIMETZ retains existing physical codec");
    check(beforeSchema.cols[3].isVariableLength,"INTERVAL retains variable text storage");
    check(g_engine.insertRow(db,"widths",{{"d",std::string("2024-01-01")},
        {"t",std::string("12:34:56")},{"i",std::string("1 day")}})==DBStatus::OK,"insert old physical codecs");
    const auto beforeRows=g_engine.query(db,"widths",{}, {"d","t","i"},{});
    check(beforeRows.size()==1,"physical row exists");
    check(!ddl.executeSql("ALTER TABLE widths RENAME COLUMN i TO renamed",session),"unrelated ALTER resynchronizes metadata");
    fields[3].name="renamed";
    verify("alter");
    g_engine.catalogService().evict(db);
    verify("reloaded");
    check(g_engine.query(db,"widths",{}, {"d","t","renamed"},{})==beforeRows,"metadata change preserves stored values");
    const auto afterSchema=g_engine.getTableSchema(db,"widths");
    for(size_t i=0;i<beforeSchema.len;++i)
        check(beforeSchema.cols[i].dsize==afterSchema.cols[i].dsize &&
              beforeSchema.cols[i].isVariableLength==afterSchema.cols[i].isVariableLength &&
              beforeSchema.cols[i].storageCodec==afterSchema.cols[i].storageCodec,
              "unchanged storage layout "+std::to_string(i));
    // R136 deliberately gives NEW SQL TIME columns an explicit microsecond
    // codec. Keep the original legacy seconds guarantee on an actual native
    // legacy table, including unrelated metadata synchronization and cold read.
    TableSchema legacyTimeSchema; legacyTimeSchema.tablename="legacy_time";
    const auto legacyTimeColumn=makeTimeColumn("t",true);
    check(legacyTimeColumn.dsize==4 && legacyTimeColumn.storageCodec==Column::StorageCodec::Legacy,
          "native TIME factory still selects four-byte seconds explicitly");
    legacyTimeSchema.append(legacyTimeColumn);
    legacyTimeSchema.append(makeIntColumn("id",false,2,true));
    check(g_engine.createTable(db,legacyTimeSchema)==DBStatus::OK,"create genuine legacy TIME table");
    // The native storage API deliberately does not register pg_class. Seed
    // the catalog image an actual pre-upgrade SQL table already owned; do
    // not mistake missing fixture metadata for an attlen conversion failure.
    {
        auto& catalog=g_engine.catalogService().get(db);
        const auto space=catalog.getNamespaceByName("public");
        check(space.has_value(),"old SQL catalog fixture has a real public namespace");
        PgClassRow relation;relation.relname="legacy_time";
        relation.relnamespace=space?space->oid:INVALID_OID;relation.relnatts=2;
        const Oid relationOid=catalog.createClass(relation);
        check(relationOid!=INVALID_OID,"seed existing SQL relation identity for legacy heap");
        PgAttributeRow timeAttribute;timeAttribute.attrelid=relationOid;
        timeAttribute.attname="t";timeAttribute.atttypid=1083;timeAttribute.attnum=1;
        timeAttribute.attlen=4; // Historical copied physical length, not the desired result.
        catalog.addAttribute(timeAttribute);
        PgAttributeRow idAttribute;idAttribute.attrelid=relationOid;
        idAttribute.attname="id";idAttribute.atttypid=23;idAttribute.attnum=2;idAttribute.attlen=4;
        catalog.addAttribute(idAttribute);
        check(catalog.persistAll(),"publish the actual old SQL catalog fixture");
    }
    check(g_engine.insertRow(db,"legacy_time",{{"id",std::string("1")},{"t",std::string("12:34:56")}})==DBStatus::OK,
          "insert genuine legacy TIME seconds");
    const auto legacyTimeRows=g_engine.query(db,"legacy_time",{},{"t"},{});
    check(legacyTimeRows==std::vector<std::string>{"12:34:56 "},"legacy TIME row has actual second-coded value");
    check(!ddl.executeSql("ALTER TABLE legacy_time RENAME COLUMN t TO renamed",session),
          "legacy metadata synchronization succeeds");
    const auto legacyTimeAfter=g_engine.getTableSchema(db,"legacy_time");
    check(legacyTimeAfter.len==2 && legacyTimeAfter.cols[0].dataName=="renamed" &&
          legacyTimeAfter.cols[0].dsize==4 && legacyTimeAfter.cols[0].storageCodec==Column::StorageCodec::Legacy,
          "unrelated ALTER never migrates legacy TIME width or units");
    check(g_engine.query(db,"legacy_time",{},{"renamed"},{})==legacyTimeRows,
          "legacy stored values survive metadata synchronization");
    {
        auto& catalog=g_engine.catalogService().get(db);
        const auto space=catalog.getNamespaceByName("public");
        const auto relation=space?catalog.getClassByName("legacy_time",space->oid):std::nullopt;
        const auto attribute=relation?catalog.getAttribute(relation->oid,"renamed"):std::nullopt;
        check(attribute && attribute->attlen==8,"legacy physical TIME still publishes logical eight-byte length");
    }
    {
        StorageEngine cold;
        const auto schema=cold.getTableSchema(db,"legacy_time");
        check(schema.len==2 && schema.cols[0].dsize==4 && schema.cols[0].storageCodec==Column::StorageCodec::Legacy,
              "cold schema retains explicit legacy TIME units");
        check(cold.query(db,"legacy_time",{},{"renamed"},{})==legacyTimeRows,
              "cold legacy TIME value survives without migration");
    }
    setCurrentSession(nullptr);

    // An old catalog is upgraded only in the parsed model: cold metadata
    // reads must not publish files, allocate OIDs or hide later dirty edits.
    const std::filesystem::path directory="legacy_attribute_lengths";
    CatalogManager old(directory.string(),false);
    old.bootstrapSystemNamespaces();old.bootstrapSystemTypes();
    const auto ns=old.getNamespaceByName("public");
    PgClassRow relation;relation.relname="old_widths";relation.relnamespace=ns->oid;
    const Oid relationOid=old.createClass(relation);
    struct Legacy {const char* name;Oid oid;int16_t original;int16_t logical;int32_t dimensions;bool dropped;};
    PgTypeRow shadow;shadow.typname="interval";shadow.typnamespace=ns->oid;shadow.typlen=16;
    const Oid shadowOid=old.createType(shadow);
    PgTypeRow domain;domain.typname="interval_domain";domain.typnamespace=ns->oid;
    domain.typtype='d';domain.typbasetype=1186;domain.typlen=16;
    const Oid domainOid=old.createType(domain);
    const std::vector<Legacy> legacy={{"d",1082,12,4,0,false},{"t",1083,4,8,0,false},
        {"z",1266,4,12,0,false},{"i",1186,-1,16,0,false},{"a",23,4,-1,1,false},
        {"dropped",23,8,8,2,true},{"shadow",shadowOid,-1,-1,0,false},
        {"domain",domainOid,-1,16,0,false}};
    int16_t number=0;
    for(const auto& item:legacy) {
        PgAttributeRow attribute;
        attribute.attrelid=relationOid;attribute.attname=item.name;attribute.atttypid=item.oid;
        attribute.attnum=++number;attribute.attlen=item.original;attribute.attndims=item.dimensions;
        attribute.attisdropped=item.dropped;
        old.addAttribute(attribute);
    }
    check(old.persistAll(),"seed legacy images");
    const auto attributeFile=directory/"pg_attribute.cat";
    const auto originalBytes=bytes(attributeFile);
    const auto oidBytes=bytes(directory/".oid_counter");
    check(!oidBytes.empty(),"seed contains a real OID image");
    const auto snapshot=CatalogManager::readMetadataSnapshot(directory.string());
    for(const auto& item:legacy) {
        const auto found=std::find_if(snapshot.attributes.begin(),snapshot.attributes.end(),
            [&](const auto& field){return field.attrelid==relationOid && field.attname==item.name;});
        check(found!=snapshot.attributes.end() && found->attlen==item.logical,"cold legacy "+std::string(item.name));
    }
    check(bytes(attributeFile)==originalBytes,"cold metadata never rewrites attribute image");
    check(bytes(directory/".oid_counter")==oidBytes,"cold metadata does not allocate OIDs");
    old.loadAll();
    for(const auto& item:legacy) {
        const auto attribute=old.getAttribute(relationOid,item.name);
        check(attribute && attribute->attlen==item.logical,"warm reload "+std::string(item.name));
    }
    check(bytes(attributeFile)==originalBytes,"owned load is not an implicit publication");
    auto changed=old.findAttributes(relationOid);
    changed.front().attlen=13;
    check(old.replaceAttributes(relationOid,changed),"manual dirty edit");
    check(old.hasUnpublishedChanges(),"dirty cached fields remain observable");
    check(old.metadataSnapshot().attributes.front().attlen==13,"snapshot does not normalize away a dirty edit");
    changed.front().attlen=4;
    check(old.replaceAttributes(relationOid,changed),"restore valid logical width");
    check(old.persistAll(),"explicit owner publication writes upgraded image");
    check(bytes(attributeFile)!=originalBytes,"upgrade becomes durable only on publication");
    const auto durable=CatalogManager::readMetadataSnapshot(directory.string());
    for(const auto& item:legacy) {
        const auto found=std::find_if(durable.attributes.begin(),durable.attributes.end(),
            [&](const auto& field){return field.attrelid==relationOid && field.attname==item.name;});
        check(found!=durable.attributes.end() && found->attlen==item.logical,"durable "+std::string(item.name));
    }
    check(fields.size()==14 && legacy.size()==8,"complete fixture identities");
    cleanupTestDb("physical_attribute_length");finalCleanupTestData();
    std::cout<<"PHYSICAL_ATTRIBUTE_LENGTH_NATIVE_COMPLETE "<<checks<<" FAILURES "<<failures<<'\n';
    return checks==115 && !failures?0:1;
}
