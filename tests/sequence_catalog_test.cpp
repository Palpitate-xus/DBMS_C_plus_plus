#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "catalog/PgSequenceCatalog.h"
#include "catalog/type_registry.h"
#include "parser/parser.h"
#include "Session.h"
#include "test_utils.h"
#include <algorithm>
#include <iostream>
#include <stdexcept>

extern dbms::StorageEngine g_engine;
using namespace dbms;
namespace {
unsigned checks=0;
void need(bool value,const std::string& reason){++checks;if(!value)throw std::runtime_error(reason);}
}
int main() {
    try {
        TypeRegistry::instance().bootstrap();
        g_engine.setBackgroundIntervals(60000,60000);
        const auto database=testDbPath("sequence_catalog");cleanupTestDb("sequence_catalog");
        need(g_engine.createDatabase(database,"utf8")==DBStatus::OK,"actual database");
        Session session;session.username="admin";session.permission=1;session.currentDB=database;
        DdlExecutor ddl;
        auto catalog=g_engine.catalogService().acquire(database);
        const auto relation=catalog->getClass(sequence_catalog::relationOid);
        need(relation && relation->relnamespace==11 && relation->relname=="pg_sequence" && relation->relnatts==8,
            "real fixed catalog relation identity and complete width");
        const auto rowtype=catalog->getType(relation->reltype);
        const auto array=rowtype?catalog->getType(rowtype->typarray):std::nullopt;
        need(rowtype && rowtype->typrelid==2224 && array && array->typelem==rowtype->oid,
            "actual allocated composite and array identities");
        const auto attributes=catalog->findAttributes(2224);
        need(attributes.size()==8,"all physical catalog attributes");
        for(size_t i=0;i<attributes.size();++i) {
            need(attributes[i].attname==sequence_catalog::fields[i].name &&
                attributes[i].atttypid==sequence_catalog::fields[i].type &&
                attributes[i].attnum==int16_t(i+1) && attributes[i].attnotnull,
                "actual declared attribute type, ordinal and NOT NULL");
        }
        need(!sequence_catalog::bootstrapRelation(*catalog,true),"valid published catalog needs no repair");
        need(g_engine.getSequenceCatalogRows(database).empty(),"catalog source creates no synthetic sequence rows");
        need(!ddl.executeSql("CREATE SEQUENCE catalog_small AS smallint START 3 INCREMENT 2",session),"real smallint DDL");
        need(!ddl.executeSql("CREATE SEQUENCE catalog_big AS bigint INCREMENT -1",session),"real descending int8 DDL");
        need(!ddl.executeSql("CREATE TABLE owner_rows(k integer PRIMARY KEY,id smallint GENERATED ALWAYS AS IDENTITY(START 3 INCREMENT 2),note text)",session),"real identity owner");
        auto table=g_engine.getTableSchema(database,"owner_rows");const auto originalOid=table.cols[1].identitySequenceOid;
        need(originalOid!=0,"real persisted sequence OID");
        const auto rows=g_engine.getSequenceCatalogRows(database);
        need(rows.size()==3,"only actual catalog sequences are read");
        bool found=false;
        for(const auto& row:rows)if(row[0].value==std::to_string(originalOid)) {
            found=true;std::vector<std::string> values;
            for(const auto& cell:row){need(!cell.isNull,"parameter is an actual non-NULL cell");values.push_back(cell.value);}
            need(values==std::vector<std::string>{std::to_string(originalOid),"21","3","2","32767","1","1","f"},
                "whole identity declaration equals PG oracle");
        }
        need(found,"owned sequence is present by actual class identity");
        const auto prepared=g_engine.prepareBoundQuery(database,"SELECT seqrelid,seqtypid,seqstart,seqincrement,seqmax,seqmin,seqcache,seqcycle FROM pg_catalog.pg_sequence");
        const std::vector<uint32_t> oids={26,26,20,20,20,20,20,16};
        need(prepared.output.size()==8,"complete bound descriptor");
        for(size_t i=0;i<oids.size();++i)need(prepared.output[i].typeOid==oids[i],"real catalog output type identity");
        const auto insert=[&](int key,const std::string& expected) {
            bool handled=false;clearLastDmlResult();
            const auto sql="INSERT INTO owner_rows(k) VALUES("+std::to_string(key)+") RETURNING id";
            need(!tryDmlBridge(sql,SQLParser::classify(sql),session,handled,sql) && handled,"ordinary identity insert");
            const auto result=takeLastDmlResult();need(result.rows==std::vector<std::vector<std::string>>{{expected}},"actual identity value after lifecycle operation");
        };
        insert(1,"3");
        need(!ddl.executeSql("ALTER SEQUENCE owner_rows_id_seq AS bigint MAXVALUE 40000",session),"independent sequence type differs from owner");
        need(!ddl.executeSql("ALTER TABLE owner_rows ALTER COLUMN id SET MAXVALUE 50000",session),"identity options validate actual sequence type");
        need(!ddl.executeSql("ALTER TABLE owner_rows ALTER COLUMN id TYPE integer",session),"identity and sequence type change with stored row");
        table=g_engine.getTableSchema(database,"owner_rows");
        need(table.cols[1].identitySequenceOid==originalOid && table.cols[1].dsize==4,"physical type changes without replacing identity OID");
        SequenceInfo info;need(g_engine.getSequenceInfo(database,"owner_rows_id_seq",info)==DBStatus::OK &&
            info.dataTypeOid==23 && info.maxValue==50000,"actual sequence type and custom bound persist");
        need(g_engine.query(database,"owner_rows",{"=k 1"},{"id"})==std::vector<std::string>{"3 "},"rewrite preserves old ALWAYS value");
        insert(2,"5");
        need(!ddl.executeSql("ALTER TABLE owner_rows ADD COLUMN extra integer",session),"ADD replays stored identity without allocating");
        need(!ddl.executeSql("ALTER TABLE owner_rows DROP COLUMN extra",session),"DROP replays stored identity without allocating");
        insert(3,"7");
        auto snapshot=catalog->metadataSnapshot();sequence_catalog::validateReadDeclaration(snapshot);
        snapshot.attributes.erase(std::remove_if(snapshot.attributes.begin(),snapshot.attributes.end(),
            [](const auto& row){return row.attrelid==2224 && row.attnum==8;}),snapshot.attributes.end());
        bool rejected=false;
        try{sequence_catalog::validateReadDeclaration(snapshot);}catch(const DbError& error){rejected=error.sqlState()=="XX001";}
        need(rejected,"copied reader rejects incomplete declaration without synthesizing fields");
        const auto copied=g_engine.dbPath(database)/"catalog-check-copy";
        std::filesystem::copy(g_engine.dbPath(database)/"pg_catalog",copied,std::filesystem::copy_options::recursive);
        CatalogManager control(copied.string(),false);
        need(control.dropClass(2224),"owned isolated missing-declaration control");rejected=false;
        try{(void)sequence_catalog::bootstrapRelation(control,true);}catch(const DbError& error){rejected=error.sqlState()=="XX001";}
        need(rejected,"published catalog cannot be silently recreated when missing");
        need(g_engine.checkpoint(database),"persist actual declaration and native parameters");
        std::cout<<"[SEQUENCE CATALOG] checks="<<checks<<" actual_catalog_type_and_identity_lifecycle_verified=true\n";return 0;
    } catch(const std::exception& error){std::cerr<<"SEQUENCE_CATALOG_FAIL "<<error.what()<<'\n';return 1;}
}
