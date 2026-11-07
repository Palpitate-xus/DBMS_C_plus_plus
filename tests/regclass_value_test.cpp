#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "executor/ExecutionPlan.h"
#include "expression/StoredRegtypeValue.h"
#include "test_utils.h"
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
        const auto database=testDbPath("regclass_value");cleanupTestDb("regclass_value");
        need(g_engine.createDatabase(database,"utf8")==DBStatus::OK,"actual database");
        Session session;session.username="admin";session.permission=1;session.currentDB=database;
        struct Scope {Session* previous=currentSession();Scope(Session& value){setCurrentSession(&value);}~Scope(){setCurrentSession(previous);}} scope(session);
        DdlExecutor ddl;
        const auto create=[&](const std::string& sql){need(!ddl.executeSql(sql,session),"actual DDL: "+sql);};
        create("CREATE TABLE reference_target(k integer)");
        create("CREATE SEQUENCE reference_generator AS smallint START 3 INCREMENT 2");
        create("CREATE TABLE \"Mixed Name\"(k integer)");
        const auto catalog=g_engine.catalogService().acquire(database);
        const auto target=catalog->getClassByName("reference_target",2200);
        const auto sequence=catalog->getClassByName("reference_generator",2200);
        const auto quoted=catalog->getClassByName("Mixed Name",2200);
        need(target && sequence && quoted,"actual allocated pg_class identities");
        const auto query=[&](const std::string& sql) {
            auto prepared=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,sql));
            auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedQueryPlan(
                &g_engine,database,prepared,prepared->ast.get()));
            result.throwIfFailed();return result;
        };
        const auto rows=[&](const std::string& sql,const std::vector<std::vector<std::string>>& expected) {
            const auto result=query(sql);need(result.structuredRows==expected,"whole actual rows: "+sql);
        };
        rows("SELECT 'reference_target'::regclass::oid",{{std::to_string(target->oid)}});
        rows("SELECT 'public.reference_target'::regclass::text",{{"reference_target"}});
        rows("SELECT ' public.\"Mixed Name\" '::regclass::oid",{{std::to_string(quoted->oid)}});
        rows("SELECT 0::regclass::oid,(-1::integer)::regclass::oid,4294967295::regclass::oid",{{"0","4294967295","4294967295"}});
        rows("SELECT seqrelid,seqtypid,seqstart,seqincrement,seqmax,seqmin,seqcache,seqcycle FROM pg_sequence WHERE seqrelid='reference_generator'::regclass",
            {{std::to_string(sequence->oid),"21","3","2","32767","1","1","f"}});
        for(const auto& item:std::vector<std::pair<std::string,std::string>>{
            {"SELECT 'no_such_reference'::regclass","42P01"},
            {"SELECT '-1'::regclass","42P01"},
            {"SELECT 4294967296::regclass","22003"}}) {
            bool rejected=false;try{(void)query(item.first);}catch(const DbError& error){rejected=error.sqlState()==item.second;}
            need(rejected,"actual SQLSTATE: "+item.first);
        }
        create("CREATE TABLE reference_values(k integer PRIMARY KEY,r regclass UNIQUE,a regclass[])");
        const auto dml=[&](const std::string& sql,const std::vector<std::vector<std::string>>& expected) {
            bool handled=false;clearLastDmlResult();
            need(!tryDmlBridge(sql,SQLParser::classify(sql),session,handled,sql) && handled,"actual DML: "+sql);
            need(takeLastDmlResult().rows==expected,"whole RETURNING rows: "+sql);
        };
        dml("INSERT INTO reference_values VALUES(1,'reference_target',ARRAY['reference_generator'::regclass,0::regclass,NULL::regclass]) RETURNING k,r,a",
            {{"1","reference_target","{reference_generator,-,NULL}"}});
        rows("SELECT k,r,r::oid,a,a[1]::oid FROM reference_values ORDER BY k",
            {{"1","reference_target",std::to_string(target->oid),"{reference_generator,-,NULL}",std::to_string(sequence->oid)}});
        create("ALTER TABLE reference_target RENAME TO moved_reference");
        rows("SELECT k,r,r::oid,a FROM reference_values ORDER BY k",
            {{"1","moved_reference",std::to_string(target->oid),"{reference_generator,-,NULL}"}});
        rows("SELECT k FROM reference_values WHERE r='moved_reference'::regclass",{{"1"}});
        dml("UPDATE reference_values SET r=0::regclass RETURNING k,r,r::oid",{{"1","-","0"}});
        rows("SELECT k,r FROM reference_values WHERE r=0::regclass",{{"1","-"}});
        dml("UPDATE reference_values SET r=4294967295::regclass RETURNING k,r,r::oid",
            {{"1","4294967295","4294967295"}});
        rows("SELECT r,r::integer,r::bigint FROM reference_values",{{"4294967295","-1","4294967295"}});
        const auto schema=g_engine.getTableSchema(database,"reference_values");
        need(schema.cols[1].dataType=="regclass" && schema.cols[1].dsize==4 && !schema.cols[1].isVariableLength,
            "real scalar heap layout is uint32 OID");
        need(schema.cols[2].dataType=="regclass" && schema.cols[2].isArray && schema.cols[2].dsize==65535,
            "real reference array heap layout");
        const auto metadata=regtype_detail::metadata(g_engine,database);
        const auto decoded=stored_regtype_detail::load("[0:2]={"+std::to_string(sequence->oid)+",0,NULL}",true,false,
            metadata,&session,{},"regclass");
        need(decoded.value=="[0:2]={reference_generator,-,NULL}" &&
            decoded.elementOids==std::vector<std::optional<uint32_t>>{sequence->oid,0,std::nullopt},
            "physical array preserves lower bound, full OIDs and SQL NULL");
        need(stored_regtype_detail::storageText(decoded)=="[0:2]={"+std::to_string(sequence->oid)+",0,NULL}",
            "owning array storage retains original OIDs");
        {
            StorageEngine cold;cold.setBackgroundIntervals(60000,60000);
            need(cold.query(database,"reference_values",{},{"k","r"})==
                std::vector<std::string>{"1 4294967295 "},"independent cold heap reader retains all 32 OID bits");
        }
        create("DROP TABLE moved_reference");
        rows("SELECT '"+std::to_string(target->oid)+"'::regclass::text",{{std::to_string(target->oid)}});
        std::cout<<"REGCLASS_VALUE_CHECKED="<<checks<<" FAILED=0\n";
        return 0;
    } catch(const std::exception& error) {
        std::cerr<<"REGCLASS_VALUE_CHECKED="<<checks<<" FAIL="<<error.what()<<'\n';return 1;
    }
}
