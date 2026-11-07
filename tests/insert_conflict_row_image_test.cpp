#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "Session.h"
#include "test_utils.h"
#include <iostream>
#include <stdexcept>

extern dbms::StorageEngine g_engine;
using namespace dbms;
namespace {
unsigned checked=0;
void need(bool value,const std::string& why){++checked;if(!value)throw std::runtime_error(why);}
}
int main() {
    try {
        TypeRegistry::instance().bootstrap();
        const auto database=testDbPath("insert_conflict_row_image");
        cleanupTestDb("insert_conflict_row_image");
        need(g_engine.createDatabase(database,"utf8")==DBStatus::OK,"real database");
        Session session;session.username="admin";session.permission=1;session.currentDB=database;
        DdlExecutor ddl;
        need(!ddl.executeSql("CREATE TABLE candidate(k integer PRIMARY KEY,id smallint GENERATED ALWAYS AS IDENTITY(START 10 INCREMENT 2),n text DEFAULT 'fallback',nullable text,derived integer GENERATED ALWAYS AS(k*3) STORED)",session),"actual named identity and stored generation");
        const auto table=g_engine.getTableSchema(database,"candidate");
        need(table.len==5 && table.cols[1].identitySequenceOid!=0,"real persisted identity binding");
        StorageEngine::SqlRow candidate{{"stale","must clear"}};
        std::vector<StorageEngine::SqlRow> returned;
        const auto insert=[&](const StorageEngine::SqlRow& input) {
            return g_engine.insertRow(database,"candidate",input,&returned,
                StorageEngine::IdentityOverride::None,nullptr,true,&candidate);
        };
        need(insert({{"k","1"},{"n","seed"},{"nullable",""}})==DBStatus::OK,"seed insert");
        need(candidate.empty() && returned.size()==1 && returned[0].at("id")==StorageEngine::SqlCell{"10"},"success clears candidate and publishes only actual inserted row");
        returned.clear();
        need(insert({{"k","1"},{"n",std::nullopt}})==DBStatus::DUPLICATE_KEY,"actual duplicate terminal");
        const StorageEngine::SqlRow expected{{"k","1"},{"id","12"},{"n",std::nullopt},
            {"nullable",std::nullopt},{"derived","3"}};
        need(candidate==expected,"whole rejected candidate retains generated identity, stored expression and genuine NULLs");
        need(returned.empty() && !g_engine.inTransaction(),"duplicate publishes no RETURNING row and releases implicit owner");
        need(g_engine.query(database,"candidate",{"=k 1"},{"id"})==std::vector<std::string>{"10 "},"original heap row unchanged");
        need(insert({{"k","2"}})==DBStatus::OK && returned.size()==1 && returned[0].at("id")==StorageEngine::SqlCell{"14"},"capture neither repeats nor re-allocates rejected identity");
        returned.clear();
        need(g_engine.beginTransaction(database)==DBStatus::OK,"actual caller transaction");
        need(insert({{"k","3"}})==DBStatus::OK && returned[0].at("id")==StorageEngine::SqlCell{"16"},"earlier caller write");
        returned.clear();const auto xid=g_engine.currentTxnId();
        need(insert({{"k","1"}})==DBStatus::DUPLICATE_KEY && candidate.at("id")==StorageEngine::SqlCell{"18"},"caller duplicate owns exact attempted value");
        need(returned.empty() && g_engine.inTransaction() && g_engine.currentTxnId()==xid,"statement rollback retains caller transaction and no inserted result");
        need(g_engine.query(database,"candidate",{"=k 3"},{"id"})==std::vector<std::string>{"16 "},"earlier caller row survives failed attempt");
        need(g_engine.rollbackTransaction()==DBStatus::OK,"caller rollback");
        need(insert({{"k","4"}})==DBStatus::OK && returned[0].at("id")==StorageEngine::SqlCell{"20"},"ordinary nontransactional allocation remains consumed after caller rollback");
        returned.clear();candidate={{"stale","must clear"}};
        need(insert({{"k",std::nullopt}})==DBStatus::NULL_NOT_ALLOWED,"non-duplicate validation failure");
        need(candidate.empty() && returned.empty(),"invalid row never exposes a conflict image");
        need(g_engine.createTrigger(database,{"rewrite_candidate","before","insert","candidate",
            "NEW.k = 1","",true,true,{}})==DBStatus::OK,"actual native BEFORE trigger");
        unsigned triggerCalls=0;
        g_engine.setTriggerExecutor([&](const std::string&){++triggerCalls;return false;});
        const auto triggerStatus=insert({{"k","9"},{"n","é中🙂"},{"nullable",""}});
        g_engine.setTriggerExecutor({});
        const StorageEngine::SqlRow rewritten{{"k","1"},{"id","24"},{"n","é中🙂"},
            {"nullable",""},{"derived","3"}};
        need(triggerStatus==DBStatus::DUPLICATE_KEY && triggerCalls==1,"BEFORE trigger produces actual conflicting key");
        need(candidate==rewritten && returned.empty(),"candidate retains trigger key, subsequent stored generation, Unicode and empty text");
        need(!g_engine.inTransaction(),"trigger conflict releases implicit transaction");
        need(g_engine.checkpoint(database),"checkpoint actual native state");
        std::cout<<"[INSERT CONFLICT ROW IMAGE] checks="<<checked<<" exact_rejected_row_and_owner_verified=true\n";
        return 0;
    } catch(const std::exception& error) {
        std::cerr<<"INSERT_CONFLICT_ROW_IMAGE_FAIL "<<error.what()<<'\n';return 1;
    }
}
