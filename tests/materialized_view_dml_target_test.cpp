#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <memory>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="materialized_view_dml_target",db=testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session;session.username="admin";session.permission=1;session.currentDB=db;
    DdlExecutor ddl;
    const auto sql=[&](const std::string& text){assert(!ddl.executeSql(text,session));};
    sql("CREATE SCHEMA reporting");
    sql("CREATE TABLE source(id INT)");
    bool handled=false;
    assert(!tryDmlBridge("INSERT INTO source VALUES(1)",SqlCommand::Insert,session,handled) && handled);
    sql("CREATE SEQUENCE effect_sentinel");
    sql("CREATE MATERIALIZED VIEW reporting.mv AS SELECT id FROM source");
    sql("CREATE MATERIALIZED VIEW reporting.empty_mv AS SELECT id FROM source WITH NO DATA");
    const auto filled=g_engine.resolveMaterializedView(db,"reporting","mv");
    const auto empty=g_engine.resolveMaterializedView(db,"reporting","empty_mv");
    assert(filled && filled->populated && empty && !empty->populated);
    const std::vector<std::pair<std::string,std::string>> cases{
        {"WITH p AS(SELECT 1) INSERT INTO reporting.mv VALUES(2) RETURNING id","42809"},
        {"WITH p AS(SELECT 1) UPDATE reporting.mv SET id=2 RETURNING id","42809"},
        {"WITH p AS(SELECT 1) DELETE FROM reporting.mv RETURNING id","42809"},
        {"WITH p AS(SELECT 1) INSERT INTO reporting.empty_mv VALUES(2)","42809"},
        {"WITH p AS(SELECT 1) UPDATE reporting.empty_mv SET id=2","42809"},
        {"WITH p AS(SELECT 1) DELETE FROM reporting.empty_mv","42809"},
        {"WITH p AS(SELECT 1) INSERT INTO reporting.mv SELECT nextval('effect_sentinel')","42809"},
        {"WITH p AS(SELECT 1) UPDATE reporting.mv SET id=(SELECT nextval('effect_sentinel'))","42809"},
        {"WITH p AS(SELECT 1) INSERT INTO reporting.mv(bad) VALUES(2)","42703"},
        {"WITH p AS(SELECT 1) INSERT INTO reporting.mv VALUES(missing_mv_function(1))","42883"},
        {"WITH p AS(SELECT 1) INSERT INTO reporting.mv VALUES('bad')","22P02"},
        {"WITH p AS(SELECT 1) UPDATE reporting.mv SET id='bad'","22P02"},
        {"WITH p AS(SELECT 1) UPDATE reporting.mv SET id=1/0","22012"},
        {"WITH p AS(SELECT 1) UPDATE reporting.mv SET id=CAST(2147483648 AS INT)","22003"},
        {"WITH p AS(SELECT 1) UPDATE reporting.mv SET id=CASE WHEN false THEN 1/0 ELSE 2 END","42809"},
        {"WITH p AS(SELECT 1) UPDATE reporting.mv SET id=(SELECT 1/0)","22012"},
        {"WITH p AS(SELECT 1) UPDATE reporting.mv SET id=CASE WHEN false THEN (SELECT 1/0) ELSE 2 END","42809"},
        {"WITH p AS(SELECT 1) DELETE FROM reporting.mv WHERE 1","42804"},
        {"WITH p AS(SELECT 1) DELETE FROM reporting.mv WHERE 'bad'","22P02"},
        {"WITH p AS(SELECT 1) UPDATE reporting.mv SET id=1/0,id=2","42601"},
        {"WITH p AS(SELECT 1) UPDATE reporting.mv SET id=CAST('bad' AS INT),id=2","22P02"},
        {"WITH p AS(SELECT 1) UPDATE reporting.mv AS m SET id=s.id FROM source s WHERE m.id=s.id","42809"},
        {"WITH p AS(SELECT 1) DELETE FROM reporting.mv AS m USING source s WHERE m.id=s.id","42809"},
    };
    size_t failures=0,childOpens=0;
    int64_t expectedSentinel=1;
    for(const auto& control:cases) {
        std::string state;
        try {
            auto prepared=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,control.first));
            auto* with=dynamic_cast<WithStmt*>(prepared->ast.get());
            assert(with && with->statement);
            prepareBoundDml(with->statement.get(),session,prepared);
            PreparedChildExecutor child=[&](const Stmt*,const RowContext&,size_t){
                ++childOpens;(void)g_engine.nextval(db,"effect_sentinel");
                return PreparedQueryRows{{ExprValue("integer","2",false)}};
            };
            (void)executeAtomicDmlUnit(session,[&]{return executeBoundDml(with->statement.get(),session,prepared,child);});
        } catch(const DbError& error) {state=error.sqlState();}
        std::cout<<"MV NATIVE "<<control.first<<" actual "<<state<<" expected "<<control.second<<std::endl;
        if(state!=control.second)++failures;
        if(g_engine.nextval(db,"effect_sentinel")!=expectedSentinel++)++failures;
        if(childOpens)++failures;
        std::vector<std::vector<std::string>> physicalRows;
        std::vector<std::vector<bool>> physicalNulls;
        (void)g_engine.query(db,filled->backingTable,{}, {"id"},{},false,false,false,0,{},
                             &physicalRows,&physicalNulls);
        if(physicalRows!=std::vector<std::vector<std::string>>{{"1"}} ||
           physicalNulls!=std::vector<std::vector<bool>>{{false}})++failures;
        if(!g_engine.resolveMaterializedView(db,"reporting","empty_mv") ||
            g_engine.resolveMaterializedView(db,"reporting","empty_mv")->populated)++failures;
    }
    std::cout<<"MV NATIVE failures "<<failures<<" child opens "<<childOpens<<std::endl;
    assert(failures==0 && childOpens==0);
    assert(g_engine.dropDatabase(db)==DBStatus::OK);
    cleanupTestDb(name);
    std::cout<<"[MV TARGET NATIVE] passed\n";
}
