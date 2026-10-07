#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "Session.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto dbA=testDbPath("enum_aggregate_argument_a"),dbB=testDbPath("enum_aggregate_argument_b");
    const std::vector<std::string> labels{"zeta","","alpha","aa","NULL","it's","longlonglong"};
    Session session;session.username="testuser";session.permission=1;
    auto* previous=currentSession();
    struct Restore {Session* value;~Restore(){setCurrentSession(value);}} restore{previous};
    DdlExecutor ddl;
    const auto quoted=[](const std::string& value){std::string out="'";for(char ch:value){out+=ch;if(ch=='\'')out+=ch;}return out+'\'';};
    for(const auto& db:{dbA,dbB}) {
        assert(g_engine.createDatabase(db)==DBStatus::OK);
        session.currentDB=db;setCurrentSession(&session);
        auto declaration=labels;if(db==dbB)std::reverse(declaration.begin(),declaration.end());
        std::string sql="CREATE TYPE rank_type AS ENUM (";
        for(size_t i=0;i<declaration.size();++i){if(i)sql+=',';sql+=quoted(declaration[i]);}
        assert(!ddl.executeSql(sql+")",session));
        assert(!ddl.executeSql("CREATE TABLE ranks(id INT,r rank_type)",session));
        assert(!ddl.executeSql("CREATE TABLE empty_ranks(id INT,r rank_type)",session));
        for(size_t i=0;i<labels.size();++i)
            assert(g_engine.insertRow(db,"ranks",{{"id",std::to_string(i+1)},{"r",labels[i]}})==DBStatus::OK);
        assert(g_engine.insertRow(db,"ranks",{{"id","8"},{"r",std::nullopt}})==DBStatus::OK);
    }
    const auto projection=[](const std::string& sql){StorageEngine::SelectExpr expr;expr.isScalar=true;expr.funcName="expreval";expr.funcArgs={sql};return expr;};
    const auto expect=[&](StorageEngine& engine,const std::string& db,const std::string& source,
            const std::string& sql,const std::vector<std::string>& conditions,
            const std::optional<std::string>& value,const StorageEngine::QueryExprExecutionOptions& options=StorageEngine::QueryExprExecutionOptions{}) {
        std::vector<std::vector<std::string>> rows;std::vector<std::vector<bool>> nulls;
        engine.queryExpr(db,source,conditions,{projection(sql)},{},&rows,&nulls,nullptr,options);
        assert(rows.size()==1 && rows[0].size()==1 && nulls.size()==1 && nulls[0].size()==1);
        assert(nulls[0][0]==!value && (!value || rows[0][0]==*value));
        assert(engine.getLockManager().captureCheckpoint().tableCounts.empty());
    };
    // The session belongs to B, but the actual source and catalog in A own its
    // argument comparison. Numeric OIDs may legitimately coincide across DBs.
    session.currentDB=dbB;
    for(const auto& db:{dbA,dbB}) {
        auto declaration=labels;if(db==dbB)std::reverse(declaration.begin(),declaration.end());
        for(size_t j=0;j<declaration.size();++j) {
            const auto label=quoted(declaration[j]);
            expect(g_engine,db,"ranks","sum(CASE WHEN r<"+label+" THEN 1 ELSE 0 END)",{},std::to_string(j));
            expect(g_engine,db,"ranks","sum(CASE WHEN r>"+label+" THEN 1 ELSE 0 END)",{},std::to_string(labels.size()-j-1));
            expect(g_engine,db,"ranks","sum(CASE r WHEN "+label+" THEN 1 ELSE 0 END)",{},"1");
        }
    }
    expect(g_engine,dbA,"ranks","sum(CASE WHEN r<'alpha' THEN 1 ELSE 0 END)",{},"2");
    expect(g_engine,dbA,"ranks","bool_and(r<'alpha')",{"<=id 2"},"t");
    expect(g_engine,dbA,"ranks","bool_or(r<'alpha')",{"=id 1"},"t");
    expect(g_engine,dbA,"ranks","sum(CASE WHEN r<'alpha' THEN 1 ELSE 0 END) FILTER(WHERE r<'alpha')",{},"2");
    expect(g_engine,dbA,"ranks","sum(CASE WHEN r<'alpha' THEN 1 ELSE 0 END)",{"=id 8"},"0");
    expect(g_engine,dbA,"ranks","bool_and(r<'alpha')",{"=id 8"},std::nullopt);
    expect(g_engine,dbA,"empty_ranks","sum(CASE WHEN r<'alpha' THEN 1 ELSE 0 END)",{},std::nullopt);
    StorageEngine::QueryExprExecutionOptions alias;alias.aggregateSourceAlias="Odd Alias";
    expect(g_engine,dbA,"ranks","sum(CASE WHEN \"Odd Alias\".r<'alpha' THEN 1 ELSE 0 END)",{},"2",alias);
    StorageEngine::QueryExprExecutionOptions overlap;overlap.conditionAlternatives={{"<=id 2"},{">=id 2"}};
    expect(g_engine,dbA,"ranks","sum(CASE WHEN r<'alpha' THEN 1 ELSE 0 END)",overlap.conditionAlternatives.front(),"2",overlap);
    session.currentDB=dbA;
    assert(!ddl.executeSql("CREATE INDEX ranks_idx ON ranks(r)",session));
    session.currentDB=dbB;
    expect(g_engine,dbA,"ranks","sum(CASE WHEN r<'alpha' THEN 1 ELSE 0 END)",{"<r 'alpha'"},"2");
    for(const auto& sql:{
            "sum(CASE WHEN r<'absent' THEN 1 ELSE 0 END)",
            "sum(CASE r WHEN 'absent' THEN 1 ELSE 0 END)",
            "count(*) FILTER(WHERE r<'absent')"}) {
        StorageEngine::QueryExprExecutionOptions noDemand;noDemand.maxProjectionRows=0;
        for(const auto& source:{"empty_ranks","ranks"}) {
            std::string state;
            try{g_engine.queryExpr(dbA,source,{}, {projection(sql)},{},noDemand);}catch(const DbError& error){state=error.sqlState();}
            assert(state=="22P02");
            assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
        }
    }
    {
        StorageEngine cold;
        expect(cold,dbA,"ranks","sum(CASE WHEN r<'alpha' THEN 1 ELSE 0 END)",{},"2");
        expect(cold,dbB,"ranks","sum(CASE WHEN r<'alpha' THEN 1 ELSE 0 END)",{},"4");
    }
    std::cout<<"[ENUM AGGREGATE ARGUMENT BINDING] real two-database rank/NULL/FILTER/alias/union/index/pure error and cold consumers passed\n";
}
