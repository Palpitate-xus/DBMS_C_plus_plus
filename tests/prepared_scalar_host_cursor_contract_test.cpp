#include "executor/ExecutionPlan.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine owner;
    const auto db=testDbPath("prepared_scalar_host_cursor_contract");
    assert(owner.createDatabase(db,"utf8")==DBStatus::OK);
    TableSchema schema;Column id;id.dataName="id";
    assert(TypeRegistry::instance().resolveColumnType(id,"integer",{},false).empty());
    schema.append(id);
    assert(owner.createTable(db,"source",schema)==DBStatus::OK);
    for(int value=1;value<=3;++value)
        assert(owner.insertRow(db,"source",StorageEngine::SqlRow{{"id",std::to_string(value)}})==DBStatus::OK);
    const auto run=[&](const std::string& sql,size_t demand=0) {
        auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedSelectPlan(
            &owner,db,"source",owner.prepareBoundQuery(db,sql)),demand);
        result.throwIfFailed();return result;
    };
    assert(!owner.hasPlpgsqlQueryExecutor());
    assert(run("SELECT(SELECT 999),id=ANY(SELECT id FROM source) FROM source").structuredRows.front()[0]=="999");
    enum class Reply { Value,Null,Empty,Multiple,Failure,FailedResult } reply=Reply::Value;
    size_t calls=0;
    owner.setPlpgsqlQueryExecutor([&](const std::string& database,const std::string&,
                                    const PlPgsqlQueryOptions& options) {
        assert(database==db && options.purpose==PlPgsqlQueryOptions::Purpose::OrdinarySubquery && options.maxRows==2);
        ++calls;PlPgsqlQueryResult value;value.ok=true;value.columnCount=1;
        value.columnTypes={"integer"};
        if(reply==Reply::Failure)throw DbError("P0002","explicit host primary error");
        if(reply==Reply::FailedResult) {
            value.ok=false;value.sqlState="22P02";value.message="explicit host input error";return value;
        }
        value.rowCount=reply==Reply::Empty?0:reply==Reply::Multiple?2:1;
        value.firstRow={reply==Reply::Null?std::optional<std::string>{}:std::optional<std::string>{"777"}};return value;
    });
    assert(owner.hasPlpgsqlQueryExecutor());
    // One execution mixes genuine streamed quantified SQL and host-owned
    // scalar SQL. The host's deliberate value differs from physical SELECT.
    auto result=run("SELECT(SELECT 999),id=ANY(SELECT id FROM source) FROM source");
    assert((result.structuredRows==std::vector<std::vector<std::string>>{{"777","t"},{"777","t"},{"777","t"}}));
    assert(calls==1);
    calls=0;
    auto values=std::make_shared<PreparedQuery>(owner.prepareBoundQuery(db,"VALUES((SELECT 999))"));
    auto valuesResult=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedQueryPlan(
        &owner,db,values,values->ast.get()));
    valuesResult.throwIfFailed();
    assert((valuesResult.structuredRows==std::vector<std::vector<std::string>>{{"777"}}) && calls==1);
    calls=0;
    result=run("SELECT id>ALL(SELECT(SELECT 999) FROM source) FROM source");
    assert(result.structuredRows.size()==3 && result.structuredRows[0][0]=="f" && calls==1);
    // Genuine scalar sites retain separate memo entries, including when
    // reached inside another default physical cursor's child graph.
    calls=0;
    result=run("SELECT(SELECT 999),(SELECT 999),id>ALL(SELECT(SELECT 999) FROM source) FROM source",1);
    assert(result.structuredRows.size()==1 && calls==3);
    calls=0;
    result=run("SELECT(SELECT s.id),s.id=ANY(SELECT id FROM source) FROM source s");
    assert(result.structuredRows.size()==3 && calls==3 && result.structuredRows.back()[0]=="777");
    for(const auto mode:{Reply::Null,Reply::Empty}) {
        reply=mode;calls=0;result=run("SELECT(SELECT 999) FROM source",1);
        assert(result.structuredRows.size()==1 && result.structuredNulls[0][0] && calls==1);
    }
    for(const auto& mode:std::vector<std::pair<Reply,std::string>>{{Reply::Multiple,"21000"},
            {Reply::Failure,"P0002"},{Reply::FailedResult,"22P02"}}) {
        reply=mode.first;calls=0;
        auto failed=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedSelectPlan(
            &owner,db,"source",owner.prepareBoundQuery(db,"SELECT(SELECT 999),id=ANY(SELECT id FROM source) FROM source")));
        assert(!failed.ok && failed.errorSqlState==mode.second && failed.rows.empty() && failed.structuredRows.empty());
        assert(calls==1);
        bool caught=false;try{failed.throwIfFailed();}catch(const DbError& error){caught=error.sqlState()==mode.second;}
        assert(caught);
    }
    reply=Reply::Value;
    // A deliberately paired cursor owner still supersedes the engine host.
    // It is not inferred from a SQL spelling or from the returned row count.
    for(const bool paired:{false,true}) {
        auto prepared=std::make_shared<PreparedQuery>(owner.prepareBoundQuery(db,
            "SELECT(SELECT 999),id=ANY(SELECT id FROM source) FROM source"));
        auto* select=static_cast<SelectStmt*>(prepared->ast.get());
        size_t readerCalls=0,cursorCalls=0;calls=0;
        PreparedChildExecutor reader=[&](const Stmt* child,const RowContext&,size_t demand) {
            assert(prepared->statementOutputs.count(child) && demand==2);++readerCalls;
            return PreparedQueryRows{{ExprValue("integer","888")}};
        };
        PreparedChildCursorFactory cursor;
        if(paired)cursor=[&](const Stmt* child,const RowContext&) {
            ++cursorCalls;const auto descriptor=prepared->statementOutputs.at(child);
            auto rows=std::make_unique<PreparedSourceRowsOp>(descriptor,[](size_t ordinal,std::vector<ExprValue>& values) {
                if(ordinal)return false;values={ExprValue("integer","555")};return true;
            });
            return QueryPlanner::makePreparedCursor(std::move(rows),descriptor);
        };
        auto source=std::make_unique<TableScanOp>(&owner,db,"source");
        auto actual=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedSelectPlan(
            &owner,db,prepared,select,schema,std::move(source),{},reader,cursor),1);
        actual.throwIfFailed();assert(calls==0 && actual.structuredRows.size()==1);
        assert(actual.structuredRows[0][0]==(paired?"555":"888"));
        assert(readerCalls==(paired?0:1));
        assert(cursorCalls==(paired?2:0));
        assert(actual.structuredRows[0][1]==(paired?"f":"t"));
    }
    // Host replacement after pure plan preparation is visible at execution;
    // clearing it restores the physical scalar fallback, not stale memo data.
    calls=0;
    auto delayed=QueryPlanner::buildPreparedSelectPlan(&owner,db,"source",
        owner.prepareBoundQuery(db,"SELECT(SELECT 999) FROM source"));
    assert(calls==0); // pure plan construction did not call the host
    owner.setPlpgsqlQueryExecutor({});assert(!owner.hasPlpgsqlQueryExecutor());
    result=QueryPlanner::executePlanChecked(std::move(delayed));result.throwIfFailed();
    assert(result.structuredRows.front()[0]=="999");
    assert(owner.dropDatabase(db)==DBStatus::OK);
    cleanupTestDb("prepared_scalar_host_cursor_contract");
    std::cout<<"[PREPARED SCALAR HOST/CURSOR CONTRACT] passed\n";
}
