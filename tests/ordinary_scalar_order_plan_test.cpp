#include "executor/ExecutionPlan.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine owner;
    const std::string db=testDbPath("ordinary_scalar_order_plan");
    assert(owner.createDatabase(db,"utf8")==DBStatus::OK);
    TableSchema schema;schema.len=3;
    schema.cols[0].dataName="id";schema.cols[0].dataType="int";schema.cols[0].dsize=4;
    schema.cols[1]=schema.cols[0];schema.cols[1].dataName="ID";schema.cols[1].dataType="bigint";schema.cols[1].dsize=8;
    schema.cols[2].dataName="payload";schema.cols[2].dataType="text";schema.cols[2].dsize=64;schema.cols[2].isNull=true;
    assert(owner.createTable(db,"source",schema)==DBStatus::OK);
    for (const auto& [id,payload] : std::vector<std::pair<int,std::optional<std::string>>>{{1,""},{2,"null"},{3,std::nullopt}})
        assert(owner.insertRow(db,"source",std::map<std::string,std::optional<std::string>>{
            {"id",std::to_string(id)},{"ID",std::to_string(2147483647LL+id)},{"payload",payload}})==DBStatus::OK);
    const auto run=[&](const std::string& sql,size_t demand=0) {
        auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedSelectPlan(
            &owner,db,"source",owner.prepareBoundQuery(db,sql)),demand);
        if (!result.ok) {
            assert(!result.errorSqlState.empty());
            assert(result.rows.empty() && result.structuredRows.empty() && result.structuredNulls.empty());
        }
        result.throwIfFailed();
        return result;
    };
    auto result=run("SELECT d.id,d.\"ID\" FROM source d ORDER BY(SELECT d.\"ID\") DESC");
    assert((result.structuredRows==std::vector<std::vector<std::string>>{{"3","2147483650"},{"2","2147483649"},{"1","2147483648"}}));
    result=run("SELECT d.id,d.payload FROM source d ORDER BY(SELECT d.payload) COLLATE \"C\" NULLS FIRST");
    assert((result.structuredRows==std::vector<std::vector<std::string>>{{"3",""},{"1",""},{"2","null"}}));
    assert(result.structuredNulls[0][1] && !result.structuredNulls[1][1] && !result.structuredNulls[2][1]);
    for (const auto& [sql,state] : std::vector<std::pair<std::string,std::string>>{
        {"SELECT id FROM source ORDER BY(SELECT CAST('bad' AS integer))","22P02"},
        {"SELECT id FROM source ORDER BY(SELECT id FROM source)","21000"}}) {
        bool failed=false;
        try {(void)run(sql);}catch(const DbError& error){failed=error.sqlState()==state;}
        assert(failed);
    }
    size_t calls=0;
    owner.setPlpgsqlQueryExecutor([&](const std::string& database,const std::string&,
                                    const PlPgsqlQueryOptions& options) {
        assert(database==db && options.purpose==PlPgsqlQueryOptions::Purpose::OrdinarySubquery && options.maxRows==2);
        ++calls;PlPgsqlQueryResult value;value.ok=true;value.rowCount=value.columnCount=1;
        value.columnTypes={"integer"};value.firstRow={std::to_string(calls)};return value;
    });
    result=run("SELECT(SELECT d.id) AS value FROM source d ORDER BY value",1);
    assert(result.ok && result.structuredRows.size()==1 && calls==3); // real target slot, full keys
    calls=0;
    result=run("SELECT(SELECT 1),(SELECT 1) FROM source ORDER BY id");
    assert(result.ok && result.structuredRows.size()==3 && calls==2); // genuine sites, not global SQL memo
    assert((result.structuredRows[0]==std::vector<std::string>{"1","2"}));
    owner.setPlpgsqlQueryExecutor({});
    assert(owner.dropDatabase(db)==DBStatus::OK);
    cleanupTestDb("ordinary_scalar_order_plan");
    std::cout<<"[ORDINARY SCALAR ORDER PLAN] passed\n";
}
